//! Command line arguments.

use std::{
    collections::BTreeMap,
    net::{SocketAddrV4, SocketAddrV6},
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::Context;
use clap::{
    error::{ContextKind, ErrorKind},
    CommandFactory, Parser, Subcommand,
};
use console::style;
use data_encoding::HEXLOWER;
use futures_buffered::BufferedStreamExt;
use indicatif::{
    HumanBytes, HumanDuration, MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle,
};
use iroh_blobs::{
    api::{
        blobs::{AddPathOptions, AddProgressItem, ImportMode},
        Store, TempTag,
    },
    format::collection::Collection,
    get::GetError,
    provider::events::{
        ConnectMode, EventMask, EventSender, ProviderMessage, RequestMode, RequestUpdate,
        TransferAborted, TransferCompleted, TransferProgress, TransferStarted,
    },
    store::fs::FsStore,
    ticket::BlobTicket,
    BlobFormat, BlobsProtocol, Hash,
};
use laplink_p2p::{
    endpoint::{build_endpoint, EndpointConfig},
    get_or_create_secret,
    paths::canonicalized_path_to_string,
    print_hash,
    receive::ReceiveProgress,
    AddrInfoOptions, Format, RelayModeOption,
};
use n0_future::{task::AbortOnDropHandle, StreamExt};
use rand::Rng;
use tokio::{select, sync::mpsc};
use tracing::{error, trace};
use walkdir::WalkDir;

/// Send a file or directory between two machines, using blake3 verified streaming.
///
/// For all subcommands, you can specify a secret key using the IROH_SECRET
/// environment variable. If you don't, a random one will be generated.
///
/// You can also specify a port for the magicsocket. If you don't, a random one
/// will be chosen.
#[derive(Parser, Debug)]
#[command(version, about)]
pub struct Args {
    #[clap(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Send a file or directory.
    Send(SendArgs),

    /// Receive a file or directory.
    #[clap(visible_alias = "recv")]
    Receive(ReceiveArgs),
}

#[derive(Parser, Debug)]
pub struct CommonArgs {
    /// The IPv4 address that magicsocket will listen on.
    ///
    /// If None, defaults to a random free port, but it can be useful to specify a fixed
    /// port, e.g. to configure a firewall rule.
    #[clap(long, default_value = None)]
    pub magic_ipv4_addr: Option<SocketAddrV4>,

    /// The IPv6 address that magicsocket will listen on.
    ///
    /// If None, defaults to a random free port, but it can be useful to specify a fixed
    /// port, e.g. to configure a firewall rule.
    #[clap(long, default_value = None)]
    pub magic_ipv6_addr: Option<SocketAddrV6>,

    #[clap(long, default_value_t = Format::Hex)]
    pub format: Format,

    #[clap(short = 'v', long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    /// Suppress progress bars.
    #[clap(long, default_value_t = false)]
    pub no_progress: bool,

    /// The relay URL to use as a home relay,
    ///
    /// Can be set to "disabled" to disable relay servers and "default"
    /// to configure default servers.
    #[clap(long, default_value_t = RelayModeOption::Default)]
    pub relay: RelayModeOption,

    #[clap(long)]
    pub show_secret: bool,
}

#[derive(Parser, Debug)]
pub struct SendArgs {
    /// Path to the file or directory to send.
    ///
    /// The last component of the path will be used as the name of the data
    /// being shared.
    pub path: PathBuf,

    /// What type of ticket to use.
    ///
    /// Use "id" for the shortest type only including the node ID,
    /// "addresses" to only add IP addresses without a relay url,
    /// "relay" to only add a relay address, and leave the option out
    /// to use the biggest type of ticket that includes both relay and
    /// address information.
    ///
    /// Generally, the more information the higher the likelyhood of
    /// a successful connection, but also the bigger a ticket to connect.
    ///
    /// This is most useful for debugging which methods of connection
    /// establishment work well.
    #[clap(long, default_value_t = AddrInfoOptions::RelayAndAddresses)]
    pub ticket_type: AddrInfoOptions,

    #[clap(flatten)]
    pub common: CommonArgs,

    /// Store the receive command in the clipboard.
    #[cfg(feature = "clipboard")]
    #[clap(short = 'c', long)]
    pub clipboard: bool,
}

#[derive(Parser, Debug)]
pub struct ReceiveArgs {
    /// The ticket to use to connect to the sender.
    pub ticket: BlobTicket,

    #[clap(flatten)]
    pub common: CommonArgs,
}

/// Import from a file or directory into the database.
///
/// The returned tag always refers to a collection. If the input is a file, this
/// is a collection with a single blob, named like the file.
///
/// If the input is a directory, the collection contains all the files in the
/// directory.
async fn import(
    path: PathBuf,
    db: &Store,
    mp: &mut MultiProgress,
) -> anyhow::Result<(TempTag, u64, Collection)> {
    let parallelism = num_cpus::get();
    let path = path.canonicalize()?;
    anyhow::ensure!(path.exists(), "path {} does not exist", path.display());
    let root = path.parent().context("context get parent")?;
    // walkdir also works for files, so we don't need to special case them
    let files = WalkDir::new(path.clone()).into_iter();
    // flatten the directory structure into a list of (name, path) pairs.
    // ignore symlinks.
    let data_sources: Vec<(String, PathBuf)> = files
        .map(|entry| {
            let entry = entry?;
            if !entry.file_type().is_file() {
                // Skip symlinks. Directories are handled by WalkDir.
                return Ok(None);
            }
            let path = entry.into_path();
            let relative = path.strip_prefix(root)?;
            let name = canonicalized_path_to_string(relative, true)?;
            anyhow::Ok(Some((name, path)))
        })
        .filter_map(Result::transpose)
        .collect::<anyhow::Result<Vec<_>>>()?;
    // import all the files, using num_cpus workers, return names and temp tags
    let op = mp.add(make_import_overall_progress());
    op.set_message(format!("importing {} files", data_sources.len()));
    op.set_length(data_sources.len() as u64);
    let mut names_and_tags = n0_future::stream::iter(data_sources)
        .map(|(name, path)| {
            let db = db.clone();
            let op = op.clone();
            let mp = mp.clone();
            async move {
                op.inc(1);
                let pb = mp.add(make_import_item_progress());
                pb.set_message(format!("copying {name}"));
                let import = db.add_path_with_opts(AddPathOptions {
                    path,
                    mode: ImportMode::TryReference,
                    format: BlobFormat::Raw,
                });
                let mut stream = import.stream().await;
                let mut item_size = 0;
                let temp_tag = loop {
                    let item = stream
                        .next()
                        .await
                        .context("import stream ended without a tag")?;
                    trace!("importing {name} {item:?}");
                    match item {
                        AddProgressItem::Size(size) => {
                            item_size = size;
                            pb.set_length(size);
                        }
                        AddProgressItem::CopyProgress(offset) => {
                            pb.set_position(offset);
                        }
                        AddProgressItem::CopyDone => {
                            pb.set_message(format!("computing outboard {name}"));
                            pb.set_position(0);
                        }
                        AddProgressItem::OutboardProgress(offset) => {
                            pb.set_position(offset);
                        }
                        AddProgressItem::Error(cause) => {
                            pb.finish_and_clear();
                            anyhow::bail!("error importing {}: {}", name, cause);
                        }
                        AddProgressItem::Done(tt) => {
                            pb.finish_and_clear();
                            break tt;
                        }
                    }
                };
                anyhow::Ok((name, temp_tag, item_size))
            }
        })
        .buffered_unordered(parallelism)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;
    op.finish_and_clear();
    names_and_tags.sort_by(|(a, _, _), (b, _, _)| a.cmp(b));
    // total size of all files
    let size = names_and_tags.iter().map(|(_, _, size)| *size).sum::<u64>();
    // collect the (name, hash) tuples into a collection
    // we must also keep the tags around so the data does not get gced.
    let (collection, tags) = names_and_tags
        .into_iter()
        .map(|(name, tag, _)| ((name, tag.hash()), tag))
        .unzip::<_, _, Collection, Vec<_>>();
    let temp_tag = collection.clone().store(db).await?;
    // now that the collection is stored, we can drop the tags
    // data is protected by the collection
    drop(tags);
    Ok((temp_tag, size, collection))
}

#[derive(Debug)]
struct PerConnectionProgress {
    main: ProgressBar,
    requests: BTreeMap<u64, ProgressBar>,
}

/// A transfer update for a single request, tagged with the connection and request it belongs to.
type TaggedRequestUpdate = (u64, u64, RequestUpdate);

async fn show_provide_progress(
    mp: MultiProgress,
    mut recv: mpsc::Receiver<ProviderMessage>,
) -> anyhow::Result<()> {
    let mut connections = BTreeMap::new();
    let (updates_tx, mut updates_rx) = mpsc::channel::<TaggedRequestUpdate>(256);
    loop {
        let (connection_id, request_id, update) = tokio::select! {
            item = recv.recv() => {
                let Some(item) = item else { break };
                trace!("got event {item:?}");
                match item {
                    ProviderMessage::ClientConnected(msg) => {
                        let connection_id = msg.inner.connection_id;
                        let node_id = msg.inner.endpoint_id;
                        msg.tx.send(Ok(())).await.ok();
                        let pb = mp.add(ProgressBar::hidden());
                        pb.set_style(
                            indicatif::ProgressStyle::default_bar()
                                .template("{msg}") // Only display the message
                                .unwrap(),
                        );
                        pb.set_message(match node_id {
                            Some(node_id) => format!("{node_id} {connection_id}"),
                            None => format!("{connection_id}"),
                        });
                        connections.insert(
                            connection_id,
                            PerConnectionProgress {
                                main: pb,
                                requests: BTreeMap::new(),
                            },
                        );
                    }
                    ProviderMessage::ConnectionClosed(msg) => {
                        let connection_id = msg.inner.connection_id;
                        let Some(connection) = connections.remove(&connection_id) else {
                            error!("got close for unknown connection {connection_id}");
                            continue;
                        };
                        for pb in connection.requests.values() {
                            pb.finish_and_clear();
                        }
                        connection.main.finish_and_clear();
                    }
                    ProviderMessage::GetRequestReceived(msg) => {
                        let connection_id = msg.inner.connection_id;
                        let request_id = msg.inner.request_id;
                        let hash = msg.inner.request.hash;
                        msg.tx.send(Ok(())).await.ok();
                        let pb = mp.add(ProgressBar::hidden());
                        pb.set_style(
                            ProgressStyle::with_template(
                                "{msg}{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes}",
                            )?
                            .progress_chars("#>-"),
                        );
                        pb.set_message(format!("{request_id} {hash}"));
                        if let Some(connection) = connections.get_mut(&connection_id) {
                            connection.requests.insert(request_id, pb);
                        } else {
                            error!("got request for unknown connection {connection_id}");
                        }
                        let updates_tx = updates_tx.clone();
                        let mut rx = msg.rx;
                        n0_future::task::spawn(async move {
                            while let Ok(Some(update)) = rx.recv().await {
                                if updates_tx
                                    .send((connection_id, request_id, update))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                        });
                    }
                    _ => {}
                }
                continue;
            }
            Some(update) = updates_rx.recv() => update,
            else => break,
        };
        let Some(connection) = connections.get_mut(&connection_id) else {
            error!("got request update for unknown connection {connection_id}");
            continue;
        };
        let Some(pb) = connection.requests.get_mut(&request_id) else {
            error!("got update for unknown request {request_id}");
            continue;
        };
        match update {
            RequestUpdate::Started(TransferStarted { index, hash, size }) => {
                pb.set_message(format!("    {} {} {}", request_id, index, hash.fmt_short()));
                pb.set_length(size);
            }
            RequestUpdate::Progress(TransferProgress { end_offset }) => {
                pb.set_position(end_offset);
            }
            RequestUpdate::Completed(TransferCompleted { .. }) => {
                // todo: show stats and hide after a delay
                if let Some(pb) = connection.requests.remove(&request_id) {
                    pb.finish_and_clear();
                }
            }
            RequestUpdate::Aborted(TransferAborted { .. }) => {
                // todo: show stats and hide after a delay
                if let Some(pb) = connection.requests.remove(&request_id) {
                    pb.finish_and_clear();
                }
            }
        }
    }
    Ok(())
}

async fn send(args: SendArgs) -> anyhow::Result<()> {
    let (secret_key, generated) = get_or_create_secret()?;
    if (generated && args.common.verbose > 0) || args.common.show_secret {
        let secret_key = hex::encode(secret_key.to_bytes());
        eprintln!("using secret key {secret_key}");
    }

    // use a flat store - todo: use a partial in mem store instead
    let suffix = rand::thread_rng().gen::<[u8; 16]>();
    let cwd = std::env::current_dir()?;
    let blobs_data_dir = cwd.join(format!(".ll-send-{}", HEXLOWER.encode(&suffix)));
    if blobs_data_dir.exists() {
        println!(
            "can not share twice from the same directory: {}",
            cwd.display(),
        );
        std::process::exit(1);
    }

    let mut mp = MultiProgress::new();
    let mp2 = mp.clone();
    let path = args.path;
    let path2 = path.clone();
    let blobs_data_dir2 = blobs_data_dir.clone();
    let (event_sender, progress_rx) = EventSender::channel(
        32,
        EventMask {
            connected: ConnectMode::Intercept,
            get: RequestMode::InterceptLog,
            ..EventMask::DEFAULT
        },
    );
    let progress = AbortOnDropHandle::new(n0_future::task::spawn(show_provide_progress(
        mp2,
        progress_rx,
    )));
    let ticket_type = args.ticket_type;
    let setup = async move {
        let t0 = Instant::now();
        tokio::fs::create_dir_all(&blobs_data_dir2).await?;

        let endpoint = build_endpoint(EndpointConfig {
            secret_key,
            alpns: vec![iroh_blobs::protocol::ALPN.to_vec()],
            relay: args.common.relay,
            magic_ipv4_addr: args.common.magic_ipv4_addr,
            magic_ipv6_addr: args.common.magic_ipv6_addr,
            publish_addr: ticket_type == AddrInfoOptions::Id,
            lookup_by_dns: false,
        })
        .await?;
        let draw_target = if args.common.no_progress {
            ProgressDrawTarget::hidden()
        } else {
            ProgressDrawTarget::stderr()
        };
        mp.set_draw_target(draw_target);
        let store = FsStore::load(&blobs_data_dir2).await?;
        let blobs = BlobsProtocol::new(&store, Some(event_sender));

        let import_result = import(path2, blobs.store(), &mut mp).await?;
        let dt = t0.elapsed();

        let router = iroh::protocol::Router::builder(endpoint)
            .accept(iroh_blobs::ALPN, blobs.clone())
            .spawn();
        // wait for the endpoint to figure out its address before making a ticket
        router.endpoint().online().await;
        anyhow::Ok((router, import_result, dt))
    };
    let (router, (temp_tag, size, collection), dt) = select! {
        x = setup => x?,
        _ = tokio::signal::ctrl_c() => {
            std::process::exit(130);
        }
    };
    let hash = temp_tag.hash();

    // make a ticket
    let mut addr = router.endpoint().addr();
    laplink_p2p::apply_options(&mut addr, ticket_type);
    let ticket = BlobTicket::new(addr, hash, BlobFormat::HashSeq);
    let entry_type = if path.is_file() { "file" } else { "directory" };
    println!(
        "imported {} {}, {}, hash {}",
        entry_type,
        path.display(),
        HumanBytes(size),
        print_hash(&hash, args.common.format),
    );
    if args.common.verbose > 1 {
        for (name, hash) in collection.iter() {
            println!("    {} {name}", print_hash(hash, args.common.format));
        }
        println!(
            "{}s, {}/s",
            dt.as_secs_f64(),
            HumanBytes(((size as f64) / dt.as_secs_f64()).floor() as u64)
        );
    }

    println!("to get this data, use");
    println!("ll receive {ticket}");

    #[cfg(feature = "clipboard")]
    handle_key_press(args.clipboard, ticket);

    tokio::signal::ctrl_c().await?;

    drop(temp_tag);

    println!("shutting down");
    tokio::time::timeout(Duration::from_secs(2), router.shutdown()).await??;
    tokio::fs::remove_dir_all(blobs_data_dir).await?;
    // drop everything that owns blobs to close the progress sender
    drop(router);
    // await progress completion so the progress bar is cleared
    progress.await.ok();

    Ok(())
}

#[cfg(feature = "clipboard")]
fn handle_key_press(set_clipboard: bool, ticket: BlobTicket) {
    use crossterm::{
        event::{Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers},
        terminal::{disable_raw_mode, enable_raw_mode},
    };

    #[cfg(any(unix, windows))]
    use std::io;

    #[cfg(unix)]
    use libc::{raise, SIGINT};

    #[cfg(windows)]
    use windows_sys::Win32::System::Console::{GenerateConsoleCtrlEvent, CTRL_C_EVENT};

    if set_clipboard {
        add_to_clipboard(&ticket);
    }

    let _keyboard = tokio::task::spawn(async move {
        println!("press c to copy command to clipboard, or use the --clipboard argument");

        // `enable_raw_mode` will remember the current terminal mode
        // and restore it when `disable_raw_mode` is called.
        enable_raw_mode().unwrap_or_else(|err| eprintln!("Failed to enable raw mode: {err}"));
        EventStream::new()
            .for_each(move |e| match e {
                Err(err) => eprintln!("Failed to process event: {err}"),
                // c is pressed
                Ok(Event::Key(KeyEvent {
                    code: KeyCode::Char('c'),
                    modifiers: KeyModifiers::NONE,
                    kind: KeyEventKind::Press,
                    ..
                })) => add_to_clipboard(&ticket),
                // Ctrl+c is pressed
                Ok(Event::Key(KeyEvent {
                    code: KeyCode::Char('c'),
                    modifiers: KeyModifiers::CONTROL,
                    kind: KeyEventKind::Press,
                    ..
                })) => {
                    disable_raw_mode()
                        .unwrap_or_else(|e| eprintln!("Failed to disable raw mode: {e}"));

                    #[cfg(unix)]
                    // Safety: Raw syscall to re-send the SIGINT signal to the console.
                    // `raise` returns nonzero for failure.
                    if unsafe { raise(SIGINT) } != 0 {
                        eprintln!("Failed to raise signal: {}", io::Error::last_os_error());
                    }

                    #[cfg(windows)]
                    // Safety: Raw syscall to re-send the `CTRL_C_EVENT` to the console.
                    // `GenerateConsoleCtrlEvent` returns 0 for failure.
                    if unsafe { GenerateConsoleCtrlEvent(CTRL_C_EVENT, 0) } == 0 {
                        eprintln!(
                            "Failed to generate console event: {}",
                            io::Error::last_os_error()
                        );
                    }
                }
                _ => {}
            })
            .await
    });
}

#[cfg(feature = "clipboard")]
fn add_to_clipboard(ticket: &BlobTicket) {
    use std::io::stdout;

    use crossterm::{clipboard::CopyToClipboard, execute};

    execute!(
        stdout(),
        CopyToClipboard::to_clipboard_from(format!("ll receive {ticket}"))
    )
    .unwrap_or_else(|e| eprintln!("Failed to copy to clipboard: {e}"));
}

const TICK_MS: u64 = 250;

fn make_import_overall_progress() -> ProgressBar {
    let pb = ProgressBar::hidden();
    pb.enable_steady_tick(std::time::Duration::from_millis(TICK_MS));
    pb.set_style(
        ProgressStyle::with_template(
            "{msg}{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len}",
        )
        .unwrap()
        .progress_chars("#>-"),
    );
    pb
}

fn make_import_item_progress() -> ProgressBar {
    let pb = ProgressBar::hidden();
    pb.enable_steady_tick(std::time::Duration::from_millis(TICK_MS));
    pb.set_style(
        ProgressStyle::with_template("{msg}{spinner:.green} XXXX [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes}")
            .unwrap()
            .progress_chars("#>-"),
    );
    pb
}

fn make_download_progress() -> ProgressBar {
    let pb = ProgressBar::hidden();
    pb.enable_steady_tick(std::time::Duration::from_millis(TICK_MS));
    pb.set_style(
        ProgressStyle::with_template("{spinner:.green}{msg} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} {binary_bytes_per_sec}")
            .unwrap()
            .progress_chars("#>-"),
    );
    pb.set_message("Downloading ...".to_string());
    pb
}

pub async fn show_download_progress(
    mp: MultiProgress,
    mut recv: mpsc::Receiver<ReceiveProgress>,
    hash: Hash,
    format: Format,
    verbose: u8,
) -> anyhow::Result<()> {
    let op = mp.add(make_download_progress());
    let mut local_size = 0;
    while let Some(item) = recv.recv().await {
        match item {
            ReceiveProgress::Sizes {
                total_files,
                total_size,
                payload_size,
                local_size: ls,
            } => {
                local_size = ls;
                eprintln!(
                    "getting collection {} {} files, {}",
                    print_hash(&hash, format),
                    total_files,
                    HumanBytes(payload_size)
                );
                if verbose > 0 {
                    eprintln!(
                        "getting {} blobs in total, {}",
                        total_files + 1,
                        HumanBytes(total_size)
                    );
                }
                op.set_length(total_size);
            }
            ReceiveProgress::Progress(offset) => {
                op.set_position(local_size + offset);
            }
        }
    }
    op.finish_and_clear();
    Ok(())
}

fn show_get_error(e: &anyhow::Error) {
    if let Some(cause) = e.downcast_ref::<GetError>() {
        match cause {
            GetError::LocalFailure { source, .. } => {
                eprintln!("{} {source:?}", style("local failure").yellow())
            }
            GetError::BadRequest { .. } => eprintln!("{}", style("bad request").yellow()),
            _ => eprintln!("{}", style(format!("transfer failed: {cause}")).yellow()),
        }
    } else {
        eprintln!("{}", style(format!("error: {e}")).yellow());
    }
}

async fn receive(args: ReceiveArgs) -> anyhow::Result<()> {
    let ticket = args.ticket;
    let (secret_key, generated) = get_or_create_secret()?;
    if (generated && args.common.verbose > 0) || args.common.show_secret {
        let secret_key = hex::encode(secret_key.to_bytes());
        eprintln!("using secret key {secret_key}");
    }

    let dir_name = format!(".ll-recv-{}", ticket.hash().to_hex());
    let store_dir = std::env::current_dir()?.join(dir_name);
    let export_root = std::env::current_dir()?;

    let lookup_by_dns = ticket.addr().is_empty();
    let cfg = EndpointConfig {
        secret_key,
        alpns: vec![],
        relay: args.common.relay,
        magic_ipv4_addr: args.common.magic_ipv4_addr,
        magic_ipv6_addr: args.common.magic_ipv6_addr,
        publish_addr: false,
        lookup_by_dns,
    };

    let mp: MultiProgress = MultiProgress::new();
    let draw_target = if args.common.no_progress {
        ProgressDrawTarget::hidden()
    } else {
        ProgressDrawTarget::stderr()
    };
    mp.set_draw_target(draw_target);

    let hash_for_display = ticket.hash();
    let format = args.common.format;
    let verbose = args.common.verbose;

    let (progress_tx, progress_rx) = mpsc::channel(32);
    let progress_task = tokio::spawn(show_download_progress(
        mp.clone(),
        progress_rx,
        hash_for_display,
        format,
        verbose,
    ));

    eprintln!("connecting...");
    let fut = laplink_p2p::receive::receive_collection(
        ticket,
        cfg,
        store_dir.clone(),
        export_root,
        Some(progress_tx),
    );

    let result = select! {
        x = fut => x,
        _ = tokio::signal::ctrl_c() => {
            std::process::exit(130);
        }
    };
    progress_task.await.ok();

    let outcome = match result {
        Ok(outcome) => outcome,
        Err(e) => {
            show_get_error(&e);
            std::process::exit(1);
        }
    };

    tokio::fs::remove_dir_all(&store_dir).await.ok();
    if verbose > 0 {
        println!(
            "downloaded {} files, {}. took {} ({}/s)",
            outcome.total_files,
            HumanBytes(outcome.payload_size),
            HumanDuration(outcome.stats.elapsed),
            HumanBytes(
                (outcome.stats.total_bytes_read() as f64 / outcome.stats.elapsed.as_secs_f64())
                    as u64
            ),
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = match Args::try_parse() {
        Ok(args) => args,
        Err(cause) => {
            if let Some(text) = cause.get(ContextKind::InvalidSubcommand) {
                eprintln!("{} \"{}\"\n", ErrorKind::InvalidSubcommand, text);
                eprintln!("Available subcommands are");
                for cmd in Args::command().get_subcommands() {
                    eprintln!("    {}", style(cmd.get_name()).bold());
                }
                std::process::exit(1);
            } else {
                cause.exit();
            }
        }
    };
    let res = match args.command {
        Commands::Send(args) => send(args).await,
        Commands::Receive(args) => receive(args).await,
    };
    if let Err(e) = &res {
        eprintln!("{e}");
    }
    match res {
        Ok(()) => std::process::exit(0),
        Err(_) => std::process::exit(1),
    }
}
