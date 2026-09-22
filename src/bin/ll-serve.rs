//! Serve a folder of files over iroh for browsing/downloading with `ll-tui`.

use std::{
    net::{SocketAddrV4, SocketAddrV6},
    path::PathBuf,
    time::Duration,
};

use clap::Parser;
use iroh_blobs::{
    BlobFormat, BlobsProtocol,
    provider::events::{
        ConnectMode, EventMask, EventSender, ProviderMessage, RequestMode, RequestUpdate,
    },
    store::fs::FsStore,
    ticket::BlobTicket,
};
use iroh_tickets::endpoint::EndpointTicket;
use laplink_p2p::{
    RelayModeOption,
    endpoint::{EndpointConfig, build_endpoint},
    listing::{Entry, Listing, ListingProtocol},
    transfer::import_flat,
};

/// Serve a folder of files over iroh for browsing/download with ll-tui.
#[derive(Parser, Debug)]
#[command(version, about)]
struct ServeArgs {
    /// Folder to serve.
    #[clap(default_value = ".")]
    folder: PathBuf,

    /// Optional ticket to remember for this folder.
    #[clap(long)]
    ticket: Option<EndpointTicket>,

    /// The IPv4 address that magicsocket will listen on.
    #[clap(long, default_value = None)]
    magic_ipv4_addr: Option<SocketAddrV4>,

    /// The IPv6 address that magicsocket will listen on.
    #[clap(long, default_value = None)]
    magic_ipv6_addr: Option<SocketAddrV6>,

    /// The relay URL to use as a home relay.
    #[clap(long, default_value_t = RelayModeOption::Default)]
    relay: RelayModeOption,

    #[clap(short = 'v', long, action = clap::ArgAction::Count)]
    verbose: u8,

    #[clap(long)]
    show_secret: bool,

    /// Directory used to store the blob database. Defaults to a
    /// `.ll-serve-store` subdirectory inside `folder`. This directory is
    /// never itself served as part of the listing.
    #[clap(long)]
    store_dir: Option<PathBuf>,

    /// Disable automatic filesystem monitoring for changes.
    #[clap(long)]
    no_watch: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    if let Err(e) = run().await {
        eprintln!("{e}");
        std::process::exit(1);
    }
    Ok(())
}

async fn run() -> anyhow::Result<()> {
    let version = env!("CARGO_PKG_VERSION");
    eprintln!("ll-serve version: {version}");
    tracing::info!(%version, "ll-serve version: {version}");
    let args = ServeArgs::parse();
    let folder = args
        .folder
        .canonicalize()?;
    let store_dir = args
        .store_dir
        .unwrap_or_else(|| folder.join(".ll-serve-store"));
    tokio::fs::create_dir_all(&store_dir).await?;

    let (secret_key, generated) =
        laplink_p2p::ticket_storage::get_or_create_serve_secret(&store_dir)?;
    if (generated && args.verbose > 0) || args.show_secret {
        eprintln!("using secret key {}", hex::encode(secret_key.to_bytes()));
    }

    let store = FsStore::load(&store_dir).await?;

    let endpoint = build_endpoint(EndpointConfig {
        secret_key,
        alpns: vec![
            iroh_blobs::protocol::ALPN.to_vec(),
            laplink_p2p::listing::ALPN.to_vec(),
        ],
        relay: args.relay,
        magic_ipv4_addr: args.magic_ipv4_addr,
        magic_ipv6_addr: args.magic_ipv6_addr,
        publish_addr: true,
        lookup_by_dns: false,
    })
    .await?;

    eprintln!("importing {}...", folder.display());
    let files = import_flat(folder.clone(), &store, &store_dir).await?;
    let addr = endpoint.addr();
    let mut entries_map = std::collections::HashMap::new();
    for (path, size, hash, tag) in files {
        let ticket = BlobTicket::new(addr.clone(), hash, BlobFormat::Raw);
        let entry = Entry {
            path: path.clone(),
            size,
            hash,
            ticket,
        };
        entries_map.insert(path, (entry, tag));
    }
    let mut entries: Vec<Entry> = entries_map
        .values()
        .map(|(e, _)| e.clone())
        .collect();
    entries.sort_by(|a, b| {
        a.path
            .cmp(&b.path)
    });
    let listing = Listing::new(entries).with_server_version(version);

    let listing_protocol = ListingProtocol::new(listing.clone());

    let (_watcher_handle, _static_tags) = if !args.no_watch {
        match laplink_p2p::monitor::spawn_watcher(
            folder.clone(),
            store_dir.clone(),
            store.clone(),
            addr.clone(),
            entries_map,
            listing_protocol.clone(),
            Duration::from_millis(200),
        ) {
            Ok(handle) => (Some(handle), None),
            Err((e, map)) => {
                eprintln!("warning: failed to start filesystem watcher: {e}");
                (None, Some(map))
            }
        }
    } else {
        (None, Some(entries_map))
    };

    let (event_sender, mut event_rx) = EventSender::channel(
        64,
        EventMask {
            connected: ConnectMode::Intercept,
            get: RequestMode::InterceptLog,
            ..EventMask::DEFAULT
        },
    );
    let blobs = BlobsProtocol::new(&store, Some(event_sender));

    let listing_protocol_for_events = listing_protocol.clone();
    tokio::spawn(async move {
        let mut connections: std::collections::HashMap<u64, Option<iroh::PublicKey>> =
            std::collections::HashMap::new();
        while let Some(item) = event_rx
            .recv()
            .await
        {
            match item {
                ProviderMessage::ClientConnected(msg) => {
                    let connection_id = msg
                        .inner
                        .connection_id;
                    let node_id = msg
                        .inner
                        .endpoint_id;
                    connections.insert(connection_id, node_id);
                    msg.tx
                        .send(Ok(()))
                        .await
                        .ok();
                }
                ProviderMessage::ConnectionClosed(msg) => {
                    let connection_id = msg
                        .inner
                        .connection_id;
                    connections.remove(&connection_id);
                }
                ProviderMessage::GetRequestReceived(msg) => {
                    let connection_id = msg
                        .inner
                        .connection_id;
                    let hash = msg
                        .inner
                        .request
                        .hash;
                    let node_id = connections
                        .get(&connection_id)
                        .copied()
                        .flatten();
                    let client_label = match node_id {
                        Some(id) => format!("client {id}"),
                        None => format!("client conn-{connection_id}"),
                    };

                    let file_path = listing_protocol_for_events
                        .listing()
                        .entries
                        .into_iter()
                        .find(|e| e.hash == hash)
                        .map(|e| e.path);

                    tracing::info!(
                        client = %client_label,
                        %hash,
                        file = ?file_path,
                        "requested"
                    );

                    msg.tx
                        .send(Ok(()))
                        .await
                        .ok();

                    let mut rx = msg.rx;
                    tokio::spawn(async move {
                        while let Ok(Some(update)) = rx
                            .recv()
                            .await
                        {
                            match update {
                                RequestUpdate::Completed(_) => {
                                    tracing::info!(
                                        client = %client_label,
                                        %hash,
                                        file = ?file_path,
                                        "completed"
                                    );
                                    break;
                                }
                                RequestUpdate::Aborted(_) => {
                                    tracing::info!(
                                        client = %client_label,
                                        %hash,
                                        file = ?file_path,
                                        "aborted"
                                    );
                                    break;
                                }
                                _ => {}
                            }
                        }
                    });
                }
                _ => {}
            }
        }
    });
    let router = iroh::protocol::Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs.clone())
        .accept(laplink_p2p::listing::ALPN, listing_protocol.clone())
        .spawn();
    router
        .endpoint()
        .online()
        .await;

    let ticket = match args.ticket {
        Some(ticket) => {
            laplink_p2p::ticket_storage::save_serve_ticket(&store_dir, &ticket)?;
            ticket
        }
        None => match laplink_p2p::ticket_storage::load_serve_ticket(&store_dir)? {
            Some(ticket) => ticket,
            None => {
                let ticket = EndpointTicket::new(
                    router
                        .endpoint()
                        .addr(),
                );
                laplink_p2p::ticket_storage::save_serve_ticket(&store_dir, &ticket)?;
                ticket
            }
        },
    };
    println!(
        "serving {} ({} files)",
        folder.display(),
        listing
            .entries
            .len()
    );
    println!("to browse, use");
    println!("ll-tui {ticket}");
    println!();
    println!("ticket breakdown:");
    println!(
        "{}",
        laplink_p2p::args::describe_endpoint_addr(ticket.endpoint_addr())
    );

    tokio::signal::ctrl_c().await?;
    println!("shutting down");
    tokio::time::timeout(Duration::from_secs(2), router.shutdown()).await??;
    Ok(())
}
