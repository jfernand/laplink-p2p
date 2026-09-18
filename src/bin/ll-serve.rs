//! Serve a folder of files over iroh for browsing/downloading with `ll-tui`.

use std::{
    net::{SocketAddrV4, SocketAddrV6},
    path::PathBuf,
    time::Duration,
};

use clap::Parser;
use iroh_blobs::{store::fs::FsStore, ticket::BlobTicket, BlobFormat, BlobsProtocol};
use iroh_tickets::endpoint::EndpointTicket;
use laplink_p2p::{
    endpoint::{build_endpoint, EndpointConfig},
    listing::{Entry, Listing, ListingProtocol},
    transfer::import_flat,
    RelayModeOption,
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
    let (entries, tags): (Vec<Entry>, Vec<_>) = files
        .into_iter()
        .map(|(path, size, hash, tag)| {
            let ticket = BlobTicket::new(addr.clone(), hash, BlobFormat::Raw);
            (
                Entry {
                    path,
                    size,
                    hash,
                    ticket,
                },
                tag,
            )
        })
        .unzip();
    // Keep every temp tag alive for the process lifetime so the blobs aren't GC'd.
    let _tags = tags;
    let listing = Listing { entries };

    let blobs = BlobsProtocol::new(&store, None);
    let router = iroh::protocol::Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs.clone())
        .accept(
            laplink_p2p::listing::ALPN,
            ListingProtocol::new(listing.clone()),
        )
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

    tokio::signal::ctrl_c().await?;
    println!("shutting down");
    tokio::time::timeout(Duration::from_secs(2), router.shutdown()).await??;
    Ok(())
}
