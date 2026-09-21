//! Progress-agnostic receive-side primitives, shared by the CLI and the TUI.

use std::path::PathBuf;

use iroh_blobs::{
    api::remote::GetProgressItem, format::collection::Collection, get::Stats, store::fs::FsStore,
    ticket::BlobTicket,
};
use n0_future::StreamExt;
use tokio::sync::mpsc;

use crate::{
    endpoint::{EndpointConfig, build_endpoint},
    transfer::{export_collection, export_one},
};

/// Progress events emitted while receiving a collection. Unlike a single-blob fetch, the
/// total size of a collection isn't known until after connecting, so this carries an extra
/// `Sizes` event the CLI uses to size its progress bar / print an upfront summary.
#[derive(Debug, Clone)]
pub enum ReceiveProgress {
    /// Emitted once, after connecting, before the actual transfer starts (skipped entirely
    /// if the collection is already fully local).
    Sizes {
        total_files: u64,
        total_size: u64,
        payload_size: u64,
        local_size: u64,
    },
    /// `offset` bytes of the whole request (missing + already-local) have been fetched so far.
    Progress(u64),
}

pub struct ReceiveCollectionOutcome {
    pub total_files: u64,
    pub payload_size: u64,
    pub stats: Stats,
}

/// Fetch the hash-seq collection referenced by `ticket` into a store at `store_dir`,
/// exporting every entry under `export_root`.
pub async fn receive_collection(
    ticket: BlobTicket,
    cfg: EndpointConfig,
    store_dir: PathBuf,
    export_root: PathBuf,
    on_progress: Option<mpsc::Sender<ReceiveProgress>>,
) -> anyhow::Result<ReceiveCollectionOutcome> {
    let addr = ticket
        .addr()
        .clone();
    let endpoint = build_endpoint(cfg).await?;
    tokio::fs::create_dir_all(&store_dir).await?;
    let db = FsStore::load(&store_dir).await?;
    let hash_and_format = ticket.hash_and_format();
    let local = db
        .remote()
        .local(hash_and_format)
        .await?;
    let (stats, total_files, payload_size) = if !local.is_complete() {
        let connection = endpoint
            .connect(addr, iroh_blobs::protocol::ALPN)
            .await?;
        let (_hash_seq, sizes) = iroh_blobs::get::request::get_hash_seq_and_sizes(
            &connection,
            &hash_and_format.hash,
            1024 * 1024 * 32,
            None,
        )
        .await?;
        let total_size = sizes
            .iter()
            .copied()
            .sum::<u64>();
        let payload_size = sizes
            .iter()
            .skip(2)
            .copied()
            .sum::<u64>();
        let total_files = (sizes
            .len()
            .saturating_sub(1)) as u64;
        let local_size = local.local_bytes();
        if let Some(tx) = &on_progress {
            tx.send(ReceiveProgress::Sizes {
                total_files,
                total_size,
                payload_size,
                local_size,
            })
            .await
            .ok();
        }
        let get = db
            .remote()
            .execute_get(connection, local.missing());
        let mut stream = get.stream();
        let mut stats = Stats::default();
        while let Some(item) = stream
            .next()
            .await
        {
            match item {
                GetProgressItem::Progress(offset) => {
                    if let Some(tx) = &on_progress {
                        tx.send(ReceiveProgress::Progress(offset))
                            .await
                            .ok();
                    }
                }
                GetProgressItem::Done(value) => {
                    stats = value;
                    break;
                }
                GetProgressItem::Error(cause) => return Err(cause.into()),
            }
        }
        (stats, total_files, payload_size)
    } else {
        let total_files = local
            .children()
            .unwrap_or(1)
            - 1;
        (Stats::default(), total_files, 0)
    };
    let collection = Collection::load(hash_and_format.hash, db.as_ref()).await?;
    export_collection(&db, collection, &export_root).await?;
    db.shutdown()
        .await?;
    Ok(ReceiveCollectionOutcome {
        total_files,
        payload_size,
        stats,
    })
}

/// Fetch a single blob referenced by `ticket` into a store at `store_dir`, exporting it to
/// `export_path`. The caller is expected to already know the blob's size (e.g. from a
/// [`crate::listing::Entry`]), so `on_progress` only reports raw byte offsets.
pub async fn receive_single(
    ticket: BlobTicket,
    cfg: EndpointConfig,
    store_dir: PathBuf,
    export_path: PathBuf,
    on_progress: Option<mpsc::Sender<u64>>,
) -> anyhow::Result<Stats> {
    let addr = ticket
        .addr()
        .clone();
    let endpoint = build_endpoint(cfg).await?;
    tokio::fs::create_dir_all(&store_dir).await?;
    let db = FsStore::load(&store_dir).await?;
    let hash_and_format = ticket.hash_and_format();
    let local = db
        .remote()
        .local(hash_and_format)
        .await?;
    let stats = if !local.is_complete() {
        let connection = endpoint
            .connect(addr, iroh_blobs::protocol::ALPN)
            .await?;
        let get = db
            .remote()
            .execute_get(connection, local.missing());
        let mut stream = get.stream();
        let mut stats = Stats::default();
        while let Some(item) = stream
            .next()
            .await
        {
            match item {
                GetProgressItem::Progress(offset) => {
                    if let Some(tx) = &on_progress {
                        tx.send(offset)
                            .await
                            .ok();
                    }
                }
                GetProgressItem::Done(value) => {
                    stats = value;
                    break;
                }
                GetProgressItem::Error(cause) => return Err(cause.into()),
            }
        }
        stats
    } else {
        Stats::default()
    };
    export_one(&db, hash_and_format.hash, &export_path).await?;
    db.shutdown()
        .await?;
    Ok(stats)
}
