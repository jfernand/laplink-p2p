//! Progress-agnostic send-side primitives, shared by the CLI.

use std::path::PathBuf;

use iroh_blobs::{
    api::TempTag, format::collection::Collection, provider::events::EventSender, BlobsProtocol,
    Hash,
};

use crate::{
    endpoint::{build_endpoint, EndpointConfig},
    transfer::import_collection,
};

/// The result of [`start_send`]: a running router serving the imported data, plus everything
/// needed to build a ticket for it. The caller owns the session's lifetime (drop `temp_tag`
/// and shut down `router` when done; the store directory is left for the caller to clean up).
pub struct SendSession {
    pub router: iroh::protocol::Router,
    pub hash: Hash,
    pub size: u64,
    pub collection: Collection,
    pub temp_tag: TempTag,
}

/// Set up a store + endpoint + router for sending `path`, and import it as a collection.
///
/// Does not print anything, does not wait for ctrl-c, and does not delete `store_dir` — all
/// of that is the caller's responsibility (see `ll.rs`'s `send` for the CLI wrapper).
pub async fn start_send(
    path: PathBuf,
    cfg: EndpointConfig,
    store_dir: PathBuf,
    event_sender: Option<EventSender>,
) -> anyhow::Result<SendSession> {
    tokio::fs::create_dir_all(&store_dir).await?;
    let endpoint = build_endpoint(cfg).await?;
    let store = iroh_blobs::store::fs::FsStore::load(&store_dir).await?;
    let blobs = BlobsProtocol::new(&store, event_sender);

    let (temp_tag, size, collection) = import_collection(path, blobs.store()).await?;
    let hash = temp_tag.hash();

    let router = iroh::protocol::Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs.clone())
        .spawn();
    router.endpoint().online().await;

    Ok(SendSession {
        router,
        hash,
        size,
        collection,
        temp_tag,
    })
}
