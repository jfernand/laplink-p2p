//! Dynamic directory-listing protocol.
//!
//! Clients connect with ALPN [`ALPN`] and send a [`ListRequest`]. The server responds
//! with a [`ListResponse`] and closes the send stream.
//!
//! The served listing can be updated dynamically as files on disk change.

use std::sync::Arc;

use iroh::{
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler},
    Endpoint,
};
use iroh_blobs::{ticket::BlobTicket, Hash};
use iroh_tickets::endpoint::EndpointTicket;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

/// ALPN for the laplink-p2p file-listing protocol.
pub const ALPN: &[u8] = b"iroh-file-server/list/0";

const MAX_LISTING_SIZE: usize = 16 * 1024 * 1024;
const MAX_REQUEST_SIZE: usize = 4096;

/// A single file entry in a listing response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// "/"-joined relative path from the served root.
    pub path: String,
    pub size: u64,
    pub hash: Hash,
    pub ticket: BlobTicket,
}

/// A full directory listing, returned in response to a listing request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Listing {
    pub entries: Vec<Entry>,
}

/// Wire request, version-tagged so the protocol can evolve without an outright wire break.
#[derive(Debug, Serialize, Deserialize)]
enum ListRequest {
    V0,
}

/// Wire response, version-tagged like [`ListRequest`].
#[derive(Debug, Serialize, Deserialize)]
enum ListResponse {
    V0(Listing),
}

/// Server-side handler for the listing protocol.
#[derive(Debug, Clone)]
pub struct ListingProtocol {
    update_tx: Arc<watch::Sender<Listing>>,
    update_rx: watch::Receiver<Listing>,
}

impl ListingProtocol {
    pub fn new(listing: Listing) -> Self {
        let (update_tx, update_rx) = watch::channel(listing);
        Self {
            update_tx: Arc::new(update_tx),
            update_rx,
        }
    }

    /// Update the current active listing.
    pub fn update(&self, new_listing: Listing) {
        let _ = self
            .update_tx
            .send(new_listing);
    }

    /// Retrieve the current listing snapshot.
    pub fn listing(&self) -> Listing {
        self.update_rx
            .borrow()
            .clone()
    }

    /// Subscribe to listing updates.
    pub fn subscribe(&self) -> watch::Receiver<Listing> {
        self.update_rx
            .clone()
    }
}

impl ProtocolHandler for ListingProtocol {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let (mut send, mut recv) = conn
            .accept_bi()
            .await
            .map_err(AcceptError::from_err)?;
        let req_bytes = recv
            .read_to_end(MAX_REQUEST_SIZE)
            .await
            .map_err(AcceptError::from_err)?;
        let _req: ListRequest = postcard::from_bytes(&req_bytes).map_err(AcceptError::from_err)?;

        let current_listing = self.listing();
        let resp = ListResponse::V0(current_listing);
        let resp_bytes = postcard::to_stdvec(&resp).map_err(AcceptError::from_err)?;
        send.write_all(&resp_bytes)
            .await
            .map_err(AcceptError::from_err)?;
        send.finish()
            .ok();
        send.stopped()
            .await
            .ok();
        Ok(())
    }
}

/// Connect to `ticket` and fetch its directory listing.
pub async fn fetch_listing(
    endpoint: &Endpoint,
    ticket: &EndpointTicket,
) -> anyhow::Result<Listing> {
    let addr = ticket
        .endpoint_addr()
        .clone();
    let conn = endpoint
        .connect(addr, ALPN)
        .await?;
    let (mut send, mut recv) = conn
        .open_bi()
        .await?;
    send.write_all(&postcard::to_stdvec(&ListRequest::V0)?)
        .await?;
    send.finish()?;
    let resp_bytes = recv
        .read_to_end(MAX_LISTING_SIZE)
        .await?;
    let ListResponse::V0(listing) = postcard::from_bytes(&resp_bytes)?;
    Ok(listing)
}
