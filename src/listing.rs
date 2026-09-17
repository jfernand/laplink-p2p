//! A minimal directory-listing protocol.
//!
//! This runs as a second ALPN handler on the same [`iroh::protocol::Router`]/[`iroh::Endpoint`]
//! as the regular iroh-blobs transfer protocol. A client connects, sends a trivial request, and
//! gets back a [`Listing`]: one [`Entry`] per file, each carrying a ready-to-use [`BlobTicket`]
//! so downloads reuse the exact same blobs-ALPN fetch path `ll receive` already uses.
//!
//! v1 limitation: the listing is a point-in-time snapshot taken once at server startup — files
//! added/removed on disk afterwards are not reflected until the server is restarted.

use std::sync::Arc;

use iroh::{
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler},
    Endpoint,
};
use iroh_blobs::{ticket::BlobTicket, Hash};
use iroh_tickets::endpoint::EndpointTicket;
use serde::{Deserialize, Serialize};

/// ALPN for the laplink-p2p file-listing protocol.
pub const ALPN: &[u8] = b"iroh-file-server/list/0";

/// Largest listing response we'll accept when reading from the wire.
const MAX_LISTING_SIZE: usize = 64 * 1024 * 1024;
/// Largest request we'll accept when reading from the wire.
const MAX_REQUEST_SIZE: usize = 4096;

/// A single file entry in a listing response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    /// "/"-joined relative path from the served root.
    pub path: String,
    pub size: u64,
    pub hash: Hash,
    /// Ready-to-use ticket: server's endpoint address + this entry's hash.
    pub ticket: BlobTicket,
}

/// A full directory listing, returned in response to a listing request.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    listing: Arc<Listing>,
}

impl ListingProtocol {
    pub fn new(listing: Listing) -> Self {
        Self {
            listing: Arc::new(listing),
        }
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

        let resp = ListResponse::V0((*self.listing).clone());
        let resp_bytes = postcard::to_stdvec(&resp).map_err(AcceptError::from_err)?;
        send.write_all(&resp_bytes)
            .await
            .map_err(AcceptError::from_err)?;
        send.finish()
            .ok();
        // Wait for the peer to receive all of the response before tearing down the
        // connection — otherwise the router may close it as soon as this future returns,
        // racing the still-in-flight bytes.
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
