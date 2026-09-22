//! Dynamic directory-listing protocol.
//!
//! This runs as a second ALPN handler on the same [`iroh::protocol::Router`]/[`iroh::Endpoint`]
//! as the regular iroh-blobs transfer protocol. A client connects, sends a trivial request, and
//! gets back a [`Listing`]: one [`Entry`] per file, each carrying a ready-to-use [`BlobTicket`]
//! so downloads reuse the exact same blobs-ALPN fetch path `ll receive` already uses.
//!
//! The served listing can be updated dynamically as files on disk change.

use std::sync::Arc;

use iroh::{
    Endpoint,
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler},
};
use iroh_blobs::{Hash, ticket::BlobTicket};
use iroh_tickets::endpoint::EndpointTicket;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

/// ALPN for the laplink-p2p file-listing protocol.
pub const ALPN: &[u8] = b"iroh-file-server/list/0";

/// Largest listing response we'll accept when reading from the wire.
const MAX_LISTING_SIZE: usize = 64 * 1024 * 1024;
/// Largest request we'll accept when reading from the wire.
const MAX_REQUEST_SIZE: usize = 4096;

/// A single file entry in a listing response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// "/"-joined relative path from the served root.
    pub path: String,
    pub size: u64,
    pub hash: Hash,
    /// Ready-to-use ticket: server's endpoint address + this entry's hash.
    pub ticket: BlobTicket,
}

/// A full directory listing, returned in response to a listing request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Listing {
    pub entries: Vec<Entry>,
    #[serde(default)]
    pub server_version: Option<String>,
}

impl Listing {
    pub fn new(entries: Vec<Entry>) -> Self {
        Self {
            entries,
            server_version: None,
        }
    }

    pub fn with_server_version(mut self, version: impl Into<String>) -> Self {
        self.server_version = Some(version.into());
        self
    }

    pub fn server_version(&self) -> Option<&str> {
        self.server_version
            .as_deref()
    }

    pub fn version(&self) -> Option<&str> {
        self.server_version
            .as_deref()
    }
}

/// Wire request, version-tagged so the protocol can evolve without an outright wire break.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ListRequest {
    /// One-off snapshot request.
    V0,
    /// Subscribe to live directory listing updates.
    /// The server will immediately send the current listing snapshot, and then stream
    /// updates as filesystem changes occur.
    SubscribeV0,
}

/// An update sent over the subscription stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ListingUpdate {
    V0(Listing),
}

/// Wire response for snapshot requests, version-tagged like [`ListRequest`].
#[derive(Debug, Serialize, Deserialize)]
pub enum ListResponse {
    V0(Listing),
}

/// Write a framed update message to an async writer.
pub async fn write_update_frame<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    update: &ListingUpdate,
) -> std::io::Result<()> {
    let bytes = postcard::to_stdvec(update)
        .map_err(|e| std::io::Error::other(format!("serialization error: {e}")))?;
    let len = u32::try_from(bytes.len())
        .map_err(|e| std::io::Error::other(format!("listing frame too large: {e}")))?;
    tokio::io::AsyncWriteExt::write_all(writer, &len.to_be_bytes()).await?;
    tokio::io::AsyncWriteExt::write_all(writer, &bytes).await?;
    tokio::io::AsyncWriteExt::flush(writer).await?;
    Ok(())
}

/// Read a framed update message from an async reader.
///
/// Returns `Ok(None)` on clean EOF before any frame bytes are read.
pub async fn read_update_frame<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
) -> std::io::Result<Option<ListingUpdate>> {
    let mut len_buf = [0u8; 4];
    match tokio::io::AsyncReadExt::read_exact(reader, &mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_LISTING_SIZE {
        return Err(std::io::Error::other(format!(
            "listing frame exceeds max size: {len} > {MAX_LISTING_SIZE}"
        )));
    }
    let mut buf = vec![0u8; len];
    tokio::io::AsyncReadExt::read_exact(reader, &mut buf).await?;
    let update: ListingUpdate = postcard::from_bytes(&buf)
        .map_err(|e| std::io::Error::other(format!("deserialization error: {e}")))?;
    Ok(Some(update))
}

/// Server-side handler for the listing protocol.
#[derive(Debug, Clone)]
pub struct ListingProtocol {
    server_version: String,
    update_tx: Arc<watch::Sender<Listing>>,
    update_rx: watch::Receiver<Listing>,
}

impl ListingProtocol {
    pub fn new(mut listing: Listing) -> Self {
        let server_version = listing
            .server_version
            .clone()
            .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string());
        listing.server_version = Some(server_version.clone());
        let (update_tx, update_rx) = watch::channel(listing);
        Self {
            server_version,
            update_tx: Arc::new(update_tx),
            update_rx,
        }
    }

    pub fn new_with_version(mut listing: Listing, server_version: impl Into<String>) -> Self {
        let server_version = server_version.into();
        listing.server_version = Some(server_version.clone());
        let (update_tx, update_rx) = watch::channel(listing);
        Self {
            server_version,
            update_tx: Arc::new(update_tx),
            update_rx,
        }
    }

    pub fn server_version(&self) -> &str {
        &self.server_version
    }

    /// Update the current active listing and notify all subscribers.
    pub fn update(&self, mut new_listing: Listing) {
        if new_listing
            .server_version
            .is_none()
        {
            new_listing.server_version = Some(
                self.server_version
                    .clone(),
            );
        }
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

    async fn handle_stream(
        &self,
        mut send: iroh::endpoint::SendStream,
        mut recv: iroh::endpoint::RecvStream,
        node_id: iroh::PublicKey,
    ) -> anyhow::Result<()> {
        let req_bytes = recv
            .read_to_end(MAX_REQUEST_SIZE)
            .await?;
        let req: ListRequest = postcard::from_bytes(&req_bytes)?;

        match req {
            ListRequest::V0 => {
                let current_listing = self.listing();
                let server_ver = self.server_version();
                tracing::info!(
                    %node_id,
                    files = current_listing.entries.len(),
                    server_version = server_ver,
                    "client requested listing snapshot"
                );
                let resp = ListResponse::V0(current_listing);
                let resp_bytes = postcard::to_stdvec(&resp)?;
                send.write_all(&resp_bytes)
                    .await?;
                send.finish()
                    .ok();
                send.stopped()
                    .await
                    .ok();
                Ok(())
            }
            ListRequest::SubscribeV0 => {
                let server_ver = self.server_version();
                tracing::info!(
                    %node_id,
                    server_version = server_ver,
                    "client subscribed to live listing updates"
                );
                let mut rx = self.subscribe();
                let initial = rx
                    .borrow_and_update()
                    .clone();
                if let Err(e) = write_update_frame(&mut send, &ListingUpdate::V0(initial)).await {
                    tracing::debug!("failed to send initial listing frame: {e}");
                    return Ok(());
                }

                loop {
                    tokio::select! {
                        res = rx.changed() => {
                            if res.is_err() {
                                break;
                            }
                            let new_listing = rx.borrow_and_update().clone();
                            tracing::info!(
                                %node_id,
                                files = new_listing.entries.len(),
                                "pushed listing update to client"
                            );
                            if let Err(e) = write_update_frame(&mut send, &ListingUpdate::V0(new_listing)).await {
                                tracing::debug!("failed to send listing update frame: {e}");
                                break;
                            }
                        }
                        _ = send.stopped() => {
                            break;
                        }
                    }
                }
                tracing::info!(%node_id, "client unsubscribed from live listing updates");
                send.finish()
                    .ok();
                Ok(())
            }
        }
    }
}

impl ProtocolHandler for ListingProtocol {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let node_id = conn.remote_id();
        while let Ok((send, recv)) = conn
            .accept_bi()
            .await
        {
            let this = self.clone();
            tokio::spawn(async move {
                if let Err(e) = this
                    .handle_stream(send, recv, node_id)
                    .await
                {
                    tracing::debug!("listing stream error: {e}");
                }
            });
        }
        Ok(())
    }
}

/// An active listing update subscription stream.
pub struct ListingStream {
    _conn: Connection,
    recv: iroh::endpoint::RecvStream,
}

impl ListingStream {
    /// Read the next directory listing update from the stream.
    ///
    /// The first update received is the server's listing snapshot at the time of
    /// connection; subsequent updates are sent whenever changes occur on disk.
    ///
    /// Returns `Ok(Some(listing))` on each update, or `Ok(None)` if the server
    /// closed the stream.
    pub async fn next(&mut self) -> anyhow::Result<Option<Listing>> {
        match read_update_frame(&mut self.recv).await? {
            Some(ListingUpdate::V0(listing)) => {
                let client_version = env!("CARGO_PKG_VERSION");
                let server_version = listing
                    .server_version()
                    .unwrap_or("unknown");
                tracing::debug!(
                    %server_version,
                    %client_version,
                    "received listing update from server"
                );
                Ok(Some(listing))
            }
            None => Ok(None),
        }
    }

    /// Convert this `ListingStream` into a [`n0_future::Stream`].
    pub fn into_stream(self) -> impl n0_future::Stream<Item = anyhow::Result<Listing>> {
        n0_future::stream::unfold(self, |mut stream| async move {
            match stream
                .next()
                .await
            {
                Ok(Some(listing)) => Some((Ok(listing), stream)),
                Ok(None) => None,
                Err(e) => Some((Err(e), stream)),
            }
        })
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
    let client_version = env!("CARGO_PKG_VERSION");
    let server_version = listing
        .server_version()
        .unwrap_or("unknown");
    tracing::info!(
        %server_version,
        %client_version,
        "fetched listing from server"
    );
    Ok(listing)
}

/// Connect to `ticket` and subscribe to continuous directory listing updates.
///
/// The returned [`ListingStream`] immediately yields the current listing snapshot,
/// followed by subsequent updates whenever changes occur on the server.
pub async fn subscribe_listing(
    endpoint: &Endpoint,
    ticket: &EndpointTicket,
) -> anyhow::Result<ListingStream> {
    let addr = ticket
        .endpoint_addr()
        .clone();
    let conn = endpoint
        .connect(addr, ALPN)
        .await?;
    let (mut send, recv) = conn
        .open_bi()
        .await?;
    send.write_all(&postcard::to_stdvec(&ListRequest::SubscribeV0)?)
        .await?;
    send.finish()?;
    Ok(ListingStream { _conn: conn, recv })
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use iroh_blobs::BlobFormat;

    use super::*;
    use crate::endpoint::{EndpointConfig, build_endpoint};

    #[tokio::test]
    async fn test_frame_serialization_roundtrip() {
        let ticket = BlobTicket::new(
            iroh::EndpointAddr::from(iroh::SecretKey::generate().public()),
            Hash::from_bytes([1u8; 32]),
            BlobFormat::Raw,
        );
        let entry = Entry {
            path: "test/file.txt".to_string(),
            size: 42,
            hash: Hash::from_bytes([1u8; 32]),
            ticket,
        };
        let listing1 = Listing::new(vec![entry.clone()]);
        let listing2 = Listing::new(vec![entry.clone(), entry]);

        let mut buf = Vec::new();
        write_update_frame(&mut buf, &ListingUpdate::V0(listing1.clone()))
            .await
            .unwrap();
        write_update_frame(&mut buf, &ListingUpdate::V0(listing2.clone()))
            .await
            .unwrap();

        let mut cursor = Cursor::new(buf);
        let read1 = read_update_frame(&mut cursor)
            .await
            .unwrap()
            .expect("should read first frame");
        assert_eq!(read1, ListingUpdate::V0(listing1));

        let read2 = read_update_frame(&mut cursor)
            .await
            .unwrap()
            .expect("should read second frame");
        assert_eq!(read2, ListingUpdate::V0(listing2));

        let eof = read_update_frame(&mut cursor)
            .await
            .unwrap();
        assert!(eof.is_none());
    }

    #[tokio::test]
    async fn test_subscription_stream_live() {
        let server_secret = iroh::SecretKey::generate();
        let server_endpoint = build_endpoint(EndpointConfig {
            secret_key: server_secret.clone(),
            alpns: vec![ALPN.to_vec()],
            relay: crate::RelayModeOption::Disabled,
            magic_ipv4_addr: None,
            magic_ipv6_addr: None,
            publish_addr: false,
            lookup_by_dns: false,
        })
        .await
        .unwrap();

        let initial_listing = Listing::new(vec![]);
        let protocol = ListingProtocol::new_with_version(initial_listing, "1.2.3");

        let router = iroh::protocol::Router::builder(server_endpoint.clone())
            .accept(ALPN, protocol.clone())
            .spawn();

        let ticket = EndpointTicket::new(
            router
                .endpoint()
                .addr(),
        );

        let client_secret = iroh::SecretKey::generate();
        let client_endpoint = build_endpoint(EndpointConfig {
            secret_key: client_secret,
            alpns: vec![],
            relay: crate::RelayModeOption::Disabled,
            magic_ipv4_addr: None,
            magic_ipv6_addr: None,
            publish_addr: false,
            lookup_by_dns: false,
        })
        .await
        .unwrap();

        // 1. Snapshot fetch also works
        let snapshot = fetch_listing(&client_endpoint, &ticket)
            .await
            .unwrap();
        assert_eq!(
            snapshot
                .entries
                .len(),
            0
        );
        assert_eq!(snapshot.server_version(), Some("1.2.3"));

        // 2. Subscribe to listing updates
        let mut stream = subscribe_listing(&client_endpoint, &ticket)
            .await
            .unwrap();

        // Initial snapshot arrives on subscription
        let first = stream
            .next()
            .await
            .unwrap()
            .expect("initial listing");
        assert_eq!(
            first
                .entries
                .len(),
            0
        );
        assert_eq!(first.server_version(), Some("1.2.3"));

        // Server pushes update 1
        let dummy_ticket = BlobTicket::new(
            router
                .endpoint()
                .addr(),
            Hash::from_bytes([2u8; 32]),
            BlobFormat::Raw,
        );
        let entry1 = Entry {
            path: "alpha.txt".to_string(),
            size: 100,
            hash: Hash::from_bytes([2u8; 32]),
            ticket: dummy_ticket.clone(),
        };
        protocol.update(Listing::new(vec![entry1.clone()]));

        let update1 = stream
            .next()
            .await
            .unwrap()
            .expect("update 1");
        assert_eq!(
            update1
                .entries
                .len(),
            1
        );
        assert_eq!(update1.entries[0].path, "alpha.txt");
        assert_eq!(update1.server_version(), Some("1.2.3"));

        // Server pushes update 2
        let entry2 = Entry {
            path: "beta.txt".to_string(),
            size: 200,
            hash: Hash::from_bytes([3u8; 32]),
            ticket: dummy_ticket,
        };
        protocol.update(Listing::new(vec![entry1, entry2]));

        let update2 = stream
            .next()
            .await
            .unwrap()
            .expect("update 2");
        assert_eq!(
            update2
                .entries
                .len(),
            2
        );
        assert_eq!(update2.entries[1].path, "beta.txt");
        assert_eq!(update2.server_version(), Some("1.2.3"));

        router
            .shutdown()
            .await
            .unwrap();
    }
}
