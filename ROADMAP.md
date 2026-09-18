# Roadmap & Planned Features

This document outlines planned capabilities, architecture, and feature designs for future versions of `laplink-p2p`.

## 1. Dynamic Filesystem Monitoring in `ll-serve` (Implemented)

### Overview
`ll-serve` monitors the served directory for filesystem changes in real time, updating the in-memory `Listing` and blob database (`FsStore`) dynamically as files are created, modified, renamed, or deleted without requiring a server restart.

### Objectives
- Automatically detect filesystem events (file additions, modifications, removals, and renames) in the served directory tree.
- Debounce and batch filesystem changes to prevent excessive re-indexing on rapid writes.
- Incrementally update the blob database (`FsStore`) and refresh the served `Listing` in memory.
- Ignore internal state directories such as `.ll-serve-store`.

### Technical Considerations
- **Filesystem Watcher**: Leverage a cross-platform file watcher (e.g. `notify`) running in an asynchronous background task.
- **Incremental Import**: Rather than re-reading the entire folder hierarchy on every event, re-hash only modified or newly created files, remove deleted items from the in-memory listing, and tag new blobs to prevent garbage collection.
- **Thread Safety**: Wrap the active `Listing` in a shared, concurrency-safe structure (such as `Arc<RwLock<Listing>>` or `tokio::sync::watch`) within `ListingProtocol`.

---

## 2. Peer Change Notifications via Subscription Stream (Implemented)

### Overview
When the served folder contents change, connected peers browsing the repository are notified immediately over a persistent QUIC subscription stream rather than having to poll or manually reconnect.

### Objectives
- Establish a lightweight publish/subscribe notification channel between `ll-serve` and clients (`ll-tui`).
- Broadcast listing update events when filesystem changes are committed.

### Technical Considerations
- **Subscription Protocol**: An ALPN-based protocol stream (`iroh-file-server/list/0`) over QUIC with length-prefixed framing and version-tagged messages (`ListRequest::SubscribeV0` and `ListingUpdate::V0`).
- **Direct Pushed Listings**: Server sends the initial listing snapshot upon subscription, and pushes updated listings immediately as filesystem changes are detected by the file monitor.
- **Multiplexing & Lifecycle**: Uses native QUIC streams multiplexed over the server connection, with automatic cleanup when peers disconnect.

---

## 3. Real-Time Listing Updates in `ll-tui` (Implemented)

### Overview
`ll-tui` maintains a live view of the served folder, updating the displayed file list when notifications are received from `ll-serve` over the live subscription stream.

### Objectives
- Listen for peer notifications broadcast by `ll-serve` via the background event loop.
- Automatically receive and update the `Listing` over the persistent QUIC subscription stream.
- Re-render the file tree seamlessly without disrupting ongoing downloads, navigation focus, or selection state whenever possible.

### Technical Considerations
- **UI State Preservation**: Reconciles the new listing with the existing file tree hierarchy so that current row selection persists on the same file path (or clamps gracefully on deletion).
- **Non-blocking Event Stream**: Integrates subscription updates into the existing `tokio::select!` event loop in `src/bin/ll-tui.rs` alongside terminal input and download progress events without blocking or stalling transfers.

---

## 4. Binary Auto-Updates in `ll-tui` via `ll-serve`

### Overview
When serving a release or development directory that includes newer versions of `laplink-p2p` binaries (`ll`, `ll-tui`, `ll-serve`), `ll-tui` should be capable of detecting available updates, comparing executable versions, and performing an in-place auto-update.

### Objectives
- Inspect served files for matching binary names corresponding to the current executable and platform (e.g. `ll-tui`, `ll`, or platform-specific archives/binaries).
- Inspect and compare versions between the currently running binary (e.g. `env!("CARGO_PKG_VERSION")` / semantic versioning) and the binary offered by `ll-serve`.
- Prompt the user in the TUI when a newer compatible version is detected.
- Securely download and replace the running executable (or stage replacement on restart) following platform-specific binary replacement best practices.

### Technical Considerations
- **Version Discovery**: Support querying binary metadata/version via dedicated naming conventions (e.g. `ll-v0.28.4-linux-x86_64`), manifest files, or by inspecting headers.
- **Platform Handling**: Safely handle executable replacement across target platforms (especially Windows where running executables are locked and require renaming/staging before replacement).
- **Security & Integrity**: Verify blake3 hashes over `iroh-blobs` to ensure downloaded executables are complete and uncorrupted prior to replacement.
