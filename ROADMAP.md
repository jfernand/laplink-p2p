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

## 4. Binary Auto-Updates in `ll-tui` via `ll-serve` (Implemented)

### Overview
When serving a release or development directory that includes newer versions of `laplink-p2p` binaries (`ll`, `ll-tui`, `ll-serve`), `ll-tui` detects available updates, compares executable versions using semantic versioning, and performs an in-place atomic update upon user confirmation ('u' key).

### Objectives
- Inspect served files for matching release archives (`ll-vX.Y.Z-{target}.tar.gz`/`.zip`) and standalone binaries matching the host platform architecture.
- Compare candidate versions against the running application version (`env!("CARGO_PKG_VERSION")`) using semantic versioning, filtering for the newest compatible version.
- Display an update prompt banner in `ll-tui` when a newer version is discovered.
- Securely download the update blob over `iroh-blobs`, unpack multi-binary archives (`ll`, `ll-serve`, `ll-tui`), and atomically replace executables in-place via `self-replace`.

### Technical Considerations
- **Target & Version Discovery**: `src/update.rs` detects the host OS and architecture (`linux-x86_64`, `linux-aarch64`, `darwin-x86_64`, `darwin-aarch64`, `windows-x86_64`), matching release artifact patterns from the CI pipeline or standalone binaries.
- **Cross-Platform In-Place Replacement**: Uses `self-replace` combined with atomic directory staging to replace the running executable without file locking issues across Linux, macOS, and Windows.
- **Security & Content Integrity**: Downloads are verified end-to-end using `iroh-blobs` blake3 hashes prior to binary extraction and replacement.
