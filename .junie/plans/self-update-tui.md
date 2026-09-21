---
sessionId: session-260918-115900-1028
---

# Requirements

### Overview & Goals
Implement Roadmap Item 4: **Binary Auto-Updates in `ll-tui` via `ll-serve`**.
When `ll-tui` connects to an `ll-serve` instance serving release archives or development binaries, it will automatically inspect the listing for newer versions matching the local platform architecture, notify the user through the TUI interface, and perform an in-place executable replacement upon confirmation.

### Scope
- **In Scope**:
  - Target platform detection matching release artifacts (`linux-x86_64`, `linux-aarch64`, `darwin-x86_64`, `darwin-aarch64`, `windows-x86_64`).
  - Semantic version parsing and comparison between the currently running binary (`env!("CARGO_PKG_VERSION")`) and candidate files in directory listings.
  - Support for official multi-binary release archives (`.tar.gz` and `.zip`) updating `ll`, `ll-serve`, and `ll-tui` simultaneously in the executable's directory.
  - Support for standalone binaries (`ll-tui` / `ll-tui-vX.Y.Z-target`) updating the running executable directly.
  - In-app interactive TUI prompt banner and keybinding (`u`) to trigger update download and in-place replacement.
  - Secure download over `iroh-blobs` with blake3 verification and atomic binary replacement via `self-replace`.
- **Out of Scope**:
  - External package manager integration (e.g., `apt`, `brew`, `cargo install`).
  - Automatic restart of the TUI process (the user will be prompted to restart after successful replacement).

### User Stories
- As a user running `ll-tui`, I want to see an immediate notification banner when the served directory contains a newer version of the application so that I am always aware of available updates.
- As a user, I want to press a single key (`u`) to download and replace the running binary in-place without manually searching for downloads or extracting archives.
- As a user with multiple `laplink-p2p` binaries installed in the same folder, I want the update to upgrade `ll`, `ll-serve`, and `ll-tui` together when a release archive is provided so that my tool suite stays in sync.

### Functional Requirements
1. **Asset & Target Detection**:
   - Detect current host operating system and target architecture (`linux-x86_64`, `linux-aarch64`, `darwin-x86_64`, `darwin-aarch64`, `windows-x86_64`).
   - Parse filenames in served `Listing` entries matching patterns:
     - `ll-v{version}-{target}.tar.gz` / `ll-v{version}-{target}.zip`
     - `ll-{version}-{target}.tar.gz` / `ll-{version}-{target}.zip`
     - `ll-tui-v{version}-{target}` / `ll-tui-v{version}-{target}.exe`
     - `ll-tui` / `ll-tui.exe` (when server indicates a newer version via `server_version`).
2. **Version Evaluation**:
   - Compare candidate version against running version using semantic versioning.
   - Ignore candidates with equal or older versions.
   - Select the highest available compatible version candidate.
3. **Interactive TUI Prompt**:
   - Display a prominent update notification banner in `ll-tui` (e.g. `[Update Available: v0.30.0 | Press 'u' to update]`).
   - Keep the banner visible across live listing updates while the candidate remains available.
4. **Download & In-Place Replacement**:
   - When `'u'` is pressed and no transfer is active, download the candidate blob to a temporary staging path via `receive_single`.
   - If the candidate is a release archive, decompress it and replace sibling binaries (`ll`, `ll-serve`, `ll-tui`) in `std::env::current_exe()?.parent()`.
   - If the candidate is a standalone binary, replace `std::env::current_exe()`.
   - Use atomic replacement (`self-replace` crate) to handle running executable replacement safely across Linux, macOS, and Windows.
   - Display status messages: `downloading update...` -> `applying update...` -> `updated successfully to vX.Y.Z! Restart ll-tui to run new version.`

### Non-Functional Requirements
- **Safety & Atomicity**: The active executable must remain functional if the download or extraction fails; binary replacement must be atomic.
- **Cross-Platform**: Support Linux, macOS, and Windows (including Windows executable file-locking semantics).
- **Integrity**: Full content verification via `iroh-blobs` blake3 hashes prior to binary substitution.

# Technical Design

### Current Implementation
- `src/bin/ll-tui.rs`: Manages the TUI event loop, listing subscription (`run_listing_watcher`), entry selection, and single-blob downloads using `laplink_p2p::receive::receive_single`.
- `src/listing.rs`: Contains `Listing`, `Entry`, and wire protocols transmitting directory snapshots and live updates along with `server_version`.
- `.github/workflows/release.yml`: Produces release assets named `ll-${RELEASE_VERSION}-${TARGET}.tar.gz` and `.zip` containing `ll`, `ll-serve`, and `ll-tui`.

### Key Decisions
1. **Interactive In-App Replacement vs Silent Auto-Update**:
   - *Chosen*: Interactive TUI banner with explicit user trigger (`u` key).
   - *Rationale*: Prevents unexpected process modifications during active browsing and file transfers while giving the user full visibility and control.
2. **Asset Packaging Support (Archives & Binaries)**:
   - *Chosen*: Support official release archives (`.tar.gz`/`.zip`) with fallback to standalone binaries.
   - *Rationale*: Matches the existing CI release pipeline for full-suite updates (`ll`, `ll-serve`, `ll-tui`) while also supporting direct single-binary deployments.
3. **Binary Replacement Strategy**:
   - *Chosen*: Use the audited `self-replace` crate combined with standard decompression crates (`tar`, `flate2`, `zip`).
   - *Rationale*: `self-replace` cleanly abstracts platform differences (e.g., unlinking running binaries on Unix vs renaming aside on Windows) and prevents runtime crashes from direct overwrite.

### Proposed Architecture & Workflow
```mermaid
graph LR
  ListStream[Listing Updates] --> Detector[src/update.rs Detector]
  Detector --> AppState[App.available_update]
  AppState --> TUIBanner[TUI Header Banner]
  TUIBanner --> KeyPress[User presses 'u']
  KeyPress --> BlobDownloader[receive_single to Temp]
  BlobDownloader --> UnpackStage[Unpack & Verify Hash]
  UnpackStage --> SelfReplace[self_replace Executable]
  SelfReplace --> StatusMsg[Status: Restart ll-tui]
```

### Data Models / Contracts
```rust
// In src/update.rs

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssetKind {
    Archive,
    StandaloneBinary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateCandidate {
    pub version: semver::Version,
    pub entry: crate::listing::Entry,
    pub kind: AssetKind,
}

/// Identifies current platform target string (e.g. "linux-x86_64", "darwin-aarch64", "windows-x86_64")
pub fn current_platform_target() -> &'static str;

/// Parses version and asset kind from a filename if it matches the current platform target
pub fn parse_update_filename(filename: &str, target: &str) -> Option<(semver::Version, AssetKind)>;

/// Inspects a Listing for the newest compatible update candidate
pub fn find_available_update(listing: &crate::listing::Listing, current_ver: &str) -> Option<UpdateCandidate>;

/// Performs in-place replacement of the running executable and sibling binaries
pub fn apply_update(staged_path: &std::path::Path, candidate: &UpdateCandidate) -> anyhow::Result<Vec<std::path::PathBuf>>;
```

### Components & File Structure
- `Cargo.toml`: Add dependencies `semver = "1.0"`, `self-replace = "1.5"`, `tar = "0.4"`, `flate2 = "1.0"`, and `zip = { version = "2", default-features = false, features = ["deflate"] }`.
- `src/update.rs` (New Module): Contains target detection, asset parsing, update discovery, archive extraction, and executable replacement logic.
- `src/lib.rs`: Expose `pub mod update;`.
- `src/bin/ll-tui.rs`:
  - Add `available_update: Option<UpdateCandidate>` to `struct App`.
  - Check for updates on startup and when receiving listing updates.
  - Render update prompt banner in `ui()`.
  - Add `KeyCode::Char('u')` event handler to initiate download and replacement.
- `ROADMAP.md`: Update Section 4 status from planned to implemented.

### Risks & Mitigations
- **Windows File Locking**:
  - *Risk*: Windows denies write access to running executables.
  - *Mitigation*: `self-replace` renames the active `.exe` to a temporary file before placing the new binary, allowing clean replacement while the process continues running.
- **Partial Downloads / Corruption**:
  - *Risk*: Replacing a binary with a broken or partial transfer.
  - *Mitigation*: Downloads go to an isolated staging file; `iroh-blobs` validates the complete blake3 hash before extraction and replacement are executed.
- **Permission Errors**:
  - *Risk*: `ll-tui` installed in a read-only system path (e.g., `/usr/bin/`) lacking write permissions.
  - *Mitigation*: Capture permission errors gracefully, preserve the running binary, and display a clear status message: `error: permission denied updating executable`.

# Testing

### Validation Approach
Automated testing via unit tests in `src/update.rs`, TUI unit tests in `src/bin/ll-tui.rs`, and CLI integration testing in `tests/cli.rs`.

### Key Scenarios
1. **Target String & Filename Parsing**:
   - Verify `current_platform_target()` correctly matches target host.
   - Verify parser identifies `.tar.gz` and `.zip` archives with `vX.Y.Z` and `X.Y.Z` versions.
   - Verify parser identifies standalone binary naming patterns.
2. **Version Comparison**:
   - Verify candidates with newer versions (e.g. `0.30.0` vs `0.29.1-dev`) are flagged as updates.
   - Verify candidates with older or equal versions are ignored.
3. **Archive Extraction & In-Place Replacement**:
   - Verify extracting a `.tar.gz` archive in a test environment correctly extracts binary executables and stages them for replacement.
   - Verify `self_replace` replaces a target test binary and sets executable permissions on Unix.
4. **TUI State & Event Handling**:
   - Verify `App::new()` and `App::update_listing()` populate `available_update`.
   - Verify rendering includes the update prompt banner when an update is present.
   - Verify pressing `'u'` triggers download and replacement flow when idle.

### Integration Tests in `tests/cli.rs`
- `ll_tui_self_update_detection_and_apply`:
  - Spin up `ll-serve` serving a temporary directory with a test release archive tagged with version `99.0.0`.
  - Connect client, verify listing contains the update candidate, and simulate the update execution verifying staged output and replacement validation.

# Delivery Steps

### ✓ Step 1: Implement update discovery and version parsing logic in src/update.rs
The core library exposes target-detection, filename-parsing, and semver-comparison logic to identify newer update assets from directory listings.

- Add `semver` dependency to `Cargo.toml`.
- Create `src/update.rs` with `current_platform_target()` mapping host OS and architecture to release target strings (`linux-x86_64`, `linux-aarch64`, `darwin-x86_64`, `darwin-aarch64`, `windows-x86_64`).
- Implement `parse_update_candidate()` to recognize both release archives (`ll-vX.Y.Z-target.tar.gz`/`.zip`) and standalone binaries (`ll-tui`, `ll-tui-vX.Y.Z-target`, `ll-tui.exe`).
- Implement `find_available_update(listing, current_version)` that filters and selects the highest compatible version newer than the running version.
- Add comprehensive unit tests in `src/update.rs` for target identification, filename pattern matching, and semantic version comparison across release formats.

### ✓ Step 2: Implement archive extraction and atomic binary replacement
The library can safely unpack archives and replace running executables across Linux, macOS, and Windows.

- Add `self-replace`, `tar`, `flate2`, and `zip` dependencies to `Cargo.toml`.
- Implement `extract_and_replace_suite()` in `src/update.rs` to extract `.tar.gz` and `.zip` archives into a temporary directory and replace sibling binaries (`ll`, `ll-serve`, `ll-tui`) located in the same directory as `std::env::current_exe()`.
- Implement standalone binary replacement using `self_replace::self_replace` to safely swap the running executable in-place without file locking conflicts.
- Implement cleanup routines that remove staging artifacts and temporary files upon completion or failure.
- Add unit tests validating archive extraction, binary staging, and executable replacement on temporary test files.

### ✓ Step 3: Integrate update detection, UI prompts, and download handling into ll-tui
The TUI displays update availability, prompts the user, and executes the in-place self-update upon keypress.

- Extend `App` in `src/bin/ll-tui.rs` to store detected `available_update: Option<UpdateCandidate>`.
- Hook `find_available_update()` into `App::new()` and `App::update_listing()` so update status refreshes dynamically when receiving initial listings or live subscription updates.
- Update `ui()` in `src/bin/ll-tui.rs` to render an update banner in the header or status bar (e.g. `[Update available: vX.Y.Z | Press 'u' to update]`).
- Add KeyCode `'u'` handler to initiate the update process: downloads the candidate blob via `receive_single`, invokes `src/update.rs` replacement routines, and updates the status line with download progress and restart instructions.
- Add TUI unit tests verifying update banner rendering, state transitions, and keypress handling.

### ✓ Step 4: Add CLI integration tests and update roadmap documentation
An end-to-end integration test verifies that ll-tui detects, downloads, and applies updates served by ll-serve.

- Add integration test `ll_tui_self_update_detection_and_apply` in `tests/cli.rs`.
- Construct a mock served release directory containing a test archive / dummy executable tagged with a higher version (e.g., `99.0.0`).
- Start `ll-serve` on the directory and verify client listing discovery recognizes the higher version candidate.
- Test download and staging of the update blob, validating integrity and executable replacement workflow.
- Update `ROADMAP.md` marking Item 4 (Binary Auto-Updates in `ll-tui` via `ll-serve`) as implemented.