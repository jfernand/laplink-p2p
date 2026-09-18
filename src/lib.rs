//! Library backing the `ll`, `ll-serve`, and `ll-tui` binaries.
//!
//! This crate is deliberately progress-agnostic: none of its public API depends on
//! `indicatif` or any other presentation layer. Callers that want progress bars drive the
//! plain `iroh_blobs` progress-item streams (or the small `ReceiveProgress` enum in
//! [`receive`]) themselves — see `src/bin/ll.rs` for the indicatif-backed example.

pub mod args;
pub mod endpoint;
pub mod listing;
pub mod monitor;
pub mod paths;
pub mod receive;
pub mod secret;
pub mod send;
pub mod ticket_storage;
pub mod transfer;

pub use args::{apply_options, print_hash, AddrInfoOptions, Format, RelayModeOption};
pub use secret::get_or_create_secret;
