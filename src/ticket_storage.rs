//! Persistent storage for tickets across sessions.
//!
//! Provides cross-platform remembering of tickets for `ll` and `ll-tui`,
//! as well as per-folder ticket and secret key persistence for `ll-serve`.

use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::Context;
use iroh::SecretKey;
use iroh_blobs::ticket::BlobTicket;
use iroh_tickets::endpoint::EndpointTicket;

/// Returns the configuration directory used for storing cross-platform persistent state.
///
/// Priority:
/// 1. `LAPLINK_CONFIG_DIR` environment variable (if set).
/// 2. If running under `cargo test` (any `CARGO_BIN_EXE_*` variable is set), use a temp directory
///    to guarantee tests never pollute the user's normal binary configuration.
/// 3. The cross-platform user config directory via `dirs::config_dir().join("laplink-p2p")`.
/// 4. Fallback to `std::env::temp_dir().join("laplink-p2p")`.
pub fn config_dir() -> anyhow::Result<PathBuf> {
    if let Some(dir) = std::env::var_os("LAPLINK_CONFIG_DIR") {
        let path = PathBuf::from(dir);
        std::fs::create_dir_all(&path)?;
        return Ok(path);
    }

    if std::env::vars().any(|(k, _)| k.starts_with("CARGO_BIN_EXE_")) {
        let test_dir = std::env::temp_dir().join("laplink-test-isolated");
        std::fs::create_dir_all(&test_dir)?;
        return Ok(test_dir);
    }

    if let Some(dir) = dirs::config_dir() {
        let path = dir.join("laplink-p2p");
        std::fs::create_dir_all(&path)?;
        return Ok(path);
    }

    let fallback = std::env::temp_dir().join("laplink-p2p");
    std::fs::create_dir_all(&fallback)?;
    Ok(fallback)
}

/// Save the last ticket given to `ll` (`BlobTicket`).
pub fn save_last_ll_ticket(ticket: &BlobTicket) -> anyhow::Result<()> {
    let dir = config_dir()?;
    let path = dir.join("last_ll_ticket");
    std::fs::write(&path, ticket.to_string())
        .with_context(|| format!("failed to write last ll ticket to {}", path.display()))?;
    Ok(())
}

/// Load the last ticket given to `ll` (`BlobTicket`).
pub fn load_last_ll_ticket() -> anyhow::Result<Option<BlobTicket>> {
    let dir = config_dir()?;
    let path = dir.join("last_ll_ticket");
    if !path.exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read last ll ticket from {}", path.display()))?;
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let ticket = BlobTicket::from_str(trimmed)
        .with_context(|| format!("invalid ticket stored in {}", path.display()))?;
    Ok(Some(ticket))
}

/// Save the last ticket given to `ll-tui` (`EndpointTicket`).
pub fn save_last_tui_ticket(ticket: &EndpointTicket) -> anyhow::Result<()> {
    let dir = config_dir()?;
    let path = dir.join("last_tui_ticket");
    std::fs::write(&path, ticket.to_string())
        .with_context(|| format!("failed to write last tui ticket to {}", path.display()))?;
    Ok(())
}

/// Load the last ticket given to `ll-tui` (`EndpointTicket`).
///
/// First checks the global config directory. If none is found, also checks
/// if the current directory contains a `.ll-serve-store/ticket`.
pub fn load_last_tui_ticket() -> anyhow::Result<Option<EndpointTicket>> {
    let dir = config_dir()?;
    let path = dir.join("last_tui_ticket");
    if path.exists() {
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read last tui ticket from {}", path.display()))?;
        let trimmed = content.trim();
        if !trimmed.is_empty() {
            let ticket = EndpointTicket::from_str(trimmed)
                .with_context(|| format!("invalid ticket stored in {}", path.display()))?;
            return Ok(Some(ticket));
        }
    }

    if let Ok(cwd) = std::env::current_dir() {
        let local_ticket = cwd
            .join(".ll-serve-store")
            .join("ticket");
        if local_ticket.exists() {
            if let Ok(content) = std::fs::read_to_string(&local_ticket) {
                let trimmed = content.trim();
                if let Ok(ticket) = EndpointTicket::from_str(trimmed) {
                    return Ok(Some(ticket));
                }
            }
        }
    }

    Ok(None)
}

/// Save the ticket for a served folder (`EndpointTicket`) inside its `store_dir`.
pub fn save_serve_ticket(store_dir: &Path, ticket: &EndpointTicket) -> anyhow::Result<()> {
    std::fs::create_dir_all(store_dir)?;
    let path = store_dir.join("ticket");
    std::fs::write(&path, ticket.to_string())
        .with_context(|| format!("failed to write serve ticket to {}", path.display()))?;
    Ok(())
}

/// Load the ticket for a served folder from its `store_dir`.
pub fn load_serve_ticket(store_dir: &Path) -> anyhow::Result<Option<EndpointTicket>> {
    let path = store_dir.join("ticket");
    if !path.exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read serve ticket from {}", path.display()))?;
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let ticket = EndpointTicket::from_str(trimmed)
        .with_context(|| format!("invalid ticket stored in {}", path.display()))?;
    Ok(Some(ticket))
}

/// Get or create the secret key for `ll-serve` on a specific folder.
///
/// If `IROH_SECRET` is set in the environment, that takes precedence.
/// Otherwise, checks `store_dir/secret_key`. If it exists, loads it;
/// if not, generates a new one and persists it to `store_dir/secret_key`.
pub fn get_or_create_serve_secret(store_dir: &Path) -> anyhow::Result<(SecretKey, bool)> {
    if let Ok(secret) = std::env::var("IROH_SECRET") {
        return Ok((
            SecretKey::from_str(&secret).context("invalid secret in IROH_SECRET")?,
            false,
        ));
    }

    let key_path = store_dir.join("secret_key");
    if key_path.exists() {
        let content = std::fs::read_to_string(&key_path)
            .with_context(|| format!("failed to read secret key from {}", key_path.display()))?;
        let trimmed = content.trim();
        if !trimmed.is_empty() {
            let key = SecretKey::from_str(trimmed)
                .with_context(|| format!("invalid secret key in {}", key_path.display()))?;
            return Ok((key, false));
        }
    }

    let key = SecretKey::generate();
    std::fs::create_dir_all(store_dir)?;
    std::fs::write(&key_path, hex::encode(key.to_bytes()))
        .with_context(|| format!("failed to write secret key to {}", key_path.display()))?;
    Ok((key, true))
}

#[cfg(test)]
mod tests {
    use super::*;

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn test_ll_ticket_save_load() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap();
        let temp = tempfile::tempdir().unwrap();
        unsafe { std::env::set_var("LAPLINK_CONFIG_DIR", temp.path()); }

        assert_eq!(load_last_ll_ticket().unwrap(), None);

        let key = SecretKey::generate();
        let addr = iroh::EndpointAddr::from(key.public());
        let hash = iroh_blobs::Hash::new([1u8; 32]);
        let ticket = BlobTicket::new(addr, hash, iroh_blobs::BlobFormat::Raw);

        save_last_ll_ticket(&ticket).unwrap();
        let loaded = load_last_ll_ticket()
            .unwrap()
            .unwrap();
        assert_eq!(loaded.to_string(), ticket.to_string());
        unsafe { std::env::remove_var("LAPLINK_CONFIG_DIR"); }
    }

    #[test]
    fn test_tui_ticket_save_load() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap();
        let temp = tempfile::tempdir().unwrap();
        unsafe { std::env::set_var("LAPLINK_CONFIG_DIR", temp.path()); }

        assert_eq!(load_last_tui_ticket().unwrap(), None);

        let key = SecretKey::generate();
        let addr = iroh::EndpointAddr::from(key.public());
        let ticket = EndpointTicket::new(addr);

        save_last_tui_ticket(&ticket).unwrap();
        let loaded = load_last_tui_ticket()
            .unwrap()
            .unwrap();
        assert_eq!(loaded.to_string(), ticket.to_string());
        unsafe { std::env::remove_var("LAPLINK_CONFIG_DIR"); }
    }

    #[test]
    fn test_serve_ticket_and_secret_per_folder() {
        let folder_a = tempfile::tempdir().unwrap();
        let store_a = folder_a
            .path()
            .join(".ll-serve-store");

        let folder_b = tempfile::tempdir().unwrap();
        let store_b = folder_b
            .path()
            .join(".ll-serve-store");

        let (secret_a, gen_a) = get_or_create_serve_secret(&store_a).unwrap();
        assert!(gen_a);
        let (secret_a_cached, gen_a_2) = get_or_create_serve_secret(&store_a).unwrap();
        assert!(!gen_a_2);
        assert_eq!(secret_a.to_bytes(), secret_a_cached.to_bytes());

        let (secret_b, gen_b) = get_or_create_serve_secret(&store_b).unwrap();
        assert!(gen_b);
        assert_ne!(secret_a.to_bytes(), secret_b.to_bytes());

        let addr_a = iroh::EndpointAddr::from(secret_a.public());
        let ticket_a = EndpointTicket::new(addr_a);
        save_serve_ticket(&store_a, &ticket_a).unwrap();

        let addr_b = iroh::EndpointAddr::from(secret_b.public());
        let ticket_b = EndpointTicket::new(addr_b);
        save_serve_ticket(&store_b, &ticket_b).unwrap();

        assert_eq!(
            load_serve_ticket(&store_a)
                .unwrap()
                .unwrap()
                .to_string(),
            ticket_a.to_string()
        );
        assert_eq!(
            load_serve_ticket(&store_b)
                .unwrap()
                .unwrap()
                .to_string(),
            ticket_b.to_string()
        );
    }
}
