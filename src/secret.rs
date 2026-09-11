//! Secret key handling shared by all binaries.

use std::str::FromStr;

use anyhow::Context;
use iroh::SecretKey;

/// Get the secret key from the `IROH_SECRET` environment variable, or generate a new one.
///
/// Returns the key plus whether it was freshly generated (as opposed to read from the
/// environment), so callers can decide whether/how to surface it to the user.
pub fn get_or_create_secret() -> anyhow::Result<(SecretKey, bool)> {
    match std::env::var("IROH_SECRET") {
        Ok(secret) => Ok((
            SecretKey::from_str(&secret).context("invalid secret")?,
            false,
        )),
        Err(_) => Ok((SecretKey::generate(), true)),
    }
}
