//! Shared endpoint construction, used by send/receive/serve/tui.

use std::net::{SocketAddrV4, SocketAddrV6};

use iroh::{
    address_lookup::{DnsAddressLookup, PkarrPublisher},
    endpoint::presets,
    Endpoint, SecretKey,
};

use crate::args::RelayModeOption;

/// Configuration for building an [`Endpoint`].
pub struct EndpointConfig {
    pub secret_key: SecretKey,
    /// ALPNs this endpoint accepts inbound connections for. Empty for client-only endpoints.
    pub alpns: Vec<Vec<u8>>,
    pub relay: RelayModeOption,
    pub magic_ipv4_addr: Option<SocketAddrV4>,
    pub magic_ipv6_addr: Option<SocketAddrV6>,
    /// Publish this endpoint's address via pkarr/DNS, so it can be found by node id alone.
    /// Used on the sending/serving side.
    pub publish_addr: bool,
    /// Look addresses up via DNS when connecting to a peer whose ticket carries no direct
    /// addresses or relay url. Used on the receiving/browsing side.
    pub lookup_by_dns: bool,
}

/// Build an [`Endpoint`] from an [`EndpointConfig`].
pub async fn build_endpoint(cfg: EndpointConfig) -> anyhow::Result<Endpoint> {
    let mut builder = Endpoint::builder(presets::Minimal)
        .alpns(cfg.alpns)
        .secret_key(cfg.secret_key)
        .relay_mode(cfg.relay.into());
    if cfg.publish_addr {
        builder = builder.address_lookup(PkarrPublisher::n0_dns());
    }
    if cfg.lookup_by_dns {
        builder = builder.address_lookup(DnsAddressLookup::n0_dns());
    }
    if let Some(addr) = cfg.magic_ipv4_addr {
        builder = builder.bind_addr(addr)?;
    }
    if let Some(addr) = cfg.magic_ipv6_addr {
        builder = builder.bind_addr(addr)?;
    }
    Ok(builder.bind().await?)
}
