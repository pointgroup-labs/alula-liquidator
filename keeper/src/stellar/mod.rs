//! Stellar / Soroban adapter.
//!
//! `Gateway` is the single concrete adapter struct. Strategies receive it as
//! `Arc<Gateway>` and rely on its trait impls (`ChainReader`, `OpBuilder`,
//! `BatchSimulator`, `EventCodec`) — never on its inherent surface.

pub mod xdr_codec; // pub: simulation + event_decode share helpers

mod client;
mod event_decode;
mod simulation;
mod tx_builder;

use {
    ed25519_dalek::SigningKey,
    std::sync::Arc,
    stellar_rpc_client::Client,
    stellar_xdr::curr::{AccountId, PublicKey, ScAddress, Uint256},
};

pub struct Gateway {
    pub(crate) rpc: Arc<Client>,
    pub(crate) source_account: String,
}

impl Gateway {
    pub fn new(rpc_url: &url::Url, source_account: String) -> anyhow::Result<Self> {
        Ok(Self {
            rpc: Arc::new(Client::new(rpc_url.as_str())?),
            source_account,
        })
    }

    /// Shared handle to the underlying RPC client. Adapters (executor,
    /// collectors) clone this `Arc` rather than constructing their own.
    pub fn rpc(&self) -> &Arc<Client> {
        &self.rpc
    }
}

/// Encode an ed25519 signing key's verifying key as a Stellar `G…` strkey.
pub fn pubkey_to_strkey(skey: &SigningKey) -> String {
    ScAddress::Account(AccountId(PublicKey::PublicKeyTypeEd25519(Uint256(
        skey.verifying_key().to_bytes(),
    ))))
    .to_string()
}
