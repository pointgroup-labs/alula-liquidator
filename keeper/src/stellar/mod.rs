//! Stellar / Soroban adapter.

pub mod errors;
pub mod xdr_codec;

mod client;
mod event_decode;
mod simulation;
mod tx_builder;

use {
    ed25519_dalek::SigningKey,
    stellar_rpc_client::Client,
    stellar_xdr::curr::{AccountId, PublicKey, ScAddress, Uint256},
};

pub struct Gateway {
    pub(crate) rpc: Client,
    pub(crate) source_account: String,
}

impl Gateway {
    pub fn new(rpc_url: &url::Url, source_account: String) -> anyhow::Result<Self> {
        Ok(Self {
            rpc: Client::new(rpc_url.as_str())?,
            source_account,
        })
    }
}

/// Encode an ed25519 signing key's verifying key as a Stellar `G…` strkey.
pub fn pubkey_to_strkey(skey: &SigningKey) -> String {
    ScAddress::Account(AccountId(PublicKey::PublicKeyTypeEd25519(Uint256(
        skey.verifying_key().to_bytes(),
    ))))
    .to_string()
}
