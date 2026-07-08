//! Stellar/Soroban adapter.

pub mod client;
pub mod errors;
pub mod xdr_codec;

mod event_decode;
mod simulation;
mod tx_builder;

use {
    ed25519_dalek::SigningKey,
    stellar_xdr::{AccountId, PublicKey, ScAddress, Uint256},
};

/// Encode an ed25519 signing key as a Stellar `G…` strkey.
pub fn pubkey_to_strkey(skey: &SigningKey) -> String {
    let public_key = PublicKey::PublicKeyTypeEd25519(Uint256(skey.verifying_key().to_bytes()));

    ScAddress::Account(AccountId(public_key)).to_string()
}
