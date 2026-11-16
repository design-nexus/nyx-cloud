//! The password that encrypts Cloud's own rclone.conf, kept in the Secret Service: the
//! file on disk holds no readable passwords or tokens, and a copy of ~/.config without
//! the keyring opens nothing.

use anyhow::Result;
use secret_service::{EncryptionType, SecretService};
use std::collections::HashMap;

const LABEL: &str = "Cloud Storage configuration";

fn attributes() -> HashMap<&'static str, &'static str> {
    HashMap::from([("application", "nyx-cloud"), ("kind", "rclone-config")])
}

/// What the keyring has for the configuration.
pub enum Stored {
    Password(String),
    /// The keyring answered and holds no password for it.
    Missing,
}

/// The saved password, or [`Stored::Missing`]. An error means the keyring couldn't be asked
/// (not running yet, or its unlock was dismissed), which says nothing about the item.
pub async fn stored_password() -> Result<Stored> {
    let service = SecretService::connect(EncryptionType::Dh).await?;
    let found = service.search_items(attributes()).await?;
    for item in found.unlocked.iter().chain(found.locked.iter()) {
        if item.is_locked().await? {
            item.unlock().await?;
        }
        let secret = item.get_secret().await?;
        if let Ok(text) = String::from_utf8(secret) {
            if !text.is_empty() {
                return Ok(Stored::Password(text));
            }
        }
    }
    Ok(Stored::Missing)
}

/// A new password, saved in place of any old one.
pub async fn new_password() -> Result<String> {
    let service = SecretService::connect(EncryptionType::Dh).await?;
    for item in service.search_items(attributes()).await?.unlocked {
        let _ = item.delete().await;
    }
    let mut bytes = [0u8; 32];
    std::io::Read::read_exact(&mut std::fs::File::open("/dev/urandom")?, &mut bytes)?;
    let password: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    let collection = service.get_default_collection().await?;
    if collection.is_locked().await? {
        collection.unlock().await?;
    }
    collection.create_item(LABEL, attributes(), password.as_bytes(), true, "text/plain").await?;
    Ok(password)
}
