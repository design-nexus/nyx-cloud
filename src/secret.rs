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

/// The password, made and saved the first time.
pub async fn config_password() -> Result<String> {
    let service = SecretService::connect(EncryptionType::Dh).await?;
    let found = service.search_items(attributes()).await?;
    for item in found.unlocked.iter().chain(found.locked.iter()) {
        if item.is_locked().await? {
            item.unlock().await?;
        }
        let secret = item.get_secret().await?;
        if let Ok(text) = String::from_utf8(secret) {
            if !text.is_empty() {
                return Ok(text);
            }
        }
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
