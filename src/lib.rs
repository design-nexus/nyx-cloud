//! Public Cloud1 metadata and asynchronous session-bus client. No credential fields.
use serde::{Deserialize, Serialize};

pub const BUS: &str = "io.github.design_nexus.nyx.Cloud1";
pub const PATH: &str = "/io/github/design_nexus/nyx/Cloud1";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Preferences {
    pub mount_on_login: bool,
    pub cache_mib: u64,
    /// rclone bandwidth specification; empty means unlimited.
    pub bandwidth: String,
}
impl Default for Preferences {
    fn default() -> Self {
        Self {
            mount_on_login: false,
            cache_mib: 2048,
            bandwidth: String::new(),
        }
    }
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NotesPreferences {
    pub enabled: bool,
    pub vault: String,
    pub folder: String,
}
fn enabled_by_default() -> bool {
    true
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Remote {
    #[serde(default)]
    pub native_account: bool,
    pub id: String,
    pub name: String,
    pub provider: String,
    pub remote_name: String,
    pub adopted: bool,
    pub managed: bool,
    pub connection: String,
    pub mount_status: String,
    pub mount_path: String,
    pub error: String,
    pub preferences: Preferences,
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(default = "enabled_by_default")]
    pub storage_enabled: bool,
    #[serde(default)]
    pub notes: NotesPreferences,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Discovery {
    pub name: String,
    pub provider: String,
}

#[zbus::proxy(
    interface = "io.github.design_nexus.nyx.Cloud1",
    default_service = "io.github.design_nexus.nyx.Cloud1",
    default_path = "/io/github/design_nexus/nyx/Cloud1"
)]
pub trait Cloud {
    fn cancel(&self, operation: &str) -> zbus::Result<()>;
    fn link_native_account(&self, account_id: &str) -> zbus::Result<String>;
    fn list_remotes(&self) -> zbus::Result<String>;
    fn discover(&self) -> zbus::Result<String>;
    fn adopt(&self, name: &str) -> zbus::Result<String>;
    fn import_remote(&self, name: &str) -> zbus::Result<String>;
    fn configure_services(
        &self,
        id: &str,
        account_id: &str,
        storage: bool,
        notes: &str,
    ) -> zbus::Result<()>;
    fn begin_notes(&self, id: &str) -> zbus::Result<String>;
    fn mount(&self, id: &str) -> zbus::Result<String>;
    fn unmount(&self, id: &str) -> zbus::Result<String>;
    fn test_connection(&self, id: &str) -> zbus::Result<String>;
    fn update_preferences(&self, id: &str, name: &str, preferences: &str) -> zbus::Result<()>;
    fn remove(&self, id: &str, delete_configuration: bool) -> zbus::Result<()>;
    fn begin_setup(&self) -> zbus::Result<String>;
    #[zbus(signal)]
    fn changed(&self) -> zbus::Result<()>;
    #[zbus(signal)]
    fn operation_finished(
        &self,
        operation: &str,
        id: &str,
        success: bool,
        message: &str,
    ) -> zbus::Result<()>;
}

/// Blocking adapter intended for application worker threads, never GTK's main thread.
pub struct Client {
    runtime: tokio::runtime::Runtime,
}
impl Client {
    pub fn connect() -> anyhow::Result<Self> {
        Ok(Self {
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?,
        })
    }
    pub fn link_native_account(&self, account_id: &str) -> anyhow::Result<String> {
        self.runtime.block_on(async {
            let connection=zbus::Connection::session().await?;
            Ok(CloudProxy::new(&connection).await?.link_native_account(account_id).await?)
        })
    }
    pub fn list(&self) -> anyhow::Result<Vec<Remote>> {
        self.runtime.block_on(async {
            let c = zbus::Connection::session().await?;
            let p = CloudProxy::new(&c).await?;
            Ok(serde_json::from_str(&p.list_remotes().await?)?)
        })
    }
    pub fn discover(&self) -> anyhow::Result<Vec<Discovery>> {
        self.runtime.block_on(async {
            let c = zbus::Connection::session().await?;
            let p = CloudProxy::new(&c).await?;
            Ok(serde_json::from_str(&p.discover().await?)?)
        })
    }
    pub fn adopt(&self, name: &str) -> anyhow::Result<String> {
        self.runtime.block_on(async {
            let c = zbus::Connection::session().await?;
            Ok(CloudProxy::new(&c).await?.adopt(name).await?)
        })
    }
    /// Copy authorization into Cloud's private config; leave the external config intact.
    pub fn import_remote(&self, name: &str) -> anyhow::Result<String> {
        self.runtime.block_on(async {
            let c = zbus::Connection::session().await?;
            Ok(CloudProxy::new(&c).await?.import_remote(name).await?)
        })
    }
    pub fn configure_services(
        &self,
        id: &str,
        account_id: Option<&str>,
        storage: bool,
        notes: &NotesPreferences,
    ) -> anyhow::Result<()> {
        let notes = serde_json::to_string(notes)?;
        self.runtime.block_on(async {
            let c = zbus::Connection::session().await?;
            Ok(CloudProxy::new(&c)
                .await?
                .configure_services(id, account_id.unwrap_or(""), storage, &notes)
                .await?)
        })
    }
    pub fn notes_session(&self, id: &str) -> anyhow::Result<SetupSession> {
        let path = self.runtime.block_on(async {
            let c = zbus::Connection::session().await?;
            anyhow::Ok(CloudProxy::new(&c).await?.begin_notes(id).await?)
        })?;
        let stream = std::os::unix::net::UnixStream::connect(path)?;
        stream.set_read_timeout(Some(std::time::Duration::from_secs(300)))?;
        stream.set_write_timeout(Some(std::time::Duration::from_secs(300)))?;
        Ok(SetupSession(std::io::BufReader::new(stream)))
    }
    pub fn mount(&self, id: &str) -> anyhow::Result<String> {
        self.runtime.block_on(async {
            let c = zbus::Connection::session().await?;
            Ok(CloudProxy::new(&c).await?.mount(id).await?)
        })
    }
    pub fn unmount(&self, id: &str) -> anyhow::Result<String> {
        self.runtime.block_on(async {
            let c = zbus::Connection::session().await?;
            Ok(CloudProxy::new(&c).await?.unmount(id).await?)
        })
    }
    pub fn test(&self, id: &str) -> anyhow::Result<String> {
        self.runtime.block_on(async {
            let c = zbus::Connection::session().await?;
            Ok(CloudProxy::new(&c).await?.test_connection(id).await?)
        })
    }
    pub fn update(&self, id: &str, name: &str, prefs: &Preferences) -> anyhow::Result<()> {
        self.runtime.block_on(async {
            let c = zbus::Connection::session().await?;
            Ok(CloudProxy::new(&c)
                .await?
                .update_preferences(id, name, &serde_json::to_string(prefs)?)
                .await?)
        })
    }
    pub fn remove(&self, id: &str, delete: bool) -> anyhow::Result<()> {
        self.runtime.block_on(async {
            let c = zbus::Connection::session().await?;
            Ok(CloudProxy::new(&c).await?.remove(id, delete).await?)
        })
    }
    pub fn setup(&self) -> anyhow::Result<SetupSession> {
        let path = self.runtime.block_on(async {
            let c = zbus::Connection::session().await?;
            Ok::<_, anyhow::Error>(CloudProxy::new(&c).await?.begin_setup().await?)
        })?;
        let stream = std::os::unix::net::UnixStream::connect(path)?;
        stream.set_read_timeout(Some(std::time::Duration::from_secs(900)))?;
        Ok(SetupSession(std::io::BufReader::new(stream)))
    }
}
pub struct SetupSession(std::io::BufReader<std::os::unix::net::UnixStream>);
impl SetupSession {
    /// A separate handle lets UI cancellation interrupt an in-flight setup read.
    pub fn cancellation_handle(&self) -> std::io::Result<std::os::unix::net::UnixStream> {
        self.0.get_ref().try_clone()
    }

    /// Requests and replies are deliberately never logged or included in error messages.
    pub fn request(&mut self, request: serde_json::Value) -> anyhow::Result<serde_json::Value> {
        use std::io::{BufRead, Write};
        serde_json::to_writer(self.0.get_mut(), &request)?;
        self.0.get_mut().write_all(b"\n")?;
        let mut line = String::new();
        self.0.read_line(&mut line)?;
        let response: serde_json::Value = serde_json::from_str(&line).map_err(|_| {
            anyhow::anyhow!("Cloud request stopped. Check the account connection and retry.")
        })?;
        if response.get("error").is_some() {
            anyhow::bail!(
                "Cloud request failed. Check account details, connection, and authorization."
            );
        }
        Ok(response)
    }
}
