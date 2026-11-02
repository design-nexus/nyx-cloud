mod native;
use anyhow::{Context, Result, bail};
use nyx_cloud::{BUS, Discovery, NotesPreferences, PATH, Preferences, Remote};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    sync::Mutex,
};
use zbus::{interface, object_server::SignalEmitter};

fn home() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .expect("HOME must be set")
}
fn xdg(key: &str, fallback: &str) -> PathBuf {
    std::env::var_os(key)
        .map(PathBuf::from)
        .unwrap_or_else(|| home().join(fallback))
}
fn config_dir() -> PathBuf {
    xdg("XDG_CONFIG_HOME", ".config").join("nyx/cloud")
}
fn config() -> PathBuf {
    config_dir().join("rclone.conf")
}
fn legacy_mounts() -> PathBuf {
    xdg("XDG_DATA_HOME", ".local/share").join("nyx/cloud/mounts")
}
fn mount_folder(name: &str) -> String {
    let folder: String = name
        .trim()
        .chars()
        .map(|c| if c == '/' || c.is_control() { '-' } else { c })
        .collect();
    if folder.is_empty() || folder.starts_with('.') {
        "Cloud".into()
    } else {
        folder
    }
}
// Earlier accounts keep their folder; later duplicates get " (2)", " (3)", ...
fn assign_mount_path(remotes: &mut [Remote], id: &str) {
    let Some(index) = remotes.iter().position(|r| r.id == id) else {
        return;
    };
    let folder = mount_folder(&remotes[index].name);
    let taken: HashSet<&str> = remotes
        .iter()
        .filter(|r| r.id != id)
        .map(|r| r.mount_path.as_str())
        .collect();
    let path = (1..)
        .map(|n| match n {
            1 => folder.clone(),
            n => format!("{folder} ({n})"),
        })
        .map(|f| home().join(f).to_string_lossy().into_owned())
        .find(|p| !taken.contains(p.as_str()))
        .expect("unbounded suffixes");
    remotes[index].mount_path = path;
}
fn mount_path_safe(path: &Path) -> bool {
    path.parent() == Some(home().as_path())
        && path
            .file_name()
            .is_some_and(|n| !n.as_encoded_bytes().starts_with(b"."))
}
fn mount_listed(mountinfo: &str, path: &str) -> bool {
    let escaped = path
        .replace('\\', "\\134")
        .replace(' ', "\\040")
        .replace('\t', "\\011")
        .replace('\n', "\\012");
    mountinfo
        .lines()
        .any(|line| line.split_whitespace().nth(4) == Some(escaped.as_str()))
}
fn cache(id: &str) -> PathBuf {
    xdg("XDG_CACHE_HOME", ".cache").join("nyx/cloud").join(id)
}
fn private_dir(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)?;
    let meta = std::fs::symlink_metadata(path)?;
    use std::os::unix::fs::MetadataExt;
    let uid = std::fs::metadata(home())?.uid();
    if !meta.is_dir() || meta.uid() != uid {
        bail!("Unsafe Cloud directory");
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    Ok(())
}
fn id_valid(id: &str) -> bool {
    uuid::Uuid::parse_str(id).is_ok()
}
fn external_config() -> Option<PathBuf> {
    let output = std::process::Command::new("rclone")
        .args(["config", "file"])
        .output()
        .ok()?;
    String::from_utf8(output.stdout)
        .ok()?
        .lines()
        .last()
        .map(PathBuf::from)
}
#[derive(Default, Serialize, Deserialize)]
struct Saved {
    remotes: Vec<Remote>,
}
struct State {
    broker_port: u16,
    broker_grants: Mutex<HashMap<String,String>>,
    version: String,
    setup_gate: Mutex<()>,
    remotes: Mutex<Vec<Remote>>,
    children: Mutex<HashMap<String, Child>>,
    busy: Mutex<HashSet<String>>,
    rc: reqwest::Client,
    connection: zbus::Connection,
    tasks: Mutex<HashMap<String, (String, &'static str, tokio::task::AbortHandle)>>,
    desired_mounts: Mutex<HashSet<String>>,
}
impl State {
    async fn notify(&self) {
        if let Ok(e) = SignalEmitter::new(&self.connection, PATH) {
            let _ = Cloud::changed(&e).await;
        }
    }
    async fn save(&self) -> Result<()> {
        let remotes = self.remotes.lock().await;
        let bytes = serde_json::to_vec_pretty(&Saved {
            remotes: remotes.clone(),
        })?;
        let tmp = config_dir().join("accounts.json.tmp");
        tokio::fs::write(&tmp, bytes).await?;
        tokio::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600)).await?;
        tokio::fs::rename(tmp, config_dir().join("accounts.json")).await?;
        Ok(())
    }
    async fn get(&self, id: &str) -> Result<Remote> {
        if !id_valid(id) {
            bail!("Invalid account identifier");
        }
        self.remotes
            .lock()
            .await
            .iter()
            .find(|r| r.id == id)
            .cloned()
            .context("Account no longer exists")
    }
    // A live mount keeps its folder; a rename takes effect at the next mount.
    async fn refresh_mount_path(&self, id: &str) -> Result<Remote> {
        let r = self.get(id).await?;
        if mounted(&r.mount_path).await {
            return Ok(r);
        }
        assign_mount_path(&mut self.remotes.lock().await, id);
        self.get(id).await
    }
    async fn status(&self, id: &str, connection: &str, mount: &str, error: &str) {
        if let Some(r) = self.remotes.lock().await.iter_mut().find(|r| r.id == id) {
            r.connection = connection.into();
            r.mount_status = mount.into();
            r.error = error.into();
        }
        self.notify().await;
    }
    async fn command(&self, r: &Remote) -> Result<Command> {
        if r.native_account { return native::command(self,r).await; }
        let path = if r.managed {
            config()
        } else {
            external_config().context("rclone configuration unavailable")?
        };
        let mut cmd = Command::new("rclone");
        cmd.arg("--config")
            .arg(path)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped());
        Ok(cmd)
    }
    async fn rc(&self, endpoint: &str, body: Value) -> Result<Value> {
        let response = self
            .rc
            .post(format!("http://localhost/{endpoint}"))
            .json(&body)
            .send()
            .await?;
        if !response.status().is_success() {
            bail!(
                "rclone could not complete configuration; check account details and authorization"
            );
        }
        Ok(response.json().await?)
    }
    async fn discover(&self) -> Result<Vec<Discovery>> {
        let output = Command::new("rclone")
            .args(["listremotes", "--long"])
            .output()
            .await?;
        if !output.status.success() {
            bail!("Unable to read rclone configuration; it may need unlocking");
        }
        Ok(String::from_utf8(output.stdout)?
            .lines()
            .filter_map(|l| {
                let (name, provider) = l.split_once(':')?;
                Some(Discovery {
                    name: name.into(),
                    provider: provider.trim().into(),
                })
            })
            .collect())
    }
    async fn start_mount(&self, id: &str) -> Result<()> {
        let r = self.get(id).await?;
        self.test(id).await?;
        if mounted(&r.mount_path).await && !self.children.lock().await.contains_key(id) {
            let disconnected = std::fs::metadata(&r.mount_path)
                .err()
                .is_some_and(|e| e.raw_os_error() == Some(107));
            if disconnected {
                let status = Command::new("fusermount3")
                    .args(["-u", &r.mount_path])
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status()
                    .await?;
                if !status.success() {
                    bail!("Previous mount is busy. Close open files and retry.");
                }
            }
        }
        if self.children.lock().await.contains_key(id) && mounted(&r.mount_path).await {
            return Ok(());
        }
        let r = self.refresh_mount_path(id).await?;
        let path = PathBuf::from(&r.mount_path);
        if !mount_path_safe(&path) {
            bail!("Unsafe mount path");
        }
        if mounted(&r.mount_path).await {
            bail!("Mountpoint already in use");
        }
        if let Ok(m) = std::fs::symlink_metadata(&path) {
            if !m.is_dir()
                || m.file_type().is_symlink()
                || std::fs::read_dir(&path)?.next().is_some()
            {
                bail!(
                    "~/{} already exists and isn't empty. Rename the account or move that folder.",
                    path.file_name().unwrap_or_default().to_string_lossy()
                );
            }
        }
        private_dir(&path)?;
        private_dir(&cache(id))?;
        self.status(id, "connecting", "mounting", "").await;
        let mut command = self.command(&r).await?;
        command
            .args([
                "mount",
                &format!("{}:", r.remote_name),
                &r.mount_path,
                "--vfs-cache-mode",
                "full",
                "--vfs-cache-max-size",
                &format!("{}M", r.preferences.cache_mib),
                "--cache-dir",
            ])
            .arg(cache(id))
            .args(["--umask", "077", "--log-level", "ERROR"]);
        if !r.preferences.bandwidth.is_empty() {
            command.args(["--bwlimit", &r.preferences.bandwidth]);
        }
        let socket = runtime()?.join(format!("mount-{id}.sock"));
        if socket.exists() {
            tokio::fs::remove_file(&socket).await?;
        }
        command
            .args(["--rc", "--rc-no-auth", "--rc-addr"])
            .arg(format!("unix://{}", socket.display()));
        command
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true);
        let mut child = command.spawn()?;
        for _ in 0..150 {
            if mounted(&r.mount_path).await {
                self.children.lock().await.insert(id.into(), child);
                self.status(id, "connected", "mounted", "").await;
                return Ok(());
            }
            if child.try_wait()?.is_some() {
                bail!("Mount failed. Test the connection or sign in again.");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let _ = child.kill().await;
        bail!("Mount timed out. Check the network and retry.")
    }
    async fn stop_mount(&self, id: &str) -> Result<()> {
        let r = self.get(id).await?;
        // A normal FUSE unmount refuses busy files; never force or lazily detach.
        if mounted(&r.mount_path).await {
            // Pending dirty files cannot safely be inferred from a successful copy.
            let client = reqwest::Client::builder()
                .unix_socket(runtime()?.join(format!("mount-{id}.sock")))
                .timeout(Duration::from_secs(3))
                .build()?;
            let stats: Value = client
                .post("http://localhost/vfs/stats")
                .json(&json!({}))
                .send()
                .await
                .context("Unable to verify upload status; mount left connected")?
                .error_for_status()?
                .json()
                .await?;
            let disk = &stats["diskCache"];
            if ["uploadsInProgress", "uploadsQueued", "erroredFiles"]
                .iter()
                .any(|key| disk[*key].as_u64().unwrap_or(1) > 0)
            {
                bail!(
                    "Files are open or uploads are pending. Close files and wait for uploads before disconnecting."
                );
            }
            let status = Command::new("fusermount3")
                .args(["-u", &r.mount_path])
                .status()
                .await?;
            if !status.success() {
                bail!("Mount is busy. Close open files and retry.");
            }
        }
        if let Some(mut child) = self.children.lock().await.remove(id) {
            if tokio::time::timeout(Duration::from_secs(10), child.wait())
                .await
                .is_err()
            {
                let _ = child.kill().await;
            }
        }
        let _ = std::fs::remove_dir(&r.mount_path);
        self.status(id, "disconnected", "unmounted", "").await;
        Ok(())
    }
    async fn test(&self, id: &str) -> Result<()> {
        let r = self.get(id).await?;
        let mut cmd = self.command(&r).await?;
        cmd.kill_on_drop(true);
        cmd.args([
            "lsjson",
            &format!("{}:", r.remote_name),
            "--max-depth",
            "1",
            "--dirs-only",
            "--contimeout",
            "10s",
            "--timeout",
            "20s",
            "--retries",
            "1",
        ]);
        let output = tokio::time::timeout(Duration::from_secs(35), cmd.output()).await??;
        if !output.status.success() {
            let msg = String::from_utf8_lossy(&output.stderr).to_lowercase();
            let auth = [
                "invalid_grant",
                "unauthorized",
                "401",
                "authentication failed",
                "token expired",
            ]
            .iter()
            .any(|x| msg.contains(x));
            self.status(
                id,
                if auth {
                    "authentication_required"
                } else {
                    "error"
                },
                &r.mount_status,
                if auth {
                    "Sign in again to reconnect."
                } else {
                    "Connection failed. Check the network and account settings."
                },
            )
            .await;
            bail!("Connection test failed; see account status");
        }
        self.status(id, "connected", &r.mount_status, "").await;
        Ok(())
    }
}
async fn mounted(path: &str) -> bool {
    let mountinfo = tokio::fs::read_to_string("/proc/self/mountinfo")
        .await
        .unwrap_or_default();
    mount_listed(&mountinfo, path)
}
struct Cloud(Arc<State>);
fn error(e: impl std::fmt::Display) -> zbus::fdo::Error {
    zbus::fdo::Error::Failed(e.to_string())
}
impl Cloud {
    async fn launch(&self, id: &str, kind: &'static str) -> zbus::fdo::Result<String> {
        self.0.get(id).await.map_err(error)?;
        let already_busy = !self.0.busy.lock().await.insert(id.into());
        if already_busy {
            if kind == "mount" {
                if let Some((operation, _)) =
                    self.0
                        .tasks
                        .lock()
                        .await
                        .iter()
                        .find(|(_, (account, operation_kind, _))| {
                            account == id && *operation_kind == "mount"
                        })
                {
                    return Ok(operation.clone());
                }
            }
            return Err(error("Account operation already in progress"));
        }
        if kind == "mount" {
            self.0.desired_mounts.lock().await.insert(id.into());
        }
        if kind == "unmount" {
            self.0.desired_mounts.lock().await.remove(id);
        }
        let op = uuid::Uuid::new_v4().to_string();
        let task_op = op.clone();
        let account_id = id.to_owned();
        let id = id.to_owned();
        let state = self.0.clone();
        let handle = tokio::spawn(async move {
            let result = match kind {
                "mount" => state.start_mount(&id).await,
                "unmount" => state.stop_mount(&id).await,
                _ => state.test(&id).await,
            };
            let message = result
                .as_ref()
                .err()
                .map(ToString::to_string)
                .unwrap_or_default();
            if result.is_err() && kind == "mount" {
                let remote = state.get(&id).await;
                if let Ok(r) = &remote {
                    let _ = std::fs::remove_dir(&r.mount_path);
                }
                let auth = remote.is_ok_and(|r| r.connection == "authentication_required");
                if !auth {
                    state.status(&id, "error", "unmounted", &message).await;
                }
            }
            state.busy.lock().await.remove(&id);
            state.tasks.lock().await.remove(&task_op);
            if let Ok(e) = SignalEmitter::new(&state.connection, PATH) {
                let _ =
                    Cloud::operation_finished(&e, &task_op, &id, result.is_ok(), &message).await;
            }
            state.notify().await;
        });
        self.0
            .tasks
            .lock()
            .await
            .insert(op.clone(), (account_id, kind, handle.abort_handle()));
        Ok(op)
    }
}
#[interface(name = "io.github.design_nexus.nyx.Cloud1")]
impl Cloud {
    #[zbus(property)]
    fn rclone_version(&self) -> &str {
        &self.0.version
    }
    async fn cancel(&self, operation: &str) -> zbus::fdo::Result<()> {
        let task = self.0.tasks.lock().await.remove(operation);
        if let Some((id, kind, handle)) = task {
            if kind == "mount" {
                self.0.desired_mounts.lock().await.remove(&id);
            }
            handle.abort();
            self.0.busy.lock().await.remove(&id);
            let is_mounted = self.0.get(&id).await.ok().is_some_and(|r| {
                mount_listed(
                    &std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default(),
                    &r.mount_path,
                )
            });
            self.0
                .status(
                    &id,
                    if is_mounted {
                        "connected"
                    } else {
                        "disconnected"
                    },
                    if is_mounted { "mounted" } else { "unmounted" },
                    "Operation cancelled. Any cached data is preserved.",
                )
                .await;
        }
        Ok(())
    }
    async fn link_native_account(&self, account_id: &str) -> zbus::fdo::Result<String> {
        let id=account_id.to_owned();
        let account=tokio::task::spawn_blocking(move|| {
            nyx_accounts_native::Client::connect()?.list()?.into_iter().find(|a|a.id==id).context("Native account not found")
        }).await.map_err(|_|error("Account lookup failed"))?.map_err(|_|error("Native account unavailable"))?;
        if !account.enabled.contains(&nyx_accounts_native::Service::Files){return Err(error("Enable Files in Online Accounts first"));}
        if let Some(existing)=self.0.remotes.lock().await.iter().find(|r|r.account_id.as_deref()==Some(account_id)){return Ok(existing.id.clone());}
        let id=uuid::Uuid::new_v4().to_string();
        let mut remote=new_remote(&id,account.label(),&format!("nyx_{}",id.replace('-',"")),match account.provider.as_str(){"google"=>"drive","microsoft_personal"=>"onedrive",_=>"webdav"},true);
        remote.native_account=true;remote.account_id=Some(account.id);self.0.remotes.lock().await.push(remote);self.0.save().await.map_err(|_|error("Could not save native storage account"))?;self.0.notify().await;Ok(id)
    }
    async fn list_remotes(&self) -> zbus::fdo::Result<String> {
        let snapshot = self.0.remotes.lock().await.clone();
        let mut public = Vec::with_capacity(snapshot.len());
        for mut remote in snapshot {
            if remote.mount_status == "mounted" && !mounted(&remote.mount_path).await {
                remote.mount_status = "unmounted".into();
                remote.connection = "disconnected".into();
            }
            public.push(remote);
        }
        serde_json::to_string(&public).map_err(error)
    }
    async fn discover(&self) -> zbus::fdo::Result<String> {
        serde_json::to_string(&self.0.discover().await.map_err(error)?).map_err(error)
    }
    async fn adopt(&self, name: &str) -> zbus::fdo::Result<String> {
        let d = self
            .0
            .discover()
            .await
            .map_err(error)?
            .into_iter()
            .find(|d| d.name == name)
            .ok_or_else(|| error("Remote not found"))?;
        let mut remotes = self.0.remotes.lock().await;
        if let Some(r) = remotes.iter().find(|r| !r.managed && r.remote_name == name) {
            return Ok(r.id.clone());
        }
        let id = uuid::Uuid::new_v4().to_string();
        remotes.push(new_remote(&id, name, name, &d.provider, false));
        assign_mount_path(&mut remotes, &id);
        drop(remotes);
        self.0.save().await.map_err(error)?;
        self.0.notify().await;
        Ok(id)
    }
    async fn import_remote(&self, name: &str) -> zbus::fdo::Result<String> {
        let _guard = self.0.setup_gate.lock().await;
        if let Some(r) = self
            .0
            .remotes
            .lock()
            .await
            .iter()
            .find(|r| r.remote_name == name || r.name == name)
            .cloned()
        {
            return Ok(r.id);
        }
        let path = external_config().ok_or_else(|| error("rclone configuration unavailable"))?;
        let output = Command::new("rclone")
            .arg("--config")
            .arg(path)
            .args(["config", "dump"])
            .output()
            .await
            .map_err(error)?;
        if !output.status.success() {
            return Err(error("Unable to unlock the existing rclone configuration"));
        }
        let mut dump: Value = serde_json::from_slice(&output.stdout)
            .map_err(|_| error("Invalid rclone configuration"))?;
        let mut params = dump
            .get_mut(name)
            .map(Value::take)
            .ok_or_else(|| error("Remote not found"))?;
        let provider = params["type"]
            .as_str()
            .ok_or_else(|| error("Provider missing"))?
            .to_owned();
        params
            .as_object_mut()
            .ok_or_else(|| error("Invalid remote"))?
            .remove("type");
        let id = uuid::Uuid::new_v4().to_string();
        let remote = format!("nyx_{}", uuid::Uuid::new_v4().simple());
        self.0.rc("config/create", json!({"name":remote,"type":provider,"parameters":params,"opt":{"nonInteractive":true,"obscure":false}})).await.map_err(|_|error("Unable to import authorization"))?;
        {
            let mut remotes = self.0.remotes.lock().await;
            remotes.push(new_remote(&id, name, &remote, &provider, true));
            assign_mount_path(&mut remotes, &id);
        }
        if let Err(e) = self.0.save().await {
            self.0.remotes.lock().await.retain(|r| r.id != id);
            let _ = self.0.rc("config/delete", json!({"name":remote})).await;
            return Err(error(e));
        }
        self.0.notify().await;
        Ok(id)
    }
    async fn configure_services(
        &self,
        id: &str,
        account_id: &str,
        storage: bool,
        notes: &str,
    ) -> zbus::fdo::Result<()> {
        let preferences: NotesPreferences = serde_json::from_str(notes).map_err(error)?;
        if account_id.len() > 256 || account_id.chars().any(char::is_control) {
            return Err(error("Invalid account identifier"));
        }
        if self.0.busy.lock().await.contains(id) {
            return Err(error("Wait for the active connection operation"));
        }
        if preferences.enabled {
            if !Path::new(&preferences.vault).is_absolute()
                || !Path::new(&preferences.vault).is_dir()
            {
                return Err(error("Choose an existing local Notes vault"));
            }
            if !notes_path_valid(&preferences.folder) {
                return Err(error("Choose a dedicated relative Notes folder"));
            }
        }
        let old = self.0.get(id).await.map_err(error)?;
        if !storage {
            self.0.desired_mounts.lock().await.remove(id);
            self.0.stop_mount(id).await.map_err(error)?;
        }
        {
            let mut remotes = self.0.remotes.lock().await;
            if !account_id.is_empty()
                && remotes
                    .iter()
                    .any(|r| r.id != id && r.account_id.as_deref() == Some(account_id))
            {
                return Err(error("This account already has storage linked"));
            }
            if preferences.enabled
                && remotes
                    .iter()
                    .any(|r| r.id != id && r.notes.enabled && r.notes.vault == preferences.vault)
            {
                return Err(error("This vault already has a sync account"));
            }
            let r = remotes
                .iter_mut()
                .find(|r| r.id == id)
                .ok_or_else(|| error("Account removed"))?;
            r.account_id = (!account_id.is_empty()).then(|| account_id.to_owned());
            r.storage_enabled = storage;
            r.notes = preferences;
        }
        if let Err(e) = self.0.save().await {
            if let Some(r) = self.0.remotes.lock().await.iter_mut().find(|r| r.id == id) {
                *r = old;
            }
            return Err(error(e));
        }
        self.0.notify().await;
        Ok(())
    }
    async fn begin_notes(&self, id: &str) -> zbus::fdo::Result<String> {
        let r = self.0.get(id).await.map_err(error)?;
        if !r.notes.enabled {
            return Err(error("Notes is disabled for this account"));
        }
        let path = runtime()
            .map_err(error)?
            .join(format!("notes-{}.sock", uuid::Uuid::new_v4()));
        let listener = tokio::net::UnixListener::bind(&path).map_err(error)?;
        tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .await
            .map_err(error)?;
        let state = self.0.clone();
        let id = id.to_owned();
        let cleanup = path.clone();
        tokio::spawn(async move {
            let _ = notes_session(state, id, listener).await;
            let _ = tokio::fs::remove_file(cleanup).await;
        });
        Ok(path.to_string_lossy().into())
    }
    async fn mount(&self, id: &str) -> zbus::fdo::Result<String> {
        self.launch(id, "mount").await
    }
    async fn unmount(&self, id: &str) -> zbus::fdo::Result<String> {
        self.launch(id, "unmount").await
    }
    async fn test_connection(&self, id: &str) -> zbus::fdo::Result<String> {
        self.launch(id, "test").await
    }
    async fn update_preferences(
        &self,
        id: &str,
        name: &str,
        preferences: &str,
    ) -> zbus::fdo::Result<()> {
        let prefs: Preferences =
            serde_json::from_str(preferences).map_err(|_| error("Invalid preferences"))?;
        if name.trim().is_empty()
            || prefs.cache_mib == 0
            || prefs.bandwidth.len() > 128
            || prefs.bandwidth.contains(['\n', '\r'])
        {
            return Err(error("Invalid account preferences"));
        }
        let mut remotes = self.0.remotes.lock().await;
        let r = remotes
            .iter_mut()
            .find(|r| r.id == id)
            .ok_or_else(|| error("Account not found"))?;
        r.name = name.trim().into();
        r.preferences = prefs;
        drop(remotes);
        self.0.refresh_mount_path(id).await.map_err(error)?;
        self.0.save().await.map_err(error)?;
        self.0.notify().await;
        Ok(())
    }
    async fn remove(&self, id: &str, delete_configuration: bool) -> zbus::fdo::Result<()> {
        let r = self.0.get(id).await.map_err(error)?;
        if self.0.busy.lock().await.contains(id) {
            return Err(error("Wait for the current account operation"));
        }
        self.0.stop_mount(id).await.map_err(error)?;
        if delete_configuration && !r.native_account {
            if !r.managed {
                return Err(error(
                    "External configuration is preserved. Remove it with its original configuration tool.",
                ));
            }
            self.0
                .rc("config/delete", json!({"name": r.remote_name}))
                .await
                .map_err(error)?;
        }
        self.0.desired_mounts.lock().await.remove(id);
        self.0.remotes.lock().await.retain(|r| r.id != id);
        self.0.save().await.map_err(error)?;
        self.0.notify().await;
        Ok(())
    }
    async fn begin_setup(&self) -> zbus::fdo::Result<String> {
        let dir = runtime().map_err(error)?;
        let path = dir.join(format!("setup-{}.sock", uuid::Uuid::new_v4()));
        let listener = tokio::net::UnixListener::bind(&path).map_err(error)?;
        let state = self.0.clone();
        let task_path = path.clone();
        tokio::spawn(async move {
            let _ = setup(state, listener).await;
            let _ = tokio::fs::remove_file(task_path).await;
        });
        Ok(path.to_string_lossy().into())
    }
    #[zbus(signal)]
    async fn changed(emitter: &SignalEmitter<'_>) -> zbus::Result<()>;
    #[zbus(signal)]
    async fn operation_finished(
        emitter: &SignalEmitter<'_>,
        operation: &str,
        id: &str,
        success: bool,
        message: &str,
    ) -> zbus::Result<()>;
}
fn new_remote(id: &str, display: &str, remote: &str, provider: &str, managed: bool) -> Remote {
    Remote {
        native_account: false,
        id: id.into(),
        name: display.into(),
        provider: provider.into(),
        remote_name: remote.into(),
        adopted: !managed,
        managed,
        connection: "disconnected".into(),
        mount_status: "unmounted".into(),
        mount_path: String::new(),
        error: String::new(),
        preferences: Preferences::default(),
        account_id: None,
        storage_enabled: true,
        notes: NotesPreferences::default(),
    }
}
fn runtime() -> Result<PathBuf> {
    let path = PathBuf::from(
        std::env::var_os("XDG_RUNTIME_DIR").context("User runtime directory unavailable")?,
    )
    .join("nyx-cloud");
    private_dir(&path)?;
    Ok(path)
}
// Setup is a single private session. Only configuration operations are allowed.
async fn setup(state: Arc<State>, listener: tokio::net::UnixListener) -> Result<()> {
    let (stream, _) = tokio::time::timeout(Duration::from_secs(900), listener.accept()).await??;
    let _setup_guard = state.setup_gate.lock().await;
    let peer = stream.peer_cred()?;
    use std::os::unix::fs::MetadataExt;
    if peer.uid() != std::fs::metadata(home())?.uid() {
        bail!("Wrong setup user");
    }
    let mut stream = BufReader::new(stream);
    let remote = format!("nyx_{}", uuid::Uuid::new_v4().simple());
    let mut created = false;
    let mut replacement: Option<Remote> = None;
    let result: Result<()> = async {
        loop {
            let mut line = String::new();
            if tokio::time::timeout(Duration::from_secs(900), stream.read_line(&mut line)).await?? == 0 { break; }
            if line.len() > 1024 * 1024 { bail!("Setup request too large"); }
            let request: Value = serde_json::from_str(&line)?;
            let response = match request["action"].as_str().unwrap_or("") {
                "providers" => state.rc("config/providers", json!({})).await?,
                "configure" => {
                    let provider = request["provider"].as_str().context("Provider required")?;
                    let mut parameters = request["parameters"].clone();
                    if !created && let Some(id) = request["replace"].as_str() {
                        let old = state.get(id).await?;
                        if old.native_account { bail!("Reauthenticate this account in Settings Online Accounts"); }
                        if !old.managed { bail!("External credentials are preserved"); }
                        if mounted(&old.mount_path).await { bail!("Disconnect the account before signing in again"); }
                        if !state.busy.lock().await.insert(id.into()) { bail!("Account is busy"); }
                        replacement = Some(old.clone());
                        let previous = state.rc("config/get", json!({"name":old.remote_name})).await?;
                        let mut merged = previous;
                        if let Some(values) = parameters.as_object() { for (key, value) in values { merged[key] = value.clone(); } }
                        if let Some(values) = merged.as_object_mut() { values.remove("type"); }
                        merged["config_refresh_token"] = json!("true");
                        parameters = merged;
                    }
                    let mut body = json!({"name":remote,"type":provider,"parameters":parameters,"opt":{"nonInteractive":true,"obscure":replacement.is_none()}});
                    if let Some(opt) = request.get("opt") { body["opt"] = opt.clone(); body["opt"]["nonInteractive"] = json!(true); }
                    let endpoint = if created { "config/update" } else { "config/create" };
                    created = true;
                    tokio::select! {
                        result = state.rc(endpoint, body) => result?,
                        peer = stream.fill_buf() => {
                            let _ = peer?;
                            let _ = state.rc("config/oauthstop", json!({})).await;
                            bail!("Setup cancelled");
                        }
                    }
                },
                "oauth" => state.rc("config/oauthstatus", json!({})).await?,
                "finish" => {
                    if !created { bail!("Configure the account first"); }
                    let id = uuid::Uuid::new_v4().to_string();
                    let name = request["name"].as_str().filter(|s| !s.trim().is_empty()).context("Display name required")?;
                    let provider = request["provider"].as_str().context("Provider required")?;
                    let mut r = new_remote(&id, name, &remote, provider, true);
                    if let Some(prefs) = request.get("preferences") { r.preferences = serde_json::from_value(prefs.clone())?; }
                    if let Some(account_id) = request["account_id"].as_str().filter(|id|!id.is_empty()) {
                        if state.remotes.lock().await.iter().any(|r|r.account_id.as_deref()==Some(account_id)) {bail!("This account already has storage linked");}
                        r.account_id=Some(account_id.into());
                    }
                    if r.preferences.cache_mib == 0 { bail!("Invalid cache size"); }
                    { let mut remotes = state.remotes.lock().await; remotes.push(r); assign_mount_path(&mut remotes, &id); }
                    let tested = tokio::select! {
                        result = state.test(&id) => result,
                        _ = stream.fill_buf() => Err(anyhow::anyhow!("Setup cancelled")),
                    };
                    if let Err(e) = tested { state.remotes.lock().await.retain(|r| r.id != id); return Err(e); }
                    let final_id = if let Some(old) = replacement.as_ref() {
                        let mut remotes = state.remotes.lock().await;
                        remotes.retain(|r| r.id != id);
                        let current = remotes.iter_mut().find(|r| r.id == old.id).context("Account removed during setup")?;
                        current.remote_name = remote.clone(); current.name = name.into(); current.connection = "connected".into(); current.error.clear();
                        assign_mount_path(&mut remotes, &old.id);
                        old.id.clone()
                    } else { id };
                    if let Err(e) = state.save().await {
                        let mut remotes = state.remotes.lock().await;
                        if let Some(old) = replacement.as_ref() {
                            if let Some(current) = remotes.iter_mut().find(|r| r.id == old.id) { *current = old.clone(); }
                        } else { remotes.retain(|r| r.id != final_id); }
                        return Err(e);
                    }
                    created = false;
                    state.notify().await;
                    if let Some(old) = replacement.as_ref() { let _ = state.rc("config/delete", json!({"name":old.remote_name})).await; }
                    stream.get_mut().write_all(format!("{}\n", json!({"id":final_id})).as_bytes()).await?;
                    break;
                },
                _ => bail!("Unknown setup action"),
            };
            stream.get_mut().write_all(format!("{response}\n").as_bytes()).await?;
        }
        Ok(())
    }.await;
    if let Some(old) = replacement {
        state.busy.lock().await.remove(&old.id);
    }
    if created {
        let _ = state.rc("config/delete", json!({"name":remote})).await;
    }
    state.notify().await;
    if result.is_err() {
        let _ = stream
            .get_mut()
            .write_all(b"{\"error\":\"Setup failed. Check account details and retry.\"}\n")
            .await;
    }
    result
}
fn notes_path_valid(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 2048
        && !path.contains('\\')
        && !path.contains(':')
        && !path.chars().any(char::is_control)
        && path
            .split('/')
            .all(|p| !p.is_empty() && p != "." && p != ".." && !p.starts_with('.'))
}
#[derive(Clone, Serialize, Deserialize)]
struct NoteRevision {
    id: String,
    path: String,
    parents: Vec<String>,
    deleted: bool,
    #[serde(default)]
    size: u64,
}
async fn notes_command(
    state: &State,
    remote: &Remote,
    args: &[&str],
    bytes: Option<Vec<u8>>,
    limit: usize,
) -> Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut command = state.command(remote).await?;
    command
        .args(args)
        .args([
            "--contimeout",
            "10s",
            "--timeout",
            "30s",
            "--retries",
            "1",
            "--log-level",
            "OFF",
        ])
        .kill_on_drop(true)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .stdin(if bytes.is_some() {
            std::process::Stdio::piped()
        } else {
            std::process::Stdio::null()
        });
    let mut child = command.spawn()?;
    let stdout = child.stdout.take().context("Notes output unavailable")?;
    let input = child.stdin.take();
    let work = async {
        let writer = async {
            if let (Some(mut input), Some(bytes)) = (input, bytes) {
                input.write_all(&bytes).await?;
                input.shutdown().await?;
            }
            anyhow::Ok(())
        };
        let reader = async {
            let mut output = Vec::new();
            stdout
                .take((limit + 1) as u64)
                .read_to_end(&mut output)
                .await?;
            if output.len() > limit {
                bail!("Notes response exceeds safety limit");
            }
            anyhow::Ok(output)
        };
        let (_, output) = tokio::try_join!(writer, reader)?;
        if !child.wait().await?.success() {
            bail!("Notes connection failed. Check the account in Settings and retry.");
        }
        Ok(output)
    };
    tokio::time::timeout(Duration::from_secs(120), work).await?
}
async fn notes_snapshot(state: &State, r: &Remote) -> Result<Vec<NoteRevision>> {
    let root = format!("{}:{}/revisions", r.remote_name, r.notes.folder);
    // A missing folder is a fresh vault. Other failures must remain errors.
    let mut command = state.command(r).await?;
    let output = command
        .args([
            "lsjson",
            &root,
            "--files-only",
            "--max-depth",
            "1",
            "--contimeout",
            "10s",
            "--timeout",
            "30s",
            "--retries",
            "1",
            "--log-level",
            "OFF",
        ])
        .kill_on_drop(true)
        .stderr(std::process::Stdio::null())
        .output();
    let output = tokio::time::timeout(Duration::from_secs(60), output).await??;
    if !output.status.success() {
        if output.status.code() == Some(3) {
            return Ok(Vec::new());
        }
        bail!("Notes folder unavailable. Check the account in Settings.");
    }
    if output.stdout.len() > 8 * 1024 * 1024 {
        bail!("Notes revision index is too large");
    }
    let entries: Vec<Value> = serde_json::from_slice(&output.stdout)?;
    if entries.len() > 10000 {
        bail!("Notes revision limit reached; retained cloud versions are safe");
    }
    let mut result = Vec::new();
    for entry in entries {
        let Some(name) = entry["Name"].as_str().and_then(|n| n.strip_suffix(".json")) else {
            continue;
        };
        if !id_valid(name) {
            continue;
        }
        let path = format!("{root}/{name}.json");
        let bytes = notes_command(state, r, &["cat", &path], None, 16384).await?;
        let revision: NoteRevision = serde_json::from_slice(&bytes)?;
        if revision.id != name
            || !notes_path_valid(&revision.path)
            || revision.parents.len() > 128
            || revision.parents.iter().any(|p| !id_valid(p))
        {
            bail!("Invalid Notes revision");
        }
        result.push(revision);
    }
    Ok(result)
}
async fn notes_session(
    state: Arc<State>,
    id: String,
    listener: tokio::net::UnixListener,
) -> Result<()> {
    use tokio::io::AsyncReadExt;
    let (stream, _) = tokio::time::timeout(Duration::from_secs(60), listener.accept()).await??;
    use std::os::unix::fs::MetadataExt;
    if stream.peer_cred()?.uid() != std::fs::metadata(home())?.uid() {
        bail!("Wrong Notes user");
    }
    let mut stream = BufReader::new(stream);
    loop {
        let mut line = Vec::new();
        let count = tokio::time::timeout(
            Duration::from_secs(300),
            (&mut stream)
                .take(160 * 1024 * 1024)
                .read_until(b'\n', &mut line),
        )
        .await??;
        if count == 0 {
            break;
        }
        if line.last() != Some(&b'\n') {
            bail!("Notes request too large");
        }
        let response: Result<Value> = async {
            let request: Value = serde_json::from_slice(&line)?;
            let r = state.get(&id).await?;
            if !r.notes.enabled {
                bail!("Notes is disabled");
            }
            match request["action"].as_str().unwrap_or("") {
                "list" => Ok(serde_json::to_value(notes_snapshot(&state, &r).await?)?),
                "get" => {
                    let revision = request["id"]
                        .as_str()
                        .filter(|id| id_valid(id))
                        .context("Invalid Notes revision")?;
                    let path = request["path"]
                        .as_str()
                        .filter(|p| notes_path_valid(p))
                        .context("Invalid Notes path")?;
                    let object = format!(
                        "{}:{}/content/{revision}/{path}",
                        r.remote_name, r.notes.folder
                    );
                    let bytes =
                        notes_command(&state, &r, &["cat", &object], None, 32 * 1024 * 1024)
                            .await?;
                    Ok(json!({"bytes":bytes}))
                }
                "put" => {
                    let path = request["path"]
                        .as_str()
                        .filter(|p| notes_path_valid(p))
                        .context("Invalid Notes path")?;
                    let parents: Vec<String> = serde_json::from_value(request["parents"].clone())?;
                    if parents.len() > 128 || parents.iter().any(|p| !id_valid(p)) {
                        bail!("Invalid Notes parents");
                    }
                    let snapshot = notes_snapshot(&state, &r).await?;
                    let referenced: HashSet<&str> = snapshot
                        .iter()
                        .filter(|r| r.path == path)
                        .flat_map(|r| r.parents.iter().map(String::as_str))
                        .collect();
                    let mut heads: Vec<String> = snapshot
                        .iter()
                        .filter(|r| r.path == path && !referenced.contains(r.id.as_str()))
                        .map(|r| r.id.clone())
                        .collect();
                    heads.sort();
                    let mut expected = parents.clone();
                    expected.sort();
                    if heads != expected {
                        bail!("Cloud note changed; synchronize and review the conflict");
                    }
                    let revision = uuid::Uuid::new_v4().to_string();
                    let deleted = request["deleted"].as_bool().unwrap_or(false);
                    let mut size = 0;
                    if !deleted {
                        let bytes: Vec<u8> = serde_json::from_value(request["bytes"].clone())?;
                        size = bytes.len() as u64;
                        if bytes.len() > 32 * 1024 * 1024 {
                            bail!("Note exceeds safety limit");
                        }
                        let object = format!(
                            "{}:{}/content/{revision}/{path}",
                            r.remote_name, r.notes.folder
                        );
                        notes_command(&state, &r, &["rcat", &object], Some(bytes), 1024).await?;
                    }
                    let entry = NoteRevision {
                        id: revision.clone(),
                        path: path.into(),
                        parents,
                        deleted,
                        size,
                    };
                    let object = format!(
                        "{}:{}/revisions/{revision}.json",
                        r.remote_name, r.notes.folder
                    );
                    notes_command(
                        &state,
                        &r,
                        &["rcat", &object],
                        Some(serde_json::to_vec(&entry)?),
                        1024,
                    )
                    .await?;
                    Ok(json!({"id":revision}))
                }
                _ => bail!("Unknown Notes action"),
            }
        }
        .await;
        let response = response.unwrap_or_else(|_|json!({"error":"Notes sync stopped. The account may be offline, disabled, or changed; retry synchronization. Existing versions are retained."}));
        stream
            .get_mut()
            .write_all(&serde_json::to_vec(&response)?)
            .await?;
        stream.get_mut().write_all(b"\n").await?;
    }
    Ok(())
}
#[tokio::main]
async fn main() -> Result<()> {
    private_dir(&config_dir())?;
    if let Ok(entries) = std::fs::read_dir(legacy_mounts()) {
        let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
        for entry in entries.flatten() {
            if !mount_listed(&mountinfo, &entry.path().to_string_lossy()) {
                let _ = std::fs::remove_dir(entry.path());
            }
        }
        let _ = std::fs::remove_dir(legacy_mounts());
    }
    if !config().exists() {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(config())?;
    }
    let runtime = runtime()?;
    let _generation = nyx_accounts_native::service_lifetime_lock(&runtime.join("service.lock"))?;
    let rc_path = runtime.join("rclone.sock");
    let connection = zbus::connection::Builder::session()?
        .name(BUS)?
        .internal_executor(false)
        .build()
        .await?;
    let executor_connection = connection.clone();
    tokio::spawn(async move {
        loop {
            executor_connection.executor().tick().await;
        }
    });
    // Acquiring the bus name first ensures only one service owns this socket.
    if rc_path.exists() {
        tokio::fs::remove_file(&rc_path).await?;
    }
    let mut rc_child = Command::new("rclone")
        .args(["rcd", "--rc-addr"])
        .arg(format!("unix://{}", rc_path.display()))
        .args(["--rc-no-auth", "--config"])
        .arg(config())
        .args(["--log-level", "ERROR"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("Install rclone to use Cloud Storage")?;
    for _ in 0..100 {
        if rc_path.exists() {
            break;
        }
        if rc_child.try_wait()?.is_some() {
            bail!("rclone configuration service failed");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if config().exists() {
        tokio::fs::set_permissions(config(), std::fs::Permissions::from_mode(0o600)).await?;
    }
    let saved: Saved = match tokio::fs::read(config_dir().join("accounts.json")).await {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .context("Cloud account metadata is invalid; original file preserved")?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Saved::default(),
        Err(e) => return Err(e.into()),
    };
    let mut remotes = saved.remotes;
    for r in &mut remotes {
        if !id_valid(&r.id) {
            bail!("Invalid persisted cloud account identifier; metadata preserved");
        }
        r.mount_path.clear();
        r.connection = "disconnected".into();
        r.mount_status = "unmounted".into();
        r.error.clear();
    }
    let ids: Vec<String> = remotes.iter().map(|r| r.id.clone()).collect();
    for id in &ids {
        assign_mount_path(&mut remotes, id);
    }
    let version_output = Command::new("rclone").arg("version").output().await?;
    if !version_output.status.success() {
        bail!("Unable to detect rclone version");
    }
    let version = String::from_utf8_lossy(&version_output.stdout)
        .lines()
        .next()
        .unwrap_or("rclone")
        .to_owned();
    let broker_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let broker_port = broker_listener.local_addr()?.port();
    let state = Arc::new(State {
        broker_port,
        broker_grants: Mutex::new(HashMap::new()),
        version,
        setup_gate: Mutex::new(()),
        remotes: Mutex::new(remotes),
        children: Mutex::new(HashMap::new()),
        busy: Mutex::new(HashSet::new()),
        rc: reqwest::Client::builder()
            .unix_socket(rc_path)
            .timeout(Duration::from_secs(600))
            .build()?,
        connection: connection.clone(),
        tasks: Mutex::new(HashMap::new()),
        desired_mounts: Mutex::new(HashSet::new()),
    });
    tokio::spawn(native::broker(state.clone(),broker_listener));
    connection
        .object_server()
        .at(PATH, Cloud(state.clone()))
        .await?;
    let snapshot = state.remotes.lock().await.clone();
    for r in snapshot {
        if r.preferences.mount_on_login && r.storage_enabled {
            let _ = Cloud(state.clone()).launch(&r.id, "mount").await;
        }
    }
    let health = state.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
            let snapshot = health.remotes.lock().await.clone();
            for r in snapshot {
                if r.mount_status == "mounted"
                    && r.connection != "authentication_required"
                    && !health.busy.lock().await.contains(&r.id)
                {
                    let health = health.clone();
                    tokio::spawn(async move {
                        let _ = health.test(&r.id).await;
                    });
                }
            }
        }
    });
    let monitor = state.clone();
    tokio::spawn(async move {
        let mut retries: HashMap<String, (u8, tokio::time::Instant)> = HashMap::new();
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            let mut children = monitor.children.lock().await;
            let exited: Vec<_> = children
                .iter_mut()
                .filter_map(|(id, c)| c.try_wait().ok().flatten().map(|_| id.clone()))
                .collect();
            for id in &exited {
                children.remove(id);
            }
            drop(children);
            for id in exited {
                if let Ok(r) = monitor.get(&id).await {
                    if mounted(&r.mount_path).await {
                        let _ = Command::new("fusermount3")
                            .args(["-u", &r.mount_path])
                            .stdout(std::process::Stdio::null())
                            .stderr(std::process::Stdio::null())
                            .status()
                            .await;
                    }
                    let _ = std::fs::remove_dir(&r.mount_path);
                }
                monitor
                    .status(
                        &id,
                        "error",
                        "unmounted",
                        "Cloud connection stopped unexpectedly. Retry connection.",
                    )
                    .await;
            }
            let desired: Vec<_> = monitor
                .desired_mounts
                .lock()
                .await
                .iter()
                .cloned()
                .collect();
            for id in desired {
                if monitor.children.lock().await.contains_key(&id) {
                    retries.remove(&id);
                    continue;
                }
                let Ok(remote) = monitor.get(&id).await else {
                    continue;
                };
                if remote.connection == "authentication_required"
                    || monitor.busy.lock().await.contains(&id)
                {
                    continue;
                }
                let retry = retries
                    .entry(id.clone())
                    .or_insert((0, tokio::time::Instant::now()));
                if retry.0 >= 5 || tokio::time::Instant::now() < retry.1 {
                    continue;
                }
                retry.0 += 1;
                retry.1 = tokio::time::Instant::now() + Duration::from_secs(10 * (1 << retry.0));
                let _ = Cloud(monitor.clone()).launch(&id, "mount").await;
            }
        }
    });
    tokio::select! { _ = nyx_accounts_native::wait_for_bus_disconnect(connection.clone()) => {}, _ = tokio::signal::ctrl_c() => {}, _ = async { let mut sig = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("signal handler"); sig.recv().await; } => {} }
    let snapshot = state.remotes.lock().await.clone();
    for r in snapshot {
        let _ = state.stop_mount(&r.id).await;
    }
    let _ = state.save().await;
    let _ = rc_child.kill().await;
    Ok(())
}
