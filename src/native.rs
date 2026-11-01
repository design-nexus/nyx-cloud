//! rclone receives access-only credentials through an anonymous inherited file.
//! Its refresh token is an ephemeral broker capability, never a provider refresh token.
use super::*;
use nyx_accounts_native::{Client, Service};
use std::{
    io::Write,
    os::fd::{AsRawFd, FromRawFd},
    os::unix::process::CommandExt,
};
use tokio::{io::AsyncReadExt, net::TcpListener};

pub async fn command(state: &State, remote: &Remote) -> Result<Command> {
    let id = remote
        .account_id
        .clone()
        .context("Native account link is missing")?;
    let lookup = id.clone();
    let (account, secret) = tokio::task::spawn_blocking(move || {
        let client = Client::connect()?;
        let account = client
            .list()?
            .into_iter()
            .find(|a| a.id == lookup)
            .context("Native account was removed")?;
        let secret = client.credential(&lookup, Service::Files)?;
        Ok::<_, anyhow::Error>((account, secret))
    })
    .await??;
    let mut config = format!("[{}]\n", remote.remote_name);
    match account.provider.as_str() {
        "google" | "microsoft_personal" => {
            let capability = state
                .broker_grants
                .lock()
                .await
                .entry(id)
                .or_insert_with(|| uuid::Uuid::new_v4().to_string())
                .clone();
            let expiry = chrono::DateTime::from_timestamp(secret.expires_at as i64, 0)
                .context("Invalid access-token expiry")?
                .to_rfc3339();
            let token = json!({"access_token":secret.access_token,"token_type":"Bearer","refresh_token":capability,"expiry":expiry});
            config += &format!(
                "type = {}\ntoken = {}\ntoken_url = http://127.0.0.1:{}/token\n",
                if account.provider == "google" {
                    "drive"
                } else {
                    "onedrive"
                },
                token,
                state.broker_port
            );
            if account.provider == "microsoft_personal" {
                let response = reqwest::Client::builder()
                    .redirect(reqwest::redirect::Policy::none())
                    .timeout(Duration::from_secs(30))
                    .build()?
                    .get("https://graph.microsoft.com/v1.0/me/drive?$select=id,driveType")
                    .bearer_auth(&secret.access_token)
                    .send()
                    .await?;
                anyhow::ensure!(response.status().is_success(), "OneDrive discovery failed");
                let drive: Value = response.json().await?;
                anyhow::ensure!(
                    drive["driveType"].as_str() == Some("personal"),
                    "Only personal OneDrive is supported"
                );
                let id = drive["id"]
                    .as_str()
                    .context("OneDrive identifier missing")?;
                anyhow::ensure!(!id.contains(['\n', '\r']), "Invalid drive identifier");
                config += &format!("drive_id = {id}\ndrive_type = personal\n");
            }
        }
        "nextcloud" | "webdav" => {
            let endpoint = account
                .endpoints
                .get("webdav")
                .context("Configure the WebDAV endpoint in Online Accounts")?;
            anyhow::ensure!(
                !account.identity.contains(['\n', '\r']) && !endpoint.contains(['\n', '\r']),
                "Invalid WebDAV configuration"
            );
            let mut obscure = Command::new("rclone")
                .args(["obscure", "-"])
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::null())
                .spawn()?;
            obscure
                .stdin
                .take()
                .context("Credential pipe unavailable")?
                .write_all(secret.password.as_bytes())
                .await?;
            let output = obscure.wait_with_output().await?;
            anyhow::ensure!(
                output.status.success(),
                "Could not prepare WebDAV credentials"
            );
            let password = String::from_utf8(output.stdout)?;
            config += &format!(
                "type = webdav\nurl = {endpoint}\nvendor = {}\nuser = {}\npass = {}\n",
                if account.provider == "nextcloud" {
                    "nextcloud"
                } else {
                    "other"
                },
                account.identity,
                password.trim()
            );
        }
        _ => bail!("This account does not support Files"),
    }
    memory_command(&config)
}
fn memory_command(config: &str) -> Result<Command> {
    let fd = unsafe { libc::memfd_create(c"nyx-cloud-access".as_ptr(), libc::MFD_CLOEXEC) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
    file.write_all(config.as_bytes())?;
    use std::io::{Seek, SeekFrom};
    file.seek(SeekFrom::Start(0))?;
    let mut command = Command::new("rclone");
    command
        .args(["--config", "/proc/self/fd/3"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    unsafe {
        command.as_std_mut().pre_exec(move || {
            if file.as_raw_fd() != 3 && libc::dup2(file.as_raw_fd(), 3) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::fcntl(3, libc::F_SETFD, 0) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    Ok(command)
}
pub async fn broker(state: Arc<State>, listener: TcpListener) {
    loop {
        let Ok((mut stream, peer)) = listener.accept().await else {
            break;
        };
        if !peer.ip().is_loopback() {
            continue;
        }
        let state = state.clone();
        tokio::spawn(async move {
            let work = async {
                let mut request = Vec::new();
                let mut chunk = [0; 4096];
                let header_end = loop {
                    let n = stream.read(&mut chunk).await?;
                    anyhow::ensure!(n != 0, "Request ended");
                    request.extend_from_slice(&chunk[..n]);
                    anyhow::ensure!(request.len() <= 16384, "Request too large");
                    if let Some(index) = request.windows(4).position(|s| s == b"\r\n\r\n") {
                        break index + 4;
                    }
                };
                let headers = std::str::from_utf8(&request[..header_end])?;
                anyhow::ensure!(
                    headers.starts_with("POST /token HTTP/1."),
                    "Invalid token request"
                );
                let length = headers
                    .lines()
                    .find_map(|s| {
                        s.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|v| v.trim().parse::<usize>())
                    })
                    .context("Content length missing")??;
                anyhow::ensure!(length <= 8192, "Request too large");
                while request.len() < header_end + length {
                    let n = stream.read(&mut chunk).await?;
                    anyhow::ensure!(n != 0, "Request ended");
                    request.extend_from_slice(&chunk[..n]);
                    anyhow::ensure!(request.len() <= 16384, "Request too large");
                }
                let form: HashMap<_, _> =
                    url::form_urlencoded::parse(&request[header_end..header_end + length])
                        .into_owned()
                        .collect();
                anyhow::ensure!(
                    form.get("grant_type").map(String::as_str) == Some("refresh_token"),
                    "Unsupported grant"
                );
                let capability = form.get("refresh_token").context("Capability missing")?;
                let id = state
                    .broker_grants
                    .lock()
                    .await
                    .iter()
                    .find(|(_, value)| *value == capability)
                    .map(|(id, _)| id.clone())
                    .context("Invalid capability")?;
                let secret = tokio::task::spawn_blocking(move || {
                    Client::connect()?.credential(&id, Service::Files)
                })
                .await??;
                let now = nyx_accounts_native::oauth::now();
                let response=json!({"access_token":secret.access_token,"token_type":"Bearer","expires_in":secret.expires_at.saturating_sub(now),"refresh_token":capability}).to_string();
                stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",response.len()).as_bytes()).await?;
                Ok::<_, anyhow::Error>(())
            };
            if !matches!(
                tokio::time::timeout(Duration::from_secs(35), work).await,
                Ok(Ok(()))
            ) {
                let _=stream.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn rclone_reads_anonymous_config_without_a_disk_file() {
        let mut command = memory_command("[fixture]\ntype = alias\nremote = /tmp\n").unwrap();
        let output = command
            .arg("listremotes")
            .stdout(std::process::Stdio::piped())
            .output()
            .await
            .unwrap();
        assert!(output.status.success());
        assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), "fixture:");
    }
}
