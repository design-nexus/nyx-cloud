# Cloud

Shared, user-scoped rclone storage for Files and Settings. This repository builds
`nyx-cloudd` and the public `nyx-cloud` Rust client library independently.
Requires Rust, rclone (tested with 1.75.2), FUSE3, a session bus, and systemd user
services. Normal operations require no root privileges.

```
cargo build --release --locked
./install.sh
```

Installation enables the service for subsequent logins and installs D-Bus
activation. It does not start or restart applications. Settings → Cloud Storage
starts the service on demand. New accounts default to mounting when selected in
Files; mount-on-login is opt-in per account.

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/design-nexus/nyx-cloud/main/install.sh | bash
```

## Storage and ownership

New accounts use `$XDG_CONFIG_HOME/nyx/cloud/rclone.conf` (0600). Account metadata
is alongside it. Existing rclone remotes are discovered without modification;
explicit adoption adds Nyx mount preferences, without copying credentials or
changing their original configuration. External reauthentication/removal stays
with the original configuration tool. Managed accounts can be reauthenticated
in Settings: the new connection is staged and tested before replacing credentials.

Each account mounts at `~/<Account Name>` (for example `~/Google Drive`); a
second account with the same name gets `~/<Account Name> (2)`. The folder is
created on connect and removed on disconnect if empty, and an existing non-empty
folder with that name is never mounted over. Renaming an account moves its folder
at the next connection. These paths work with bookmarks and ordinary desktop
apps; runtime socket paths are used
only for IPC. Flatpak apps may need a file portal grant to access mounted files.
Each account has an isolated `$XDG_CACHE_HOME/nyx/cloud/<UUID>` VFS cache.
The 2 GiB default is an eviction target, not a hard quota. Open or dirty files
can exceed it. Full VFS caching allows sparse, partial reads where the renderer
and provider support them.

A completed filesystem copy may still await upload. Disconnect checks rclone's
upload queue and refuses pending/failed uploads. Never clear dirty caches by hand.
Service shutdown preserves cached data for rclone recovery on the next connection.
An unexpected mount-process exit gets bounded reconnect attempts; authentication
errors wait for user action. A mounted account is probed periodically in the
background; rclone handles transient network reconnection within a live mount.

## Cloud1 public interface

Bus/interface: `io.github.design_nexus.nyx.Cloud1`.
Object: `/io/github/design_nexus/nyx/Cloud1`.

| Method | Input | Result |
| --- | --- | --- |
| ListRemotes | — | JSON array of public `Remote` metadata |
| Discover | — | JSON array of external names/provider types |
| Adopt | external remote name | stable UUID |
| Mount / Unmount / TestConnection | UUID | operation UUID |
| Cancel | operation UUID | — |
| UpdatePreferences | UUID, display name, preferences JSON | — |
| Remove | UUID, delete-local-configuration boolean | — |
| BeginSetup | — | private setup socket path |

Read-only property: `RcloneVersion` reports the detected rclone version.

Signals: `Changed()` and `OperationFinished(operation, account, success, message)`.
Rust types in `src/lib.rs` define metadata/preferences. Consumers must tolerate
unknown connection states and added JSON fields. Breaking changes require a new
D-Bus interface version. Calls never return configuration dumps, passwords, OAuth
tokens, or SSH key contents. `Remove` never invokes delete/purge against cloud data.

Setup uses one owner-only runtime Unix socket per session. The same-user peer
exchanges newline-delimited JSON (`providers`, `configure`, `oauth`, `finish`).
Configure drives rclone's returned State/Option questions; the client returns the
answer using opt.continue/state/result. The graphical client uses inherited
stream state, not subprocess arguments or D-Bus strings, for credentials.
Reauthentication supplies an existing managed UUID as `replace` on the private
configure request. No general-purpose RC or shell-command proxy is exposed.

## Security and limitations

rclone obscured passwords are reversible; file permissions are not encryption.
OAuth tokens remain in rclone's configuration. Other processes running as the same
user can access the user's files and private RC sockets. RC sockets never bind a
network port; provider OAuth may use rclone's loopback callback server.

This release does not automatically enable encrypted configurations or store a
master password in Secret Service. Encrypted external configurations may need
unlocking through rclone's existing mechanisms before adoption/mounting. No GOA
credential reuse is assumed. Credential-bearing rclone errors and RC response
bodies are suppressed from logs and ordinary IPC.

Quota display, sharing links, offline pinning, cache clearing, and cloud-wide
search are not implemented. Providers retain their own move/resume semantics.
Live third-party OAuth requires user authorization and is not covered by the
credential-free integration test.

## Targeted integration check

```
cargo build --locked
dbus-run-session -- python3 tests/integration.py
```

The check uses a temporary HOME/config/cache/runtime and disposable alias remotes.
It verifies discovery, adoption, fresh setup, permissions, multiple accounts,
reauthentication, login mounting, FUSE reads/writes/rename/delete, safe disconnect,
configuration-only removal, cancelled setup cleanup, local WebDAV onboarding,
and expired authentication. No real accounts are read or changed.

### Unified accounts and Notes

`ImportRemote(name) -> UUID` copies an explicitly selected external remote into
Cloud's private configuration so Settings can reauthorize it. The original rclone
configuration is preserved. `ConfigureServices(id, account_id, storage, notes_json)`
links a stable GOA ID, controls Files visibility, and configures Notes
(`enabled`, absolute local `vault`, dedicated relative remote `folder`). Existing
accounts default to Storage enabled. Credentials are never part of these fields.
`BeginNotes(id) -> socket_path` opens a same-UID private Unix session, not a
credential-bearing D-Bus interface. Disabled accounts reject subsequent requests.

Notes sessions accept `list`, `get` (revision ID and relative path), and `put`
(relative path, expected parent IDs, bytes, optional deleted flag). The dedicated
folder contains `content/<UUID>/<original-path>` ordinary Markdown/attachments
and `revisions/<UUID>.json` immutable revision records. Updates append revisions;
concurrent writers retain separate heads instead of overwriting bytes. Deletes
append tombstones and do not erase cloud history. This is intentionally different
from a flat folder mirror: portable rclone backends cannot guarantee conditional
replacement. Revisions are retained without automatic pruning; 10,000 records
and 32 MiB per file bound this initial implementation. Existing WebDAV sync stays
available in Notes for ordinary flat-folder synchronization.

Settings is the account manager. Files only displays Storage-enabled accounts
and offers connection preferences. Notes does not require Storage visibility or
a FUSE mount. Disabling Storage safely disconnects any Files mount; pending uploads
must finish first. Mount-on-login applies only to Storage-enabled accounts.
