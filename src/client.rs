//! CLI side of the control API: JSON over HTTP/1.1 on the daemon's unix socket.

use std::path::Path;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request};
use hyper_util::rt::TokioIo;
use serde::{Serialize, de::DeserializeOwned};
use tokio::net::UnixStream;

pub async fn request<T: DeserializeOwned>(
    method: Method,
    path: &str,
    body: Option<&impl Serialize>,
) -> Result<T> {
    let socket = crate::config::socket_path();
    let stream = UnixStream::connect(&socket).await.with_context(|| {
        format!(
            "lazy-cow-tree is not running ({}); start it with `lazy-cow-tree serve` (devenv up)",
            socket.display()
        )
    })?;
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let body = match body {
        Some(b) => Bytes::from(serde_json::to_vec(b)?),
        None => Bytes::new(),
    };
    let req = Request::builder()
        .method(method)
        .uri(path)
        .header("host", "lazy-cow-tree")
        .header("content-type", "application/json")
        .body(Full::new(body))?;
    let resp = sender.send_request(req).await?;
    let status = resp.status();
    let bytes = resp.into_body().collect().await?.to_bytes();
    if !status.is_success() {
        bail!("{}", String::from_utf8_lossy(&bytes).trim());
    }
    Ok(serde_json::from_slice(&bytes)?)
}

/// Starts the daemon (`serve --saved`) when none answers on its socket: after a reboot
/// or `devenv up` stopping, `worktree rm` and the like would otherwise fail and leave
/// the worktree, its databases and redis behind. The saved projects come back with it,
/// with the settings and GH_TOKEN their last `devenv up` registered. A daemon `devenv
/// up` starts later finds this one running and registers with it.
/// LAZY_COW_TREE_NO_AUTOSTART=1: fail as before.
pub async fn ensure_running(root: &Path) -> Result<()> {
    if std::env::var("LAZY_COW_TREE_NO_AUTOSTART").is_ok_and(|v| !v.is_empty() && v != "0") {
        return Ok(());
    }
    if !refused().await {
        return Ok(());
    }
    // Only a saved project comes back; anything else needs its `devenv up` (settings).
    if !crate::daemon::registered_projects()
        .iter()
        .any(|p| p.root == root)
    {
        return Ok(());
    }
    // One starter at a time; the others find it running once they get the lock.
    let home = crate::config::home();
    std::fs::create_dir_all(home.join("logs"))?;
    let lock = std::fs::File::create(home.join("autostart.lock"))?;
    let lock = tokio::task::spawn_blocking(move || {
        use std::os::fd::AsRawFd;
        // SAFETY: flock on a file descriptor this closure owns.
        (unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } == 0)
            .then_some(lock)
            .ok_or_else(std::io::Error::last_os_error)
    })
    .await??;
    if !refused().await {
        return Ok(());
    }
    let log_path = home.join("logs").join("daemon.log");
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;
    let mut child = {
        use std::os::unix::process::CommandExt;
        let mut cmd = std::process::Command::new(std::env::current_exe()?);
        // A worktree shell's TMPDIR (`worktree_isolation_env`) goes with that worktree.
        if std::env::var("TMPDIR").is_ok_and(|t| t.starts_with("/tmp/lazy-cow-tree-")) {
            cmd.env_remove("TMPDIR");
        }
        cmd.args(["serve", "--saved"])
            // Not in a worktree: removing it would take the daemon along.
            .current_dir(&home)
            .stdin(std::process::Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            // Its own process group: the caller's Ctrl-C doesn't reach it.
            .process_group(0)
            .spawn()
            .context("starting the daemon")?
    };
    eprintln!(
        "the lazy-cow-tree daemon wasn't running; started it (log: {})",
        log_path.display()
    );
    // Up once it serves this project again (it re-registers the saved ones after binding).
    for _ in 0..300 {
        if registered(root).await {
            drop(lock);
            return Ok(());
        }
        if let Some(status) = child.try_wait()? {
            bail!(
                "the daemon exited right after starting ({status}); see {}",
                log_path.display()
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    bail!(
        "the daemon didn't come up within 30s; see {}",
        log_path.display()
    )
}

async fn registered(root: &Path) -> bool {
    get::<crate::daemon::Status>("/status")
        .await
        .is_ok_and(|s| s.projects.iter().any(|p| p.project.root == root))
}

/// Nothing listens on the socket: no file, or a stale one left by a daemon that died.
async fn refused() -> bool {
    match UnixStream::connect(crate::config::socket_path()).await {
        Ok(_) => false,
        Err(e) => matches!(
            e.kind(),
            std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
        ),
    }
}

pub async fn get<T: DeserializeOwned>(path: &str) -> Result<T> {
    request(Method::GET, path, None::<&()>).await
}

pub async fn post<T: DeserializeOwned>(path: &str, body: &impl Serialize) -> Result<T> {
    request(Method::POST, path, Some(body)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn refused_without_a_daemon_and_with_a_stale_socket() {
        let socket = crate::config::socket_path();
        std::fs::create_dir_all(socket.parent().unwrap()).unwrap();
        let _ = std::fs::remove_file(&socket);
        assert!(refused().await, "no socket file");
        // What a daemon that died (a reboot) leaves: the file, nobody listening.
        drop(std::os::unix::net::UnixListener::bind(&socket).unwrap());
        assert!(socket.exists());
        assert!(refused().await, "stale socket file");
        let listener = tokio::net::UnixListener::bind({
            std::fs::remove_file(&socket).unwrap();
            &socket
        })
        .unwrap();
        assert!(!refused().await, "a listening daemon");
        drop(listener);
        let _ = std::fs::remove_file(&socket);
    }
}
