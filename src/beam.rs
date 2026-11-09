//! How busy a BEAM (Erlang/Elixir node) is, from the probe the devenv module loads
//! into every one (`devenv-module/lazy_cow_tree_probe.erl`, via ERL_AFLAGS): it answers
//! on `<state>/beam/<os pid>.sock`. Removing a worktree stops the nodes nobody uses
//! (a forgotten `iex -S mix phx.server`) and spares the working ones.

use std::io::Read;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::time::Duration;

/// Idle at least this long, with nothing else going on: `git worktree remove` stops it.
pub const IDLE_AFTER_S: u64 = 60;

#[derive(Debug, Default, PartialEq)]
pub struct Activity {
    /// Scheduler utilization over the last minute, 0..1.
    pub util: f64,
    /// Connections to its listening TCP ports now.
    pub clients: u64,
    /// Since it last had clients or used its schedulers.
    pub idle_s: u64,
    /// An interactive shell (iex, erl) on a terminal.
    pub shell: bool,
    pub uptime_s: u64,
}

impl Activity {
    fn parse(line: &str) -> Option<Self> {
        let mut a = Activity::default();
        let mut seen = 0;
        for kv in line.split_whitespace() {
            let (k, v) = kv.split_once('=')?;
            match k {
                "util" => a.util = v.parse().ok()?,
                "clients" => a.clients = v.parse().ok()?,
                "idle_s" => a.idle_s = v.parse().ok()?,
                "shell" => a.shell = v == "1",
                "uptime_s" => a.uptime_s = v.parse().ok()?,
                // A newer probe's.
                _ => continue,
            }
            seen += 1;
        }
        (seen == 5).then_some(a)
    }

    /// Why it is still needed; None when nobody used it for `idle_after` seconds.
    pub fn busy(&self, idle_after: u64) -> Option<String> {
        if self.shell {
            Some("an interactive shell on a terminal".into())
        } else if self.clients > 0 {
            Some(match self.clients {
                1 => "a client connected".into(),
                n => format!("{n} clients connected"),
            })
        } else if self.idle_s < idle_after {
            Some(format!(
                "{:.0}% of its schedulers busy over the last minute",
                self.util * 100.0
            ))
        } else {
            None
        }
    }
}

fn dir() -> PathBuf {
    crate::config::home().join("beam")
}

/// What the probe in BEAM `pid` says; None for a process without one (not a BEAM,
/// or one started without the devenv module's ERL_AFLAGS).
pub fn activity(pid: i32) -> Option<Activity> {
    let path = dir().join(format!("{pid}.sock"));
    let mut s = UnixStream::connect(&path).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    let mut line = String::new();
    s.read_to_string(&mut line).ok()?;
    Activity::parse(&line)
}

/// Remove the sockets of nodes that are gone (SIGKILLed nodes leave theirs behind).
pub fn sweep() {
    let Ok(entries) = std::fs::read_dir(dir()) else {
        return;
    };
    for e in entries.flatten() {
        let name = e.file_name();
        let Some(pid) = name
            .to_str()
            .and_then(|n| n.strip_suffix(".sock"))
            .and_then(|p| p.parse::<i32>().ok())
        else {
            continue;
        };
        // SAFETY: signal 0 only checks that the process exists.
        if unsafe { libc::kill(pid, 0) } != 0
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
        {
            let _ = std::fs::remove_file(e.path());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_probe_line() {
        let a = Activity::parse("util=0.003 clients=0 idle_s=95 shell=0 uptime_s=300\n").unwrap();
        assert_eq!(a.busy(IDLE_AFTER_S), None);
        let a = Activity::parse("util=0.400 clients=0 idle_s=0 shell=0 uptime_s=9 x=1").unwrap();
        assert!(
            a.busy(IDLE_AFTER_S)
                .unwrap()
                .starts_with("40% of its schedulers")
        );
        let a = Activity::parse("util=0.000 clients=2 idle_s=0 shell=0 uptime_s=9").unwrap();
        assert_eq!(a.busy(IDLE_AFTER_S).unwrap(), "2 clients connected");
        let a = Activity::parse("util=0.000 clients=0 idle_s=900 shell=1 uptime_s=900").unwrap();
        assert!(a.busy(IDLE_AFTER_S).is_some());
        assert_eq!(Activity::parse("util=0.1 clients=0"), None);
    }
}
