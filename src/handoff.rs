//! Upgrading the daemon in place: the running daemon stops its services and hands its
//! listening sockets (control socket, HTTPS, PostgreSQL, Redis, devenv's control
//! socket) to the new one over a unix socket (SCM_RIGHTS), so they never close.
//! Connections made meanwhile wait in the kernel's backlog instead of being refused.

use std::io::{IoSlice, IoSliceMut};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;

use anyhow::{Context, Result, bail};

/// Listening sockets by key: `control`, `devenv`, `tcp:<port>`.
pub type Fds = Vec<(String, OwnedFd)>;

const MAX_FDS: usize = 16;

/// Bytes of control message for `n` fds. Exactly this as `msg_controllen`: macOS
/// refuses more (EINVAL).
fn control_space(n: usize) -> usize {
    // SAFETY: CMSG_SPACE is arithmetic.
    unsafe { libc::CMSG_SPACE((n * size_of::<RawFd>()) as u32) as usize }
}

/// A zeroed buffer of `bytes` with cmsghdr alignment.
fn control_buffer(bytes: usize) -> Vec<u64> {
    vec![0; bytes.div_ceil(size_of::<u64>())]
}

pub fn send(stream: &UnixStream, fds: &Fds) -> Result<()> {
    if fds.is_empty() || fds.len() > MAX_FDS {
        bail!("{} sockets to hand over", fds.len());
    }
    let keys = serde_json::to_vec(&fds.iter().map(|(k, _)| k).collect::<Vec<_>>())?;
    let raw: Vec<RawFd> = fds.iter().map(|(_, f)| f.as_raw_fd()).collect();
    let iov = [IoSlice::new(&keys)];
    let space = control_space(raw.len());
    let mut control = control_buffer(space);
    // SAFETY: msghdr points at live buffers sized with CMSG_SPACE; the one cmsghdr
    // written is within it.
    let sent = unsafe {
        let mut msg: libc::msghdr = std::mem::zeroed();
        msg.msg_iov = iov.as_ptr() as *mut libc::iovec;
        msg.msg_iovlen = 1;
        msg.msg_control = control.as_mut_ptr().cast();
        msg.msg_controllen = space as _;
        let c = libc::CMSG_FIRSTHDR(&msg);
        (*c).cmsg_level = libc::SOL_SOCKET;
        (*c).cmsg_type = libc::SCM_RIGHTS;
        (*c).cmsg_len = libc::CMSG_LEN((raw.len() * size_of::<RawFd>()) as u32) as _;
        std::ptr::copy_nonoverlapping(raw.as_ptr(), libc::CMSG_DATA(c).cast(), raw.len());
        libc::sendmsg(stream.as_raw_fd(), &msg, 0)
    };
    if sent < 0 {
        return Err(std::io::Error::last_os_error()).context("sending the sockets");
    }
    if sent as usize != keys.len() {
        bail!("sent {sent} of {} bytes", keys.len());
    }
    Ok(())
}

pub fn recv(stream: &UnixStream) -> Result<Fds> {
    let mut buf = vec![0u8; 8192];
    let mut iov = [IoSliceMut::new(&mut buf)];
    let space = control_space(MAX_FDS);
    let mut control = control_buffer(space);
    let mut fds = Vec::new();
    // SAFETY: as in `send`; every fd received is owned right away (closed on error).
    let (n, flags) = unsafe {
        let mut msg: libc::msghdr = std::mem::zeroed();
        msg.msg_iov = iov.as_mut_ptr().cast();
        msg.msg_iovlen = 1;
        msg.msg_control = control.as_mut_ptr().cast();
        msg.msg_controllen = space as _;
        let n = libc::recvmsg(stream.as_raw_fd(), &mut msg, 0);
        if n < 0 {
            return Err(std::io::Error::last_os_error()).context("receiving the sockets");
        }
        let mut c = libc::CMSG_FIRSTHDR(&msg);
        while !c.is_null() {
            if (*c).cmsg_level == libc::SOL_SOCKET && (*c).cmsg_type == libc::SCM_RIGHTS {
                let len = (*c).cmsg_len as usize - libc::CMSG_LEN(0) as usize;
                let data = libc::CMSG_DATA(c).cast::<RawFd>();
                for i in 0..len / size_of::<RawFd>() {
                    let fd = std::ptr::read_unaligned(data.add(i));
                    libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
                    fds.push(OwnedFd::from_raw_fd(fd));
                }
            }
            c = libc::CMSG_NXTHDR(&msg, c);
        }
        (n as usize, msg.msg_flags)
    };
    if flags & libc::MSG_CTRUNC != 0 {
        bail!("too many sockets handed over");
    }
    let keys: Vec<String> = serde_json::from_slice(&buf[..n]).context("handoff message")?;
    if keys.len() != fds.len() {
        bail!("{} socket names for {} sockets", keys.len(), fds.len());
    }
    Ok(keys.into_iter().zip(fds).collect())
}

/// The socket handed over as `key`, if any.
pub fn take(fds: &mut Fds, key: &str) -> Option<OwnedFd> {
    let i = fds.iter().position(|(k, _)| k == key)?;
    Some(fds.remove(i).1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sockets_survive_the_handoff() {
        // Six: a control message size that isn't a multiple of 8 on macOS.
        let listeners: Vec<_> = (0..6)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let ports: Vec<u16> = listeners
            .iter()
            .map(|l| l.local_addr().unwrap().port())
            .collect();
        let fds: Fds = listeners
            .into_iter()
            .map(|l| (format!("tcp:{}", l.local_addr().unwrap().port()), l.into()))
            .collect();
        let (a, b) = UnixStream::pair().unwrap();
        send(&a, &fds).unwrap();
        drop(fds);
        let mut got = recv(&b).unwrap();
        for port in ports {
            let tcp = std::net::TcpListener::from(take(&mut got, &format!("tcp:{port}")).unwrap());
            // Still listening: a connection is accepted.
            let _c = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
            tcp.accept().unwrap();
        }
        assert!(got.is_empty());
    }
}
