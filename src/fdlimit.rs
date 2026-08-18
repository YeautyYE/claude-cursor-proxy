//! Raise `RLIMIT_NOFILE` so long-lived `serve` can hold many concurrent
//! grok-cli / Cursor sockets without dying as a fake Cursor auth failure.
//!
//! macOS GUI/launchd soft limit is often 256. A 64-way CARVE wave plus Surge
//! CONNECT tunnels fills that, and the next `/usr/bin/security` spawn returns
//! `Too many open files (os error 24)`.

/// Soft limit we try to reach. Hard limit still wins when it is lower.
pub const TARGET_NOFILE_SOFT: u64 = 65_536;

#[cfg(unix)]
pub fn current_nofile_limit() -> std::io::Result<(u64, u64)> {
    let mut lim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `getrlimit` writes the two-word `rlimit` we pass.
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok((rlim_to_u64(lim.rlim_cur), rlim_to_u64(lim.rlim_max)))
}

#[cfg(unix)]
fn rlim_is_infinity(value: u64) -> bool {
    value == 0 || value == rlim_to_u64(libc::RLIM_INFINITY)
}

/// `rlim_t` is `u64` on the CI targets and `u32` on some 32-bit Unix.
#[cfg(unix)]
#[allow(clippy::unnecessary_cast)]
fn rlim_to_u64(value: libc::rlim_t) -> u64 {
    value as u64
}

#[cfg(unix)]
#[allow(clippy::unnecessary_cast)]
fn u64_to_rlim(value: u64) -> libc::rlim_t {
    value as libc::rlim_t
}

/// Lift the process file-descriptor ceiling. Best-effort: never fails the
/// process if the OS refuses; callers should keep serving with the old limit.
pub fn raise_nofile_limit() -> std::io::Result<u64> {
    #[cfg(not(unix))]
    {
        Ok(0)
    }
    #[cfg(unix)]
    {
        let (soft, hard) = current_nofile_limit()?;
        let cap = if rlim_is_infinity(hard) {
            TARGET_NOFILE_SOFT
        } else {
            TARGET_NOFILE_SOFT.min(hard)
        };
        if soft >= cap {
            return Ok(soft);
        }
        let lim = libc::rlimit {
            rlim_cur: u64_to_rlim(cap),
            rlim_max: if rlim_is_infinity(hard) {
                libc::RLIM_INFINITY
            } else {
                u64_to_rlim(hard)
            },
        };
        // SAFETY: `setrlimit` only reads the `rlimit` we constructed.
        let rc = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &lim) };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(cap)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn raise_nofile_limit_reaches_target_or_hard_cap() {
        let raised = raise_nofile_limit().expect("setrlimit RLIMIT_NOFILE");
        let (soft, hard) = current_nofile_limit().unwrap();
        assert_eq!(raised, soft);
        let expected = if rlim_is_infinity(hard) {
            TARGET_NOFILE_SOFT.max(soft)
        } else {
            TARGET_NOFILE_SOFT.min(hard).max(soft)
        };
        assert!(
            soft >= 10240 || soft >= expected.min(TARGET_NOFILE_SOFT),
            "soft={soft} hard={hard} expected at least 10240 or the OS cap"
        );
    }
}
