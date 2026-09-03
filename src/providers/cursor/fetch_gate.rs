//! Keyed single-flight gates for Cursor metadata requests.
//!
//! Cursor model metadata is requested by several consumers at startup (the
//! proxy warm-up, `/v1/models`, and the TUI).  A plain cache check followed by
//! an asynchronous HTTP request lets all of those callers observe a cold
//! entry at the same time and issue duplicate requests.  This module keeps a
//! small process-local mutex per `(resource, account, client identity)` key.
//! The first caller performs the request while the other callers wait; each
//! waiter then re-checks the corresponding cache before doing any work.
//!
//! Only a stable, domain-separated digest of the account identity is used in
//! the key. The bearer itself never enters the gate map or logs.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::sync::{Mutex, OwnedMutexGuard};

/// Keep the map bounded for long-lived processes that rotate many accounts.
/// Entries whose only strong reference is the map can be evicted; an entry
/// currently in use has at least one additional reference held by its guard.
const MAX_KEYS: usize = 512;

type KeyLock = Arc<Mutex<()>>;

fn gates() -> &'static Mutex<HashMap<String, KeyLock>> {
    static GATES: OnceLock<Mutex<HashMap<String, KeyLock>>> = OnceLock::new();
    GATES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Build a credential-free key for one metadata resource.
pub(crate) fn key(resource: &str, token: &str, client_type: &str) -> String {
    // Cursor rotates access JWTs for the same login.  Use the stable account
    // digest (subject/email when available, opaque-token fallback otherwise)
    // so a refresh does not create a second in-flight lane and issue another
    // metadata request within the same TTL window.
    let digest = crate::providers::cursor::auth::cursor_account_digest(token);
    let identity = client_type.trim();
    let identity = if identity.is_empty() { "cli" } else { identity };
    format!("{resource}:{digest}:{}", identity.to_ascii_lowercase())
}

/// Acquire the single-flight lease for a metadata key.
///
/// The returned owned guard keeps the per-key lock alive while the caller is
/// on the network.  Callers must perform a fresh cache lookup after acquiring
/// it; this is what turns the gate into a single-flight cache fill rather than
/// merely serializing duplicate requests.
pub(crate) async fn acquire(key: String) -> OwnedMutexGuard<()> {
    acquire_with_status(key).await.0
}

/// Acquire a lease and report whether this caller had to wait for an already
/// active flight.  The status is useful for explicit refresh operations: a
/// waiter can consume the snapshot produced by the flight it waited on,
/// rather than immediately issuing a second forced request.
pub(crate) async fn acquire_with_status(key: String) -> (OwnedMutexGuard<()>, bool) {
    let lock = {
        let mut map = gates().lock().await;
        if let Some(lock) = map.get(&key) {
            lock.clone()
        } else {
            if map.len() >= MAX_KEYS {
                // Retain active flights and remove arbitrary idle entries. A
                // HashMap has no ordering requirement here: this is only a
                // bounded guard against untrusted account rotation.
                map.retain(|_, lock| Arc::strong_count(lock) > 1);
                if map.len() >= MAX_KEYS {
                    // If every entry is briefly active, allow one additional
                    // key rather than blocking metadata requests indefinitely.
                    // The next acquisition will prune once flights complete.
                }
            }
            let lock = Arc::new(Mutex::new(()));
            map.insert(key, lock.clone());
            lock
        }
    };
    // Claim an idle lock synchronously when possible.  Using
    // `try_lock_owned` (rather than `try_lock` followed by a separate
    // `lock_owned`) closes the small race where two callers could both report
    // themselves as leaders; that distinction matters to forced refreshes.
    match lock.clone().try_lock_owned() {
        Ok(guard) => (guard, false),
        Err(_) => (lock.lock_owned().await, true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn same_key_serializes_and_different_keys_overlap() {
        // Do not clear the process-global map here: other async tests may be
        // exercising an unrelated key at the same time, and removing its
        // lock while a flight is active would invalidate the single-flight
        // guarantee. The fixed keys below are safe to reuse because every
        // lease is released before the assertions complete.
        let entered = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let (first_ready_tx, first_ready_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (second_attempt_tx, second_attempt_rx) = tokio::sync::oneshot::channel();
        let (second_ready_tx, mut second_ready_rx) = tokio::sync::oneshot::channel();
        let first_enter = {
            let entered = entered.clone();
            let peak = peak.clone();
            tokio::spawn(async move {
                let _lease = acquire(key("usable", "token-a", "sand")).await;
                let now = entered.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                first_ready_tx.send(()).expect("first task is observed");
                release_rx.await.expect("release first task");
                entered.fetch_sub(1, Ordering::SeqCst);
            })
        };
        // Wait for the first task to hold its lease before launching the
        // second task. This avoids scheduler-dependent sleeps in the test.
        first_ready_rx.await.expect("first task ready");
        let second = {
            let entered = entered.clone();
            let peak = peak.clone();
            tokio::spawn(async move {
                second_attempt_tx.send(()).expect("second task is observed");
                let _lease = acquire(key("usable", "token-a", "sand")).await;
                let now = entered.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                second_ready_tx.send(()).expect("second task is observed");
                entered.fetch_sub(1, Ordering::SeqCst);
            })
        };
        second_attempt_rx.await.expect("second task attempted");
        assert!(
            second_ready_rx.try_recv().is_err(),
            "the second same-key task must wait while the first lease is held"
        );
        release_tx.send(()).expect("release first lease");
        first_enter.await.unwrap();
        second_ready_rx.await.expect("second task acquired");
        second.await.unwrap();
        assert_eq!(peak.load(Ordering::SeqCst), 1);

        // Identity and account are part of the key, so unrelated requests do
        // not unnecessarily wait behind one another. Hold the first lease
        // while acquiring the second and require both readiness signals.
        let (a_ready_tx, a_ready_rx) = tokio::sync::oneshot::channel();
        let (a_release_tx, a_release_rx) = tokio::sync::oneshot::channel();
        let (b_ready_tx, b_ready_rx) = tokio::sync::oneshot::channel();
        let (b_release_tx, b_release_rx) = tokio::sync::oneshot::channel();
        let a = tokio::spawn(async move {
            let _lease = acquire(key("usable", "token-a", "sand")).await;
            a_ready_tx.send(()).expect("key A ready");
            a_release_rx.await.expect("release key A");
        });
        a_ready_rx.await.expect("key A acquired");
        let b = tokio::spawn(async move {
            let _lease = acquire(key("usable", "token-b", "sand")).await;
            b_ready_tx.send(()).expect("key B ready");
            b_release_rx.await.expect("release key B");
        });
        b_ready_rx.await.expect("different key must not wait on A");
        a_release_tx.send(()).expect("release key A");
        b_release_tx.send(()).expect("release key B");
        a.await.unwrap();
        b.await.unwrap();
    }

    #[test]
    fn key_never_contains_bearer_and_normalizes_identity() {
        let key = key("available", "secret-token", " SAND ");
        assert!(!key.contains("secret-token"));
        assert!(key.ends_with(":sand"));
    }
}
