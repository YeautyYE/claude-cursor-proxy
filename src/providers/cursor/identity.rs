//! Cursor client identity: machine ids + `x-cursor-checksum`.
//!
//! Algorithm matches Cursor IDE `workbench.desktop.main.js` (`acf`/`ccf`) and the
//! independent reimplementation in `cursor-free-vip` (`check_user_authorized.py`):
//! ```text
//! E = floor(Date.now() / 1e6)
//! x = big-endian 6 bytes of E
//! A = acf(x)  // rolling XOR/add, seed 165
//! I = base64(A)  // 6 bytes → 8 chars, no padding needed
//! checksum = I + machineId + "/" + macMachineId
//! ```
//!
//! Machine ids: Cursor Desktop first uses the abuse-service machine ids and
//! falls back to telemetry ids persisted in `storage.json` or `state.vscdb`.
//! A proxy does not have the Desktop abuse service, so persisted ids are the
//! closest stable identity and are preferred whenever available. Token-derived
//! ids remain a deterministic last resort for headless installations (and
//! preserve compatibility with older CLI clients).

use sha2::{Digest, Sha256};
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const STATE_DB_QUERY_TIMEOUT: Duration = Duration::from_secs(2);

/// Rolling obfuscation matching Cursor's `acf` helper.
pub fn acf_obfuscate(input: &[u8]) -> Vec<u8> {
    // JS: let t=165; for (n...) e[n]=(e[n]^t)+n%256; t=e[n]
    let mut out = input.to_vec();
    let mut t: u8 = 165;
    for (n, byte) in out.iter_mut().enumerate() {
        *byte = (*byte ^ t).wrapping_add((n % 256) as u8);
        t = *byte;
    }
    out
}

fn b64_standard_no_pad(bytes: &[u8]) -> String {
    use base64::Engine;
    // The Sand Stream client uses standard RFC 4648 Base64 for the six-byte
    // timestamp prefix. Six bytes encode to eight characters, so stripping
    // padding is a no-op but keeps this helper correct for arbitrary inputs.
    base64::engine::general_purpose::STANDARD_NO_PAD.encode(bytes)
}

/// `sha256(input + salt)` hex, as in cursor-free-vip `generate_hashed64_hex`.
pub fn hashed64_hex(input: &str, salt: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hasher.update(salt.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Derive machine ids from the access token (cursor-free-vip style).
pub fn machine_ids_from_token(token: &str) -> CursorMachineIds {
    let clean = token.trim();
    CursorMachineIds {
        machine_id: Some(hashed64_hex(clean, "machineId")),
        mac_machine_id: Some(hashed64_hex(clean, "macMachineId")),
        dev_device_id: None,
    }
}

/// Build `x-cursor-checksum` for the current wall clock.
pub fn build_cursor_checksum(machine_id: &str, mac_machine_id: Option<&str>) -> String {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    // free-vip: int(time.time()*1000)//1000000  == Date.now()/1e6 in IDE
    let e = (now_ms / 1_000_000) as u64;
    let raw = [
        ((e >> 40) & 0xff) as u8,
        ((e >> 32) & 0xff) as u8,
        ((e >> 24) & 0xff) as u8,
        ((e >> 16) & 0xff) as u8,
        ((e >> 8) & 0xff) as u8,
        (e & 0xff) as u8,
    ];
    let hashed = acf_obfuscate(&raw);
    let prefix = b64_standard_no_pad(&hashed);
    match mac_machine_id.filter(|s| !s.is_empty()) {
        Some(mac) => format!("{prefix}{machine_id}/{mac}"),
        None => format!("{prefix}{machine_id}"),
    }
}

/// Preferred checksum for headless Agent/API calls: token-derived machine ids
/// + acf time.
pub fn build_cursor_checksum_for_token(token: &str) -> String {
    let ids = machine_ids_from_token(token);
    build_cursor_checksum(
        ids.machine_id.as_deref().unwrap_or(""),
        ids.mac_machine_id.as_deref(),
    )
}

/// Build a checksum using the same stable machine identity as Cursor Desktop
/// whenever the local Cursor storage is available.  Sand requests originate
/// from the patched Desktop/local-agent route, so using a token-derived device
/// id here can make the server classify every account as a different machine
/// and reject the request or route it away from managed-local.  If no storage
/// identity exists (for example a fresh headless install), fall back to the
/// deterministic token-derived identity.
pub fn build_cursor_checksum_for_storage_or_token(token: &str) -> String {
    build_cursor_checksum_for_ids_or_token(load_cursor_machine_ids(), token)
}

/// Pure counterpart of [`build_cursor_checksum_for_storage_or_token`] used by
/// callers that already loaded an identity and by tests.  Keeping the fallback
/// decision in one place prevents the buffered and live paths from diverging.
pub fn build_cursor_checksum_for_ids_or_token(ids: CursorMachineIds, token: &str) -> String {
    if let Some(machine_id) = ids
        .machine_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return build_cursor_checksum(machine_id, ids.mac_machine_id.as_deref());
    }
    build_cursor_checksum_for_token(token)
}

#[derive(Debug, Clone, Default)]
pub struct CursorMachineIds {
    pub machine_id: Option<String>,
    pub mac_machine_id: Option<String>,
    pub dev_device_id: Option<String>,
}

/// Fill missing identity fields from `fallback` while retaining explicit
/// values.  Cursor profiles occasionally contain only one telemetry key, and
/// environment overrides are intentionally field-scoped rather than all-or-
/// nothing.
fn merge_machine_ids(
    mut primary: CursorMachineIds,
    fallback: CursorMachineIds,
) -> CursorMachineIds {
    if primary.machine_id.is_none() {
        primary.machine_id = fallback.machine_id;
    }
    if primary.mac_machine_id.is_none() {
        primary.mac_machine_id = fallback.mac_machine_id;
    }
    if primary.dev_device_id.is_none() {
        primary.dev_device_id = fallback.dev_device_id;
    }
    primary
}

/// Resolve machine ids from explicit environment overrides and Cursor's local
/// storage.  Each field is merged independently: setting only
/// `CCP_CURSOR_MACHINE_ID` must not discard `telemetry.macMachineId` from
/// storage, since Desktop sends the pair when both are present.
pub fn load_cursor_machine_ids() -> CursorMachineIds {
    let mut ids = CursorMachineIds::default();

    if let Ok(v) = std::env::var("CCP_CURSOR_MACHINE_ID") {
        let t = v.trim();
        if !t.is_empty() {
            ids.machine_id = Some(t.to_string());
        }
    }
    if let Ok(v) = std::env::var("CCP_CURSOR_MAC_MACHINE_ID") {
        let t = v.trim();
        if !t.is_empty() {
            ids.mac_machine_id = Some(t.to_string());
        }
    }

    // Prefer IDE telemetry ids when present (matches official desktop `ccf`).
    for path in cursor_storage_json_candidates() {
        if let Some(parsed) = read_storage_json(&path) {
            ids = merge_machine_ids(ids, parsed);
            // Do not stop after machineId: macMachineId/devDeviceId may be in
            // a later candidate when users have migrated Cursor profiles.
        }
    }

    // Cursor 3.x can keep telemetry ids in the SQLite state database instead
    // of (or in addition to) storage.json.  Read it as a best-effort fallback
    // and cache the result: checksum construction happens on every request,
    // while these ids are stable for the lifetime of a Cursor profile.
    if ids.machine_id.is_none() || ids.mac_machine_id.is_none() {
        ids = merge_machine_ids(ids, cached_state_db_machine_ids());
    }

    // Fallback: Application Support/Cursor/machineid (UUID-ish device id)
    if ids.machine_id.is_none() {
        for path in cursor_machineid_file_candidates() {
            if let Ok(raw) = fs::read_to_string(&path) {
                let t = raw.trim();
                if !t.is_empty() {
                    ids.machine_id = Some(t.to_string());
                    break;
                }
            }
        }
    }

    ids
}

static STATE_DB_MACHINE_IDS: OnceLock<CursorMachineIds> = OnceLock::new();

fn cached_state_db_machine_ids() -> CursorMachineIds {
    // An explicit path is commonly used by tests and portable installs. Read
    // it directly so a previous/default profile cannot leak into that lookup.
    if std::env::var_os("CCP_CURSOR_STATE_DB").is_some() {
        return cursor_state_db_candidates()
            .into_iter()
            .find_map(|path| read_state_db_machine_ids(&path))
            .unwrap_or_default();
    }
    if let Some(ids) = STATE_DB_MACHINE_IDS.get() {
        return ids.clone();
    }
    for path in cursor_state_db_candidates() {
        if let Some(ids) = read_state_db_machine_ids(&path) {
            // Cache only a positive lookup. If Cursor creates its profile
            // after the proxy starts, a later request gets another chance.
            let _ = STATE_DB_MACHINE_IDS.set(ids.clone());
            return ids;
        }
    }
    CursorMachineIds::default()
}

fn cursor_storage_json_candidates() -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Some(home) = dirs_home() {
        out.push(home.join("Library/Application Support/Cursor/User/globalStorage/storage.json"));
        out.push(home.join(".config/Cursor/User/globalStorage/storage.json"));
        out.push(home.join("AppData/Roaming/Cursor/User/globalStorage/storage.json"));
    }
    out
}

fn cursor_state_db_candidates() -> Vec<PathBuf> {
    if let Some(path) = std::env::var_os("CCP_CURSOR_STATE_DB") {
        let path = PathBuf::from(path);
        return (!path.as_os_str().is_empty())
            .then_some(path)
            .into_iter()
            .collect();
    }
    let mut out = Vec::new();
    if let Some(home) = dirs_home() {
        // Keep all platform layouts here.  This also makes cross-compiled
        // binaries useful when a state directory is mounted from another OS.
        out.push(home.join("Library/Application Support/Cursor/User/globalStorage/state.vscdb"));
        out.push(home.join(".config/Cursor/User/globalStorage/state.vscdb"));
        out.push(home.join("AppData/Roaming/Cursor/User/globalStorage/state.vscdb"));
    }
    out
}

fn cursor_machineid_file_candidates() -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Some(home) = dirs_home() {
        out.push(home.join("Library/Application Support/Cursor/machineid"));
        out.push(home.join(".config/Cursor/machineid"));
        out.push(home.join("AppData/Roaming/Cursor/machineid"));
    }
    out
}

fn dirs_home() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
}

fn read_storage_json(path: &std::path::Path) -> Option<CursorMachineIds> {
    let raw = fs::read_to_string(path).ok()?;
    let v: serde_json::Value = serde_json::from_str(&raw).ok()?;
    Some(CursorMachineIds {
        machine_id: v
            .get("telemetry.machineId")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string()),
        mac_machine_id: v
            .get("telemetry.macMachineId")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string()),
        dev_device_id: v
            .get("telemetry.devDeviceId")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string()),
    })
}

/// Read telemetry identities from Cursor's read-only `state.vscdb`.
///
/// We intentionally invoke the platform sqlite CLI instead of linking a
/// SQLite library: the database may be actively opened by Cursor, and the
/// existing CLI gives us a bounded, read-only snapshot without adding a large
/// native dependency to the proxy binary.  A locked/missing database simply
/// returns `None` and callers continue with storage.json or token ids.
fn read_state_db_machine_ids(path: &std::path::Path) -> Option<CursorMachineIds> {
    if !path.is_file() {
        return None;
    }
    let sqlite = sqlite_binary()?;
    let queries = [
        "SELECT hex(key), hex(value) FROM ItemTable WHERE key IN ('telemetry.machineId','telemetry.macMachineId','telemetry.devDeviceId');",
        "SELECT hex(key), hex(value) FROM cursorDiskKV WHERE key IN ('telemetry.machineId','telemetry.macMachineId','telemetry.devDeviceId');",
    ];
    let mut ids = CursorMachineIds::default();
    for query in queries {
        let Some(output) = run_sqlite_query(sqlite, path, query) else {
            continue;
        };
        if !output.status.success() {
            continue;
        }
        ids = merge_machine_ids(ids, parse_state_db_rows(&output.stdout));
        if ids.machine_id.is_some() && ids.mac_machine_id.is_some() {
            break;
        }
    }
    (ids.machine_id.is_some() || ids.mac_machine_id.is_some() || ids.dev_device_id.is_some())
        .then_some(ids)
}

fn parse_state_db_rows(raw: &[u8]) -> CursorMachineIds {
    let mut ids = CursorMachineIds::default();
    for line in String::from_utf8_lossy(raw).lines() {
        let Some((encoded_key, encoded_value)) = line.split_once('\t') else {
            continue;
        };
        let Some(key) = decode_hex_text(encoded_key) else {
            continue;
        };
        let Some(value) = decode_hex_text(encoded_value)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        match key.as_str() {
            "telemetry.machineId" if ids.machine_id.is_none() => ids.machine_id = Some(value),
            "telemetry.macMachineId" if ids.mac_machine_id.is_none() => {
                ids.mac_machine_id = Some(value)
            }
            "telemetry.devDeviceId" if ids.dev_device_id.is_none() => {
                ids.dev_device_id = Some(value)
            }
            _ => {}
        }
    }
    ids
}

fn sqlite_binary() -> Option<&'static str> {
    [
        "/usr/bin/sqlite3",
        "/opt/homebrew/bin/sqlite3",
        "/usr/local/bin/sqlite3",
    ]
    .into_iter()
    .find(|path| std::path::Path::new(path).is_file())
    .or(Some("sqlite3"))
}

fn run_sqlite_query(sqlite: &str, path: &std::path::Path, query: &str) -> Option<Output> {
    let mut child = Command::new(sqlite)
        .args(["-readonly", "-batch", "-noheader", "-separator", "\t"])
        .arg(path)
        .arg(query)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .ok()?;
    let deadline = std::time::Instant::now() + STATE_DB_QUERY_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return child.wait_with_output().ok(),
            Ok(None) if std::time::Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
            Ok(None) => thread::sleep(Duration::from_millis(20)),
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    }
}

fn decode_hex_text(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(raw.len() / 2);
    for index in (0..raw.len()).step_by(2) {
        bytes.push(u8::from_str_radix(&raw[index..index + 2], 16).ok()?);
    }
    String::from_utf8(bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acf_matches_js_seed_example() {
        // Deterministic: fixed 6-byte input
        let input = [0u8, 0, 0, 0, 0, 1];
        let out = acf_obfuscate(&input);
        // Hand-walk JS:
        // t=165
        // n=0: (0^165)+0 = 165, t=165
        // n=1: (0^165)+1 = 166, t=166
        // n=2: (0^166)+2 = 168, t=168
        // n=3: (0^168)+3 = 171, t=171
        // n=4: (0^171)+4 = 175, t=175
        // n=5: (1^175)+5 = 175+5? 1^175=174, +5=179
        assert_eq!(out, vec![165, 166, 168, 171, 175, 179]);
    }

    #[test]
    fn checksum_contains_machine_id() {
        let cs = build_cursor_checksum("abc", Some("def"));
        assert!(cs.contains("abc/def"), "{cs}");
        // prefix is base64 of 6 bytes -> 8 chars
        assert!(cs.len() > 8 + 3);
    }

    #[test]
    fn checksum_prefix_uses_standard_base64() {
        // Sand Stream uses the standard `+`/`/` alphabet.
        assert_eq!(b64_standard_no_pad(&[0xfb, 0xef, 0xbe]), "++++");
        assert_eq!(b64_standard_no_pad(&[0xff, 0xff, 0xff]), "////");
    }

    #[test]
    fn token_derived_ids_are_stable_sha256() {
        let ids = machine_ids_from_token("tok");
        assert_eq!(
            ids.machine_id.as_deref(),
            Some(hashed64_hex("tok", "machineId").as_str())
        );
        assert_eq!(
            ids.mac_machine_id.as_deref(),
            Some(hashed64_hex("tok", "macMachineId").as_str())
        );
    }

    #[test]
    fn storage_or_token_checksum_prefers_persisted_machine_identity_when_present() {
        // Keep this test independent of the host's Cursor installation.  The
        // pure builder is used by the integration path after the storage
        // lookup, so it verifies that a persisted pair is encoded as
        // `machine/mac` rather than silently switching to token-derived ids.
        let persisted = build_cursor_checksum_for_ids_or_token(
            CursorMachineIds {
                machine_id: Some("machine-id".into()),
                mac_machine_id: Some("mac-id".into()),
                dev_device_id: None,
            },
            "tok",
        );
        assert!(persisted.contains("machine-id/mac-id"));

        let token = build_cursor_checksum_for_ids_or_token(CursorMachineIds::default(), "tok");
        assert!(token.contains(&hashed64_hex("tok", "machineId")));
    }

    #[test]
    fn load_machine_ids_merges_partial_environment_overrides() {
        // Exercise the merge behavior without mutating process environment:
        // this guards the field-wise semantics used by `load_cursor_machine_ids`.
        let ids = merge_machine_ids(
            CursorMachineIds {
                machine_id: Some("env-machine".into()),
                mac_machine_id: None,
                dev_device_id: None,
            },
            CursorMachineIds {
                machine_id: Some("storage-machine".into()),
                mac_machine_id: Some("storage-mac".into()),
                dev_device_id: Some("storage-device".into()),
            },
        );
        assert_eq!(ids.machine_id.as_deref(), Some("env-machine"));
        assert_eq!(ids.mac_machine_id.as_deref(), Some("storage-mac"));
        assert_eq!(ids.dev_device_id.as_deref(), Some("storage-device"));
    }

    #[test]
    fn state_db_rows_supply_cursor_telemetry_identity() {
        fn hex_text(value: &str) -> String {
            value
                .as_bytes()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect()
        }

        let rows = format!(
            "{}\t{}\n{}\t{}\n{}\t{}\nmalformed\n",
            hex_text("telemetry.machineId"),
            hex_text("machine-from-db"),
            hex_text("telemetry.macMachineId"),
            hex_text("mac-from-db"),
            hex_text("telemetry.devDeviceId"),
            hex_text("device-from-db"),
        );
        let ids = parse_state_db_rows(rows.as_bytes());
        assert_eq!(ids.machine_id.as_deref(), Some("machine-from-db"));
        assert_eq!(ids.mac_machine_id.as_deref(), Some("mac-from-db"));
        assert_eq!(ids.dev_device_id.as_deref(), Some("device-from-db"));
    }
}
