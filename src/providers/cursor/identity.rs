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
//! Machine ids: unofficial clients commonly derive them as
//! `sha256(token + "machineId")` / `sha256(token + "macMachineId")` (see
//! cursor-free-vip). IDE traffic instead uses `telemetry.machineId` from
//! storage.json. We prefer token-derived ids for API calls (matches free-vip
//! DashboardService requests), with storage.json as fallback.

use sha2::{Digest, Sha256};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

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

fn b64_std(bytes: &[u8]) -> String {
    use base64::Engine;
    // free-vip uses standard base64; 6-byte blocks need no padding.
    base64::engine::general_purpose::STANDARD.encode(bytes)
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
    let prefix = b64_std(&hashed);
    match mac_machine_id.filter(|s| !s.is_empty()) {
        Some(mac) => format!("{prefix}{machine_id}/{mac}"),
        None => format!("{prefix}{machine_id}"),
    }
}

/// Preferred checksum for Agent/API calls: token-derived machine ids + acf time.
pub fn build_cursor_checksum_for_token(token: &str) -> String {
    let ids = machine_ids_from_token(token);
    build_cursor_checksum(
        ids.machine_id.as_deref().unwrap_or(""),
        ids.mac_machine_id.as_deref(),
    )
}

#[derive(Debug, Clone, Default)]
pub struct CursorMachineIds {
    pub machine_id: Option<String>,
    pub mac_machine_id: Option<String>,
    pub dev_device_id: Option<String>,
}

/// Resolve machine ids: env → token-derived (if token given later) → IDE storage.
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

    if ids.machine_id.is_some() {
        return ids;
    }

    // Prefer IDE telemetry ids when present (matches official desktop `ccf`).
    for path in cursor_storage_json_candidates() {
        if let Some(parsed) = read_storage_json(&path) {
            if ids.machine_id.is_none() {
                ids.machine_id = parsed.machine_id;
            }
            if ids.mac_machine_id.is_none() {
                ids.mac_machine_id = parsed.mac_machine_id;
            }
            if ids.dev_device_id.is_none() {
                ids.dev_device_id = parsed.dev_device_id;
            }
            if ids.machine_id.is_some() {
                break;
            }
        }
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

fn cursor_storage_json_candidates() -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Some(home) = dirs_home() {
        out.push(home.join("Library/Application Support/Cursor/User/globalStorage/storage.json"));
        out.push(home.join(".config/Cursor/User/globalStorage/storage.json"));
        out.push(home.join("AppData/Roaming/Cursor/User/globalStorage/storage.json"));
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
}
