use assert_cmd::Command;
use base64::Engine;
use predicates::str::contains;
use std::env;
use tempfile::TempDir;

#[test]
fn version_aliases_print_expected_version() -> Result<(), Box<dyn std::error::Error>> {
    let expected = format!("claude-cursor-proxy {}", env!("CARGO_PKG_VERSION"));

    for arg in ["--version", "-v", "version"] {
        let mut cmd = Command::cargo_bin("claude-cursor-proxy")?;
        cmd.arg(arg)
            .assert()
            .success()
            .stdout(contains(expected.clone()));
    }
    Ok(())
}

#[test]
fn models_prints_all_providers() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("claude-cursor-proxy")?;
    cmd.arg("models");
    let out = String::from_utf8(cmd.output()?.stdout)?;
    assert!(out.contains("codex:"));
    assert!(out.contains("kimi:"));
    assert!(out.contains("cursor:"));

    let mut cmd = Command::cargo_bin("claude-cursor-proxy")?;
    cmd.args(["models", "--full"]);
    cmd.output()?;
    Ok(())
}

#[test]
fn help_discovers_serverless_tui_demo() -> Result<(), Box<dyn std::error::Error>> {
    Command::cargo_bin("claude-cursor-proxy")?
        .arg("--help")
        .assert()
        .success()
        .stdout(contains("demo"))
        .stdout(contains("mock data and no proxy server"));
    Ok(())
}

#[test]
fn invalid_command_exits_two() -> Result<(), Box<dyn std::error::Error>> {
    Command::cargo_bin("claude-cursor-proxy")?
        .arg("definitely-not-a-command")
        .assert()
        .failure()
        .code(2);
    Ok(())
}

#[test]
fn unsupported_provider_auth_command_exits_two() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("claude-cursor-proxy")?;
    cmd.args(["cursor", "auth", "device"]);
    let output = cmd.output()?;
    assert_eq!(output.status.code(), Some(2));
    let out = String::from_utf8(output.stderr)?;
    assert!(out.contains("not yet implemented") || out.contains("unsupported"));
    Ok(())
}

#[test]
fn provider_logout_without_auth_is_success() -> Result<(), Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let mut cmd = Command::cargo_bin("claude-cursor-proxy")?;
    cmd.args(["kimi", "auth", "logout"]);
    cmd.env("CCP_CONFIG_DIR", temp.path());
    cmd.assert().success();
    Ok(())
}

#[test]
fn models_output_is_stable_order() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("claude-cursor-proxy")?;
    cmd.args(["models", "--full"]);
    let output = cmd.output()?;
    let out = String::from_utf8(output.stdout)?;
    let codex_pos = out.find("codex:").unwrap_or(0);
    let kimi_pos = out.find("kimi:").unwrap_or(0);
    let cursor_pos = out.find("cursor:").unwrap_or(0);
    assert!(codex_pos < kimi_pos);
    assert!(kimi_pos < cursor_pos);
    Ok(())
}

#[test]
fn kimi_auth_status_reads_stored_auth() -> Result<(), Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let auth_dir = temp.path().join("kimi");
    std::fs::create_dir_all(&auth_dir)?;
    std::fs::write(
        auth_dir.join("auth.json"),
        r#"{"access":"a","refresh":"r","expires":4102444800000,"scope":"openid","userId":"u"}"#,
    )?;
    let mut cmd = Command::cargo_bin("claude-cursor-proxy")?;
    cmd.args(["kimi", "auth", "status"]);
    cmd.env("CCP_CONFIG_DIR", temp.path());
    cmd.assert().success().stdout(contains("User: u"));
    Ok(())
}

#[test]
fn cursor_auth_list_reads_multi_account_registry() -> Result<(), Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let auth_dir = temp.path().join("cursor");
    std::fs::create_dir_all(&auth_dir)?;
    std::fs::write(
        auth_dir.join("accounts.json"),
        r#"{"activeId":"account-a","accounts":[{"id":"account-a","label":"primary","auth":{"accessToken":"token-a"}},{"id":"account-b","label":"backup","auth":{"accessToken":"token-b"}}]}"#,
    )?;

    let mut cmd = Command::cargo_bin("claude-cursor-proxy")?;
    cmd.args(["cursor", "auth", "list"]);
    cmd.env("CCP_CONFIG_DIR", temp.path());
    cmd.assert()
        .success()
        .stdout(contains("Cursor accounts (2):"))
        .stdout(contains("* account-a"))
        .stdout(contains("account-b"));
    Ok(())
}

#[test]
fn cursor_sand_status_reports_managed_local_markers_without_network()
-> Result<(), Box<dyn std::error::Error>> {
    // Sand status is intentionally a local diagnostic: it must be useful
    // before login and must not spend quota just to inspect the route.
    let temp = TempDir::new()?;
    let mut cmd = Command::cargo_bin("claude-cursor-proxy")?;
    let output = cmd
        .args(["cursor", "sand-status"])
        .env("CCP_CONFIG_DIR", temp.path())
        .env("CCP_CURSOR_SAND_MODELS", "gemini-3.1-pro,claude-fable-5")
        .env("CCP_CURSOR_CLI_KEYCHAIN_FALLBACK", "0")
        .env_remove("CCP_CURSOR_AUTH_TOKEN")
        .env_remove("CURSOR_AUTH_TOKEN")
        .output()?;
    assert!(output.status.success(), "{:?}", output);
    let text = String::from_utf8(output.stdout)?;
    assert!(text.contains("SandClientMode status"));
    assert!(text.contains("routing: enabled"));
    assert!(text.contains("managed-local: ready"));
    assert!(text.contains("local-runtime: ready"));
    assert!(text.contains("direct-stream: ready"));
    assert!(text.contains("transport: h2-only"));
    assert!(!text.contains("accessToken"));
    assert!(!text.contains("refreshToken"));
    Ok(())
}

#[test]
fn cursor_sand_status_json_exposes_policy_and_account_free_cache_state()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let mut cmd = Command::cargo_bin("claude-cursor-proxy")?;
    let output = cmd
        .args(["cursor", "sand-status", "--json"])
        .env("CCP_CONFIG_DIR", temp.path())
        .env("CCP_CURSOR_SAND_MODELS", "gemini-3.6-flash")
        .env("CCP_CURSOR_BASE_URL", "http://127.0.0.1:1")
        .env("CCP_CURSOR_CLI_KEYCHAIN_FALLBACK", "0")
        .env_remove("CCP_CURSOR_AUTH_TOKEN")
        .env_remove("CURSOR_AUTH_TOKEN")
        .output()?;
    assert!(output.status.success(), "{:?}", output);
    let value: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(value["protocol"], "sand-client-mode");
    assert_eq!(value["enabled"], true);
    assert_eq!(value["modelPatterns"][0], "gemini-3.6-flash");
    assert_eq!(value["markers"]["managedLocalRoute"], true);
    assert_eq!(value["markers"]["localRuntimeLoad"], true);
    assert_eq!(value["markers"]["directStream"], true);
    assert_eq!(value["transport"], "h2-prior-knowledge");
    assert_eq!(value["accounts"].as_array().map(Vec::len), Some(0));
    assert!(value["usageCachePath"].as_str().is_some());
    // The JSON report is safe to log: no credential fields or bearer values.
    let encoded = serde_json::to_string(&value)?;
    assert!(!encoded.contains("accessToken"));
    assert!(!encoded.contains("refreshToken"));
    Ok(())
}

#[test]
fn cursor_auth_use_switches_legacy_active_file() -> Result<(), Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let auth_dir = temp.path().join("cursor");
    std::fs::create_dir_all(&auth_dir)?;
    std::fs::write(
        auth_dir.join("accounts.json"),
        r#"{"activeId":"account-a","accounts":[{"id":"account-a","label":"Primary","auth":{"accessToken":"token-a"}},{"id":"account-b","label":"Backup","auth":{"accessToken":"token-b"}}]}"#,
    )?;

    let mut cmd = Command::cargo_bin("claude-cursor-proxy")?;
    cmd.args(["cursor", "auth", "use", "  backup  "]);
    cmd.env("CCP_CONFIG_DIR", temp.path());
    cmd.assert()
        .success()
        .stdout(contains("Account id: account-b"));

    let active: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(auth_dir.join("auth.json"))?)?;
    assert_eq!(active["accessToken"], "token-b");
    let accounts: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(auth_dir.join("accounts.json"))?)?;
    assert_eq!(accounts["activeId"], "account-b");
    Ok(())
}

#[test]
fn cursor_auth_list_keeps_expired_legacy_auth_visible() -> Result<(), Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let auth_dir = temp.path().join("cursor");
    std::fs::create_dir_all(&auth_dir)?;
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(br#"{"exp":1,"sub":"legacy-user","email":"legacy@example.com"}"#);
    let token = format!("e30.{payload}.sig");
    std::fs::write(
        auth_dir.join("auth.json"),
        serde_json::json!({
            "accessToken": token,
            "refreshToken": "legacy-refresh"
        })
        .to_string(),
    )?;

    let mut cmd = Command::cargo_bin("claude-cursor-proxy")?;
    cmd.args(["cursor", "auth", "list"])
        .env("CCP_CONFIG_DIR", temp.path())
        // Force the refresh probe to fail quickly; list must then use the raw
        // auth.json credential instead of dropping the legacy account.
        .env("CCP_CURSOR_BASE_URL", "http://127.0.0.1:1")
        .assert()
        .success()
        .stdout(contains("legacy@example.com"));
    Ok(())
}

#[test]
fn cursor_auth_remove_does_not_mutate_pool_when_env_auth_is_active()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let auth_dir = temp.path().join("cursor");
    std::fs::create_dir_all(&auth_dir)?;
    let registry = r#"{"activeId":"account-a","accounts":[{"id":"account-a","auth":{"accessToken":"token-a"}}]}"#;
    std::fs::write(auth_dir.join("accounts.json"), registry)?;

    let mut cmd = Command::cargo_bin("claude-cursor-proxy")?;
    cmd.args(["cursor", "auth", "remove", "account-a"])
        .env("CCP_CONFIG_DIR", temp.path())
        .env("CCP_CURSOR_AUTH_TOKEN", "environment-token")
        .assert()
        .failure()
        .stderr(contains("environment Cursor token"));
    assert_eq!(
        std::fs::read_to_string(auth_dir.join("accounts.json"))?,
        registry
    );
    Ok(())
}

#[test]
fn cursor_auth_remove_migrates_and_removes_legacy_single_account()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let auth_dir = temp.path().join("cursor");
    std::fs::create_dir_all(&auth_dir)?;
    std::fs::write(
        auth_dir.join("auth.json"),
        serde_json::json!({"accessToken": "legacy-token"}).to_string(),
    )?;

    let mut list = Command::cargo_bin("claude-cursor-proxy")?;
    let output = list
        .args(["cursor", "auth", "list"])
        .env("CCP_CONFIG_DIR", temp.path())
        .output()?;
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout)?;
    let id = text
        .lines()
        .filter(|line| line.trim_start().starts_with('*'))
        .find_map(|line| line.split_whitespace().nth(1))
        .ok_or("legacy account id missing")?
        .to_string();

    let mut remove = Command::cargo_bin("claude-cursor-proxy")?;
    remove
        .args(["cursor", "auth", "remove", &id])
        .env("CCP_CONFIG_DIR", temp.path())
        .assert()
        .success()
        .stdout(contains("No Cursor account is active."));
    let accounts: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(auth_dir.join("accounts.json"))?)?;
    assert_eq!(accounts["activeId"], serde_json::Value::Null);
    assert!(accounts["accounts"].as_array().is_some_and(Vec::is_empty));
    assert!(!auth_dir.join("auth.json").exists());
    Ok(())
}

#[test]
fn cursor_auth_remove_inactive_keeps_active_mirror_and_reports_it()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let auth_dir = temp.path().join("cursor");
    std::fs::create_dir_all(&auth_dir)?;
    std::fs::write(
        auth_dir.join("accounts.json"),
        r#"{"activeId":"account-a","accounts":[{"id":"account-a","label":"primary","auth":{"accessToken":"token-a"}},{"id":"account-b","label":"backup","auth":{"accessToken":"token-b"}}]}"#,
    )?;
    std::fs::write(auth_dir.join("auth.json"), r#"{"accessToken":"token-a"}"#)?;

    let mut remove = Command::cargo_bin("claude-cursor-proxy")?;
    remove
        .args(["cursor", "auth", "remove", "account-b"])
        .env("CCP_CONFIG_DIR", temp.path())
        .assert()
        .success()
        .stdout(contains("Active Cursor account"));
    let active: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(auth_dir.join("auth.json"))?)?;
    assert_eq!(active["accessToken"], "token-a");
    let accounts: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(auth_dir.join("accounts.json"))?)?;
    assert_eq!(accounts["activeId"], "account-a");
    assert_eq!(accounts["accounts"].as_array().map(Vec::len), Some(1));
    Ok(())
}
