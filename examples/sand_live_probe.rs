use claude_cursor_proxy::providers::cursor::sand_inference::{
    SandInferenceClient, SandInferenceMessage, SandInferenceRequest,
};
use futures_util::StreamExt;
use serde::Deserialize;
use std::time::Instant;

#[derive(Deserialize)]
struct Auth {
    #[serde(rename = "accessToken")]
    access_token: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let path = std::env::var("CCP_CURSOR_AUTH_FILE").unwrap_or_else(|_| {
        format!(
            "{}/.config/claude-cursor-proxy/cursor/auth.json",
            std::env::var("HOME").unwrap()
        )
    });
    let auth: Auth = serde_json::from_slice(&std::fs::read(path)?)?;
    let model =
        std::env::var("CCP_CURSOR_SAND_PROBE_MODEL").unwrap_or_else(|_| "claude-fable-5".into());
    let timeout = std::env::var("CCP_CURSOR_SAND_PROBE_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);
    let client = SandInferenceClient::with_base_url_timeout(
        std::env::var("CCP_CURSOR_SAND_BASE_URL")
            .unwrap_or_else(|_| "https://api2.cursor.sh".into()),
        timeout,
    )?;
    let req = SandInferenceRequest::new(
        model,
        uuid::Uuid::new_v4().to_string(),
        uuid::Uuid::new_v4().to_string(),
        vec![SandInferenceMessage::user("Reply exactly pong")],
    );
    let start = Instant::now();
    eprintln!("open start timeout={timeout}s");
    let mut stream = client.open(&auth.access_token, &req).await?;
    eprintln!("open accepted {:.2}s", start.elapsed().as_secs_f64());
    while let Some(event) = stream.next().await {
        eprintln!("event {:.2}s {:?}", start.elapsed().as_secs_f64(), event?);
    }
    eprintln!("done {:.2}s", start.elapsed().as_secs_f64());
    Ok(())
}
