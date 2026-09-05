//! Bounded, process-lifetime metrics. Collection shares the lifecycle lock;
//! exposition never rebuilds TUI sessions or retains request/session IDs.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::time::{Duration, Instant};

use super::{ActiveRequest, EndpointKind, MonitorStore, RequestStatus};

const MAX_LABEL_VALUES: usize = 256;
const MAX_LABEL_BYTES: usize = 96;
const OVERFLOW: &str = "__other__";
const BUCKETS: [f64; 11] = [
    0.01, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0,
];

#[derive(Debug, Clone, Default)]
pub(super) struct RequestMetrics {
    pub(super) account: Option<String>,
    queued_at: Option<Instant>,
    opening_at: Option<Instant>,
    first_byte_at: Option<Instant>,
}

#[derive(Debug, Clone, Default)]
struct Histogram {
    buckets: [u64; BUCKETS.len() + 1],
    sum: f64,
    count: u64,
}

impl Histogram {
    fn observe(&mut self, duration: Duration) {
        let seconds = duration.as_secs_f64();
        let bucket = BUCKETS
            .iter()
            .position(|bound| seconds <= *bound)
            .unwrap_or(BUCKETS.len());
        self.buckets[bucket] = self.buckets[bucket].saturating_add(1);
        self.sum += seconds;
        self.count = self.count.saturating_add(1);
    }

    fn render(&self, out: &mut String, name: &str, help: &str) {
        let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} histogram");
        let mut cumulative = 0u64;
        for (index, count) in self.buckets.iter().enumerate() {
            cumulative = cumulative.saturating_add(*count);
            let bound = BUCKETS
                .get(index)
                .map(ToString::to_string)
                .unwrap_or_else(|| "+Inf".to_string());
            let _ = writeln!(out, "{name}_bucket{{le=\"{bound}\"}} {cumulative}");
        }
        let _ = writeln!(out, "{name}_sum {}\n{name}_count {}", self.sum, self.count);
    }
}

#[derive(Debug, Clone, Default)]
struct Labels(BTreeMap<String, u64>);

impl Labels {
    fn increment(&mut self, value: &str) {
        let value = if value.len() > MAX_LABEL_BYTES {
            OVERFLOW
        } else {
            value
        };
        let value = if self.0.contains_key(value) || self.0.len() < MAX_LABEL_VALUES {
            value
        } else {
            OVERFLOW
        };
        let count = self.0.entry(value.to_string()).or_default();
        *count = count.saturating_add(1);
    }

    fn render(&self, out: &mut String, name: &str, label: &str, kind: &str, help: &str) {
        let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}");
        for (value, count) in &self.0 {
            let _ = writeln!(out, "{name}{{{label}=\"{}\"}} {count}", escape_label(value));
        }
    }
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[derive(Debug, Clone, Default)]
pub(super) struct MetricsStore {
    started: [u64; 2],
    outcomes: [u64; 3],
    retries: u64,
    terminal_codes: BTreeMap<u16, u64>,
    upstream_codes: BTreeMap<u16, u64>,
    providers: Labels,
    models: Labels,
    clients: Labels,
    accounts: Labels,
    queue_wait: Histogram,
    opening: Histogram,
    first_byte: Histogram,
    duration: Histogram,
    stream_lifetime: Histogram,
}

impl MetricsStore {
    pub(super) fn request_started(&mut self, endpoint: EndpointKind) {
        let index = match endpoint {
            EndpointKind::Messages => 0,
            EndpointKind::CountTokens => 1,
        };
        self.started[index] = self.started[index].saturating_add(1);
    }

    pub(super) fn phase_changed(&mut self, active: &mut ActiveRequest, phase: &RequestStatus) {
        if *phase == RequestStatus::Retrying {
            self.retries = self.retries.saturating_add(1);
        }
        if active.status == *phase {
            return;
        }
        if let Some(started) = active.metrics.queued_at.take() {
            self.queue_wait.observe(started.elapsed());
        }
        if let Some(started) = active.metrics.opening_at.take() {
            self.opening.observe(started.elapsed());
        }
        match phase {
            RequestStatus::Queued => active.metrics.queued_at = Some(Instant::now()),
            RequestStatus::Opening => active.metrics.opening_at = Some(Instant::now()),
            _ => {}
        }
    }

    pub(super) fn stream_progress(&mut self, active: &mut ActiveRequest, bytes: u64) {
        if bytes > 0 && active.metrics.first_byte_at.is_none() {
            self.first_byte.observe(active.started_instant.elapsed());
            active.metrics.first_byte_at = Some(Instant::now());
        }
        // StreamProgress owns the status transition in monitor.rs; only close
        // any pending queue/open phase timers here.
        if let Some(started) = active.metrics.queued_at.take() {
            self.queue_wait.observe(started.elapsed());
        }
        if let Some(started) = active.metrics.opening_at.take() {
            self.opening.observe(started.elapsed());
        }
    }

    pub(super) fn upstream_error(&mut self, status: u16) {
        increment_code(&mut self.upstream_codes, status);
    }

    pub(super) fn request_finished(
        &mut self,
        active: &ActiveRequest,
        status: &RequestStatus,
        code: Option<u16>,
    ) {
        let index = match status {
            RequestStatus::Completed => 0,
            RequestStatus::Failed => 1,
            RequestStatus::Abandoned => 2,
            _ => return,
        };
        self.outcomes[index] = self.outcomes[index].saturating_add(1);
        increment_code(&mut self.terminal_codes, code.unwrap_or(0));
        self.duration.observe(active.started_instant.elapsed());
        if let Some(started) = active.metrics.queued_at {
            self.queue_wait.observe(started.elapsed());
        }
        if let Some(started) = active.metrics.opening_at {
            self.opening.observe(started.elapsed());
        }
        if let Some(started) = active.metrics.first_byte_at {
            self.stream_lifetime.observe(started.elapsed());
        }
        self.providers
            .increment(active.provider.as_deref().unwrap_or("unknown"));
        self.models
            .increment(active.model.as_deref().unwrap_or("unknown"));
        self.clients
            .increment(active.client_type.as_deref().unwrap_or("unknown"));
        self.accounts
            .increment(active.metrics.account.as_deref().unwrap_or("unknown"));
    }
}

fn increment_code(codes: &mut BTreeMap<u16, u64>, status: u16) {
    let status = if (100..=599).contains(&status) {
        status
    } else {
        0
    };
    let count = codes.entry(status).or_default();
    *count = count.saturating_add(1);
}

#[derive(Debug, Clone, Default)]
pub struct MetricsSnapshot {
    totals: MetricsStore,
    active: usize,
    phases: Labels,
    providers: Labels,
    models: Labels,
    clients: Labels,
    accounts: Labels,
    recent: usize,
    recent_outcomes: [usize; 3],
}

impl MetricsSnapshot {
    pub(super) fn new(store: &MonitorStore) -> Self {
        let mut snapshot = Self {
            totals: store.metrics.clone(),
            active: store.active.len(),
            recent: store.recent.len(),
            ..Self::default()
        };
        for phase in [
            "started",
            "selected",
            "upstream",
            "queued",
            "opening",
            "retrying",
            "streaming",
            "waiting_tool",
        ] {
            snapshot.phases.0.insert(phase.to_string(), 0);
        }
        for active in store.active.values() {
            snapshot.phases.increment(active.status.label());
            if let Some(value) = active.provider.as_deref() {
                snapshot.providers.increment(value);
            }
            if let Some(value) = active.model.as_deref() {
                snapshot.models.increment(value);
            }
            if let Some(value) = active.client_type.as_deref() {
                snapshot.clients.increment(value);
            }
            if let Some(value) = active.metrics.account.as_deref() {
                snapshot.accounts.increment(value);
            }
        }
        for recent in &store.recent {
            let index = match recent.status {
                RequestStatus::Completed => 0,
                RequestStatus::Failed => 1,
                RequestStatus::Abandoned => 2,
                _ => continue,
            };
            snapshot.recent_outcomes[index] += 1;
        }
        snapshot
    }

    pub fn render_prometheus(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(
            out,
            "# HELP ccp_active_requests Current monitored requests.\n# TYPE ccp_active_requests gauge\nccp_active_requests {}",
            self.active
        );
        self.phases.render(
            &mut out,
            "ccp_active_requests_phase",
            "phase",
            "gauge",
            "Current requests by reported lifecycle phase.",
        );
        self.providers.render(
            &mut out,
            "ccp_active_requests_provider",
            "provider",
            "gauge",
            "Current requests by provider.",
        );
        self.models.render(
            &mut out,
            "ccp_active_requests_model",
            "model",
            "gauge",
            "Current requests by model.",
        );
        self.clients.render(
            &mut out,
            "ccp_active_requests_client",
            "client_type",
            "gauge",
            "Current requests by client type.",
        );
        self.accounts.render(
            &mut out,
            "ccp_active_requests_account",
            "account",
            "gauge",
            "Current requests by opaque account identifier.",
        );
        let _ = writeln!(
            out,
            "# HELP ccp_recent_requests Retained terminal requests; a rolling window, not a counter.\n# TYPE ccp_recent_requests gauge\nccp_recent_requests {}",
            self.recent
        );
        let _ = writeln!(
            out,
            "# HELP ccp_recent_request_outcomes Terminal outcomes in the retained rolling window.\n# TYPE ccp_recent_request_outcomes gauge"
        );
        for (index, status) in ["completed", "failed", "abandoned"].iter().enumerate() {
            let _ = writeln!(
                out,
                "ccp_recent_request_outcomes{{status=\"{status}\"}} {}",
                self.recent_outcomes[index]
            );
        }
        let _ = writeln!(
            out,
            "# HELP ccp_requests_started_total Monitored requests since process start.\n# TYPE ccp_requests_started_total counter"
        );
        for (index, endpoint) in ["messages", "count_tokens"].iter().enumerate() {
            let _ = writeln!(
                out,
                "ccp_requests_started_total{{endpoint=\"{endpoint}\"}} {}",
                self.totals.started[index]
            );
        }
        let _ = writeln!(
            out,
            "# HELP ccp_requests_finished_total First terminal outcomes since process start.\n# TYPE ccp_requests_finished_total counter"
        );
        for (index, status) in ["completed", "failed", "abandoned"].iter().enumerate() {
            let _ = writeln!(
                out,
                "ccp_requests_finished_total{{status=\"{status}\"}} {}",
                self.totals.outcomes[index]
            );
        }
        let _ = writeln!(
            out,
            "# HELP ccp_retry_events_total Reported retry lifecycle events, excluding unreported internal transport attempts.\n# TYPE ccp_retry_events_total counter\nccp_retry_events_total {}",
            self.totals.retries
        );
        render_codes(
            &mut out,
            "ccp_terminal_http_status_total",
            &self.totals.terminal_codes,
            "Terminal response HTTP status; 0 indicates no HTTP status, and SSE errors may retain HTTP 200.",
        );
        render_codes(
            &mut out,
            "ccp_upstream_errors_total",
            &self.totals.upstream_codes,
            "Reported upstream attempt failures by HTTP status, independent of final client outcome.",
        );
        self.totals.providers.render(
            &mut out,
            "ccp_requests_finished_provider_total",
            "provider",
            "counter",
            "Terminal requests by provider.",
        );
        self.totals.models.render(
            &mut out,
            "ccp_requests_finished_model_total",
            "model",
            "counter",
            "Terminal requests by model.",
        );
        self.totals.clients.render(
            &mut out,
            "ccp_requests_finished_client_total",
            "client_type",
            "counter",
            "Terminal requests by client type.",
        );
        self.totals.accounts.render(
            &mut out,
            "ccp_requests_finished_account_total",
            "account",
            "counter",
            "Terminal requests by opaque account identifier.",
        );
        self.totals.queue_wait.render(
            &mut out,
            "ccp_queue_phase_seconds",
            "Time in explicitly reported queued phases, including abandoned queue waits.",
        );
        self.totals.opening.render(
            &mut out,
            "ccp_opening_phase_seconds",
            "Time in reported opening phases; may include provider policy waits.",
        );
        self.totals.first_byte.render(&mut out, "ccp_first_response_byte_seconds", "Time to first observed downstream response bytes; includes protocol frames, not model TTFT.");
        self.totals.duration.render(
            &mut out,
            "ccp_request_duration_seconds",
            "Request lifetime until the first terminal outcome.",
        );
        self.totals.stream_lifetime.render(
            &mut out,
            "ccp_response_stream_seconds",
            "Time from first observed downstream bytes to terminal outcome.",
        );
        out
    }
}

fn render_codes(out: &mut String, name: &str, codes: &BTreeMap<u16, u64>, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} counter");
    for (code, count) in codes {
        let _ = writeln!(out, "{name}{{status=\"{code}\"}} {count}");
    }
    for code in [429, 502, 503, 504] {
        if !codes.contains_key(&code) {
            let _ = writeln!(out, "{name}{{status=\"{code}\"}} 0");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitor::MonitorHandle;

    #[test]
    fn labels_escape_prometheus_control_characters() {
        assert_eq!(escape_label("a\\b\"c\nd"), "a\\\\b\\\"c\\nd");
    }

    #[test]
    fn labels_have_bounded_cardinality_without_losing_total() {
        let mut labels = Labels::default();
        for index in 0..2048 {
            labels.increment(&format!("model-{index}"));
        }
        assert!(labels.0.len() <= MAX_LABEL_VALUES + 1);
        assert_eq!(labels.0.values().sum::<u64>(), 2048);
        labels.increment(&"x".repeat(10_000));
        assert_eq!(labels.0.values().sum::<u64>(), 2049);
    }

    #[test]
    fn histogram_exports_cumulative_buckets() {
        let mut histogram = Histogram::default();
        for seconds in [0.1, 2.0, 301.0] {
            histogram.observe(Duration::from_secs_f64(seconds));
        }
        let mut out = String::new();
        histogram.render(&mut out, "test_seconds", "Test.");
        assert!(out.contains("test_seconds_bucket{le=\"0.1\"} 1\n"));
        assert!(out.contains("test_seconds_bucket{le=\"2\"} 2\n"));
        assert!(out.contains("test_seconds_bucket{le=\"+Inf\"} 3\n"));
        assert!(out.contains("test_seconds_count 3\n"));
    }

    #[test]
    fn cumulative_metrics_survive_recent_eviction_and_ignore_duplicate_terminals() {
        let monitor = MonitorHandle::new(1);
        for (id, code) in [("a", 429), ("b", 502), ("c", 503), ("d", 504)] {
            monitor.request_started(id, None, None, EndpointKind::Messages);
            monitor.provider_selected(id, "cursor", "test-model", None);
            monitor.client_type_resolved(id, "sand");
            monitor.account_resolved(id, "opaque-account-1");
            monitor.queued(id);
            monitor.opening(id);
            monitor.upstream_error(id, code);
            monitor.retrying(id);
            monitor.request_failed(id, Some(code), "test");
            monitor.request_failed(id, Some(code), "duplicate terminal");
        }
        let out = monitor.metrics_snapshot().render_prometheus();
        assert!(out.contains("ccp_requests_started_total{endpoint=\"messages\"} 4\n"));
        assert!(out.contains("ccp_requests_finished_total{status=\"failed\"} 4\n"));
        assert!(out.contains("ccp_retry_events_total 4\n"));
        assert!(out.contains("ccp_queue_phase_seconds_count 4\n"));
        assert!(out.contains("ccp_opening_phase_seconds_count 4\n"));
        assert!(out.contains("ccp_request_duration_seconds_count 4\n"));
        assert!(
            out.contains("ccp_requests_finished_account_total{account=\"opaque-account-1\"} 4\n")
        );
        for code in [429, 502, 503, 504] {
            assert!(out.contains(&format!(
                "ccp_terminal_http_status_total{{status=\"{code}\"}} 1\n"
            )));
            assert!(out.contains(&format!(
                "ccp_upstream_errors_total{{status=\"{code}\"}} 1\n"
            )));
        }
    }

    #[test]
    fn first_response_bytes_and_stream_lifetime_are_observed_once() {
        let monitor = MonitorHandle::new(1);
        monitor.request_started("a", None, None, EndpointKind::Messages);
        monitor.stream_progress("a", 0, 0, None, None);
        assert!(
            monitor
                .metrics_snapshot()
                .render_prometheus()
                .contains("ccp_first_response_byte_seconds_count 0\n")
        );
        monitor.stream_progress("a", 10, 1, None, None);
        monitor.stream_progress("a", 20, 1, None, None);
        monitor.request_completed("a", 200, None, None);
        monitor.request_abandoned("a", "late body drop");
        let out = monitor.metrics_snapshot().render_prometheus();
        assert!(out.contains("ccp_first_response_byte_seconds_count 1\n"));
        assert!(out.contains("ccp_response_stream_seconds_count 1\n"));
        assert!(out.contains("ccp_requests_finished_total{status=\"completed\"} 1\n"));
        assert!(out.contains("ccp_requests_finished_total{status=\"abandoned\"} 0\n"));
    }
}
