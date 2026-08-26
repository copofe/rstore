//! JSON envelope builders, the handler result type, version rendering and the
//! stable error-code → process-exit-code mapping. Envelope timestamps arrive
//! as a parameter so the injected clock flows end-to-end and tests produce
//! deterministic output.

use rstore_domain::clock::iso8601;
use rstore_domain::{Error, ErrorCode};
use serde::Serialize;
use serde_json::{json, Value};
use time::OffsetDateTime;

/// A handler's renderable result: structured `data` (for JSON) and a pre-rendered
/// human-readable string. Both are produced so dispatch can pick based on `--json`
/// without re-running the handler.
#[derive(Default, Debug)]
pub struct HandlerOutput {
    pub data: Option<Value>,
    pub human: String,
    /// Non-fatal notes surfaced in the envelope's `warnings` array.
    pub warnings: Vec<String>,
    /// Process exit-code override for non-error outcomes: an interrupted run
    /// reports 130 while still emitting its envelope.
    pub exit_code: Option<i32>,
}

impl HandlerOutput {
    /// A human-readable-only result with no data, warnings or exit override.
    pub fn human(text: impl Into<String>) -> Self {
        Self {
            data: None,
            human: text.into(),
            warnings: Vec::new(),
            exit_code: None,
        }
    }

    /// Attach structured `data` for JSON mode.
    pub fn with_data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }

    /// Attach a warning (rendered to stderr in human mode).
    pub fn with_warning(mut self, warning: impl Into<String>) -> Self {
        self.warnings.push(warning.into());
        self
    }

    /// Force the process exit code (e.g. 130 after an interrupt).
    pub fn with_exit_code(mut self, code: i32) -> Self {
        self.exit_code = Some(code);
        self
    }
}

/// Error entry inside the envelope's `errors` array.
#[derive(Serialize)]
pub struct ErrorEntry {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

/// Build the success envelope value. `now` is supplied by the caller so the
/// injected clock controls the timestamp.
pub fn envelope(command: &str, output: &HandlerOutput, now: OffsetDateTime) -> Value {
    json!({
        "command": command,
        "ok": true,
        "data": output.data.clone().unwrap_or(Value::Null),
        "warnings": output.warnings,
        "errors": [],
        "meta": {
            "generated_at": iso8601(now),
        }
    })
}

/// Build a failure envelope from a domain [`Error`]; the envelope still goes
/// to stdout in JSON mode.
pub fn error_envelope(command: &str, error: &Error, now: OffsetDateTime) -> Value {
    json!({
        "command": command,
        "ok": false,
        "data": Value::Null,
        "warnings": [],
        "errors": [{
            "code": error.code.as_str(),
            "message": error.message,
            "retryable": error.is_retryable(),
        }],
        "meta": {
            "generated_at": iso8601(now),
        }
    })
}

/// Single source of the `version` command's output, shared by library dispatch
/// and the binary's DB-free short-circuit.
pub fn version_output() -> HandlerOutput {
    let data = json!({
        "cli_version": env!("CARGO_PKG_VERSION"),
    });
    let human = format!("rstore {}\n", env!("CARGO_PKG_VERSION"));
    HandlerOutput::human(human).with_data(data)
}

/// A batch (or probe) in which any item failed — partial-failure semantics
/// shared by every batch-shaped command; documented in `docs/cli-guide.md`.
pub const EXIT_ITEMS_FAILED: i32 = 13;

/// Interrupted by Ctrl+C; the full envelope is still printed before exit.
pub const EXIT_INTERRUPTED: i32 = 130;

/// Map a domain error code to a stable process exit code.
pub fn exit_code_for(error: &Error) -> i32 {
    match error.code {
        ErrorCode::Unknown => 1,
        ErrorCode::InvalidInput => 2,
        ErrorCode::Config => 3,
        ErrorCode::Unauthorized => 4,
        ErrorCode::Forbidden => 5,
        ErrorCode::NetworkUnreachable => 6,
        ErrorCode::NotFound => 7,
        ErrorCode::Conflict => 8,
        ErrorCode::Checksum => 9,
        ErrorCode::Database | ErrorCode::LockBusy | ErrorCode::DatabaseBusy => 10,
        ErrorCode::CapabilityUnsupported => 11,
        ErrorCode::Cancelled => 12,
        ErrorCode::Timeout => 6,
        ErrorCode::LocalIo => 14,
    }
}
