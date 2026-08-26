//! Translation of AWS SDK errors into the unified [`rstore_domain::Error`]
//! model with stable [`ErrorCode`]s.
//!
//! The single adapter-level decision is [`classify`]: a *pure* function over
//! a provider error-code string with no SDK types in its signature, so the
//! most branching-heavy logic in the adapter is table-testable without a
//! live MinIO.

use aws_sdk_s3::error::SdkError;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use rstore_domain::{Error, ErrorCode};

/// Map any `SdkError<E>` into a domain error.
pub(crate) fn map_sdk<E>(err: SdkError<E>) -> Error
where
    E: std::fmt::Debug + ProvideErrorMetadata,
{
    let code = err.code().unwrap_or("");
    // Providers often omit the message field (MinIO HeadObject NoSuchKey has
    // none) — synthesise from the code instead of dumping the SDK Debug repr,
    // which leaks transport internals to users.
    let message = err.message().map(str::to_string).unwrap_or_else(|| {
        if code.is_empty() {
            // A codeless failure still carries a decisive fact: an HTTP
            // response arrived (and with what status) or it did not. Hiding
            // that behind "unspecified" makes addressing/DNS-vs-gateway
            // problems undiagnosable from the CLI.
            match transport_detail(&err) {
                Some(detail) => format!("provider returned an unspecified error ({detail})"),
                None => "provider returned an unspecified error".to_string(),
            }
        } else {
            format!("provider error `{code}`")
        }
    });
    let (ec, retryable) = classify(code, transport(&err));
    let mut error = Error::new(ec, message).with_context("provider_error_code", code);
    if retryable {
        error = error.retryable(true);
    }
    error
}

/// The transport-level facts a failure carries even when the provider sent
/// no error code — the decisive input [`classify`] needs for codeless
/// responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Transport {
    /// A modelled service error with this HTTP status.
    Status(u16),
    /// A response arrived but its body was unparseable (still carries a
    /// status).
    UnparseableBody(u16),
    /// The request timed out before a response.
    TimedOut,
    /// The request failed before a response (DNS/connect/protocol).
    NoResponse,
}

impl Transport {
    fn status(self) -> Option<u16> {
        match self {
            Transport::Status(status) | Transport::UnparseableBody(status) => Some(status),
            _ => None,
        }
    }

    /// A bare 429/5xx status, or a timeout, is transient regardless of the
    /// (missing) code: proxies and MinIO regularly fail without S3 error
    /// bodies, so codeless transient responses are retried.
    fn is_transient(self) -> bool {
        self.status()
            .is_some_and(|s| s == 429 || (500..=599).contains(&s))
            || self == Transport::TimedOut
    }
}

/// One line about how a codeless failure failed: the HTTP status that came
/// back, or that the request never got a response at all.
fn transport_detail<E>(err: &SdkError<E>) -> Option<String>
where
    E: std::fmt::Debug + ProvideErrorMetadata,
{
    transport(err).map(|t| match t {
        Transport::Status(status) => format!("HTTP {status}"),
        Transport::UnparseableBody(status) => format!("HTTP {status} with an unparseable body"),
        Transport::TimedOut => "request timed out before a response".into(),
        Transport::NoResponse => "request failed before a response (DNS/connect/protocol)".into(),
    })
}

fn transport<E>(err: &SdkError<E>) -> Option<Transport>
where
    E: std::fmt::Debug + ProvideErrorMetadata,
{
    match err {
        SdkError::ServiceError(ctx) => Some(Transport::Status(ctx.raw().status().as_u16())),
        SdkError::ResponseError(ctx) => {
            Some(Transport::UnparseableBody(ctx.raw().status().as_u16()))
        }
        SdkError::TimeoutError(_) => Some(Transport::TimedOut),
        SdkError::DispatchFailure(_) => Some(Transport::NoResponse),
        SdkError::ConstructionFailure(_) => None,
        _ => None,
    }
}

/// Pure classification of a provider error code (plus whatever transport
/// facts a codeless failure still carries) into `(ErrorCode, retryable)`.
///
/// `retryable` is the adapter's transient-error signal (throttle / timeout /
/// 5xx); it is the only place that decision is made, so the retry engine
/// reads it from [`Error::is_retryable`] instead of re-deriving it. A
/// decisive code wins over the status; an indecisive one leaves the status
/// to speak — but only for transience: a bare 404 from a proxy is not proof
/// the *object* is missing.
pub(crate) fn classify(code: &str, transport: Option<Transport>) -> (ErrorCode, bool) {
    let c = code.to_ascii_lowercase();
    let timed_out = c.contains("timeout") || transport == Some(Transport::TimedOut);
    let transient = is_transient(&c) || transport.is_some_and(Transport::is_transient);
    let ec = if c.contains("nosuchkey")
        || c.contains("nosuchbucket")
        || c.contains("notfound")
        || c.contains("nosuchupload")
        || c == "404"
    {
        (ErrorCode::NotFound, false)
    } else if c.contains("invalidpart")
        || c.contains("entitytoosmall")
        || c.contains("invalidargument")
    {
        // Multipart legality violations (unknown/gapped/out-of-order part,
        // undersized non-final part, out-of-range part number): decisive
        // caller errors, never transient.
        (ErrorCode::InvalidInput, false)
    } else if c.contains("accessdenied") || c == "403" {
        (ErrorCode::Forbidden, false)
    } else if c.contains("invalidaccesskeyid")
        || c.contains("signaturedoesnotmatch")
        || c.contains("unauthorized")
        || c == "invalididentitytoken"
    {
        (ErrorCode::Unauthorized, false)
    } else if c.contains("bucketalreadyexists")
        || c.contains("bucketalreadyownedbyyou")
        || c.contains("preconditionfailed")
        || c == "412"
    {
        (ErrorCode::Conflict, false)
    } else if timed_out {
        // A timed-out request is not proof the endpoint is unreachable; the
        // distinct code keeps exit codes and retry decisions honest.
        (ErrorCode::Timeout, true)
    } else if transient {
        (ErrorCode::NetworkUnreachable, true)
    } else {
        (ErrorCode::Unknown, false)
    };
    (ec.0, transient)
}

/// Heuristic transient-error detection from the provider error code.
fn is_transient(lowercased_code: &str) -> bool {
    matches!(
        lowercased_code,
        "requesttimeout"
            | "requesttimeoutexception"
            | "throttling"
            | "throttlingexception"
            | "slowdown"
            | "toomanyrequests"
            | "serviceunavailable"
            | "internalerror"
            | "internal"
            | "5xx"
    ) || lowercased_code.contains("throttl")
        || lowercased_code.contains("timeout")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Table-driven coverage of the classify mapping — no MinIO required.
    #[test]
    fn classify_provider_codes() {
        let cases: &[(&str, Option<Transport>, ErrorCode, bool)] = &[
            ("NoSuchKey", None, ErrorCode::NotFound, false),
            ("NoSuchBucket", None, ErrorCode::NotFound, false),
            ("NoSuchUpload", None, ErrorCode::NotFound, false),
            ("InvalidPart", None, ErrorCode::InvalidInput, false),
            ("InvalidPartOrder", None, ErrorCode::InvalidInput, false),
            ("EntityTooSmall", None, ErrorCode::InvalidInput, false),
            ("InvalidArgument", None, ErrorCode::InvalidInput, false),
            ("404", None, ErrorCode::NotFound, false),
            ("AccessDenied", None, ErrorCode::Forbidden, false),
            ("403", None, ErrorCode::Forbidden, false),
            ("InvalidAccessKeyId", None, ErrorCode::Unauthorized, false),
            (
                "SignatureDoesNotMatch",
                None,
                ErrorCode::Unauthorized,
                false,
            ),
            ("BucketAlreadyOwnedByYou", None, ErrorCode::Conflict, false),
            ("PreconditionFailed", None, ErrorCode::Conflict, false),
            ("SlowDown", None, ErrorCode::NetworkUnreachable, true),
            (
                "ThrottlingException",
                None,
                ErrorCode::NetworkUnreachable,
                true,
            ),
            ("RequestTimeout", None, ErrorCode::Timeout, true),
            (
                "ServiceUnavailable",
                None,
                ErrorCode::NetworkUnreachable,
                true,
            ),
            ("InternalError", None, ErrorCode::NetworkUnreachable, true),
            ("SomethingWeird", None, ErrorCode::Unknown, false),
            ("", None, ErrorCode::Unknown, false),
            // Codeless responses speak through the transport: a bare 429/5xx
            // or a timeout is transient and retryable.
            (
                "",
                Some(Transport::Status(503)),
                ErrorCode::NetworkUnreachable,
                true,
            ),
            (
                "",
                Some(Transport::Status(429)),
                ErrorCode::NetworkUnreachable,
                true,
            ),
            (
                "",
                Some(Transport::UnparseableBody(502)),
                ErrorCode::NetworkUnreachable,
                true,
            ),
            ("", Some(Transport::TimedOut), ErrorCode::Timeout, true),
            // Only transience reads from the status: a proxy 404 without a
            // provider code is not proof the object is missing.
            ("", Some(Transport::Status(404)), ErrorCode::Unknown, false),
            ("", Some(Transport::NoResponse), ErrorCode::Unknown, false),
            // A decisive code wins the ErrorCode; transience still ORs — a
            // response tagged 500 is transient even if the code says NotFound.
            (
                "NoSuchKey",
                Some(Transport::Status(500)),
                ErrorCode::NotFound,
                true,
            ),
        ];
        for (code, transport, expect_ec, expect_retry) in cases {
            let (ec, retry) = classify(code, *transport);
            assert_eq!(ec, *expect_ec, "code `{code}` -> ErrorCode");
            assert_eq!(retry, *expect_retry, "code `{code}` -> retryable");
        }
    }

    #[test]
    fn codeless_errors_expose_transport_detail() {
        use aws_smithy_types::body::SdkBody;
        #[derive(Debug)]
        struct Unmodeled;
        impl ProvideErrorMetadata for Unmodeled {
            fn meta(&self) -> &aws_smithy_types::error::metadata::ErrorMetadata {
                &aws_smithy_types::error::metadata::EMPTY_ERROR_METADATA
            }
        }
        let ctx = aws_smithy_runtime_api::client::result::ServiceError::builder()
            .source(Unmodeled)
            .raw(aws_smithy_runtime_api::http::Response::new(
                aws_smithy_runtime_api::http::StatusCode::try_from(200u16).unwrap(),
                SdkBody::from(""),
            ))
            .build();
        let err: SdkError<Unmodeled> = SdkError::ServiceError(ctx);
        assert_eq!(transport_detail(&err).as_deref(), Some("HTTP 200"));
    }
}
