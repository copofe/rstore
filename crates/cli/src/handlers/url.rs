//! `url` handlers (LINK-): signed/public links over resolved remote URIs.
//!
//! Lives beside the access/link surface it drives (`app.access()`),
//! not in a shared junk drawer.

use super::{Ctx, HandlerResult};
use crate::output::HandlerOutput;

pub(super) async fn url(ctx: &Ctx<'_>, args: &crate::args::UrlArgs) -> HandlerResult {
    let app = ctx.app;
    // The shared multi-URI entry: `-` reads one URI per stdin line like
    // every other batch command. (The human output below stays the
    // documented exception to the batch grammar: one pipeable link per
    // line — the exit-13 policy still rides the batch severity.)
    let inputs = super::tty::resolve_uris(&args.references, "url needs at least one remote URI")?;
    let uris = super::expand_remote_batch(app, &inputs).await?;
    let format = rstore_engine::LinkFormat::parse(&args.format)?;
    let ttl = match &args.ttl {
        Some(raw) => Some(rstore_engine::parse_ttl(raw)?),
        None => None,
    };
    let request = rstore_engine::LinkRequest {
        format,
        // A lifetime only means something for a presigned link.
        signed: args.signed || args.ttl.is_some(),
        ttl_seconds: ttl,
    };
    let outcome = app.access().generate(&uris, &request).await;

    let mut human = String::new();
    let mut errors = Vec::new();
    let mut links = Vec::with_capacity(outcome.len());
    // Buckets whose links were presigned for want of a public base, with
    // the first expiry: one note each.
    let mut unpublished: Vec<(&rstore_engine::RemoteUri, String)> = Vec::new();
    // Outcomes come back in submission order; errors name the reference as
    // the user wrote it.
    for ((input, uri), item) in inputs.iter().zip(&uris).zip(outcome.items()) {
        match &item.value {
            Some(link) => {
                if let (false, None, Some(expires)) =
                    (request.signed, &link.public_url, &link.expires_at)
                {
                    let seen = unpublished.iter().any(|(other, _)| {
                        other.bucket == uri.bucket
                            && other.connection.display() == uri.connection.display()
                    });
                    if !seen {
                        unpublished.push((uri, expires.clone()));
                    }
                }
                human.push_str(&link.requested);
                human.push('\n');
                links.push(super::json_value(link)?);
            }
            None => match &item.disposition {
                rstore_engine::Disposition::Failed(failure) => {
                    // Diagnostics go to stderr: stdout stays one pipeable
                    // link per line even in mixed batches.
                    errors.push(format!("error: {input}: {}\n", failure.message));
                }
                other => errors.push(format!("error: {input}: no link ({other:?})\n")),
            },
        }
    }
    if !errors.is_empty() {
        eprint!("{}", errors.concat());
    }
    let data = serde_json::json!({ "links": links, "failed": outcome.failed() });
    let mut out = HandlerOutput::human(human).with_data(data);
    for (uri, expires) in unpublished {
        let connection = match app.registry().connections().resolve(&uri.connection).await {
            Ok(config) => config.name,
            Err(_) => uri.connection.display(),
        };
        let bucket = format!("{connection}/{}", uri.bucket);
        out = out.with_warning(format!(
            "`{bucket}` has no public base, so the link is presigned and expires at \
             {expires}; for a permanent link set one with \
             `rstore bucket edit {bucket} --public-base <url>`"
        ));
    }
    Ok(super::report::with_severity(out, outcome.severity()))
}
