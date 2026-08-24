//! `url` handlers (LINK-): signed/public links over resolved remote URIs.
//!
//! Lives beside the access/link surface it drives (`app.access().links()`),
//! not in a shared junk drawer.

use super::HandlerResult;
use crate::output::HandlerOutput;

pub(super) async fn url(
    args: &crate::args::UrlArgs,
    app: &rstore_application::ApplicationApi,
) -> HandlerResult {
    // The shared multi-URI entry: `-` reads one URI per stdin line like
    // every other batch command. (The human output below stays the
    // documented exception to the batch grammar: one pipeable link per
    // line — the exit-13 policy still rides the batch severity.)
    let uris = super::tty::resolve_uris(&args.uris, "url needs at least one remote URI")?;
    let uris = super::expand_remote_batch(app, &uris).await?;
    let format = rstore_application::LinkFormat::parse(&args.format)?;
    let ttl = match &args.ttl {
        Some(raw) => Some(rstore_application::parse_ttl(raw)?),
        None => None,
    };
    let request = rstore_application::LinkRequest {
        format,
        signed: args.signed,
        ttl_seconds: ttl,
    };
    let outcome = app.access().links().generate(&uris, &request).await;

    let mut human = String::new();
    let mut errors = Vec::new();
    let mut links = Vec::with_capacity(outcome.len());
    for item in outcome.items() {
        match &item.value {
            Some(link) => {
                human.push_str(&link.requested);
                human.push('\n');
                links.push(super::json_value(link)?);
            }
            None => match &item.disposition {
                rstore_application::Disposition::Failed(failure) => {
                    // Diagnostics go to stderr: stdout stays one pipeable
                    // link per line even in mixed batches.
                    errors.push(format!("error: {}: {}\n", item.target, failure.message));
                }
                other => unreachable!("link items are succeeded or failed, got {other:?}"),
            },
        }
    }
    if !errors.is_empty() {
        eprint!("{}", errors.concat());
    }
    let data = serde_json::json!({ "links": links, "failed": outcome.failed() });
    Ok(super::report::with_severity(
        HandlerOutput::human(human).with_data(data),
        outcome.severity(),
    ))
}
