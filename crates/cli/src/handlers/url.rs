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
    let format = rstore_application::LinkFormat::parse(&args.format)?;
    let ttl = match &args.ttl {
        Some(raw) => Some(rstore_application::parse_ttl(raw)?),
        None => None,
    };
    let request = rstore_application::LinkRequest {
        format,
        // A lifetime only means something for a presigned link.
        signed: args.signed || args.ttl.is_some(),
        ttl_seconds: ttl,
    };
    let outcome = app.access().generate(&uris, &request).await;

    let mut human = String::new();
    let mut errors = Vec::new();
    let mut links = Vec::with_capacity(outcome.len());
    // Outcomes come back in submission order; errors name the reference as
    // the user wrote it.
    for (input, item) in inputs.iter().zip(outcome.items()) {
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
    Ok(super::report::with_severity(
        HandlerOutput::human(human).with_data(data),
        outcome.severity(),
    ))
}
