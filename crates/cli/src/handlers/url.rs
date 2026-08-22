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
    // line — the exit-13 policy still rides `with_partial_failure`.)
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
    let outcomes = app.access().links().generate(&uris, &request).await;

    let mut human = String::new();
    let mut failed = 0usize;
    let mut links = Vec::with_capacity(outcomes.len());
    for outcome in &outcomes {
        match (&outcome.link, &outcome.error) {
            (Some(link), _) => {
                human.push_str(&link.requested);
                human.push('\n');
                links.push(super::json_value(link)?);
            }
            (None, Some(err)) => {
                failed += 1;
                human.push_str(&format!("error: {}: {}\n", outcome.uri, err));
            }
            (None, None) => unreachable!("an outcome carries a link or an error"),
        }
    }
    let data = serde_json::json!({ "links": links, "failed": failed });
    Ok(super::report::with_partial_failure(
        HandlerOutput::human(human).with_data(data),
        failed,
        outcomes.len() - failed,
    ))
}
