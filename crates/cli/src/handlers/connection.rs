//! `connection …` handlers (CON-).

use super::tty::{read_credential_input, read_edit_credential_input};
use super::HandlerResult;
use crate::args::ConnectionCommand;
use crate::output::HandlerOutput;
use rstore_application::{AddConnectionInput, ConnectionRef, UpdateConnectionInput};
use rstore_domain::connection::{AddressingStyle, Region};

// --- connection -----------------------------------------------------------

pub(super) async fn connection(
    action: &ConnectionCommand,
    app: &rstore_application::ApplicationApi,
    session: &super::tty::Session,
) -> HandlerResult {
    let svc = app.registry().connections();
    match action {
        ConnectionCommand::Add(args) => {
            let credentials = read_credential_input(args, session)?;
            let mut warnings = Vec::new();
            if credentials.is_none() {
                // Only warn when the credential chain cannot resolve the
                // reference either (env vars may already carry it).
                let secret_ref = args
                    .secret_ref
                    .clone()
                    .unwrap_or_else(|| rstore_secrets::default_secret_ref(&args.name));
                let resolvable = app.diagnostics().secret_ref_resolvable(&secret_ref).await;
                if !resolvable {
                    warnings.push(
                        "no credentials provided; remote operations will fail until they are set \
                         (re-add with --access-key/--secret-key, `connection edit`, or export \
                         RSTORE_SECRET_* variables)"
                            .to_string(),
                    );
                }
            }
            let config = svc
                .add(AddConnectionInput {
                    name: args.name.clone(),
                    endpoint: args.endpoint.clone(),
                    region: Region::parse_token(&args.region),
                    addressing_style: AddressingStyle::parse(&args.addressing_style)?,
                    tls_verify: args.tls_verify,
                    secret_ref: args.secret_ref.clone(),
                    credentials,
                })
                .await?;
            let data = super::json_value(&config)?;
            let human = format!(
                "Added connection `{}` (id: {})\n  endpoint: {}\n",
                config.name, config.id, config.endpoint
            );
            let mut out = HandlerOutput::human(human).with_data(data);
            for warning in warnings {
                out = out.with_warning(warning);
            }
            Ok(out)
        }
        ConnectionCommand::List { filter } => {
            let list = match filter {
                Some(needle) => svc.list_matching(needle).await?,
                None => svc.list().await?,
            };
            let data = super::json_value(&list)?;
            let mut table = super::format::Table::new(
                &["NAME", "STATUS", "ENDPOINT"],
                &[
                    super::format::Align::Left,
                    super::format::Align::Left,
                    super::format::Align::Left,
                ],
            );
            for c in &list {
                let status = match c.last_test_status {
                    Some(rstore_domain::connection::ConnectionTestStatus::Ok) => "ok",
                    Some(rstore_domain::connection::ConnectionTestStatus::Failed) => "failed",
                    None => "untested",
                };
                table
                    .row(vec![c.name.clone(), status.into(), c.endpoint.clone()])
                    .detail(format!("id: {}", c.id));
            }
            let human = super::format::listing(
                "Connections:",
                "No connections configured. Try `rstore connection add`.",
                table,
            );
            Ok(HandlerOutput::human(human).with_data(data))
        }
        ConnectionCommand::Show {
            reference,
            capabilities,
        } => {
            let config = svc.resolve(&ConnectionRef::parse(reference)).await?;
            let capability_report = if *capabilities {
                Some(
                    app.diagnostics()
                        .capabilities(&ConnectionRef::parse(reference))
                        .await?,
                )
            } else {
                None
            };
            let data = super::merge_field(
                super::json_value(&config)?,
                "capabilities",
                capability_report
                    .as_ref()
                    .map(super::json_value)
                    .transpose()?,
            )?;
            let mut fields = super::format::Fields::new();
            fields
                .header(&config.name)
                .push("id", config.id)
                .push("endpoint", &config.endpoint)
                .push("region", config.region.as_token())
                .push("addressing", config.addressing_style.as_str())
                .push("tls verify", config.tls_verify)
                .push(
                    "secret ref",
                    config
                        .secret_ref
                        .as_ref()
                        .map(rstore_domain::CredentialRef::as_str)
                        .unwrap_or("(none)"),
                )
                .push("last tested", {
                    match (config.last_tested_at, config.last_test_status) {
                        (Some(at), Some(status)) => {
                            format!(
                                "{} ({})",
                                rstore_domain::clock::iso8601(at),
                                status.as_str()
                            )
                        }
                        _ => "never".to_string(),
                    }
                });
            if let Some(report) = &capability_report {
                fields.push_lines(
                    "capabilities",
                    report
                        .entries
                        .iter()
                        .map(|(name, state)| {
                            format!(
                                "{:<18} {}",
                                name,
                                serde_json::to_value(state)
                                    .unwrap_or_default()
                                    .as_str()
                                    .unwrap_or("?")
                            )
                        })
                        .collect(),
                );
            }
            Ok(HandlerOutput::human(fields.render()).with_data(data))
        }
        ConnectionCommand::Test {
            reference,
            write_test,
        } => {
            let report = app
                .diagnostics()
                .test(&ConnectionRef::parse(reference), *write_test)
                .await?;
            let data = super::json_value(&report)?;
            let mut human = format!(
                "Testing connection `{}` — {}\n",
                report.connection,
                if report.status.as_str() == "ok" {
                    "OK"
                } else {
                    "FAILED"
                }
            );
            for check in &report.checks {
                human.push_str(&format!(
                    "  [{}] {:<14} {}\n",
                    if check.passed { "✓" } else { "✗" },
                    check.name,
                    check.detail
                ));
            }
            let mut out = HandlerOutput::human(human).with_data(data);
            if report.status == rstore_domain::connection::ConnectionTestStatus::Failed {
                out = out.with_warning("one or more connection checks failed");
            }
            Ok(out)
        }
        ConnectionCommand::Edit(args) => {
            let reference = ConnectionRef::parse(&args.reference);
            let existing = svc.resolve(&reference).await?;
            let credentials = read_edit_credential_input(args, session)?;
            let input = UpdateConnectionInput {
                name: args.name.clone(),
                endpoint: args.endpoint.clone(),
                region: args.region.as_deref().map(Region::parse_token),
                addressing_style: match &args.addressing_style {
                    Some(token) => Some(AddressingStyle::parse(token)?),
                    None => None,
                },
                tls_verify: args.tls_verify,
                secret_ref: args.secret_ref.clone(),
                credentials,
            };
            let config = svc.update(existing.id, input).await?;
            let data = super::json_value(&config)?;
            let human = format!("Updated connection `{}` (id: {})\n", config.name, config.id);
            Ok(HandlerOutput::human(human).with_data(data))
        }
        ConnectionCommand::Remove { reference, yes } => {
            let resolved = ConnectionRef::parse(reference);
            super::tty::remove_flow(
                format!("remove connection `{reference}`"),
                *yes,
                session,
                svc.remove(&resolved),
                format!("Removed connection `{reference}` (local config only).\n"),
            )
            .await
        }
    }
}
