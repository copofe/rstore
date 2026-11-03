//! `connection …` handlers (CON-). `connection add` is the one-step setup:
//! register, test, then register buckets into the workspace.

use super::tty::{read_credential_input, read_edit_credential_input};
use super::{Ctx, HandlerResult};
use crate::args::{ConnectionAddArgs, ConnectionCommand};
use crate::output::HandlerOutput;
use rstore_domain::connection::{AddressingStyle, ConnectionTestStatus, Region};
use rstore_domain::ConnectionConfig;
use rstore_engine::{
    AddConnectionInput, ConnectionRef, ConnectionTestReport, NewBucket, ProviderPreset,
    UpdateConnectionInput,
};

fn masked_credential(value: Option<&rstore_domain::Secret<String>>) -> String {
    value
        .map(|secret| rstore_domain::mask_secret(secret.expose()))
        .unwrap_or_else(|| "(none)".to_string())
}

/// The addressing style a command asks for: the explicit flag wins over the
/// provider preset; `None` when neither was given.
fn addressing_style(
    explicit: Option<&str>,
    provider: Option<&str>,
) -> rstore_domain::Result<Option<AddressingStyle>> {
    if let Some(token) = explicit {
        return AddressingStyle::parse(token).map(Some);
    }
    provider
        .map(|token| ProviderPreset::parse(token).map(ProviderPreset::addressing_style))
        .transpose()
}

/// One line per probe, headed by the verdict.
fn test_lines(report: &ConnectionTestReport) -> String {
    let mut human = format!(
        "Testing connection `{}` — {}\n",
        report.connection,
        if report.status == ConnectionTestStatus::Ok {
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
    human
}

// --- connection -----------------------------------------------------------

pub(super) async fn connection(ctx: &Ctx<'_>, action: &ConnectionCommand) -> HandlerResult {
    let svc = ctx.app.registry().connections();
    match action {
        ConnectionCommand::Add(args) => add(ctx, args).await,
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
                let status = c
                    .last_test_status
                    .map(|status| status.as_str())
                    .unwrap_or("untested");
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
            connection,
            capabilities,
        } => {
            let reference = ConnectionRef::parse(connection);
            let config = svc.resolve(&reference).await?;
            let capability_report = if *capabilities {
                Some(ctx.app.diagnostics().capabilities(&reference).await?)
            } else {
                None
            };
            let access_key = masked_credential(config.access_key.as_ref());
            let secret_key = masked_credential(config.secret_key.as_ref());
            let mut data = super::json_value(&config)?;
            if let serde_json::Value::Object(fields) = &mut data {
                fields.insert(
                    "access_key".into(),
                    serde_json::Value::String(access_key.clone()),
                );
                fields.insert(
                    "secret_key".into(),
                    serde_json::Value::String(secret_key.clone()),
                );
            }
            let data = super::merge_field(
                data,
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
                .push(
                    "public base",
                    config
                        .public_base_url
                        .as_deref()
                        .unwrap_or("(not configured)"),
                )
                .push(
                    "cdn base",
                    config.cdn_base_url.as_deref().unwrap_or("(not configured)"),
                )
                .push("region", config.region.as_token())
                .push("addressing", config.addressing_style.as_str())
                .push("tls verify", config.tls_verify)
                .push("access key", access_key)
                .push("secret key", secret_key)
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
        ConnectionCommand::Test { connection, write } => {
            let report = ctx
                .app
                .diagnostics()
                .test(&ConnectionRef::parse(connection), *write)
                .await?;
            let data = super::json_value(&report)?;
            let mut out = HandlerOutput::human(test_lines(&report)).with_data(data);
            if report.status == ConnectionTestStatus::Failed {
                out = out.with_warning("one or more connection checks failed");
                // A failed probe is a failed check: scripts chaining on
                // `connection test &&` must see it. Same partial-failure
                // code the batch vocabulary uses.
                out = out.with_exit_code(crate::output::EXIT_ITEMS_FAILED);
            }
            Ok(out)
        }
        ConnectionCommand::Edit(args) => {
            let reference = ConnectionRef::parse(&args.connection);
            let existing = svc.resolve(&reference).await?;
            let credentials = read_edit_credential_input(args, &ctx.session)?;
            let input = UpdateConnectionInput {
                name: args.name.clone(),
                endpoint: args.endpoint.clone(),
                public_base_url: args.public_base.clone().map(Some),
                cdn_base_url: args.cdn_base.clone().map(Some),
                region: args.region.as_deref().map(Region::parse_token),
                addressing_style: addressing_style(
                    args.addressing_style.as_deref(),
                    args.provider.as_deref(),
                )?,
                tls_verify: args.tls_verify,
                credentials,
            };
            let config = svc.update(existing.id, input).await?;
            let data = super::json_value(&config)?;
            let human = format!("Updated connection `{}` (id: {})\n", config.name, config.id);
            Ok(HandlerOutput::human(human).with_data(data))
        }
        ConnectionCommand::Remove { connection } => {
            let resolved = ConnectionRef::parse(connection);
            super::tty::remove_flow(
                format!("remove connection `{connection}`"),
                ctx.yes,
                &ctx.session,
                svc.remove(&resolved),
                format!("Removed connection `{connection}` (local config only).\n"),
            )
            .await
        }
    }
}

/// `connection add`: register the connection, test it (unless `--no-test`
/// or no credentials), then register buckets — the `--buckets` list, every
/// visible bucket for `all`, or a choice from the discovered ones on a
/// terminal. Each step after the registration degrades to a warning: the
/// connection exists either way.
async fn add(ctx: &Ctx<'_>, args: &ConnectionAddArgs) -> HandlerResult {
    let credentials = read_credential_input(args, &ctx.session)?;
    let has_credentials = credentials.is_some();
    let config = ctx
        .app
        .registry()
        .connections()
        .add(AddConnectionInput {
            name: args.name.clone(),
            endpoint: args.endpoint.clone(),
            public_base_url: args.public_base.clone(),
            cdn_base_url: args.cdn_base.clone(),
            region: Region::parse_token(&args.region),
            addressing_style: addressing_style(
                args.addressing_style.as_deref(),
                args.provider.as_deref(),
            )?
            .unwrap_or(AddressingStyle::Auto),
            tls_verify: args.tls_verify,
            credentials,
        })
        .await?;
    let mut human = format!(
        "Added connection `{}` (id: {})\n  endpoint: {}\n",
        config.name, config.id, config.endpoint
    );
    let mut warnings = Vec::new();
    if !has_credentials {
        warnings.push(
            "no credentials provided; remote operations will fail until they are set \
             with `connection edit --access-key … --secret-key …`"
                .to_string(),
        );
    }

    let test = if args.no_test || !has_credentials {
        None
    } else {
        match ctx
            .app
            .diagnostics()
            .test(&ConnectionRef::Id(config.id), false)
            .await
        {
            Ok(report) => Some(report),
            Err(error) => {
                warnings.push(format!("connection test could not run: {error}"));
                None
            }
        }
    };
    let passed = test
        .as_ref()
        .is_some_and(|report| report.status == ConnectionTestStatus::Ok);
    if let Some(report) = &test {
        human.push_str(&test_lines(report));
        if !passed {
            warnings.push(
                "connection test failed — check endpoint, credentials, and \
                 `--provider`/`--addressing-style`"
                    .to_string(),
            );
        }
    }

    let names = bucket_choice(ctx, args, &config, passed, &mut warnings).await?;
    let mut data = super::merge_field(
        super::json_value(&config)?,
        "test",
        test.as_ref().map(super::json_value).transpose()?,
    )?;
    if names.is_empty() {
        human.push_str(
            "No buckets registered yet: `rstore bucket list --remote` shows what is \
             available, `rstore bucket add <bucket>` registers one.\n",
        );
        return Ok(finish(
            HandlerOutput::human(human).with_data(data),
            warnings,
        ));
    }
    let buckets: Vec<NewBucket> = names
        .into_iter()
        .map(|name| NewBucket {
            connection: ConnectionRef::Id(config.id),
            name,
        })
        .collect();
    let registered = super::bucket::register(
        ctx,
        &buckets,
        &rstore_domain::bucket::BucketConfig::default(),
        true,
    )
    .await?;
    human.push_str("Buckets:\n");
    human.push_str(&registered.human);
    data = super::merge_field(data, "buckets", Some(registered.data.clone()))?;
    let out = finish(HandlerOutput::human(human).with_data(data), warnings);
    Ok(registered.finish(out))
}

/// The bucket names `connection add` registers: the explicit `--buckets`
/// list, every not-yet-registered visible bucket for `all`, or — on a
/// terminal after a passing test — the user's choice among them.
async fn bucket_choice(
    ctx: &Ctx<'_>,
    args: &ConnectionAddArgs,
    config: &ConnectionConfig,
    passed: bool,
    warnings: &mut Vec<String>,
) -> rstore_domain::Result<Vec<String>> {
    let all = matches!(args.buckets.as_slice(), [only] if only == "all");
    if !args.buckets.is_empty() && !all {
        return Ok(args.buckets.clone());
    }
    let interactive = args.buckets.is_empty() && passed && ctx.session.is_interactive();
    if !all && !interactive {
        return Ok(Vec::new());
    }
    let available = match ctx
        .app
        .registry()
        .onboarding()
        .unregistered(&ConnectionRef::Id(config.id))
        .await
    {
        Ok(available) => available,
        Err(error) => {
            warnings.push(format!(
                "could not list buckets on `{}`: {error} — register them by name with \
                 `rstore bucket add`",
                config.name
            ));
            return Ok(Vec::new());
        }
    };
    if all || available.is_empty() {
        return Ok(available);
    }
    super::tty::select(
        &format!(
            "Connection test passed. Buckets visible on `{}`:",
            config.name
        ),
        "Register which buckets?",
        &available,
        &ctx.session,
    )
}

fn finish(mut out: HandlerOutput, warnings: Vec<String>) -> HandlerOutput {
    for warning in warnings {
        out = out.with_warning(warning);
    }
    out
}
