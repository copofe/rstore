//! `bucket …` handlers (BKT-): registration (shared with `connection add`),
//! listing local or remote buckets, and the per-bucket settings, link
//! profile, and index ignore rules.

use rstore_application::{BucketRef, ConnectionRef, Disposition, NewBucket, UpdateBucketInput};
use rstore_domain::bucket::BucketConfig;
use rstore_domain::policy::ConflictPolicy;
use rstore_domain::{Bucket, Result};

use super::{Ctx, HandlerResult};
use crate::args::{BucketAddArgs, BucketCommand, BucketEditArgs};
use crate::output::HandlerOutput;

pub(super) async fn bucket(ctx: &Ctx<'_>, action: &BucketCommand) -> HandlerResult {
    match action {
        BucketCommand::List { connection, remote } => {
            if *remote {
                list_remote(ctx, connection.as_deref()).await
            } else {
                list(ctx, connection.as_deref()).await
            }
        }
        BucketCommand::Add(args) => add(ctx, args).await,
        BucketCommand::Show { bucket } => show(ctx, bucket).await,
        BucketCommand::Edit(args) => edit(ctx, args).await,
        BucketCommand::Remove { buckets } => {
            let svc = ctx.app.registry().buckets();
            // Resolve everything first so the prompt names exactly what goes.
            let mut resolved = Vec::with_capacity(buckets.len());
            for reference in buckets {
                resolved.push(svc.resolve(&BucketRef::parse(reference)).await?);
            }
            let names = buckets.join(", ");
            super::tty::confirm_destructive(
                &format!("remove bucket registration(s) {names} (remote data untouched)"),
                ctx.yes,
                &ctx.session,
            )?;
            for bucket in &resolved {
                svc.remove(&BucketRef::Id(bucket.id)).await?;
            }
            let ids: Vec<String> = resolved.iter().map(|b| b.id.to_string()).collect();
            Ok(HandlerOutput::human(format!(
                "Removed bucket registration(s) {names} (remote data untouched).\n"
            ))
            .with_data(serde_json::json!({ "removed": ids })))
        }
    }
}

/// What registering a batch of buckets produced, ready to render.
pub(super) struct Registered {
    pub(super) human: String,
    pub(super) data: serde_json::Value,
    warnings: Vec<String>,
    failed: bool,
}

impl Registered {
    /// Attach the warnings and the partial-failure exit code to `out`.
    pub(super) fn finish(self, mut out: HandlerOutput) -> HandlerOutput {
        for warning in self.warnings {
            out = out.with_warning(warning);
        }
        if self.failed {
            out = out.with_exit_code(crate::output::EXIT_ITEMS_FAILED);
        }
        out
    }
}

/// Register the buckets through the application's onboarding use case (add,
/// then enroll into the workspace from `-w`, else the current or only one)
/// and render one line per bucket.
pub(super) async fn register(
    ctx: &Ctx<'_>,
    buckets: &[NewBucket],
    config: &BucketConfig,
    verify: bool,
) -> Result<Registered> {
    let outcome = ctx
        .app
        .registry()
        .onboarding()
        .register(buckets, config, verify, ctx.explicit_workspace.as_ref())
        .await?;
    let mut lines = super::report::BatchLines::new(8, 11);
    let mut warnings = Vec::new();
    for item in outcome.items() {
        let label = &item.target;
        match (&item.value, &item.disposition) {
            (Some(registration), _) => {
                let detail = match &registration.workspace {
                    Some(ws) => format!("{} · workspace `{}`", registration.bucket.id, ws.name),
                    None => registration.bucket.id.to_string(),
                };
                lines.push("ok", label, Some(&detail));
                if let Some(check) = registration
                    .readability
                    .as_ref()
                    .filter(|check| !check.can_list)
                {
                    warnings.push(format!(
                        "`{label}` is not readable: {} — registered anyway",
                        check.detail
                    ));
                }
                if registration.workspace.is_none() {
                    warnings.push(format!(
                        "`{label}` joined no workspace: several exist and none is current; \
                         run `rstore workspace add {label} -w <name>`"
                    ));
                }
            }
            (None, Disposition::Failed(failure)) => {
                lines.push("failed", label, Some(&failure.message));
            }
            (None, _) => lines.push("failed", label, None),
        }
    }
    Ok(Registered {
        human: lines.finish(),
        data: super::json_value(&outcome)?,
        warnings,
        failed: outcome.failed() > 0,
    })
}

async fn add(ctx: &Ctx<'_>, args: &BucketAddArgs) -> HandlerResult {
    let config = BucketConfig {
        display_name: None,
        read_only: args.read_only,
        allow_delete: !args.no_delete,
        index_enabled: !args.no_index,
        default_conflict: ConflictPolicy::parse(&args.conflict)?,
        concurrency: None,
    };
    let references = ctx.app.registry().references();
    let mut buckets = Vec::with_capacity(args.buckets.len());
    for input in &args.buckets {
        buckets.push(references.new_bucket(input).await?);
    }
    let registered = register(ctx, &buckets, &config, !args.skip_verify).await?;
    let out = HandlerOutput::human(registered.human.clone()).with_data(registered.data.clone());
    Ok(registered.finish(out))
}

/// Registered buckets, optionally narrowed to one connection.
async fn list(ctx: &Ctx<'_>, connection: Option<&str>) -> HandlerResult {
    let only = match connection {
        Some(reference) => Some(
            ctx.app
                .registry()
                .connections()
                .resolve(&ConnectionRef::parse(reference))
                .await?
                .id,
        ),
        None => None,
    };
    let list: Vec<Bucket> = ctx
        .app
        .registry()
        .buckets()
        .list()
        .await?
        .into_iter()
        .filter(|b| match only {
            Some(id) => b.connection_id == id,
            None => true,
        })
        .collect();
    let labels = super::bucket_labels(ctx.app).await?;
    let data = super::json_value(&list)?;
    let mut table = super::format::Table::new(
        &["BUCKET", "DISPLAY", "RO", "DEL", "IDX"],
        &[
            super::format::Align::Left,
            super::format::Align::Left,
            super::format::Align::Left,
            super::format::Align::Left,
            super::format::Align::Left,
        ],
    );
    for b in &list {
        let display = b
            .config
            .display_name
            .clone()
            .unwrap_or_else(|| b.name.clone());
        table
            .row(vec![
                labels.get(&b.id).cloned().unwrap_or_else(|| b.name.clone()),
                display,
                if b.config.read_only { "y" } else { "n" }.into(),
                if b.config.allow_delete { "y" } else { "n" }.into(),
                if b.config.index_enabled { "y" } else { "n" }.into(),
            ])
            .detail(format!("id: {}", b.id));
    }
    let human = super::format::listing(
        "Registered buckets:",
        "No buckets registered. See what is available with `rstore bucket list --remote`.",
        table,
    );
    Ok(HandlerOutput::human(human).with_data(data))
}

/// Buckets visible on the remote, per connection, marking registered ones.
/// Without a connection every one is asked; a connection that cannot list
/// becomes a warning instead of failing the others.
async fn list_remote(ctx: &Ctx<'_>, connection: Option<&str>) -> HandlerResult {
    let connections = ctx.app.registry().connections();
    let targets = match connection {
        Some(reference) => vec![
            connections
                .resolve(&ConnectionRef::parse(reference))
                .await?,
        ],
        None => connections.list().await?,
    };
    let mut table = super::format::Table::new(
        &["BUCKET", "STATUS"],
        &[super::format::Align::Left, super::format::Align::Left],
    );
    let mut data = Vec::new();
    let mut warnings = Vec::new();
    for target in &targets {
        match ctx
            .app
            .registry()
            .buckets()
            .discover(&ConnectionRef::Id(target.id))
            .await
        {
            Ok(found) => {
                for b in found {
                    table.row(vec![
                        format!("{}/{}", target.name, b.name),
                        if b.already_registered {
                            "registered"
                        } else {
                            "available"
                        }
                        .into(),
                    ]);
                    data.push(serde_json::json!({
                        "connection": target.name,
                        "name": b.name,
                        "registered": b.already_registered,
                    }));
                }
            }
            Err(error) if connection.is_some() => return Err(error),
            Err(error) => warnings.push(format!("`{}`: {error}", target.name)),
        }
    }
    let human = super::format::listing(
        "Buckets visible on the remote:",
        "No buckets visible (no ListBuckets permission? register them by name with \
         `rstore bucket add`).",
        table,
    );
    let mut out = HandlerOutput::human(human).with_data(serde_json::json!(data));
    for warning in warnings {
        out = out.with_warning(warning);
    }
    Ok(out)
}

async fn show(ctx: &Ctx<'_>, reference: &str) -> HandlerResult {
    let bucket = ctx
        .app
        .registry()
        .buckets()
        .resolve(&BucketRef::parse(reference))
        .await?;
    let id_ref = BucketRef::Id(bucket.id);
    let connection = ctx
        .app
        .registry()
        .connections()
        .resolve(&ConnectionRef::Id(bucket.connection_id))
        .await?;
    let profile = ctx.app.access().profile_for(&id_ref).await?;
    let rules = ctx.app.catalog().workspaces().ignore_list(&id_ref).await?;
    let data = serde_json::json!({
        "bucket": super::json_value(&bucket)?,
        "link_profile": super::json_value(&profile)?,
        "ignore_rules": super::json_value(&rules)?,
    });
    let mut fields = super::format::Fields::new();
    fields
        .header(format!("{} (id: {})", bucket.name, bucket.id))
        .push("connection", &connection.name)
        .push(
            "display name",
            bucket
                .config
                .display_name
                .as_deref()
                .unwrap_or(bucket.name.as_str()),
        )
        .push("read-only", bucket.config.read_only)
        .push("allow delete", bucket.config.allow_delete)
        .push("indexed", bucket.config.index_enabled)
        .push("conflict", bucket.config.default_conflict.as_str());
    push_link_profile(&mut fields, &profile, &connection);
    fields.push(
        "ignored",
        if rules.is_empty() {
            "-".to_string()
        } else {
            rules
                .iter()
                .map(|r| r.directory.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        },
    );
    Ok(HandlerOutput::human(fields.render()).with_data(data))
}

async fn edit(ctx: &Ctx<'_>, args: &BucketEditArgs) -> HandlerResult {
    let svc = ctx.app.registry().buckets();
    let bucket = svc.resolve(&BucketRef::parse(&args.bucket)).await?;
    let id_ref = BucketRef::Id(bucket.id);
    let input = UpdateBucketInput {
        display_name: args.display_name.as_ref().map(|name| {
            if name.is_empty() {
                None
            } else {
                Some(name.clone())
            }
        }),
        read_only: args.read_only,
        allow_delete: args.allow_delete,
        index_enabled: args.index,
        default_conflict: match &args.conflict {
            Some(token) => Some(ConflictPolicy::parse(token)?),
            None => None,
        },
        concurrency: match args.concurrency {
            Some(0) => Some(None),
            Some(n) => Some(Some(n)),
            None => None,
        },
    };
    let updated = svc.update(bucket.id, input).await?;
    let profile = if args.link.any() {
        Some(
            ctx.app
                .access()
                .set_profile(&id_ref, link_input(&args.link)?)
                .await?,
        )
    } else {
        None
    };
    let workspaces = ctx.app.catalog().workspaces();
    for directory in &args.ignore {
        workspaces.ignore_add(&id_ref, directory).await?;
    }
    for directory in &args.unignore {
        workspaces.ignore_remove(&id_ref, directory).await?;
    }
    let rules = if args.ignore.is_empty() && args.unignore.is_empty() {
        None
    } else {
        Some(workspaces.ignore_list(&id_ref).await?)
    };
    let data = serde_json::json!({
        "bucket": super::json_value(&updated)?,
        "link_profile": super::json_value(&profile)?,
        "ignore_rules": super::json_value(&rules)?,
    });
    let human = format!("Updated bucket `{}` (id: {})\n", updated.name, updated.id);
    Ok(HandlerOutput::human(human).with_data(data))
}

/// The one flags → [`rstore_application::LinkProfileInput`] translation for
/// `bucket edit`.
fn link_input(
    flags: &crate::args::LinkProfileFlags,
) -> Result<rstore_application::LinkProfileInput> {
    Ok(rstore_application::LinkProfileInput {
        public_base_url: flags.public_base.clone().map(Some),
        cdn_base_url: flags.cdn_base.clone().map(Some),
        allow_public_url: flags.public_url,
        allow_presigned: flags.presigned,
        default_ttl_seconds: match &flags.default_ttl {
            Some(raw) => Some(rstore_application::parse_ttl(raw)?),
            None => None,
        },
    })
}

/// Render the configured public/CDN bases. A connection default is marked in
/// the label; a bucket override keeps the shorter base name.
fn push_link_profile(
    fields: &mut super::format::Fields,
    profile: &rstore_domain::link::LinkProfile,
    connection: &rstore_domain::ConnectionConfig,
) {
    push_base(
        fields,
        "public base",
        profile.public_base_url.as_deref(),
        connection.public_base_url.as_deref(),
        "(not configured; endpoint used)",
    );
    push_base(
        fields,
        "cdn base",
        profile.cdn_base_url.as_deref(),
        connection.cdn_base_url.as_deref(),
        "(not configured)",
    );
    fields
        .push(
            "public urls",
            if profile.allow_public_url {
                "allowed"
            } else {
                "blocked"
            },
        )
        .push(
            "presigned urls",
            if profile.allow_presigned {
                "allowed"
            } else {
                "blocked"
            },
        )
        .push("default ttl", format!("{}s", profile.default_ttl_seconds));
}

fn push_base(
    fields: &mut super::format::Fields,
    label: &str,
    bucket_base: Option<&str>,
    connection_base: Option<&str>,
    empty_value: &str,
) {
    match (bucket_base, connection_base) {
        (Some(base), _) => {
            fields.push(label, base);
        }
        (None, Some(base)) => {
            fields.push(&format!("{label} (connection)"), base);
        }
        (None, None) => {
            fields.push(label, empty_value);
        }
    }
}
