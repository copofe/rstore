//! `bucket …` handlers (BKT-).

use rstore_application::ConnectionRef;

use super::HandlerResult;
use crate::output::HandlerOutput;

// --- bucket ----------------------------------------------------------------

pub(super) async fn bucket(
    action: &crate::args::BucketCommand,
    app: &rstore_application::ApplicationApi,
    session: &super::tty::Session,
) -> HandlerResult {
    use rstore_application::{AddBucketInput, BucketRef, UpdateBucketInput};
    use rstore_domain::bucket::BucketConfig;
    use rstore_domain::policy::ConflictPolicy;

    let svc = app.registry().buckets();
    match action {
        crate::args::BucketCommand::Discover { connection } => {
            let found = svc.discover(&ConnectionRef::parse(connection)).await?;
            let data = super::json_value(&found)?;
            let mut table = super::format::Table::new(
                &["BUCKET", "STATUS"],
                &[super::format::Align::Left, super::format::Align::Left],
            );
            for b in &found {
                table.row(vec![
                    b.name.clone(),
                    if b.already_registered {
                        "registered".into()
                    } else {
                        "available".into()
                    },
                ]);
            }
            let human = super::format::listing(
                &format!("Buckets on `{connection}`:"),
                &format!(
                    "No buckets visible on `{connection}` (no ListBuckets permission? try `bucket add`)"
                ),
                table,
            );
            Ok(HandlerOutput::human(human).with_data(data))
        }
        crate::args::BucketCommand::Add(args) => {
            let input = AddBucketInput {
                connection: ConnectionRef::parse(&args.connection),
                name: args.name.clone(),
                config: BucketConfig {
                    display_name: args.display_name.clone(),
                    read_only: args.read_only,
                    allow_delete: args.allow_delete,
                    index_enabled: args.index_enabled,
                    default_conflict: ConflictPolicy::parse(&args.conflict)?,
                    concurrency: None,
                },
            };
            let (bucket, check) = svc.add(input, !args.skip_verify).await?;
            let data = super::merge_field(
                super::json_value(&bucket)?,
                "readability",
                check.as_ref().map(super::json_value).transpose()?,
            )?;
            let mut out = HandlerOutput::human(format!(
                "Registered bucket `{}` (id: {})\n",
                bucket.name, bucket.id
            ))
            .with_data(data);
            if let Some(check) = &check {
                if !check.can_list {
                    out = out.with_warning(format!(
                        "bucket not readable: {} — registered anyway",
                        check.detail
                    ));
                }
            }
            Ok(out)
        }
        crate::args::BucketCommand::List => {
            let list = svc.list().await?;
            let connections = app.registry().connections();
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
                let conn_name = connections
                    .resolve(&ConnectionRef::Id(b.connection_id))
                    .await
                    .map(|c| c.name)
                    .unwrap_or_else(|_| b.connection_id.to_string());
                let display = b
                    .config
                    .display_name
                    .clone()
                    .unwrap_or_else(|| b.name.clone());
                table
                    .row(vec![
                        format!("{conn_name}/{}", b.name),
                        display,
                        if b.config.read_only { "y" } else { "n" }.into(),
                        if b.config.allow_delete { "y" } else { "n" }.into(),
                        if b.config.index_enabled { "y" } else { "n" }.into(),
                    ])
                    .detail(format!("id: {}", b.id));
            }
            let human = super::format::listing(
                "Registered buckets:",
                "No buckets registered. Try `rstore bucket discover <connection>`.",
                table,
            );
            Ok(HandlerOutput::human(human).with_data(data))
        }
        crate::args::BucketCommand::Show { reference } => {
            let bucket_ref = BucketRef::parse(reference);
            let bucket = svc.resolve(&bucket_ref).await?;
            let connection = app
                .registry()
                .connections()
                .resolve(&ConnectionRef::Id(bucket.connection_id))
                .await?;
            let profile = app.access().profile_for(&bucket_ref).await?;
            let data = serde_json::json!({
                "bucket": super::json_value(&bucket)?,
                "link_profile": super::json_value(&profile)?,
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
            Ok(HandlerOutput::human(fields.render()).with_data(data))
        }
        crate::args::BucketCommand::Edit(args) => {
            let bucket_ref = BucketRef::parse(&args.reference);
            let bucket = svc.resolve(&bucket_ref).await?;
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
                index_enabled: args.index_enabled,
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
            // The link flags ride `bucket edit` through the same one path
            // `bucket link set` walks — two entrances, one implementation.
            let profile = if args.link.any() {
                Some(
                    app.access()
                        .set_profile(&bucket_ref, link_input(&args.link)?)
                        .await?,
                )
            } else {
                None
            };
            let profile_data = match &profile {
                Some(profile) => super::json_value(profile)?,
                None => serde_json::Value::Null,
            };
            let data = super::merge_field(
                serde_json::json!({ "bucket": super::json_value(&updated)? }),
                "link_profile",
                Some(profile_data),
            )?;
            let human = format!("Updated bucket `{}` (id: {})\n", updated.name, updated.id);
            Ok(HandlerOutput::human(human).with_data(data))
        }
        crate::args::BucketCommand::Remove { reference, yes } => {
            let resolved = BucketRef::parse(reference);
            super::tty::remove_flow(
                format!("remove bucket registration `{reference}`"),
                *yes,
                session,
                svc.remove(&resolved),
                format!("Removed bucket registration `{reference}` (remote data untouched).\n"),
            )
            .await
        }
        crate::args::BucketCommand::Ignore { action } => {
            super::workspace::ignore(action, app).await
        }
        crate::args::BucketCommand::Link { action } => link(action, app).await,
    }
}

/// `bucket link show|set` — the per-bucket public/CDN overrides, presign
/// permissions, and default TTL; connection values are the defaults.
async fn link(
    action: &crate::args::LinkProfileCommand,
    app: &rstore_application::ApplicationApi,
) -> HandlerResult {
    use crate::args::LinkProfileCommand;
    match action {
        LinkProfileCommand::Show { bucket } => {
            let reference = rstore_application::BucketRef::parse(bucket);
            let resolved_bucket = app.registry().buckets().resolve(&reference).await?;
            let connection = app
                .registry()
                .connections()
                .resolve(&ConnectionRef::Id(resolved_bucket.connection_id))
                .await?;
            let profile = app.access().profile_for(&reference).await?;
            let data = super::json_value(&profile)?;
            let mut fields = super::format::Fields::new();
            push_link_profile(&mut fields, &profile, &connection);
            Ok(HandlerOutput::human(fields.render()).with_data(data))
        }
        LinkProfileCommand::Set { bucket, link } => {
            let reference = rstore_application::BucketRef::parse(bucket);
            let profile = app
                .access()
                .set_profile(&reference, link_input(link)?)
                .await?;
            let data = super::json_value(&profile)?;
            Ok(
                HandlerOutput::human(format!("updated link profile for `{}`\n", bucket))
                    .with_data(data),
            )
        }
    }
}

/// The one flags → [`rstore_application::LinkProfileInput`] translation —
/// `bucket edit` and `bucket link set` share the same bucket link settings.
fn link_input(
    flags: &crate::args::LinkProfileFlags,
) -> Result<rstore_application::LinkProfileInput, rstore_domain::Error> {
    Ok(rstore_application::LinkProfileInput {
        public_base_url: flags.public_base.clone().map(Some),
        cdn_base_url: flags.cdn_base.clone().map(Some),
        allow_public_url: flags.allow_public_url,
        allow_presigned: flags.allow_presigned,
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
