//! Terminal interaction policy in one home: hidden credential input, y/N
//! confirmations, conflict resolution, and stdin URI lists all read a single
//! [`Session`] for "may we prompt?" instead of re-deriving it from the
//! terminal per call site. JSON and non-terminal sessions never block on an
//! interactive prompt.

use std::io::{IsTerminal, Write};

use rstore_application::CredentialInput;
use rstore_domain::policy::ConflictPolicy;
use rstore_domain::Secret;
use rstore_domain::{Error, Result};

use crate::args::ConnectionAddArgs;

/// The one definition of "may we prompt" and the one place prompts read
/// their answers: machine output (`--json`) or a non-terminal stdin means
/// no. Constructed once per run and threaded to every prompt; an embedded
/// consumer passes its own always-non-interactive session and inherits
/// every prompting rule for free.
#[derive(Clone)]
pub struct Session {
    json: bool,
    stdin_is_tty: bool,
}

impl Session {
    pub fn detect(json: bool) -> Self {
        Self {
            json,
            stdin_is_tty: std::io::stdin().is_terminal(),
        }
    }

    /// Whether a human can answer a prompt.
    pub fn is_interactive(&self) -> bool {
        !self.json && self.stdin_is_tty
    }

    /// Read one prompt answer from stdin.
    fn read_answer(&self) -> Result<String> {
        let mut line = String::new();
        std::io::stdin()
            .read_line(&mut line)
            .map_err(|e| Error::invalid_input("could not read confirmation").with_source(e))?;
        Ok(line.trim().to_string())
    }
}

/// Prints `prompt [y/N]` and reads one answer through the session; anything
/// but a bare or case-insensitive `y` answers no.
pub(super) fn confirm(prompt: &str, session: &Session) -> Result<bool> {
    let mut stderr = std::io::stderr();
    write!(stderr, "{prompt} [y/N] ").ok();
    stderr.flush().ok();
    Ok(session.read_answer()?.eq_ignore_ascii_case("y"))
}

/// Gates destructive commands: `--yes` skips the prompt, non-interactive
/// sessions require `--yes`, interactive sessions get a y/N prompt.
pub(super) fn confirm_destructive(prompt: &str, yes: bool, session: &Session) -> Result<()> {
    if yes {
        return Ok(());
    }
    if !session.is_interactive() {
        return Err(Error::invalid_input("non-interactive mode requires --yes"));
    }
    if confirm(&format!("{prompt}?"), session)? {
        Ok(())
    } else {
        Err(Error::cancelled())
    }
}

/// The single destructive-removal order: confirm the named target *before*
/// the removal runs, then emit the fixed success report. Callers resolve
/// their display name first so the prompt is exact — a remove command cannot
/// forget the order or confirm after mutating. (`remote rm` is the counted
/// sibling: plan, confirm the exact object count, then remove.)
pub(super) async fn remove_flow(
    prompt: String,
    yes: bool,
    session: &Session,
    remove: impl std::future::Future<Output = Result<()>>,
    done: String,
) -> super::HandlerResult {
    confirm_destructive(&prompt, yes, session)?;
    remove.await?;
    Ok(crate::output::HandlerOutput::human(done))
}

/// Reads one line of hidden input (credentials) from the terminal.
pub(super) fn prompt_hidden(prompt: &str) -> Result<String> {
    rpassword::prompt_password(format!("{prompt}: "))
        .map_err(|e| Error::invalid_input("could not read hidden input").with_source(e))
}

/// The single credential-input policy: a flag pair must be complete and
/// non-empty; with neither flag, a non-tty (or `--json`) session stores
/// nothing while a tty prompts hidden — an empty pair means "keep what
/// exists / store nothing", a half pair is an error. Callers supply the
/// prompts; everything else is one policy.
pub(super) fn read_credential_pair(
    access_key: Option<&str>,
    secret_key: Option<&str>,
    access_prompt: &str,
    secret_prompt: &str,
    keep_hint: Option<String>,
    session: &Session,
) -> Result<Option<Secret<CredentialInput>>> {
    match (access_key, secret_key) {
        (Some(access), Some(secret)) => {
            if access.trim().is_empty() || secret.trim().is_empty() {
                return Err(Error::invalid_input(
                    "--access-key and --secret-key must be non-empty",
                ));
            }
            Ok(Some(Secret::new(CredentialInput {
                access_key: access.trim().to_string(),
                secret_key: secret.trim().to_string(),
            })))
        }
        (Some(_), None) | (None, Some(_)) => Err(Error::invalid_input(
            "--access-key and --secret-key must be provided together",
        )),
        (None, None) if !session.is_interactive() => Ok(None),
        (None, None) => {
            if let Some(hint) = keep_hint {
                eprintln!("{hint}");
            }
            let access = prompt_hidden(access_prompt)?;
            let secret = prompt_hidden(secret_prompt)?;
            if access.is_empty() && secret.is_empty() {
                return Ok(None);
            }
            if access.is_empty() || secret.is_empty() {
                return Err(Error::invalid_input(
                    "both access key and secret key are required",
                ));
            }
            Ok(Some(Secret::new(CredentialInput {
                access_key: access,
                secret_key: secret,
            })))
        }
    }
}

/// Collects credentials for `connection add` from flags or a hidden prompt.
pub(super) fn read_credential_input(
    args: &ConnectionAddArgs,
    session: &Session,
) -> Result<Option<Secret<CredentialInput>>> {
    read_credential_pair(
        args.access_key.as_deref(),
        args.secret_key.as_deref(),
        &format!("Access key for `{}`", args.name),
        "Secret key",
        None,
        session,
    )
}

/// Collects credentials for `connection edit`: without either flag nothing
/// is touched; otherwise an empty hidden answer keeps the stored value.
pub(super) fn read_edit_credential_input(
    args: &crate::args::ConnectionEditArgs,
    session: &Session,
) -> Result<Option<Secret<CredentialInput>>> {
    if args.access_key.is_none() && args.secret_key.is_none() {
        return Ok(None);
    }

    read_credential_pair(
        args.access_key.as_deref(),
        args.secret_key.as_deref(),
        "New access key (empty = keep)",
        "New secret key (empty = keep)",
        Some(format!(
            "Leave both empty to keep the existing credentials for `{}`.",
            args.reference
        )),
        session,
    )
}

/// Resolves the interactive part of the conflict policy. The Application API
/// has already applied the explicit-policy / bucket-default precedence; this
/// function only handles `ask`, `--yes`, and the terminal prompt.
pub(super) fn resolve_conflict(
    policy: ConflictPolicy,
    yes: bool,
    session: &Session,
) -> Result<ConflictPolicy> {
    if policy != ConflictPolicy::Ask {
        return Ok(policy);
    }
    if yes {
        return Ok(ConflictPolicy::Overwrite);
    }
    if !session.is_interactive() {
        return Err(Error::invalid_input(
            "the conflict policy is unresolved (`ask`) and cannot prompt in \
             non-interactive mode; pass --conflict skip|overwrite|rename or \
             --yes",
        ));
    }
    let mut stderr = std::io::stderr();
    write!(
        stderr,
        "If a target already exists — [o]verwrite, [s]kip, [r]ename, [a]bort? "
    )
    .ok();
    stderr.flush().ok();
    match session.read_answer()?.to_ascii_lowercase().as_str() {
        "o" => Ok(ConflictPolicy::Overwrite),
        "s" => Ok(ConflictPolicy::Skip),
        "r" => Ok(ConflictPolicy::Rename),
        _ => Err(Error::cancelled()),
    }
}

/// Resolves a multi-URI argument list: `-` reads one URI per stdin line;
/// an empty list is the caller's `empty_msg` error. The one entry point
/// every multi-URI command shares.
pub(super) fn resolve_uris(args: &[String], empty_msg: &str) -> rstore_domain::Result<Vec<String>> {
    if args.len() == 1 && args[0] == "-" {
        let uris = read_stdin_uris()?;
        if uris.is_empty() {
            return Err(rstore_domain::Error::invalid_input(empty_msg));
        }
        Ok(uris)
    } else if args.is_empty() {
        Err(rstore_domain::Error::invalid_input(empty_msg))
    } else {
        Ok(args.to_vec())
    }
}

/// Reads one remote reference per line from stdin. Blank lines and `#`
/// comments are ignored.
pub(super) fn read_stdin_uris() -> rstore_domain::Result<Vec<String>> {
    use std::io::BufRead;
    if std::io::stdin().is_terminal() {
        return Err(rstore_domain::Error::invalid_input(
            "`-` reads URIs from stdin, which is a terminal; pipe them instead",
        ));
    }
    let mut uris = Vec::new();
    for line in std::io::stdin().lock().lines() {
        let line =
            line.map_err(|e| rstore_domain::Error::invalid_input("read stdin").with_source(e))?;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        uris.push(line.to_string());
    }
    Ok(uris)
}
