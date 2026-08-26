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
/// every prompting rule for free. Tests inject scripted answers through
/// [`Session::with_answers`] — the interactive paths are the test surface,
/// not dead code behind `--yes`.
#[derive(Clone)]
pub struct Session {
    json: bool,
    stdin_is_tty: bool,
    /// Scripted answers for tests; `None` reads stdin.
    answers: Option<std::sync::Arc<std::sync::Mutex<std::vec::IntoIter<String>>>>,
}

impl Session {
    pub fn detect(json: bool) -> Self {
        Self {
            json,
            stdin_is_tty: std::io::stdin().is_terminal(),
            answers: None,
        }
    }

    /// A test session whose prompts consume the given answers in order.
    #[cfg(test)]
    pub fn with_answers(json: bool, answers: Vec<String>) -> Self {
        Self {
            json,
            stdin_is_tty: !answers.is_empty(),
            answers: Some(std::sync::Arc::new(std::sync::Mutex::new(
                answers.into_iter(),
            ))),
        }
    }

    /// The same scripted session, downgraded to non-interactive — the
    /// prompt must be refused before any answer is read.
    #[cfg(test)]
    pub fn into_non_interactive(mut self) -> Self {
        self.stdin_is_tty = false;
        self
    }

    /// Whether a human can answer a prompt.
    pub fn is_interactive(&self) -> bool {
        !self.json && self.stdin_is_tty
    }

    /// Read one prompt answer: the next scripted line in tests, one stdin
    /// line in production.
    fn read_answer(&self) -> Result<String> {
        if let Some(answers) = &self.answers {
            let mut queue = answers.lock().expect("answer queue poisoned");
            Ok(queue.next().unwrap_or_default().trim().to_string())
        } else {
            let mut line = String::new();
            std::io::stdin()
                .read_line(&mut line)
                .map_err(|e| Error::invalid_input("could not read confirmation").with_source(e))?;
            Ok(line.trim().to_string())
        }
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

#[cfg(test)]
mod conflict_chain_tests {
    use super::*;

    fn non_interactive() -> Session {
        Session {
            json: false,
            stdin_is_tty: false,
            answers: None,
        }
    }

    #[test]
    fn explicit_flag_wins_over_bucket_default() {
        assert_eq!(
            resolve_conflict(ConflictPolicy::Rename, true, &non_interactive()).unwrap(),
            ConflictPolicy::Rename
        );
    }

    #[test]
    fn omitted_flag_falls_back_to_bucket_default() {
        assert_eq!(
            resolve_conflict(ConflictPolicy::Skip, true, &non_interactive()).unwrap(),
            ConflictPolicy::Skip
        );
    }

    #[test]
    fn unresolved_ask_resolves_to_overwrite_with_yes() {
        assert_eq!(
            resolve_conflict(ConflictPolicy::Ask, true, &non_interactive()).unwrap(),
            ConflictPolicy::Overwrite
        );
        assert_eq!(
            resolve_conflict(ConflictPolicy::Ask, true, &non_interactive()).unwrap(),
            ConflictPolicy::Overwrite
        );
    }

    #[test]
    fn unresolved_ask_errors_non_interactively() {
        // Injected session: deterministic regardless of the host's stdin.
        let err = resolve_conflict(ConflictPolicy::Ask, false, &non_interactive()).unwrap_err();
        assert_eq!(err.code, rstore_domain::ErrorCode::InvalidInput);
    }

    #[test]
    fn json_session_is_never_interactive_even_on_a_tty() {
        let machine_on_a_tty = Session::detect(true);
        assert!(!machine_on_a_tty.is_interactive());
        let human = Session {
            json: false,
            stdin_is_tty: true,
            answers: None,
        };
        assert!(human.is_interactive());
    }
}

#[cfg(test)]
mod credential_pair_tests {
    use super::*;

    fn session(json: bool, tty: bool) -> Session {
        Session {
            json,
            stdin_is_tty: tty,
            answers: None,
        }
    }

    #[test]
    fn edit_without_credential_flags_does_not_prompt() {
        let args = crate::args::ConnectionEditArgs {
            reference: "supabase".into(),
            name: None,
            endpoint: None,
            public_base: Some("https://public.example.com".into()),
            cdn_base: None,
            region: None,
            addressing_style: None,
            tls_verify: None,
            access_key: None,
            secret_key: None,
        };

        assert!(read_edit_credential_input(&args, &session(false, true))
            .unwrap()
            .is_none());
    }

    fn pair(access: Option<&str>, secret: Option<&str>) -> Result<Option<Secret<CredentialInput>>> {
        read_credential_pair(access, secret, "A", "S", None, &session(false, false))
    }

    #[test]
    fn complete_flag_pair_is_trimmed_and_wrapped() {
        let creds = pair(Some(" AK "), Some(" SK ")).unwrap().unwrap();
        let input = creds.expose();
        assert_eq!(input.access_key, "AK");
        assert_eq!(input.secret_key, "SK");
    }

    #[test]
    fn empty_flag_values_are_rejected() {
        for (access, secret) in [(Some("x"), Some(" ")), (Some(""), Some("x"))] {
            let err = pair(access, secret).unwrap_err();
            assert_eq!(err.code, rstore_domain::ErrorCode::InvalidInput);
            assert!(err.message.contains("non-empty"));
        }
    }

    #[test]
    fn half_a_flag_pair_is_rejected() {
        for (access, secret) in [(Some("x"), None), (None, Some("x"))] {
            let err = pair(access, secret).unwrap_err();
            assert_eq!(err.code, rstore_domain::ErrorCode::InvalidInput);
            assert!(err.message.contains("together"));
        }
    }

    #[test]
    fn json_mode_never_prompts() {
        assert!(
            read_credential_pair(None, None, "A", "S", None, &session(true, true))
                .unwrap()
                .is_none(),
            "--json stores nothing without flags, tty or not"
        );
    }

    #[test]
    fn non_tty_session_stores_nothing() {
        assert!(
            read_credential_pair(None, None, "A", "S", None, &session(false, false))
                .unwrap()
                .is_none()
        );
    }
}

#[cfg(test)]
mod interactive_tests {
    use super::*;

    fn scripted(answers: &[&str]) -> Session {
        Session::with_answers(false, answers.iter().map(|s| s.to_string()).collect())
    }

    #[test]
    fn confirm_reads_scripted_answers() {
        let yes = scripted(&["y"]);
        assert!(confirm("remove", &yes).unwrap());
        let no = scripted(&["n"]);
        assert!(!confirm("remove", &no).unwrap());
        // Anything but a bare y answers no.
        let gibberish = scripted(&["  YES!  "]);
        assert!(!confirm("remove", &gibberish).unwrap());
    }

    #[test]
    fn destructive_gate_passes_on_yes_and_cancels_on_no() {
        assert!(confirm_destructive("purge", false, &scripted(&["y"])).is_ok());
        let cancelled = confirm_destructive("purge", false, &scripted(&["n"]))
            .expect_err("a refused prompt cancels");
        assert_eq!(cancelled.code, rstore_domain::ErrorCode::Cancelled);
    }

    #[test]
    fn destructive_gate_still_requires_yes_when_non_interactive() {
        let err = confirm_destructive("purge", false, &scripted(&["y"]).into_non_interactive())
            .expect_err("non-interactive must refuse without --yes");
        assert_eq!(err.code, rstore_domain::ErrorCode::InvalidInput);
    }

    #[test]
    fn conflict_prompt_maps_each_choice_and_rejects_gibberish() {
        for (input, expected) in [
            ("o", ConflictPolicy::Overwrite),
            ("s", ConflictPolicy::Skip),
            ("r", ConflictPolicy::Rename),
        ] {
            assert_eq!(
                resolve_conflict(ConflictPolicy::Ask, false, &scripted(&[input])).unwrap(),
                expected
            );
        }
        let cancelled = resolve_conflict(ConflictPolicy::Ask, false, &scripted(&["x"]))
            .expect_err("an unrecognized choice cancels");
        assert_eq!(cancelled.code, rstore_domain::ErrorCode::Cancelled);
    }
}
