//! Bounded span parsing (LINK-006/007, TAG-026).
//!
//! One parser for every `--ttl`-style flag: plain seconds (`30`) or unit
//! sums (`1h30m`) over the caller's allowed unit set, overflow-checked and
//! bounded. Callers decide the unit set and the bound — presign TTLs cap at
//! S3's seven days, tombstone retention at a year.

use crate::{Error, Result};

fn unit_secs(ch: char) -> Option<u64> {
    match ch {
        's' => Some(1),
        'm' => Some(60),
        'h' => Some(3_600),
        'd' => Some(86_400),
        'w' => Some(604_800),
        _ => None,
    }
}

/// Parse a span: plain seconds (`30`) or unit sums (`1h30m`) over `allowed`
/// (a subset of `smhdw`), bounded to `[1s, max_secs]`. `what` names the
/// value in error messages; `limit_note` completes the bound message
/// (e.g. `7d (S3 presign limit)`).
pub fn parse_span(
    raw: &str,
    allowed: &str,
    max_secs: u64,
    what: &str,
    limit_note: &str,
) -> Result<u64> {
    let invalid = || Error::invalid_input(format!("invalid {what} `{raw}`"));
    let secs = if raw.chars().all(|c| c.is_ascii_digit()) && !raw.is_empty() {
        raw.parse::<u64>().map_err(|_| invalid())?
    } else {
        let mut total: u64 = 0;
        let mut number = String::new();
        for ch in raw.chars() {
            if ch.is_ascii_digit() {
                number.push(ch);
                continue;
            }
            let unit = unit_secs(ch)
                .filter(|_| allowed.contains(ch))
                .ok_or_else(invalid)?;
            let n: u64 = number.parse().map_err(|_| invalid())?;
            total = total
                .checked_add(n.checked_mul(unit).ok_or_else(invalid)?)
                .ok_or_else(invalid)?;
            number.clear();
        }
        if !number.is_empty() {
            return Err(invalid());
        }
        total
    };
    if secs == 0 || secs > max_secs {
        return Err(Error::invalid_input(format!(
            "{what} must be between 1s and {limit_note}"
        )));
    }
    Ok(secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_seconds_and_unit_sums() {
        assert_eq!(parse_span("30", "smhd", 604_800, "TTL", "7d").unwrap(), 30);
        assert_eq!(
            parse_span("15m", "smhd", 604_800, "TTL", "7d").unwrap(),
            900
        );
        assert_eq!(
            parse_span("1h30m", "smhd", 604_800, "TTL", "7d").unwrap(),
            5_400
        );
        assert_eq!(
            parse_span("2w", "smhdw", 31_536_000, "retention", "365d").unwrap(),
            1_209_600
        );
    }

    #[test]
    fn bounds_and_bad_input() {
        let ttl = |raw| parse_span(raw, "smhd", 604_800, "TTL", "7d (S3 presign limit)");
        assert!(ttl("8d").is_err());
        assert!(ttl("0").is_err());
        assert!(ttl("1x").is_err());
        assert!(ttl("").is_err());
        // `w` is not in the TTL unit set.
        assert!(ttl("1w").is_err());
    }
}
