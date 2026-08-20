//! Human-readable formatting shared by handler modules (CLI-003).

pub(super) fn human_size(size: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = size as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{size} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

pub(super) fn speed(bytes: u64, elapsed: std::time::Duration) -> String {
    let secs = elapsed.as_secs_f64().max(0.001);
    let bps = bytes as f64 / secs;
    if bps >= 1024.0 * 1024.0 {
        format!("{:.1}MB/s", bps / (1024.0 * 1024.0))
    } else if bps >= 1024.0 {
        format!("{:.1}KB/s", bps / 1024.0)
    } else {
        format!("{bps:.0}B/s")
    }
}

/// Column alignment for [`Table`].
#[derive(Clone, Copy)]
pub(super) enum Align {
    Left,
    Right,
}

/// A human-readable columnar listing (CLI-003): header plus rows, column
/// widths adapting to the widest cell, two-space gutters, an optional detail
/// line nested under a row. One renderer so every `… list` command shares
/// the same table grammar instead of hand-picked `{:<N}` widths.
pub(super) struct Table {
    header: Vec<String>,
    aligns: Vec<Align>,
    rows: Vec<(Vec<String>, Option<String>)>,
}

impl Table {
    /// `header` names the columns (rendered as given, uppercase by
    /// convention); `aligns` must have one entry per column.
    pub(super) fn new(header: &[&str], aligns: &[Align]) -> Self {
        debug_assert_eq!(header.len(), aligns.len(), "one align per column");
        Self {
            header: header.iter().map(|h| h.to_string()).collect(),
            aligns: aligns.to_vec(),
            rows: Vec::new(),
        }
    }

    /// Append a row; `cells` must have one entry per column.
    pub(super) fn row(&mut self, cells: Vec<String>) -> &mut Self {
        debug_assert_eq!(cells.len(), self.header.len(), "one cell per column");
        self.rows.push((cells, None));
        self
    }

    /// Whether any row was pushed — [`listing`]'s empty-state input.
    pub(super) fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Attach a detail line under the most recently added row (indented to
    /// nest under the row, e.g. the `id:` line of a listing).
    pub(super) fn detail(&mut self, detail: String) -> &mut Self {
        if let Some(last) = self.rows.last_mut() {
            last.1 = Some(detail);
        }
        self
    }

    /// Render header + rows; empty tables render nothing (callers print
    /// their own empty-state copy).
    pub(super) fn render(&self) -> String {
        let mut widths: Vec<usize> = self.header.iter().map(|h| h.len()).collect();
        for (cells, _) in &self.rows {
            for (i, cell) in cells.iter().enumerate() {
                widths[i] = widths[i].max(cell.chars().count());
            }
        }
        let mut out = String::new();
        if self.rows.is_empty() {
            return out;
        }
        out.push_str(&self.line(&self.header, &widths));
        for (cells, detail) in &self.rows {
            out.push_str(&self.line(cells, &widths));
            if let Some(d) = detail {
                out.push_str(&format!("    {d}\n"));
            }
        }
        out
    }

    fn line(&self, cells: &[String], widths: &[usize]) -> String {
        let mut line = String::from("  ");
        for (i, cell) in cells.iter().enumerate() {
            if i > 0 {
                line.push_str("  ");
            }
            if i == cells.len() - 1 {
                // The last column is never padded: trailing gaps read as bugs.
                line.push_str(cell);
            } else {
                let padded = format!("{}", Pad(cell, widths[i], &self.aligns[i]));
                line.push_str(&padded);
            }
        }
        line.push('\n');
        line
    }
}

/// Pad `s` to `width` on the side `align` picks, char-wise (paths can carry
/// wide glyphs).
struct Pad<'a>(&'a str, usize, &'a Align);

/// One empty-state grammar for titled listings: the title line over the
/// rendered table, or the empty-state line, never a bare table.
pub(super) fn listing(title: &str, empty: &str, table: Table) -> String {
    if table.is_empty() {
        format!("{empty}\n")
    } else {
        format!("{title}\n{}", table.render())
    }
}

/// The pagination footnote for a titled listing: one grammar, so `remote ls`
/// and `files list` hint the same way. `flag` names the caller's own option.
pub(super) fn more(flag: &str, cursor: &str) -> String {
    format!("  -- more: {flag} {cursor}\n")
}

/// A directory listing keeps its title when empty — an empty remote
/// directory is still named — and paginates through the shared [`more`]
/// footnote.
pub(super) fn dir_listing(
    title: &str,
    empty_line: &str,
    cursor: Option<(&str, &str)>,
    table: Table,
) -> String {
    let mut out = if table.is_empty() {
        format!("{title}\n{empty_line}\n")
    } else {
        format!("{title}\n{}", table.render())
    };
    if let Some((flag, cursor)) = cursor {
        out.push_str(&more(flag, cursor));
    }
    out
}

enum FieldRow {
    Value { label: String, value: String },
    Lines { label: String, lines: Vec<String> },
}

/// A human-readable labeled detail view (CLI-003): an optional header line,
/// then `label : value` lines under one two-space indent, label widths
/// adapting to the longest label, `Option` values rendering as `(none)`.
/// One renderer so every `… show`/`stat` view shares the same grammar
/// instead of hand-counted padding.
pub(super) struct Fields {
    header: Option<String>,
    label_width: usize,
    rows: Vec<FieldRow>,
}

impl Fields {
    pub(super) fn new() -> Self {
        Self {
            header: None,
            label_width: 0,
            rows: Vec::new(),
        }
    }

    /// Render a header line above the fields (the subject of the view).
    pub(super) fn header(&mut self, header: impl std::fmt::Display) -> &mut Self {
        self.header = Some(header.to_string());
        self
    }

    /// Append one `label : value` line.
    pub(super) fn push(&mut self, label: &str, value: impl std::fmt::Display) -> &mut Self {
        self.label_width = self.label_width.max(label.chars().count());
        self.rows.push(FieldRow::Value {
            label: label.to_string(),
            value: value.to_string(),
        });
        self
    }

    /// Append one `label : value` line; `None` renders as `(none)`.
    pub(super) fn push_opt(
        &mut self,
        label: &str,
        value: Option<impl std::fmt::Display>,
    ) -> &mut Self {
        match value {
            Some(v) => self.push(label, v),
            None => self.push(label, "(none)"),
        }
    }

    /// Append a labeled block of pre-rendered lines nested at a four-space
    /// indent (e.g. capability probes).
    pub(super) fn push_lines(&mut self, label: &str, lines: Vec<String>) -> &mut Self {
        self.label_width = self.label_width.max(label.chars().count());
        self.rows.push(FieldRow::Lines {
            label: label.to_string(),
            lines,
        });
        self
    }

    pub(super) fn render(&self) -> String {
        let mut out = String::new();
        if let Some(header) = &self.header {
            out.push_str(header);
            out.push('\n');
        }
        for row in &self.rows {
            match row {
                FieldRow::Value { label, value } => {
                    let pad = " ".repeat(self.label_width.saturating_sub(label.chars().count()));
                    out.push_str(&format!("  {label}{pad} : {value}\n"));
                }
                FieldRow::Lines { label, lines } => {
                    let pad = " ".repeat(self.label_width.saturating_sub(label.chars().count()));
                    out.push_str(&format!("  {label}{pad} :\n"));
                    for line in lines {
                        out.push_str(&format!("    {line}\n"));
                    }
                }
            }
        }
        out
    }
}

impl Default for Fields {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for Pad<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.0.chars().count();
        let pad = self.1.saturating_sub(len);
        match self.2 {
            Align::Left => write!(f, "{}{}", self.0, " ".repeat(pad)),
            Align::Right => write!(f, "{}{}", " ".repeat(pad), self.0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_adapts_widths_and_aligns() {
        let mut t = Table::new(
            &["NAME", "SIZE", "STATE"],
            &[Align::Left, Align::Right, Align::Left],
        );
        t.row(vec!["a.txt".into(), "1024B".into(), "confirmed".into()])
            .row(vec!["much-longer-name".into(), "8B".into(), "stale".into()]);
        assert_eq!(
            t.render(),
            "  NAME               SIZE  STATE\n  \
             a.txt             1024B  confirmed\n  \
             much-longer-name     8B  stale\n"
        );
    }

    #[test]
    fn detail_lines_nest_under_their_row() {
        let mut t = Table::new(&["NAME"], &[Align::Left]);
        t.row(vec!["one".into()])
            .detail("id: conn_x".into())
            .row(vec!["two".into()]);
        assert_eq!(t.render(), "  NAME\n  one\n    id: conn_x\n  two\n");
    }

    #[test]
    fn empty_table_renders_nothing() {
        let t = Table::new(&["A", "B"], &[Align::Left, Align::Left]);
        assert_eq!(t.render(), "");
    }

    #[test]
    fn fields_adapt_label_widths() {
        let mut f = Fields::new();
        f.header("media (id: bucket_x)")
            .push("connection", "nas")
            .push("read-only", false)
            .push_opt("display name", None::<&str>);
        assert_eq!(
            f.render(),
            "media (id: bucket_x)\n  connection   : nas\n  \
             read-only    : false\n  \
             display name : (none)\n"
        );
    }

    #[test]
    fn fields_without_header_and_line_blocks() {
        let mut f = Fields::new();
        f.push("etag", "\"abc\"")
            .push_lines("capabilities", vec!["list : ok".into(), "copy : ok".into()]);
        assert_eq!(
            f.render(),
            "  etag         : \"abc\"\n  \
             capabilities :\n    \
             list : ok\n    \
             copy : ok\n"
        );
    }
}
