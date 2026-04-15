// Interactive TUI for inspecting an Elide volume's ext4 filesystem.
//
// Stage 2a: tree navigation only. The left pane shows a lazy-expanded
// directory tree served by ext4-view over a live `BlockReader`. Refresh
// (`r`) tears down and rebuilds the reader + filesystem — that is the
// ephemeral-"now" snapshot semantics we rely on: `BlockReader::open_live`
// produces a detached lbamap + extent_index at the instant it is called,
// and everything in this TUI reads through that one frozen view until the
// user asks for another.

use std::io::{self, Stdout};
use std::path::Path;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ext4_view::{Ext4, FileType, PathBuf as Ext4PathBuf};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph};

use crate::ls::open_live_reader;

/// Entry point: open the TUI against the volume directory `dir` (typically
/// `<data-dir>/by_name/<name>`). `vol_name` is purely for the header.
pub fn run(dir: &Path, vol_name: &str) -> io::Result<()> {
    let mut state = TuiState::new(dir.to_owned(), vol_name.to_owned());
    state.refresh()?;

    let mut terminal = setup_terminal()?;
    let result = event_loop(&mut terminal, &mut state);
    restore_terminal(&mut terminal)?;
    result
}

// ── terminal lifecycle ───────────────────────────────────────────────────

type Term = Terminal<CrosstermBackend<Stdout>>;

fn setup_terminal() -> io::Result<Term> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    Terminal::new(CrosstermBackend::new(stdout))
}

fn restore_terminal(terminal: &mut Term) -> io::Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

// ── state ────────────────────────────────────────────────────────────────

struct TuiState {
    vol_dir: std::path::PathBuf,
    vol_name: String,
    /// The ext4 view from the most recent refresh. `None` if the volume
    /// could not be loaded as ext4 — an error banner is shown instead and
    /// the tree stays empty.
    fs: Option<Ext4>,
    /// Flattened visible rows. Collapsing a directory removes all of its
    /// descendants from this vector; expanding splices them back in.
    rows: Vec<TreeRow>,
    list_state: ListState,
    refreshed_at: chrono::DateTime<chrono::Local>,
    error: Option<String>,
}

#[derive(Clone)]
struct TreeRow {
    /// Absolute path within the ext4 filesystem. Root is "/".
    path: String,
    /// Leaf name for display. Root's label is "/".
    name: String,
    depth: u16,
    is_dir: bool,
    expanded: bool,
}

impl TuiState {
    fn new(vol_dir: std::path::PathBuf, vol_name: String) -> Self {
        Self {
            vol_dir,
            vol_name,
            fs: None,
            rows: Vec::new(),
            list_state: ListState::default(),
            refreshed_at: chrono::Local::now(),
            error: None,
        }
    }

    /// Tear down and rebuild: reopen_live, reload ext4, reset the tree to
    /// a collapsed root. This is the whole "grab a fresh now" primitive.
    fn refresh(&mut self) -> io::Result<()> {
        self.fs = None;
        self.rows.clear();
        self.error = None;
        self.refreshed_at = chrono::Local::now();

        let reader = open_live_reader(&self.vol_dir)?;
        match Ext4::load(Box::new(reader)) {
            Ok(fs) => {
                self.fs = Some(fs);
                self.rows.push(TreeRow {
                    path: "/".to_owned(),
                    name: "/".to_owned(),
                    depth: 0,
                    is_dir: true,
                    expanded: false,
                });
                if self.list_state.selected().is_none() {
                    self.list_state.select(Some(0));
                }
            }
            Err(e) => {
                self.error = Some(format!("not a readable ext4 volume: {e}"));
            }
        }
        Ok(())
    }

    fn move_cursor(&mut self, delta: isize) {
        if self.rows.is_empty() {
            return;
        }
        let cur = self.list_state.selected().unwrap_or(0) as isize;
        let next = (cur + delta).clamp(0, self.rows.len() as isize - 1) as usize;
        self.list_state.select(Some(next));
    }

    fn toggle_selected(&mut self) {
        let Some(idx) = self.list_state.selected() else {
            return;
        };
        let Some(row) = self.rows.get(idx).cloned() else {
            return;
        };
        if !row.is_dir {
            return;
        }
        if row.expanded {
            self.collapse_at(idx);
        } else {
            self.expand_at(idx);
        }
    }

    fn expand_at(&mut self, idx: usize) {
        let Some(fs) = self.fs.as_ref() else { return };
        let row = self.rows[idx].clone();
        let children = match list_dir_rows(fs, &row.path, row.depth + 1) {
            Ok(c) => c,
            Err(e) => {
                self.error = Some(format!("read_dir {}: {e}", row.path));
                return;
            }
        };
        self.rows[idx].expanded = true;
        for (i, child) in children.into_iter().enumerate() {
            self.rows.insert(idx + 1 + i, child);
        }
    }

    fn collapse_at(&mut self, idx: usize) {
        let depth = self.rows[idx].depth;
        self.rows[idx].expanded = false;
        let mut end = idx + 1;
        while end < self.rows.len() && self.rows[end].depth > depth {
            end += 1;
        }
        self.rows.drain(idx + 1..end);
    }
}

// ── directory listing via ext4-view ──────────────────────────────────────

fn list_dir_rows(fs: &Ext4, abs_path: &str, depth: u16) -> io::Result<Vec<TreeRow>> {
    let path = Ext4PathBuf::new(abs_path);
    let mut entries: Vec<(String, ext4_view::Metadata)> = fs
        .read_dir(&path)
        .map_err(|e| io::Error::other(format!("read_dir: {e}")))?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name() != "." && e.file_name() != "..")
        .filter_map(|e| {
            let name = e.file_name().display().to_string();
            e.metadata().ok().map(|m| (name, m))
        })
        .collect();

    // Dirs first, then files, both alpha within their group.
    entries.sort_by(|a, b| {
        let ad = a.1.is_dir();
        let bd = b.1.is_dir();
        bd.cmp(&ad).then_with(|| a.0.cmp(&b.0))
    });

    let mut rows = Vec::with_capacity(entries.len());
    for (name, meta) in entries {
        let is_dir = matches!(meta.file_type(), FileType::Directory);
        let child_path = if abs_path == "/" {
            format!("/{name}")
        } else {
            format!("{abs_path}/{name}")
        };
        rows.push(TreeRow {
            path: child_path,
            name,
            depth,
            is_dir,
            expanded: false,
        });
    }
    Ok(rows)
}

// ── event loop + rendering ───────────────────────────────────────────────

fn event_loop(terminal: &mut Term, state: &mut TuiState) -> io::Result<()> {
    loop {
        terminal.draw(|f| draw(f, state))?;

        if !event::poll(Duration::from_millis(250))? {
            continue;
        }
        let Event::Key(key) = event::read()? else {
            continue;
        };
        if key.kind != KeyEventKind::Press {
            continue;
        }

        match (key.code, key.modifiers) {
            (KeyCode::Char('q'), _) | (KeyCode::Esc, _) => return Ok(()),
            (KeyCode::Char('c'), KeyModifiers::CONTROL) => return Ok(()),
            (KeyCode::Char('r'), _) => {
                if let Err(e) = state.refresh() {
                    state.error = Some(format!("refresh failed: {e}"));
                }
            }
            (KeyCode::Down, _) | (KeyCode::Char('j'), _) => state.move_cursor(1),
            (KeyCode::Up, _) | (KeyCode::Char('k'), _) => state.move_cursor(-1),
            (KeyCode::PageDown, _) => state.move_cursor(10),
            (KeyCode::PageUp, _) => state.move_cursor(-10),
            (KeyCode::Home, _) | (KeyCode::Char('g'), _) => state.move_cursor(isize::MIN / 2),
            (KeyCode::End, _) | (KeyCode::Char('G'), _) => state.move_cursor(isize::MAX / 2),
            (KeyCode::Enter, _) | (KeyCode::Char(' '), _) | (KeyCode::Right, _) => {
                state.toggle_selected();
            }
            (KeyCode::Left, _) | (KeyCode::Char('h'), _) => {
                // Collapse if selected dir is expanded; otherwise jump to parent row.
                let Some(idx) = state.list_state.selected() else {
                    continue;
                };
                let Some(row) = state.rows.get(idx).cloned() else {
                    continue;
                };
                if row.is_dir && row.expanded {
                    state.collapse_at(idx);
                } else if row.depth > 0 {
                    // Find nearest ancestor row.
                    let mut j = idx;
                    while j > 0 {
                        j -= 1;
                        if state.rows[j].depth < row.depth {
                            break;
                        }
                    }
                    state.list_state.select(Some(j));
                }
            }
            _ => {}
        }
    }
}

fn draw(f: &mut ratatui::Frame<'_>, state: &mut TuiState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // header
            Constraint::Min(1),    // body
            Constraint::Length(1), // footer / help
        ])
        .split(f.area());

    // Header.
    let header = format!(
        " elide tui — {}  @ {}",
        state.vol_name,
        state.refreshed_at.format("%H:%M:%S")
    );
    f.render_widget(
        Paragraph::new(header).style(Style::default().add_modifier(Modifier::BOLD)),
        chunks[0],
    );

    // Body: tree on the left, placeholder on the right.
    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[1]);

    let items: Vec<ListItem> = state
        .rows
        .iter()
        .map(|row| {
            let indent = "  ".repeat(row.depth as usize);
            let marker = if row.is_dir {
                if row.expanded { "▾ " } else { "▸ " }
            } else {
                "  "
            };
            let suffix = if row.is_dir && row.name != "/" {
                "/"
            } else {
                ""
            };
            ListItem::new(Line::from(format!("{indent}{marker}{}{suffix}", row.name)))
        })
        .collect();

    let tree = List::new(items)
        .block(Block::default().title(" files ").borders(Borders::ALL))
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
    f.render_stateful_widget(tree, body[0], &mut state.list_state);

    let detail_text = if let Some(err) = &state.error {
        format!("error: {err}")
    } else if let Some(idx) = state.list_state.selected() {
        state
            .rows
            .get(idx)
            .map(|row| {
                if row.is_dir {
                    format!("{}\n\n(directory)", row.path)
                } else {
                    format!("{}\n\n(fragment detail pane — Stage 2b)", row.path)
                }
            })
            .unwrap_or_default()
    } else {
        String::new()
    };
    f.render_widget(
        Paragraph::new(detail_text).block(Block::default().title(" detail ").borders(Borders::ALL)),
        body[1],
    );

    // Footer.
    let footer = " q/esc quit  r refresh  ↑↓/jk move  ←→/hl collapse/expand  enter toggle ";
    f.render_widget(Paragraph::new(footer), chunks[2]);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(path: &str, depth: u16, is_dir: bool, expanded: bool) -> TreeRow {
        TreeRow {
            path: path.to_owned(),
            name: path.rsplit('/').next().unwrap_or(path).to_owned(),
            depth,
            is_dir,
            expanded,
        }
    }

    fn state_with_rows(rows: Vec<TreeRow>) -> TuiState {
        let mut s = TuiState::new(std::path::PathBuf::new(), String::new());
        s.rows = rows;
        s.list_state.select(Some(0));
        s
    }

    /// collapse_at removes all descendants of the selected row and flips
    /// `expanded` to false, without touching unrelated siblings.
    #[test]
    fn collapse_at_removes_descendants_only() {
        let rows = vec![
            row("/", 0, true, true),
            row("/etc", 1, true, true),
            row("/etc/fstab", 2, false, false),
            row("/etc/hostname", 2, false, false),
            row("/usr", 1, true, false),
        ];
        let mut state = state_with_rows(rows);
        state.collapse_at(1);

        assert_eq!(state.rows.len(), 3);
        assert_eq!(state.rows[0].path, "/");
        assert_eq!(state.rows[1].path, "/etc");
        assert!(!state.rows[1].expanded);
        assert_eq!(state.rows[2].path, "/usr");
    }

    /// move_cursor clamps to [0, len-1] and accepts saturating deltas.
    #[test]
    fn move_cursor_clamps() {
        let rows = vec![
            row("/a", 0, false, false),
            row("/b", 0, false, false),
            row("/c", 0, false, false),
        ];
        let mut state = state_with_rows(rows);

        state.move_cursor(1);
        assert_eq!(state.list_state.selected(), Some(1));
        state.move_cursor(100);
        assert_eq!(state.list_state.selected(), Some(2));
        state.move_cursor(isize::MIN / 2);
        assert_eq!(state.list_state.selected(), Some(0));
    }
}
