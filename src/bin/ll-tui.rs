//! Browse and download files from a `ll-serve` instance.

use std::{
    collections::HashSet,
    io::Stdout,
    net::{SocketAddrV4, SocketAddrV6},
    path::PathBuf,
};

use clap::Parser;
use crossterm::{
    event::{Event, EventStream, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use iroh_tickets::endpoint::EndpointTicket;
use laplink_p2p::{
    endpoint::{build_endpoint, EndpointConfig},
    get_or_create_secret,
    listing::{fetch_listing, Entry, Listing},
    RelayModeOption,
};
use n0_future::StreamExt;
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::Line,
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph},
    Terminal,
};
use tokio::sync::mpsc;

#[derive(Parser, Debug)]
#[command(version, about)]
struct TuiArgs {
    /// The ticket printed by ll-serve.
    ticket: EndpointTicket,

    #[clap(long, default_value = None)]
    magic_ipv4_addr: Option<SocketAddrV4>,

    #[clap(long, default_value = None)]
    magic_ipv6_addr: Option<SocketAddrV6>,

    #[clap(long, default_value_t = RelayModeOption::Default)]
    relay: RelayModeOption,
}

struct Row {
    label: String,
    depth: usize,
    /// Index into `Listing::entries`, `Some` only for file rows.
    entry: Option<usize>,
}

fn build_rows(listing: &Listing) -> Vec<Row> {
    let mut entries: Vec<(usize, &Entry)> = listing.entries.iter().enumerate().collect();
    entries.sort_by(|a, b| a.1.path.cmp(&b.1.path));
    let mut rows = Vec::new();
    let mut seen_dirs: HashSet<String> = HashSet::new();
    for (idx, entry) in entries {
        let parts: Vec<&str> = entry.path.split('/').collect();
        let mut prefix = String::new();
        for (depth, part) in parts.iter().enumerate() {
            let is_last = depth + 1 == parts.len();
            if !prefix.is_empty() {
                prefix.push('/');
            }
            prefix.push_str(part);
            if is_last {
                rows.push(Row {
                    label: format!("📄 {part}"),
                    depth,
                    entry: Some(idx),
                });
            } else if seen_dirs.insert(prefix.clone()) {
                rows.push(Row {
                    label: format!("📁 {part}/"),
                    depth,
                    entry: None,
                });
            }
        }
    }
    rows
}

enum DownloadEvent {
    Progress(u64),
    Done { path: PathBuf },
    Error(String),
}

struct App {
    listing: Listing,
    rows: Vec<Row>,
    /// Indices into `rows` that are selectable (file rows), in display order.
    file_rows: Vec<usize>,
    /// Index into `file_rows`.
    selected: usize,
    status: String,
    downloading: bool,
    download_rx: Option<mpsc::Receiver<DownloadEvent>>,
}

impl App {
    fn new(listing: Listing) -> Self {
        let rows = build_rows(&listing);
        let file_rows = rows
            .iter()
            .enumerate()
            .filter(|(_, r)| r.entry.is_some())
            .map(|(i, _)| i)
            .collect();
        Self {
            listing,
            rows,
            file_rows,
            selected: 0,
            status: "Enter to download, q to quit".to_string(),
            downloading: false,
            download_rx: None,
        }
    }

    fn move_up(&mut self) {
        self.selected = self.selected.saturating_sub(1);
    }

    fn move_down(&mut self) {
        if self.selected + 1 < self.file_rows.len() {
            self.selected += 1;
        }
    }

    fn selected_entry(&self) -> Option<&Entry> {
        let row = *self.file_rows.get(self.selected)?;
        let idx = self.rows[row].entry?;
        self.listing.entries.get(idx)
    }
}

async fn recv_download(rx: &mut Option<mpsc::Receiver<DownloadEvent>>) -> Option<DownloadEvent> {
    match rx {
        Some(r) => r.recv().await,
        None => std::future::pending().await,
    }
}

fn setup_terminal() -> anyhow::Result<Terminal<CrosstermBackend<Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    Ok(Terminal::new(CrosstermBackend::new(stdout))?)
}

struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(std::io::stdout(), LeaveAlternateScreen);
    }
}

fn ui(f: &mut ratatui::Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(3),
        ])
        .split(f.area());

    let header = Paragraph::new(format!("{} entries", app.listing.entries.len()))
        .block(Block::default().borders(Borders::ALL).title("ll-tui"));
    f.render_widget(header, chunks[0]);

    let items: Vec<ListItem> = app
        .rows
        .iter()
        .map(|row| {
            let indent = "  ".repeat(row.depth);
            let style = if row.entry.is_some() {
                Style::default()
            } else {
                Style::default().add_modifier(Modifier::DIM)
            };
            ListItem::new(Line::from(format!("{indent}{}", row.label))).style(style)
        })
        .collect();
    let mut state = ListState::default();
    if let Some(&row) = app.file_rows.get(app.selected) {
        state.select(Some(row));
    }
    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title("files"))
        .highlight_style(
            Style::default()
                .bg(Color::Blue)
                .add_modifier(Modifier::BOLD),
        );
    f.render_stateful_widget(list, chunks[1], &mut state);

    let status = Paragraph::new(app.status.as_str())
        .block(Block::default().borders(Borders::ALL).title("status"));
    f.render_widget(status, chunks[2]);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = TuiArgs::parse();
    let (secret_key, _) = get_or_create_secret()?;

    let lookup_by_dns = args.ticket.endpoint_addr().addrs.is_empty();
    let endpoint = build_endpoint(EndpointConfig {
        secret_key: secret_key.clone(),
        alpns: vec![],
        relay: args.relay.clone(),
        magic_ipv4_addr: args.magic_ipv4_addr,
        magic_ipv6_addr: args.magic_ipv6_addr,
        publish_addr: false,
        lookup_by_dns,
    })
    .await?;

    eprintln!("fetching listing...");
    let listing = fetch_listing(&endpoint, &args.ticket).await?;
    let mut app = App::new(listing);

    let store_dir = std::env::current_dir()?.join(".ll-tui-store");

    let mut terminal = setup_terminal()?;
    let _guard = TerminalGuard;
    let mut events = EventStream::new();

    loop {
        terminal.draw(|f| ui(f, &app))?;
        tokio::select! {
            maybe_event = events.next() => {
                match maybe_event {
                    Some(Ok(Event::Key(key))) if key.kind == KeyEventKind::Press => {
                        match key.code {
                            KeyCode::Char('q') | KeyCode::Esc => break,
                            KeyCode::Up | KeyCode::Char('k') => app.move_up(),
                            KeyCode::Down | KeyCode::Char('j') => app.move_down(),
                            KeyCode::Enter if !app.downloading => {
                                if let Some(entry) = app.selected_entry() {
                                    let entry = entry.clone();
                                    let export_path = laplink_p2p::paths::get_export_path(
                                        &std::env::current_dir()?,
                                        &entry.path,
                                    )?;
                                    let cfg = EndpointConfig {
                                        secret_key: secret_key.clone(),
                                        alpns: vec![],
                                        relay: args.relay.clone(),
                                        magic_ipv4_addr: args.magic_ipv4_addr,
                                        magic_ipv6_addr: args.magic_ipv6_addr,
                                        publish_addr: false,
                                        lookup_by_dns: false,
                                    };
                                    let store_dir = store_dir.clone();
                                    let (tx, rx) = mpsc::channel(32);
                                    app.download_rx = Some(rx);
                                    app.downloading = true;
                                    app.status = format!("downloading {}...", entry.path);
                                    let progress_tx = tx.clone();
                                    tokio::spawn(async move {
                                        let (byte_tx, mut byte_rx) = mpsc::channel(32);
                                        let fwd = tokio::spawn(async move {
                                            while let Some(offset) = byte_rx.recv().await {
                                                progress_tx.send(DownloadEvent::Progress(offset)).await.ok();
                                            }
                                        });
                                        let result = laplink_p2p::receive::receive_single(
                                            entry.ticket.clone(),
                                            cfg,
                                            store_dir,
                                            export_path.clone(),
                                            Some(byte_tx),
                                        )
                                        .await;
                                        fwd.await.ok();
                                        match result {
                                            Ok(_) => {
                                                tx.send(DownloadEvent::Done { path: export_path }).await.ok();
                                            }
                                            Err(e) => {
                                                tx.send(DownloadEvent::Error(e.to_string())).await.ok();
                                            }
                                        }
                                    });
                                }
                            }
                            _ => {}
                        }
                    }
                    Some(Err(_)) | None => break,
                    _ => {}
                }
            }
            ev = recv_download(&mut app.download_rx) => {
                match ev {
                    Some(DownloadEvent::Progress(offset)) => {
                        app.status = format!("downloading... {offset} bytes");
                    }
                    Some(DownloadEvent::Done { path }) => {
                        app.status = format!("saved to {}", path.display());
                        app.downloading = false;
                        app.download_rx = None;
                    }
                    Some(DownloadEvent::Error(e)) => {
                        app.status = format!("error: {e}");
                        app.downloading = false;
                        app.download_rx = None;
                    }
                    None => {
                        app.download_rx = None;
                    }
                }
            }
        }
    }

    drop(terminal);
    tokio::fs::remove_dir_all(&store_dir).await.ok();
    Ok(())
}
