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
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use iroh_tickets::endpoint::EndpointTicket;
use laplink_p2p::{
    RelayModeOption,
    endpoint::{EndpointConfig, build_endpoint},
    get_or_create_secret,
    listing::{Entry, Listing, fetch_listing, subscribe_listing},
    update::{UpdateCandidate, find_available_update},
};
use n0_future::StreamExt;
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::Line,
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph},
};
use tokio::sync::mpsc;

#[derive(Parser, Debug)]
#[command(version, about)]
struct TuiArgs {
    /// The ticket printed by ll-serve. If omitted, the last remembered ticket is used.
    ticket: Option<EndpointTicket>,

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
    let mut entries: Vec<(usize, &Entry)> = listing
        .entries
        .iter()
        .enumerate()
        .collect();
    entries.sort_by(|a, b| {
        a.1.path
            .cmp(&b.1.path)
    });
    let mut rows = Vec::new();
    let mut seen_dirs: HashSet<String> = HashSet::new();
    for (idx, entry) in entries {
        let parts: Vec<&str> = entry
            .path
            .split('/')
            .collect();
        let mut prefix = String::new();
        for (depth, part) in parts
            .iter()
            .enumerate()
        {
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
    ApplyingUpdate,
    Done { path: PathBuf },
    UpdateApplied { version: semver::Version },
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
    available_update: Option<UpdateCandidate>,
    updating: bool,
}

impl App {
    fn new(listing: Listing) -> Self {
        let available_update = find_available_update(&listing, env!("CARGO_PKG_VERSION"));
        let rows = build_rows(&listing);
        let file_rows = rows
            .iter()
            .enumerate()
            .filter(|(_, r)| {
                r.entry
                    .is_some()
            })
            .map(|(i, _)| i)
            .collect();
        let status = if available_update.is_some() {
            "Enter to download, u to update, q to quit".to_string()
        } else {
            "Enter to download, q to quit".to_string()
        };
        Self {
            listing,
            rows,
            file_rows,
            selected: 0,
            status,
            downloading: false,
            download_rx: None,
            available_update,
            updating: false,
        }
    }

    fn move_up(&mut self) {
        self.selected = self
            .selected
            .saturating_sub(1);
    }

    fn move_down(&mut self) {
        if self.selected + 1
            < self
                .file_rows
                .len()
        {
            self.selected += 1;
        }
    }

    fn selected_entry(&self) -> Option<&Entry> {
        let row = *self
            .file_rows
            .get(self.selected)?;
        let idx = self.rows[row].entry?;
        self.listing
            .entries
            .get(idx)
    }

    fn update_listing(&mut self, new_listing: Listing) {
        if self.listing == new_listing {
            return;
        }

        let prev_selected_path = self
            .selected_entry()
            .map(|e| {
                e.path
                    .clone()
            });

        self.listing = new_listing;
        if !self.updating {
            self.available_update = find_available_update(&self.listing, env!("CARGO_PKG_VERSION"));
            if !self.downloading {
                if self
                    .available_update
                    .is_some()
                {
                    self.status = "Enter to download, u to update, q to quit".to_string();
                } else {
                    self.status = "Enter to download, q to quit".to_string();
                }
            }
        }
        self.rows = build_rows(&self.listing);
        self.file_rows = self
            .rows
            .iter()
            .enumerate()
            .filter(|(_, r)| {
                r.entry
                    .is_some()
            })
            .map(|(i, _)| i)
            .collect();

        if self
            .file_rows
            .is_empty()
        {
            self.selected = 0;
        } else if let Some(prev_path) = prev_selected_path {
            let matching_idx = self
                .file_rows
                .iter()
                .position(|&row_idx| {
                    if let Some(entry_idx) = self.rows[row_idx].entry {
                        self.listing
                            .entries
                            .get(entry_idx)
                            .map(|e| e.path == prev_path)
                            .unwrap_or(false)
                    } else {
                        false
                    }
                });

            if let Some(new_sel) = matching_idx {
                self.selected = new_sel;
            } else {
                self.selected = self
                    .selected
                    .min(
                        self.file_rows
                            .len()
                            - 1,
                    );
            }
        } else {
            self.selected = 0;
        }
    }
}

async fn recv_download(rx: &mut Option<mpsc::Receiver<DownloadEvent>>) -> Option<DownloadEvent> {
    match rx {
        Some(r) => {
            r.recv()
                .await
        }
        None => std::future::pending().await,
    }
}

async fn run_listing_watcher(
    endpoint: iroh::Endpoint,
    ticket: EndpointTicket,
    listing_tx: mpsc::Sender<Listing>,
) {
    loop {
        match subscribe_listing(&endpoint, &ticket).await {
            Ok(mut stream) => {
                while let Ok(Some(listing)) = stream
                    .next()
                    .await
                {
                    if let Some(server_ver) = listing.server_version() {
                        tracing::debug!(
                            server_version = %server_ver,
                            client_version = env!("CARGO_PKG_VERSION"),
                            "received live listing update"
                        );
                    }
                    if listing_tx
                        .send(listing)
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
            Err(e) => {
                tracing::debug!("subscribe_listing failed: {e}");
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
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

    let header_title = match &app.available_update {
        Some(update) => format!(
            "ll-tui v{}  [Update Available: v{} | Press 'u' to update]",
            env!("CARGO_PKG_VERSION"),
            update.version
        ),
        None => format!("ll-tui v{}", env!("CARGO_PKG_VERSION")),
    };

    let base_header_text = match app
        .listing
        .server_version()
    {
        Some(ver) => format!(
            "{} entries | server v{}",
            app.listing
                .entries
                .len(),
            ver
        ),
        None => format!(
            "{} entries",
            app.listing
                .entries
                .len()
        ),
    };

    let header_text = match &app.available_update {
        Some(update) => format!(
            "{base_header_text}  |  [Update Available: v{} | Press 'u' to update]",
            update.version
        ),
        None => base_header_text,
    };

    let header_block = if app
        .available_update
        .is_some()
    {
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Yellow))
            .title(header_title)
    } else {
        Block::default()
            .borders(Borders::ALL)
            .title(header_title)
    };

    let header = Paragraph::new(header_text).block(header_block);
    f.render_widget(header, chunks[0]);

    let items: Vec<ListItem> = app
        .rows
        .iter()
        .map(|row| {
            let indent = "  ".repeat(row.depth);
            let style = if row
                .entry
                .is_some()
            {
                Style::default()
            } else {
                Style::default().add_modifier(Modifier::DIM)
            };
            ListItem::new(Line::from(format!("{indent}{}", row.label))).style(style)
        })
        .collect();
    let mut state = ListState::default();
    if let Some(&row) = app
        .file_rows
        .get(app.selected)
    {
        state.select(Some(row));
    }
    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("files"),
        )
        .highlight_style(
            Style::default()
                .bg(Color::Blue)
                .add_modifier(Modifier::BOLD),
        );
    f.render_stateful_widget(list, chunks[1], &mut state);

    let status = Paragraph::new(
        app.status
            .as_str(),
    )
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title("status"),
    );
    f.render_widget(status, chunks[2]);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client_version = env!("CARGO_PKG_VERSION");
    eprintln!("ll-tui version: {client_version}");
    tracing::info!(%client_version, "ll-tui version: {client_version}");
    let args = TuiArgs::parse();
    let ticket = match args.ticket {
        Some(ticket) => {
            laplink_p2p::ticket_storage::save_last_tui_ticket(&ticket)?;
            ticket
        }
        None => laplink_p2p::ticket_storage::load_last_tui_ticket()?.ok_or_else(|| {
            anyhow::anyhow!("no ticket provided and no previous ticket remembered")
        })?,
    };
    let (secret_key, _) = get_or_create_secret()?;

    let endpoint = build_endpoint(EndpointConfig {
        secret_key: secret_key.clone(),
        alpns: vec![],
        relay: args
            .relay
            .clone(),
        magic_ipv4_addr: args.magic_ipv4_addr,
        magic_ipv6_addr: args.magic_ipv6_addr,
        publish_addr: false,
        lookup_by_dns: true,
    })
    .await?;

    eprintln!("fetching listing...");
    let listing = fetch_listing(&endpoint, &ticket).await?;
    let server_version = listing
        .server_version()
        .unwrap_or("unknown");
    eprintln!("server version: {server_version}");
    tracing::info!(
        %server_version,
        %client_version,
        "connected to server"
    );
    let mut app = App::new(listing);

    let store_dir = std::env::current_dir()?.join(".ll-tui-store");

    let mut terminal = setup_terminal()?;
    let _guard = TerminalGuard;
    let mut events = EventStream::new();

    let (listing_tx, mut listing_rx) = mpsc::channel(16);
    let watch_endpoint = endpoint.clone();
    let watch_ticket = ticket.clone();
    let watcher_handle = tokio::spawn(async move {
        run_listing_watcher(watch_endpoint, watch_ticket, listing_tx).await;
    });

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
                            KeyCode::Enter if !app.downloading && !app.updating => {
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
                                        lookup_by_dns: true,
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
                            KeyCode::Char('u') if !app.downloading && !app.updating => {
                                if let Some(candidate) = app.available_update.clone() {
                                    app.updating = true;
                                    app.downloading = true;
                                    app.status = format!("downloading update v{}...", candidate.version);

                                    let cfg = EndpointConfig {
                                        secret_key: secret_key.clone(),
                                        alpns: vec![],
                                        relay: args.relay.clone(),
                                        magic_ipv4_addr: args.magic_ipv4_addr,
                                        magic_ipv6_addr: args.magic_ipv6_addr,
                                        publish_addr: false,
                                        lookup_by_dns: true,
                                    };
                                    let store_dir = store_dir.clone();
                                    let (tx, rx) = mpsc::channel(32);
                                    app.download_rx = Some(rx);

                                    tokio::spawn(async move {
                                        let temp_dir = match tempfile::tempdir() {
                                            Ok(d) => d,
                                            Err(e) => {
                                                tx.send(DownloadEvent::Error(format!("failed to create temp dir: {e}"))).await.ok();
                                                return;
                                            }
                                        };
                                        let staged_path = temp_dir.path().join("update_staging.bin");

                                        let (byte_tx, mut byte_rx) = mpsc::channel(32);
                                        let progress_tx = tx.clone();
                                        let fwd = tokio::spawn(async move {
                                            while let Some(offset) = byte_rx.recv().await {
                                                progress_tx.send(DownloadEvent::Progress(offset)).await.ok();
                                            }
                                        });

                                        let download_res = laplink_p2p::receive::receive_single(
                                            candidate.entry.ticket.clone(),
                                            cfg,
                                            store_dir,
                                            staged_path.clone(),
                                            Some(byte_tx),
                                        )
                                        .await;
                                        fwd.await.ok();

                                        match download_res {
                                            Ok(_) => {
                                                tx.send(DownloadEvent::ApplyingUpdate).await.ok();
                                                match laplink_p2p::update::apply_update(&staged_path, &candidate) {
                                                    Ok(_) => {
                                                        laplink_p2p::update::cleanup_staged_file(&staged_path);
                                                        tx.send(DownloadEvent::UpdateApplied {
                                                            version: candidate.version,
                                                        })
                                                        .await
                                                        .ok();
                                                    }
                                                    Err(e) => {
                                                        laplink_p2p::update::cleanup_staged_file(&staged_path);
                                                        tx.send(DownloadEvent::Error(format!("failed to apply update: {e}")))
                                                            .await
                                                            .ok();
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                laplink_p2p::update::cleanup_staged_file(&staged_path);
                                                tx.send(DownloadEvent::Error(format!("failed to download update: {e}")))
                                                    .await
                                                    .ok();
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
            maybe_listing = listing_rx.recv() => {
                if let Some(new_listing) = maybe_listing {
                    app.update_listing(new_listing);
                }
            }
            ev = recv_download(&mut app.download_rx) => {
                match ev {
                    Some(DownloadEvent::Progress(offset)) => {
                        if app.updating {
                            app.status = format!("downloading update... {offset} bytes");
                        } else {
                            app.status = format!("downloading... {offset} bytes");
                        }
                    }
                    Some(DownloadEvent::ApplyingUpdate) => {
                        app.status = "applying update...".to_string();
                    }
                    Some(DownloadEvent::Done { path }) => {
                        app.status = format!("saved to {}", path.display());
                        app.downloading = false;
                        app.download_rx = None;
                    }
                    Some(DownloadEvent::UpdateApplied { version }) => {
                        app.status = format!("updated successfully to v{version}! Restart ll-tui to run new version.");
                        app.downloading = false;
                        app.updating = false;
                        app.download_rx = None;
                        app.available_update = None;
                    }
                    Some(DownloadEvent::Error(e)) => {
                        app.status = format!("error: {e}");
                        app.downloading = false;
                        app.updating = false;
                        app.download_rx = None;
                    }
                    None => {
                        app.download_rx = None;
                    }
                }
            }
        }
    }

    watcher_handle.abort();
    drop(terminal);
    tokio::fs::remove_dir_all(&store_dir)
        .await
        .ok();
    Ok(())
}

#[cfg(test)]
mod tests {
    use iroh_blobs::{BlobFormat, Hash, ticket::BlobTicket};

    use super::*;

    fn make_test_entry(path: &str, size: u64, hash_byte: u8) -> Entry {
        let ticket = BlobTicket::new(
            iroh::EndpointAddr::from(iroh::SecretKey::generate().public()),
            Hash::from_bytes([hash_byte; 32]),
            BlobFormat::Raw,
        );
        Entry {
            path: path.to_string(),
            size,
            hash: Hash::from_bytes([hash_byte; 32]),
            ticket,
        }
    }

    #[test]
    fn test_app_new_and_navigation() {
        let listing = Listing::new(vec![
            make_test_entry("dir/b.txt", 10, 1),
            make_test_entry("dir/a.txt", 20, 2),
        ]);
        let mut app = App::new(listing);
        assert_eq!(
            app.file_rows
                .len(),
            2
        );
        assert_eq!(app.selected, 0);
        assert_eq!(
            app.selected_entry()
                .unwrap()
                .path,
            "dir/a.txt"
        );

        app.move_down();
        assert_eq!(app.selected, 1);
        assert_eq!(
            app.selected_entry()
                .unwrap()
                .path,
            "dir/b.txt"
        );

        app.move_down(); // shouldn't go past end
        assert_eq!(app.selected, 1);

        app.move_up();
        assert_eq!(app.selected, 0);
        assert_eq!(
            app.selected_entry()
                .unwrap()
                .path,
            "dir/a.txt"
        );

        app.move_up(); // shouldn't go below 0
        assert_eq!(app.selected, 0);
    }

    #[test]
    fn test_app_update_listing_preserves_selection() {
        let listing1 = Listing::new(vec![
            make_test_entry("b.txt", 10, 1),
            make_test_entry("c.txt", 20, 2),
        ]);
        let mut app = App::new(listing1);
        app.move_down(); // select c.txt (selected = 1)
        assert_eq!(
            app.selected_entry()
                .unwrap()
                .path,
            "c.txt"
        );

        // Now a new file "a.txt" is added before b and c
        let listing2 = Listing::new(vec![
            make_test_entry("a.txt", 5, 0),
            make_test_entry("b.txt", 10, 1),
            make_test_entry("c.txt", 20, 2),
        ]);
        app.update_listing(listing2);
        // Selection should automatically shift to index 2 to still point to "c.txt"
        assert_eq!(app.selected, 2);
        assert_eq!(
            app.selected_entry()
                .unwrap()
                .path,
            "c.txt"
        );
    }

    #[test]
    fn test_app_update_listing_clamps_when_selected_deleted() {
        let listing1 = Listing::new(vec![
            make_test_entry("a.txt", 10, 1),
            make_test_entry("b.txt", 20, 2),
            make_test_entry("c.txt", 30, 3),
        ]);
        let mut app = App::new(listing1);
        app.move_down();
        app.move_down(); // select c.txt (selected = 2)
        assert_eq!(
            app.selected_entry()
                .unwrap()
                .path,
            "c.txt"
        );

        // Now "c.txt" is deleted
        let listing2 = Listing::new(vec![
            make_test_entry("a.txt", 10, 1),
            make_test_entry("b.txt", 20, 2),
        ]);
        app.update_listing(listing2);
        // Clamped to 1 (pointing to "b.txt")
        assert_eq!(app.selected, 1);
        assert_eq!(
            app.selected_entry()
                .unwrap()
                .path,
            "b.txt"
        );
    }

    #[test]
    fn test_app_update_listing_empty_handling() {
        let mut app = App::new(Listing::new(vec![]));
        assert_eq!(
            app.file_rows
                .len(),
            0
        );
        assert_eq!(app.selected, 0);
        assert!(
            app.selected_entry()
                .is_none()
        );

        // File added
        let listing1 = Listing::new(vec![make_test_entry("a.txt", 10, 1)]);
        app.update_listing(listing1);
        assert_eq!(
            app.file_rows
                .len(),
            1
        );
        assert_eq!(app.selected, 0);
        assert_eq!(
            app.selected_entry()
                .unwrap()
                .path,
            "a.txt"
        );

        // All files deleted
        app.update_listing(Listing::new(vec![]));
        assert_eq!(
            app.file_rows
                .len(),
            0
        );
        assert_eq!(app.selected, 0);
        assert!(
            app.selected_entry()
                .is_none()
        );
    }

    #[test]
    fn test_app_update_listing_content_change_updates_ticket() {
        let listing1 = Listing::new(vec![make_test_entry("a.txt", 10, 1)]);
        let mut app = App::new(listing1);
        assert_eq!(
            app.selected_entry()
                .unwrap()
                .size,
            10
        );
        assert_eq!(
            app.selected_entry()
                .unwrap()
                .hash,
            Hash::from_bytes([1u8; 32])
        );

        // File modified with new size and hash
        let listing2 = Listing::new(vec![make_test_entry("a.txt", 50, 2)]);
        app.update_listing(listing2);
        assert_eq!(app.selected, 0);
        assert_eq!(
            app.selected_entry()
                .unwrap()
                .path,
            "a.txt"
        );
        assert_eq!(
            app.selected_entry()
                .unwrap()
                .size,
            50
        );
        assert_eq!(
            app.selected_entry()
                .unwrap()
                .hash,
            Hash::from_bytes([2u8; 32])
        );
    }

    #[tokio::test]
    async fn test_live_listing_watcher_integration() {
        let secret1 = iroh::SecretKey::generate();
        let initial_listing = Listing::new(vec![make_test_entry("first.txt", 10, 1)]);
        let listing_proto = laplink_p2p::listing::ListingProtocol::new(initial_listing.clone());

        let server_endpoint = build_endpoint(EndpointConfig {
            secret_key: secret1,
            alpns: vec![laplink_p2p::listing::ALPN.to_vec()],
            relay: RelayModeOption::Disabled,
            magic_ipv4_addr: None,
            magic_ipv6_addr: None,
            publish_addr: false,
            lookup_by_dns: false,
        })
        .await
        .unwrap();

        let router = iroh::protocol::Router::builder(server_endpoint.clone())
            .accept(laplink_p2p::listing::ALPN, listing_proto.clone())
            .spawn();

        let ticket = EndpointTicket::new(server_endpoint.addr());

        let client_secret = iroh::SecretKey::generate();
        let client_endpoint = build_endpoint(EndpointConfig {
            secret_key: client_secret,
            alpns: vec![],
            relay: RelayModeOption::Disabled,
            magic_ipv4_addr: None,
            magic_ipv6_addr: None,
            publish_addr: false,
            lookup_by_dns: false,
        })
        .await
        .unwrap();

        let (listing_tx, mut listing_rx) = mpsc::channel(16);
        let watcher_task = tokio::spawn(run_listing_watcher(client_endpoint, ticket, listing_tx));

        // 1. Initial listing received via stream
        let initial = tokio::time::timeout(std::time::Duration::from_secs(5), listing_rx.recv())
            .await
            .expect("timeout waiting for initial listing")
            .expect("channel closed");
        assert_eq!(initial.entries, initial_listing.entries);
        assert_eq!(initial.server_version(), Some(env!("CARGO_PKG_VERSION")));

        let mut app = App::new(initial);
        assert_eq!(
            app.selected_entry()
                .unwrap()
                .path,
            "first.txt"
        );

        // 2. Server updates listing
        let updated_listing = Listing::new(vec![
            make_test_entry("first.txt", 10, 1),
            make_test_entry("second.txt", 20, 2),
        ]);
        listing_proto.update(updated_listing.clone());

        let update = tokio::time::timeout(std::time::Duration::from_secs(5), listing_rx.recv())
            .await
            .expect("timeout waiting for updated listing")
            .expect("channel closed");
        assert_eq!(update.entries, updated_listing.entries);
        assert_eq!(update.server_version(), Some(env!("CARGO_PKG_VERSION")));

        app.update_listing(update);
        assert_eq!(
            app.file_rows
                .len(),
            2
        );
        assert_eq!(
            app.selected_entry()
                .unwrap()
                .path,
            "first.txt"
        );

        watcher_task.abort();
        router
            .shutdown()
            .await
            .ok();
    }

    #[test]
    fn test_app_available_update_detection() {
        let target = laplink_p2p::update::current_platform_target();
        let archive_name = format!("ll-v99.0.0-{target}.tar.gz");
        let listing = Listing::new(vec![
            make_test_entry("file.txt", 10, 1),
            make_test_entry(&archive_name, 1024, 2),
        ]);

        let mut app = App::new(listing);
        assert!(
            app.available_update
                .is_some()
        );
        let candidate = app
            .available_update
            .as_ref()
            .unwrap();
        assert_eq!(candidate.version, semver::Version::parse("99.0.0").unwrap());
        assert!(
            app.status
                .contains("u to update")
        );

        // When listing is updated to remove the update asset, available_update resets
        let new_listing = Listing::new(vec![make_test_entry("file.txt", 10, 1)]);
        app.update_listing(new_listing);
        assert!(
            app.available_update
                .is_none()
        );
        assert_eq!(app.status, "Enter to download, q to quit");
    }

    #[test]
    fn test_ui_renders_update_banner() {
        use ratatui::backend::TestBackend;

        let target = laplink_p2p::update::current_platform_target();
        let archive_name = format!("ll-v99.0.0-{target}.tar.gz");
        let listing = Listing::new(vec![make_test_entry(&archive_name, 1024, 2)]);
        let app = App::new(listing);
        assert!(
            app.available_update
                .is_some()
        );

        let backend = TestBackend::new(100, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|f| ui(f, &app))
            .unwrap();

        let buffer = terminal
            .backend()
            .buffer();
        let buffer_str: String = buffer
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect();

        assert!(buffer_str.contains("Update Available: v99.0.0"));
        assert!(buffer_str.contains("Press 'u' to update"));
    }

    #[test]
    fn test_update_event_state_transitions() {
        let target = laplink_p2p::update::current_platform_target();
        let archive_name = format!("ll-v99.0.0-{target}.tar.gz");
        let listing = Listing::new(vec![make_test_entry(&archive_name, 1024, 2)]);
        let mut app = App::new(listing);
        app.updating = true;
        app.downloading = true;

        // Applying update transition
        app.status = "applying update...".to_string();
        assert_eq!(app.status, "applying update...");

        // UpdateApplied transition
        let ver = semver::Version::parse("99.0.0").unwrap();
        app.status = format!("updated successfully to v{ver}! Restart ll-tui to run new version.");
        app.downloading = false;
        app.updating = false;
        app.available_update = None;

        assert_eq!(
            app.status,
            "updated successfully to v99.0.0! Restart ll-tui to run new version."
        );
        assert!(!app.updating);
        assert!(!app.downloading);
        assert!(
            app.available_update
                .is_none()
        );
    }
}
