//! Filesystem monitoring and dynamic listing updates for `ll-serve`.

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    time::Duration,
};

use iroh::EndpointAddr;
use iroh_blobs::{
    BlobFormat,
    api::{Store, TempTag},
    store::fs::FsStore,
    ticket::BlobTicket,
};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::{mpsc, watch};
use walkdir::WalkDir;

use crate::{
    listing::{Entry, Listing, ListingProtocol},
    paths::canonicalized_path_to_string,
    transfer::import_one,
};

/// Mapping from relative file paths to their listing entry and active blob pin (`TempTag`).
pub type EntriesMap = HashMap<String, (Entry, TempTag)>;

/// Context parameters used during incremental path processing.
pub struct ProcessContext<'a> {
    pub folder: &'a Path,
    pub store_dir: &'a Path,
    pub store: &'a Store,
    pub addr: &'a EndpointAddr,
    pub listing_protocol: &'a ListingProtocol,
}

/// Handle representing an active filesystem watcher.
///
/// Dropping the handle aborts the background task and unregisters the OS watcher.
pub struct WatcherHandle {
    _watcher: RecommendedWatcher,
    task: tokio::task::JoinHandle<()>,
    listing_protocol: ListingProtocol,
}

impl WatcherHandle {
    /// Subscribe to listing updates.
    pub fn subscribe(&self) -> watch::Receiver<Listing> {
        self.listing_protocol
            .subscribe()
    }

    /// Abort the background watcher task.
    pub fn abort(&self) {
        self.task
            .abort();
    }
}

impl Drop for WatcherHandle {
    fn drop(&mut self) {
        self.task
            .abort();
    }
}

/// Helper to determine if a path is inside `store_dir` or is an internal store path.
pub fn is_store_dir_or_internal(store_dir: &Path, path: &Path) -> bool {
    if path.starts_with(store_dir) {
        return true;
    }
    for component in path.components() {
        if component.as_os_str() == ".ll-serve-store" {
            return true;
        }
    }
    false
}

/// Convert an event path into a relative, "/"-joined listing path relative to `folder`.
pub fn get_relative_name(folder: &Path, path: &Path) -> Option<String> {
    if let Ok(rel) = path.strip_prefix(folder) {
        if rel
            .as_os_str()
            .is_empty()
        {
            return None;
        }
        return canonicalized_path_to_string(rel, true).ok();
    }
    if path.is_relative() {
        let full = folder.join(path);
        if let Ok(rel) = full.strip_prefix(folder)
            && !rel
                .as_os_str()
                .is_empty()
        {
            return canonicalized_path_to_string(rel, true).ok();
        }
    }
    if path.exists()
        && let Ok(canon) = path.canonicalize()
        && let Ok(rel) = canon.strip_prefix(folder)
        && !rel
            .as_os_str()
            .is_empty()
    {
        return canonicalized_path_to_string(rel, true).ok();
    }
    None
}

/// Import a single file into the blob store and update `entries_map` if new or modified.
async fn import_and_update(
    store: &Store,
    addr: &EndpointAddr,
    path: &Path,
    rel_name: String,
    entries_map: &mut HashMap<String, (Entry, TempTag)>,
    changed: &mut bool,
) {
    match import_one(store, path.to_path_buf()).await {
        Ok((tag, size)) => {
            let hash = tag.hash();
            let is_existing = entries_map.get(&rel_name);
            let needs_update = match is_existing {
                Some((existing, _)) => existing.hash != hash || existing.size != size,
                None => true,
            };
            if needs_update {
                let is_new = is_existing.is_none();
                if is_new {
                    eprintln!("file added: {rel_name}");
                    tracing::info!(file = %rel_name, "file added");
                } else {
                    eprintln!("file modified: {rel_name}");
                    tracing::info!(file = %rel_name, "file modified");
                }
                let ticket = BlobTicket::new(addr.clone(), hash, BlobFormat::Raw);
                let entry = Entry {
                    path: rel_name.clone(),
                    size,
                    hash,
                    ticket,
                };
                entries_map.insert(rel_name, (entry, tag));
                *changed = true;
            }
        }
        Err(e) => {
            tracing::warn!("failed to import {}: {e}", path.display());
        }
    }
}

/// Incrementally process a batch of changed paths.
///
/// Returns `true` if any entry in the listing was added, modified, or removed.
pub async fn process_paths(
    ctx: &ProcessContext<'_>,
    entries_map: &mut EntriesMap,
    paths: Vec<PathBuf>,
) -> bool {
    let mut changed = false;

    for path in paths {
        if is_store_dir_or_internal(ctx.store_dir, &path) {
            continue;
        }

        if path.exists() {
            if path.is_dir() {
                // A directory was created or modified. Walk its contents for files.
                for entry in WalkDir::new(&path)
                    .into_iter()
                    .filter_map(|e| e.ok())
                {
                    let child_path = entry.into_path();
                    if is_store_dir_or_internal(ctx.store_dir, &child_path) {
                        continue;
                    }
                    if let Ok(meta) = std::fs::symlink_metadata(&child_path) {
                        if meta
                            .file_type()
                            .is_symlink()
                            || !meta
                                .file_type()
                                .is_file()
                        {
                            continue;
                        }
                    } else {
                        continue;
                    }
                    if let Some(rel_name) = get_relative_name(ctx.folder, &child_path) {
                        import_and_update(
                            ctx.store,
                            ctx.addr,
                            &child_path,
                            rel_name,
                            entries_map,
                            &mut changed,
                        )
                        .await;
                    }
                }
            } else if path.is_file()
                && let Ok(meta) = std::fs::symlink_metadata(&path)
                && !meta
                    .file_type()
                    .is_symlink()
                && let Some(rel_name) = get_relative_name(ctx.folder, &path)
            {
                import_and_update(
                    ctx.store,
                    ctx.addr,
                    &path,
                    rel_name,
                    entries_map,
                    &mut changed,
                )
                .await;
            }
        } else {
            // Path no longer exists: handle single file deletion or directory tree deletion.
            if let Some(rel_name) = get_relative_name(ctx.folder, &path) {
                if entries_map
                    .remove(&rel_name)
                    .is_some()
                {
                    eprintln!("file removed: {rel_name}");
                    tracing::info!(file = %rel_name, "file removed");
                    changed = true;
                }
                let dir_prefix = format!("{rel_name}/");
                let removed_keys: Vec<String> = entries_map
                    .keys()
                    .filter(|k| k.starts_with(&dir_prefix))
                    .cloned()
                    .collect();
                for k in removed_keys {
                    entries_map.remove(&k);
                    eprintln!("file removed: {k}");
                    tracing::info!(file = %k, "file removed");
                    changed = true;
                }
            }
        }
    }

    if changed {
        let mut entries: Vec<Entry> = entries_map
            .values()
            .map(|(e, _)| e.clone())
            .collect();
        entries.sort_by(|a, b| {
            a.path
                .cmp(&b.path)
        });
        let new_listing = Listing::new(entries);
        ctx.listing_protocol
            .update(new_listing.clone());
        eprintln!(
            "filesystem change detected: updated listing ({} files)",
            new_listing
                .entries
                .len()
        );
        tracing::info!(
            files = new_listing
                .entries
                .len(),
            "filesystem change detected: updated listing"
        );
    }

    changed
}

/// Spawn an asynchronous filesystem watcher on `folder`.
pub fn spawn_watcher(
    folder: PathBuf,
    store_dir: PathBuf,
    store: FsStore,
    addr: EndpointAddr,
    mut entries_map: EntriesMap,
    listing_protocol: ListingProtocol,
    debounce_interval: Duration,
) -> Result<WatcherHandle, (anyhow::Error, EntriesMap)> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut watcher = match RecommendedWatcher::new(
        move |res| {
            let _ = tx.send(res);
        },
        notify::Config::default(),
    ) {
        Ok(w) => w,
        Err(e) => return Err((e.into(), entries_map)),
    };

    if let Err(e) = watcher.watch(&folder, RecursiveMode::Recursive) {
        return Err((e.into(), entries_map));
    }

    let lp = listing_protocol.clone();
    let task = tokio::spawn(async move {
        let mut debounced_paths = HashSet::new();
        let max_debounce_time = Duration::from_secs(2);
        let mut first_event_time: Option<tokio::time::Instant> = None;

        let sleep = tokio::time::sleep(debounce_interval);
        tokio::pin!(sleep);
        let mut sleeping = false;

        let ctx = ProcessContext {
            folder: &folder,
            store_dir: &store_dir,
            store: store.as_ref(),
            addr: &addr,
            listing_protocol: &lp,
        };

        loop {
            tokio::select! {
                maybe_event = rx.recv() => {
                    match maybe_event {
                        Some(Ok(event)) => {
                            if matches!(event.kind, notify::EventKind::Access(_)) {
                                continue;
                            }
                            for path in event.paths {
                                debounced_paths.insert(path);
                            }
                            let now = tokio::time::Instant::now();
                            if !sleeping {
                                sleep.as_mut().reset(now + debounce_interval);
                                sleeping = true;
                                first_event_time = Some(now);
                            } else if let Some(first) = first_event_time {
                                let max_deadline = first + max_debounce_time;
                                let next_deadline = (now + debounce_interval).min(max_deadline);
                                sleep.as_mut().reset(next_deadline);
                            }
                        }
                        Some(Err(err)) => {
                            tracing::warn!("filesystem watcher error: {err}");
                        }
                        None => break,
                    }
                }
                () = &mut sleep, if sleeping => {
                    sleeping = false;
                    first_event_time = None;
                    let paths: Vec<PathBuf> = debounced_paths.drain().collect();
                    process_paths(&ctx, &mut entries_map, paths).await;
                }
            }
        }
    });

    Ok(WatcherHandle {
        _watcher: watcher,
        task,
        listing_protocol,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store_dir_detection() {
        let root = PathBuf::from("/tmp/serve");
        let store = root.join(".ll-serve-store");
        assert!(is_store_dir_or_internal(&store, &store));
        assert!(is_store_dir_or_internal(&store, &store.join("blobs.db")));
        assert!(is_store_dir_or_internal(
            &store,
            &root.join(".ll-serve-store/ticket")
        ));
        assert!(!is_store_dir_or_internal(&store, &root.join("hello.txt")));
        assert!(!is_store_dir_or_internal(&store, &root.join("sub/foo.txt")));
    }

    #[test]
    fn test_relative_name() {
        let root = PathBuf::from("/tmp/serve");
        assert_eq!(get_relative_name(&root, &root), None);
        assert_eq!(
            get_relative_name(&root, &root.join("a.txt")),
            Some("a.txt".to_string())
        );
        assert_eq!(
            get_relative_name(&root, &root.join("sub/b.txt")),
            Some("sub/b.txt".to_string())
        );
    }

    #[tokio::test]
    async fn test_process_paths_lifecycle() {
        let temp_dir = tempfile::tempdir().unwrap();
        let folder = temp_dir
            .path()
            .canonicalize()
            .unwrap();
        let store_dir = folder.join(".ll-serve-store");
        tokio::fs::create_dir_all(&store_dir)
            .await
            .unwrap();

        let store = FsStore::load(&store_dir)
            .await
            .unwrap();
        let key = iroh::SecretKey::generate();
        let addr = iroh::EndpointAddr::from(key.public());

        let initial_listing = Listing::new(vec![]);
        let listing_protocol = ListingProtocol::new(initial_listing);
        let mut update_rx = listing_protocol.subscribe();
        let mut entries_map = HashMap::new();

        let ctx = ProcessContext {
            folder: &folder,
            store_dir: &store_dir,
            store: store.as_ref(),
            addr: &addr,
            listing_protocol: &listing_protocol,
        };

        // 1. Add a file
        let file1 = folder.join("file1.txt");
        std::fs::write(&file1, b"hello world").unwrap();

        let changed = process_paths(&ctx, &mut entries_map, vec![file1.clone()]).await;
        assert!(changed);
        assert_eq!(
            listing_protocol
                .listing()
                .entries
                .len(),
            1
        );
        assert_eq!(
            listing_protocol
                .listing()
                .entries[0]
                .path,
            "file1.txt"
        );
        assert_eq!(
            listing_protocol
                .listing()
                .entries[0]
                .size,
            11
        );
        assert_eq!(
            update_rx
                .borrow_and_update()
                .entries
                .len(),
            1
        );

        // 2. Modify the file
        std::fs::write(&file1, b"hello world updated").unwrap();
        let changed = process_paths(&ctx, &mut entries_map, vec![file1.clone()]).await;
        assert!(changed);
        assert_eq!(
            listing_protocol
                .listing()
                .entries
                .len(),
            1
        );
        assert_eq!(
            listing_protocol
                .listing()
                .entries[0]
                .size,
            19
        );

        // 3. Add a file in a subdirectory
        let sub = folder.join("sub");
        std::fs::create_dir_all(&sub).unwrap();
        let file2 = sub.join("file2.txt");
        std::fs::write(&file2, b"sub content").unwrap();

        let changed = process_paths(&ctx, &mut entries_map, vec![sub.clone()]).await;
        assert!(changed);
        assert_eq!(
            listing_protocol
                .listing()
                .entries
                .len(),
            2
        );
        assert_eq!(
            listing_protocol
                .listing()
                .entries[1]
                .path,
            "sub/file2.txt"
        );

        // 4. Activity in store_dir is ignored
        let store_file = store_dir.join("some_db.bin");
        std::fs::write(&store_file, b"ignored db content").unwrap();
        let changed = process_paths(&ctx, &mut entries_map, vec![store_file]).await;
        assert!(!changed);
        assert_eq!(
            listing_protocol
                .listing()
                .entries
                .len(),
            2
        );

        // 5. Delete a file
        std::fs::remove_file(&file1).unwrap();
        let changed = process_paths(&ctx, &mut entries_map, vec![file1]).await;
        assert!(changed);
        assert_eq!(
            listing_protocol
                .listing()
                .entries
                .len(),
            1
        );
        assert_eq!(
            listing_protocol
                .listing()
                .entries[0]
                .path,
            "sub/file2.txt"
        );

        // 6. Delete directory
        std::fs::remove_dir_all(&sub).unwrap();
        let changed = process_paths(&ctx, &mut entries_map, vec![sub]).await;
        assert!(changed);
        assert_eq!(
            listing_protocol
                .listing()
                .entries
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn test_live_watcher_updates() {
        let temp_dir = tempfile::tempdir().unwrap();
        let folder = temp_dir
            .path()
            .canonicalize()
            .unwrap();
        let store_dir = folder.join(".ll-serve-store");
        tokio::fs::create_dir_all(&store_dir)
            .await
            .unwrap();

        let store = FsStore::load(&store_dir)
            .await
            .unwrap();
        let key = iroh::SecretKey::generate();
        let addr = iroh::EndpointAddr::from(key.public());

        let initial_listing = Listing::new(vec![]);
        let listing_protocol = ListingProtocol::new(initial_listing);
        let entries_map = HashMap::new();

        let handle = spawn_watcher(
            folder.clone(),
            store_dir.clone(),
            store,
            addr,
            entries_map,
            listing_protocol.clone(),
            Duration::from_millis(50),
        )
        .unwrap();

        let mut rx = handle.subscribe();

        // Create a file and wait for debounced update
        let test_file = folder.join("live.txt");
        std::fs::write(&test_file, b"live watcher test").unwrap();

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                rx.changed()
                    .await
                    .unwrap();
                if rx
                    .borrow()
                    .entries
                    .len()
                    == 1
                {
                    break;
                }
            }
        })
        .await
        .expect("timed out waiting for live watcher file addition");

        assert_eq!(
            listing_protocol
                .listing()
                .entries
                .len(),
            1
        );
        assert_eq!(
            listing_protocol
                .listing()
                .entries[0]
                .path,
            "live.txt"
        );

        // Modify the file and wait for update
        std::fs::write(&test_file, b"live watcher modified content").unwrap();

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                rx.changed()
                    .await
                    .unwrap();
                if rx
                    .borrow()
                    .entries
                    .first()
                    .map(|e| e.size)
                    == Some(29)
                {
                    break;
                }
            }
        })
        .await
        .expect("timed out waiting for live watcher file modification");

        assert_eq!(
            listing_protocol
                .listing()
                .entries[0]
                .size,
            29
        );

        // Delete the file and wait for update
        std::fs::remove_file(&test_file).unwrap();

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                rx.changed()
                    .await
                    .unwrap();
                if rx
                    .borrow()
                    .entries
                    .is_empty()
                {
                    break;
                }
            }
        })
        .await
        .expect("timed out waiting for live watcher file deletion");

        assert_eq!(
            listing_protocol
                .listing()
                .entries
                .len(),
            0
        );
    }
}
