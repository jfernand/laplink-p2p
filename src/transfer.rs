//! Progress-agnostic import/export primitives, shared by the CLI, server, and TUI.
//!
//! None of this module renders progress; callers that want progress bars drive
//! `iroh_blobs`'s own progress-item streams themselves (see `ll.rs`'s `send`/`receive`
//! for the indicatif-backed example).

use std::path::{Path, PathBuf};

use anyhow::Context;
use futures_buffered::BufferedStreamExt;
use iroh_blobs::{
    api::{
        blobs::{AddPathOptions, AddProgressItem, ExportMode, ExportOptions, ExportProgressItem},
        Store, TempTag,
    },
    format::collection::Collection,
    BlobFormat, Hash,
};
use n0_future::StreamExt;
use walkdir::WalkDir;

use crate::paths::{canonicalized_path_to_string, get_export_path};

/// Walk `path` (a file or directory) and return `(relative_name, absolute_path)` pairs for
/// every regular file found. Symlinks are skipped; if `path` is itself a file, a single pair
/// is returned.
pub fn walk_data_sources(path: &Path) -> anyhow::Result<Vec<(String, PathBuf)>> {
    let path = path.canonicalize()?;
    anyhow::ensure!(path.exists(), "path {} does not exist", path.display());
    let root = path.parent().context("context get parent")?;
    let files = WalkDir::new(path.clone()).into_iter();
    files
        .map(|entry| {
            let entry = entry?;
            if !entry.file_type().is_file() {
                // Skip symlinks. Directories are handled by WalkDir.
                return Ok(None);
            }
            let path = entry.into_path();
            let relative = path.strip_prefix(root)?;
            let name = canonicalized_path_to_string(relative, true)?;
            anyhow::Ok(Some((name, path)))
        })
        .filter_map(Result::transpose)
        .collect::<anyhow::Result<Vec<_>>>()
}

/// Import a single file into `db`, draining its progress stream and returning the resulting
/// temp tag plus the file's size.
async fn import_one(db: &Store, path: PathBuf) -> anyhow::Result<(TempTag, u64)> {
    let import = db.add_path_with_opts(AddPathOptions {
        path,
        mode: iroh_blobs::api::blobs::ImportMode::TryReference,
        format: BlobFormat::Raw,
    });
    let mut stream = import.stream().await;
    let mut item_size = 0;
    let temp_tag = loop {
        let item = stream
            .next()
            .await
            .context("import stream ended without a tag")?;
        match item {
            AddProgressItem::Size(size) => item_size = size,
            AddProgressItem::CopyProgress(_) | AddProgressItem::OutboardProgress(_) => {}
            AddProgressItem::CopyDone => {}
            AddProgressItem::Error(cause) => anyhow::bail!("error importing: {cause}"),
            AddProgressItem::Done(tt) => break tt,
        }
    };
    Ok((temp_tag, item_size))
}

/// Import from a file or directory into the database.
///
/// The returned tag always refers to a collection. If the input is a file, this
/// is a collection with a single blob, named like the file.
///
/// If the input is a directory, the collection contains all the files in the
/// directory.
pub async fn import_collection(
    path: PathBuf,
    db: &Store,
) -> anyhow::Result<(TempTag, u64, Collection)> {
    let parallelism = num_cpus::get();
    let data_sources = walk_data_sources(&path)?;
    let mut names_and_tags = n0_future::stream::iter(data_sources)
        .map(|(name, path)| {
            let db = db.clone();
            async move {
                let (temp_tag, size) = import_one(&db, path).await?;
                anyhow::Ok((name, temp_tag, size))
            }
        })
        .buffered_unordered(parallelism)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;
    names_and_tags.sort_by(|(a, _, _), (b, _, _)| a.cmp(b));
    let size = names_and_tags.iter().map(|(_, _, size)| *size).sum::<u64>();
    // collect the (name, hash) tuples into a collection
    // we must also keep the tags around so the data does not get gced.
    let (collection, tags) = names_and_tags
        .into_iter()
        .map(|(name, tag, _)| ((name, tag.hash()), tag))
        .unzip::<_, _, Collection, Vec<_>>();
    let temp_tag = collection.clone().store(db).await?;
    // now that the collection is stored, we can drop the tags
    // data is protected by the collection
    drop(tags);
    Ok((temp_tag, size, collection))
}

/// Walk `root` (a directory) and return `(relative_name, absolute_path)` pairs for every
/// regular file found, with names relative to `root` itself (unlike [`walk_data_sources`],
/// which names entries relative to the *parent* of the given path so a whole directory can be
/// reconstructed under its own name on the receiving side — for a served folder there's only
/// one root, so entries shouldn't be prefixed with its name).
fn walk_dir_contents(root: &Path) -> anyhow::Result<Vec<(String, PathBuf)>> {
    let root = root.canonicalize()?;
    anyhow::ensure!(root.exists(), "path {} does not exist", root.display());
    WalkDir::new(&root)
        .into_iter()
        .map(|entry| {
            let entry = entry?;
            if !entry.file_type().is_file() {
                return Ok(None);
            }
            let path = entry.into_path();
            let relative = path.strip_prefix(&root)?;
            let name = canonicalized_path_to_string(relative, true)?;
            anyhow::Ok(Some((name, path)))
        })
        .filter_map(Result::transpose)
        .collect::<anyhow::Result<Vec<_>>>()
}

/// Import every file under `root` individually (not as one collection/hashseq), skipping
/// anything under `exclude` (e.g. the server's own blob-store directory).
///
/// Returns one entry per file: `(relative "/"-joined name, size, hash, temp_tag)`. Callers
/// must keep the returned temp tags alive for as long as the blobs should remain reachable.
pub async fn import_flat(
    root: PathBuf,
    db: &Store,
    exclude: &Path,
) -> anyhow::Result<Vec<(String, u64, Hash, TempTag)>> {
    let parallelism = num_cpus::get();
    let data_sources = walk_dir_contents(&root)?
        .into_iter()
        .filter(|(_, path)| !path.starts_with(exclude))
        .collect::<Vec<_>>();
    let mut entries = n0_future::stream::iter(data_sources)
        .map(|(name, path)| {
            let db = db.clone();
            async move {
                let (temp_tag, size) = import_one(&db, path).await?;
                let hash = temp_tag.hash();
                anyhow::Ok((name, size, hash, temp_tag))
            }
        })
        .buffered_unordered(parallelism)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<anyhow::Result<Vec<_>>>()?;
    entries.sort_by(|(a, ..), (b, ..)| a.cmp(b));
    Ok(entries)
}

/// Export every `(name, hash)` in `collection` to `root`.
pub async fn export_collection(
    db: &Store,
    collection: Collection,
    root: &Path,
) -> anyhow::Result<()> {
    for (name, hash) in collection.iter() {
        export_one(db, *hash, &get_export_path(root, name)?).await?;
    }
    Ok(())
}

/// Export a single blob to `target`, failing if `target` already exists.
pub async fn export_one(db: &Store, hash: Hash, target: &Path) -> anyhow::Result<()> {
    if target.exists() {
        anyhow::bail!("target {} already exists", target.display());
    }
    if let Some(parent) = target.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut stream = db
        .export_with_opts(ExportOptions {
            hash,
            target: target.to_path_buf(),
            mode: ExportMode::TryReference,
        })
        .stream()
        .await;
    while let Some(item) = stream.next().await {
        match item {
            ExportProgressItem::Size(_) | ExportProgressItem::CopyProgress(_) => {}
            ExportProgressItem::Done => {}
            ExportProgressItem::Error(cause) => {
                anyhow::bail!("error exporting {}: {}", target.display(), cause);
            }
        }
    }
    Ok(())
}
