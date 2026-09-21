//! Self-update detection, asset verification, and executable replacement.
//!
//! Provides utilities to inspect directory listings from `ll-serve` for newer releases
//! matching the local host platform, download updates, and atomically replace the
//! running executable in-place.

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::listing::{Entry, Listing};

/// The packaging kind of an update asset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssetKind {
    /// Multi-binary archive (.tar.gz or .zip) containing `ll`, `ll-serve`, and `ll-tui`.
    Archive,
    /// Standalone binary executable (e.g. `ll-tui`).
    StandaloneBinary,
}

/// A candidate update asset found in a directory listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateCandidate {
    /// Parsed semantic version of the update candidate.
    pub version: semver::Version,
    /// Listing entry pointing to the downloadable blob.
    pub entry: Entry,
    /// Whether the asset is a multi-binary archive or standalone binary.
    pub kind: AssetKind,
}

/// Identifies current platform target string (e.g. "linux-x86_64", "darwin-aarch64", "windows-x86_64").
pub fn current_platform_target() -> &'static str {
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    {
        "linux-x86_64"
    }
    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    {
        "linux-aarch64"
    }
    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    {
        "darwin-x86_64"
    }
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    {
        "darwin-aarch64"
    }
    #[cfg(all(target_os = "windows", target_arch = "x86_64"))]
    {
        "windows-x86_64"
    }
    #[cfg(not(any(
        all(target_os = "linux", target_arch = "x86_64"),
        all(target_os = "linux", target_arch = "aarch64"),
        all(target_os = "macos", target_arch = "x86_64"),
        all(target_os = "macos", target_arch = "aarch64"),
        all(target_os = "windows", target_arch = "x86_64")
    )))]
    {
        "unknown"
    }
}

/// Parses a version string leniently, stripping an optional 'v' or 'V' prefix.
pub fn parse_version(s: &str) -> Option<semver::Version> {
    let s = s.trim();
    let s = s
        .strip_prefix(|c| c == 'v' || c == 'V')
        .unwrap_or(s);
    semver::Version::parse(s).ok()
}

/// Parses version and asset kind from a filename if it matches the specified target architecture.
///
/// Supported patterns include:
/// - Release archives: `ll-v{version}-{target}.tar.gz`, `ll-{version}-{target}.tar.gz`, `.zip`
/// - Standalone binaries: `ll-tui-v{version}-{target}`, `ll-tui-{version}-{target}` (with optional `.exe`)
pub fn parse_update_filename(filename: &str, target: &str) -> Option<(semver::Version, AssetKind)> {
    if let Some(stem) = filename.strip_suffix(".tar.gz") {
        if let Some(version) = parse_archive_stem(stem, target) {
            return Some((version, AssetKind::Archive));
        }
    }
    if let Some(stem) = filename.strip_suffix(".zip") {
        if let Some(version) = parse_archive_stem(stem, target) {
            return Some((version, AssetKind::Archive));
        }
    }

    let stem = filename
        .strip_suffix(".exe")
        .unwrap_or(filename);
    if let Some(version) = parse_binary_stem(stem, target) {
        return Some((version, AssetKind::StandaloneBinary));
    }

    None
}

fn parse_archive_stem(stem: &str, target: &str) -> Option<semver::Version> {
    let rest = stem.strip_prefix("ll-")?;
    let target_suffix = format!("-{target}");
    let ver_str = rest.strip_suffix(&target_suffix)?;
    parse_version(ver_str)
}

fn parse_binary_stem(stem: &str, target: &str) -> Option<semver::Version> {
    let rest = stem.strip_prefix("ll-tui-")?;
    let target_suffix = format!("-{target}");
    let ver_str = rest.strip_suffix(&target_suffix)?;
    parse_version(ver_str)
}

/// Evaluates a single listing entry against target architecture and optional server version.
pub fn parse_update_candidate(
    entry: &Entry,
    target: &str,
    server_version: Option<&str>,
) -> Option<UpdateCandidate> {
    let filename = entry
        .path
        .rsplit('/')
        .next()
        .unwrap_or(&entry.path);
    if let Some((version, kind)) = parse_update_filename(filename, target) {
        return Some(UpdateCandidate {
            version,
            entry: entry.clone(),
            kind,
        });
    }

    // Standalone binary named exactly "ll-tui" or "ll-tui.exe"
    // when the server advertises its version.
    let is_exact_tui = filename == "ll-tui" || filename == "ll-tui.exe";
    if is_exact_tui {
        if let Some(sv) = server_version {
            if let Some(version) = parse_version(sv) {
                return Some(UpdateCandidate {
                    version,
                    entry: entry.clone(),
                    kind: AssetKind::StandaloneBinary,
                });
            }
        }
    }

    None
}

/// Inspects a `Listing` for the highest compatible version newer than `current_ver`.
pub fn find_available_update(listing: &Listing, current_ver: &str) -> Option<UpdateCandidate> {
    let target = current_platform_target();
    find_available_update_for_target(listing, current_ver, target)
}

/// Inspects a `Listing` for the highest compatible version newer than `current_ver` for a given target.
pub fn find_available_update_for_target(
    listing: &Listing,
    current_ver: &str,
    target: &str,
) -> Option<UpdateCandidate> {
    let current_version = parse_version(current_ver)?;
    let server_ver = listing.server_version();

    let mut best_candidate: Option<UpdateCandidate> = None;

    for entry in &listing.entries {
        if let Some(candidate) = parse_update_candidate(entry, target, server_ver) {
            if candidate.version > current_version {
                let replace = match &best_candidate {
                    None => true,
                    Some(current_best) => {
                        if candidate.version > current_best.version {
                            true
                        } else if candidate.version == current_best.version {
                            // Prefer archive over standalone binary if versions match
                            candidate.kind == AssetKind::Archive
                                && current_best.kind != AssetKind::Archive
                        } else {
                            false
                        }
                    }
                };
                if replace {
                    best_candidate = Some(candidate);
                }
            }
        }
    }

    best_candidate
}

/// Decompresses an archive (.tar.gz or .zip) into the destination directory.
pub fn extract_archive(archive_path: &Path, dest_dir: &Path) -> anyhow::Result<()> {
    let mut file = File::open(archive_path)?;
    let mut magic = [0u8; 4];
    let bytes_read = file.read(&mut magic)?;
    drop(file);

    let is_gzip = bytes_read >= 2 && magic[0] == 0x1f && magic[1] == 0x8b;
    let is_zip = bytes_read >= 4
        && magic[0] == 0x50
        && magic[1] == 0x4b
        && magic[2] == 0x03
        && magic[3] == 0x04;

    let path_str = archive_path.to_string_lossy();
    if is_gzip || path_str.ends_with(".tar.gz") || path_str.ends_with(".tgz") {
        let file = File::open(archive_path)?;
        let decompressor = flate2::read::GzDecoder::new(file);
        let mut archive = tar::Archive::new(decompressor);
        archive.unpack(dest_dir)?;
    } else if is_zip || path_str.ends_with(".zip") {
        let file = File::open(archive_path)?;
        let mut archive = zip::ZipArchive::new(file)?;
        archive.extract(dest_dir)?;
    } else {
        anyhow::bail!("unsupported archive format for {}", archive_path.display());
    }

    Ok(())
}

/// Safely replaces an executable binary at `dest` with the file at `source`.
///
/// If `dest` matches `current_exe`, `self_replace::self_replace` is used to handle
/// active process replacement across Linux, macOS, and Windows. Otherwise, an atomic
/// file move via temporary file in the destination folder is performed.
pub fn replace_binary(
    dest: &Path,
    source: &Path,
    current_exe: Option<&Path>,
) -> anyhow::Result<()> {
    let is_current = if let Some(current) = current_exe {
        if let (Ok(dest_canon), Ok(current_canon)) = (dest.canonicalize(), current.canonicalize()) {
            dest_canon == current_canon
        } else {
            dest == current
        }
    } else {
        false
    };

    if is_current {
        self_replace::self_replace(source)
            .map_err(|e| anyhow::anyhow!("failed to replace running executable: {e}"))?;
    } else {
        replace_file_atomic(dest, source)
            .map_err(|e| anyhow::anyhow!("failed to replace {}: {e}", dest.display()))?;
    }

    Ok(())
}

fn replace_file_atomic(dest: &Path, src: &Path) -> std::io::Result<()> {
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let parent = dest
        .parent()
        .ok_or_else(|| std::io::Error::other("destination has no parent folder"))?;

    let prefix = if let Some(stem) = dest
        .file_stem()
        .and_then(|s| s.to_str())
    {
        format!(".{}.__temp__", stem)
    } else {
        ".__update_temp__".to_string()
    };

    let tmp = tempfile::Builder::new()
        .prefix(&prefix)
        .tempfile_in(parent)?;
    std::fs::copy(src, tmp.path())?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(0o755);
        let _ = std::fs::set_permissions(tmp.path(), perms);
    }

    let (_, tmp_path) = tmp.keep()?;
    match std::fs::rename(&tmp_path, dest) {
        Ok(()) => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let perms = std::fs::Permissions::from_mode(0o755);
                let _ = std::fs::set_permissions(dest, perms);
            }
            Ok(())
        }
        Err(err) => {
            let _ = std::fs::remove_file(&tmp_path);
            Err(err)
        }
    }
}

/// Extracts a release archive and replaces suite binaries (`ll`, `ll-serve`, `ll-tui`) in `install_dir`.
pub fn extract_and_replace_suite_in_dir(
    archive_path: &Path,
    install_dir: &Path,
    current_exe: Option<&Path>,
) -> anyhow::Result<Vec<PathBuf>> {
    let extract_dir = tempfile::tempdir()?;
    extract_archive(archive_path, extract_dir.path())?;

    let valid_names = [
        "ll",
        "ll-serve",
        "ll-tui",
        "ll.exe",
        "ll-serve.exe",
        "ll-tui.exe",
    ];

    let mut replaced = Vec::new();
    for entry in walkdir::WalkDir::new(extract_dir.path())
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if entry
            .file_type()
            .is_file()
        {
            let file_name = entry
                .file_name()
                .to_string_lossy();
            if valid_names.contains(&file_name.as_ref()) {
                let dest = install_dir.join(file_name.as_ref());
                replace_binary(&dest, entry.path(), current_exe)?;
                replaced.push(dest);
            }
        }
    }

    if replaced.is_empty() {
        anyhow::bail!("no suite binaries (ll, ll-serve, ll-tui) found in archive");
    }

    Ok(replaced)
}

/// Extracts a release archive and replaces suite binaries in the directory of the running executable.
pub fn extract_and_replace_suite(staged_path: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let current_exe = std::env::current_exe()?;
    let install_dir = current_exe
        .parent()
        .ok_or_else(|| anyhow::anyhow!("could not determine executable parent directory"))?;
    extract_and_replace_suite_in_dir(staged_path, install_dir, Some(&current_exe))
}

/// Safely removes a staged temporary file or artifact.
pub fn cleanup_staged_file(path: &Path) {
    if path.exists() {
        let _ = std::fs::remove_file(path);
    }
}

/// Performs in-place replacement of the running executable and sibling binaries.
pub fn apply_update(
    staged_path: &Path,
    candidate: &UpdateCandidate,
) -> anyhow::Result<Vec<PathBuf>> {
    let current_exe = std::env::current_exe()?;
    let install_dir = current_exe
        .parent()
        .ok_or_else(|| anyhow::anyhow!("could not determine executable parent directory"))?;

    apply_update_to_dir(staged_path, candidate, install_dir, Some(&current_exe))
}

/// Performs in-place replacement into a specified target directory.
pub fn apply_update_to_dir(
    staged_path: &Path,
    candidate: &UpdateCandidate,
    install_dir: &Path,
    current_exe: Option<&Path>,
) -> anyhow::Result<Vec<PathBuf>> {
    match candidate.kind {
        AssetKind::Archive => {
            extract_and_replace_suite_in_dir(staged_path, install_dir, current_exe)
        }
        AssetKind::StandaloneBinary => {
            let dest = if let Some(exe) = current_exe {
                exe.to_path_buf()
            } else {
                let bin_name = if cfg!(windows) {
                    "ll-tui.exe"
                } else {
                    "ll-tui"
                };
                install_dir.join(bin_name)
            };
            replace_binary(&dest, staged_path, current_exe)?;
            Ok(vec![dest])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh_blobs::{Hash, ticket::BlobTicket};

    fn dummy_entry(path: &str) -> Entry {
        let ticket = BlobTicket::new(
            iroh::EndpointAddr::from(iroh::SecretKey::generate().public()),
            Hash::from_bytes([1u8; 32]),
            iroh_blobs::BlobFormat::Raw,
        );
        Entry {
            path: path.to_string(),
            size: 1024,
            hash: Hash::from_bytes([1u8; 32]),
            ticket,
        }
    }

    #[test]
    fn test_current_platform_target() {
        let target = current_platform_target();
        assert!(!target.is_empty());
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        assert_eq!(target, "linux-x86_64");
    }

    #[test]
    fn test_parse_update_filename_archives() {
        let (ver, kind) =
            parse_update_filename("ll-v0.30.0-linux-x86_64.tar.gz", "linux-x86_64").unwrap();
        assert_eq!(ver, semver::Version::parse("0.30.0").unwrap());
        assert_eq!(kind, AssetKind::Archive);

        let (ver, kind) =
            parse_update_filename("ll-0.30.0-windows-x86_64.zip", "windows-x86_64").unwrap();
        assert_eq!(ver, semver::Version::parse("0.30.0").unwrap());
        assert_eq!(kind, AssetKind::Archive);

        // Mismatched target returns None
        assert!(
            parse_update_filename("ll-v0.30.0-darwin-aarch64.tar.gz", "linux-x86_64").is_none()
        );
    }

    #[test]
    fn test_parse_update_filename_standalone_binaries() {
        let (ver, kind) =
            parse_update_filename("ll-tui-v0.30.0-linux-x86_64", "linux-x86_64").unwrap();
        assert_eq!(ver, semver::Version::parse("0.30.0").unwrap());
        assert_eq!(kind, AssetKind::StandaloneBinary);

        let (ver, kind) =
            parse_update_filename("ll-tui-v0.30.0-windows-x86_64.exe", "windows-x86_64").unwrap();
        assert_eq!(ver, semver::Version::parse("0.30.0").unwrap());
        assert_eq!(kind, AssetKind::StandaloneBinary);
    }

    #[test]
    fn test_parse_update_candidate_standalone_with_server_version() {
        let entry = dummy_entry("bin/ll-tui");
        let candidate = parse_update_candidate(&entry, "linux-x86_64", Some("0.31.0")).unwrap();
        assert_eq!(candidate.version, semver::Version::parse("0.31.0").unwrap());
        assert_eq!(candidate.kind, AssetKind::StandaloneBinary);
    }

    #[test]
    fn test_find_available_update_selection() {
        let entries = vec![
            dummy_entry("ll-v0.28.0-linux-x86_64.tar.gz"),
            dummy_entry("ll-v0.29.0-linux-x86_64.tar.gz"),
            dummy_entry("ll-v0.30.0-linux-x86_64.tar.gz"),
            dummy_entry("ll-v0.31.0-linux-aarch64.tar.gz"), // different target
            dummy_entry("ll-tui-v0.30.0-linux-x86_64"),     // archive preferred over binary
        ];
        let listing = Listing::new(entries);

        // Current version is 0.29.1-dev -> should select 0.30.0 archive
        let update =
            find_available_update_for_target(&listing, "0.29.1-dev", "linux-x86_64").unwrap();
        assert_eq!(update.version, semver::Version::parse("0.30.0").unwrap());
        assert_eq!(update.kind, AssetKind::Archive);
        assert_eq!(
            update
                .entry
                .path,
            "ll-v0.30.0-linux-x86_64.tar.gz"
        );

        // Current version is 0.30.0 -> no update available
        let update = find_available_update_for_target(&listing, "0.30.0", "linux-x86_64");
        assert!(update.is_none());

        // Current version is 0.32.0 -> no update available
        let update = find_available_update_for_target(&listing, "0.32.0", "linux-x86_64");
        assert!(update.is_none());
    }

    fn create_test_tar_gz(path: &Path, files: &[(&str, &[u8])]) {
        let file = File::create(path).unwrap();
        let enc = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        let mut tar = tar::Builder::new(enc);
        for (name, content) in files {
            let mut header = tar::Header::new_gnu();
            header.set_size(content.len() as u64);
            header.set_mode(0o755);
            header.set_cksum();
            tar.append_data(&mut header, *name, *content)
                .unwrap();
        }
        tar.into_inner()
            .unwrap()
            .finish()
            .unwrap();
    }

    fn create_test_zip(path: &Path, files: &[(&str, &[u8])]) {
        use std::io::Write;
        let file = File::create(path).unwrap();
        let mut zip = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        for (name, content) in files {
            zip.start_file(*name, options)
                .unwrap();
            zip.write_all(content)
                .unwrap();
        }
        zip.finish()
            .unwrap();
    }

    #[test]
    fn test_extract_tar_gz() {
        let dir = tempfile::tempdir().unwrap();
        let archive_path = dir
            .path()
            .join("archive.tar.gz");
        create_test_tar_gz(&archive_path, &[("hello.txt", b"hello world")]);

        let extract_dir = dir
            .path()
            .join("extracted");
        std::fs::create_dir(&extract_dir).unwrap();
        extract_archive(&archive_path, &extract_dir).unwrap();

        let extracted_file = extract_dir.join("hello.txt");
        assert_eq!(
            std::fs::read_to_string(extracted_file).unwrap(),
            "hello world"
        );
    }

    #[test]
    fn test_extract_zip() {
        let dir = tempfile::tempdir().unwrap();
        let archive_path = dir
            .path()
            .join("archive.zip");
        create_test_zip(&archive_path, &[("hello.txt", b"hello zip")]);

        let extract_dir = dir
            .path()
            .join("extracted");
        std::fs::create_dir(&extract_dir).unwrap();
        extract_archive(&archive_path, &extract_dir).unwrap();

        let extracted_file = extract_dir.join("hello.txt");
        assert_eq!(
            std::fs::read_to_string(extracted_file).unwrap(),
            "hello zip"
        );
    }

    #[test]
    fn test_extract_and_replace_suite_in_dir() {
        let dir = tempfile::tempdir().unwrap();
        let install_dir = dir
            .path()
            .join("bin");
        std::fs::create_dir(&install_dir).unwrap();

        // Create old mock binaries
        std::fs::write(install_dir.join("ll"), b"old ll").unwrap();
        std::fs::write(install_dir.join("ll-serve"), b"old ll-serve").unwrap();
        std::fs::write(install_dir.join("ll-tui"), b"old ll-tui").unwrap();

        // Create release archive with new binaries
        let archive_path = dir
            .path()
            .join("ll-v0.30.0-linux-x86_64.tar.gz");
        create_test_tar_gz(
            &archive_path,
            &[
                ("ll", b"new ll"),
                ("ll-serve", b"new ll-serve"),
                ("ll-tui", b"new ll-tui"),
            ],
        );

        let replaced = extract_and_replace_suite_in_dir(&archive_path, &install_dir, None).unwrap();
        assert_eq!(replaced.len(), 3);

        assert_eq!(std::fs::read(install_dir.join("ll")).unwrap(), b"new ll");
        assert_eq!(
            std::fs::read(install_dir.join("ll-serve")).unwrap(),
            b"new ll-serve"
        );
        assert_eq!(
            std::fs::read(install_dir.join("ll-tui")).unwrap(),
            b"new ll-tui"
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::metadata(install_dir.join("ll"))
                .unwrap()
                .permissions();
            assert_eq!(perms.mode() & 0o111, 0o111);
        }
    }

    #[test]
    fn test_apply_update_to_dir_standalone() {
        let dir = tempfile::tempdir().unwrap();
        let install_dir = dir
            .path()
            .join("bin");
        std::fs::create_dir(&install_dir).unwrap();

        let bin_name = if cfg!(windows) {
            "ll-tui.exe"
        } else {
            "ll-tui"
        };
        let dest = install_dir.join(bin_name);
        std::fs::write(&dest, b"old tui").unwrap();

        let staged = dir
            .path()
            .join("staged_binary");
        std::fs::write(&staged, b"new standalone tui").unwrap();

        let candidate = UpdateCandidate {
            version: semver::Version::parse("0.30.0").unwrap(),
            entry: dummy_entry("ll-tui-v0.30.0-linux-x86_64"),
            kind: AssetKind::StandaloneBinary,
        };

        let replaced = apply_update_to_dir(&staged, &candidate, &install_dir, None).unwrap();
        assert_eq!(replaced, vec![dest.clone()]);
        assert_eq!(std::fs::read(&dest).unwrap(), b"new standalone tui");

        cleanup_staged_file(&staged);
        assert!(!staged.exists());
    }
}
