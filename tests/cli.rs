use std::{
    io::{self, Read},
    path::{Path, PathBuf},
    str::FromStr,
};

use iroh_blobs::ticket::BlobTicket;

// binary path
fn ll_bin() -> &'static str {
    env!("CARGO_BIN_EXE_ll")
}

fn ll_tui_bin() -> &'static str {
    env!("CARGO_BIN_EXE_ll-tui")
}

fn ll_serve_bin() -> &'static str {
    env!("CARGO_BIN_EXE_ll-serve")
}

/// Read `n` lines from `reader`, returning the bytes read including the newlines.
///
/// This assumes that the header lines are ASCII and can be parsed byte by byte.
fn read_ascii_lines(mut n: usize, reader: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut buf = [0u8; 1];
    let mut res = Vec::new();
    loop {
        if reader.read(&mut buf)? != 1 {
            break;
        }
        let char = buf[0];
        res.push(char);
        if char != b'\n' {
            continue;
        }
        if n > 1 {
            n -= 1;
        } else {
            break;
        }
    }
    Ok(res)
}

// fn wait2() -> Arc<Barrier> {
//     Arc::new(Barrier::new(2))
// }

// /// generate a random, non privileged port
// fn random_port() -> u16 {
//     rand::thread_rng().gen_range(10000u16..60000)
// }

#[test]
fn send_recv_file() {
    let name = "somefile.bin";
    let data = vec![0u8; 100];
    // create src and tgt dir, and src file
    let src_dir = tempfile::tempdir().unwrap();
    let tgt_dir = tempfile::tempdir().unwrap();
    let config_dir = tempfile::tempdir().unwrap();
    let src_file = src_dir
        .path()
        .join(name);
    std::fs::write(&src_file, &data).unwrap();
    let mut send_cmd = duct::cmd(
        ll_bin(),
        [
            "send",
            src_file
                .as_os_str()
                .to_str()
                .unwrap(),
        ],
    )
    .dir(src_dir.path())
    .env("LAPLINK_CONFIG_DIR", config_dir.path())
    .env_remove("RUST_LOG") // disable tracing
    .stderr_to_stdout()
    .reader()
    .unwrap();
    let output = read_ascii_lines(3, &mut send_cmd).unwrap();
    let output = String::from_utf8(output).unwrap();
    let ticket = output
        .split_ascii_whitespace()
        .last()
        .unwrap();
    let ticket = BlobTicket::from_str(ticket).unwrap();
    let receive_output = duct::cmd(ll_bin(), ["receive", &ticket.to_string()])
        .dir(tgt_dir.path())
        .env("LAPLINK_CONFIG_DIR", config_dir.path())
        .env_remove("RUST_LOG") // disable tracing
        .stderr_to_stdout()
        .run()
        .unwrap();
    assert!(receive_output
        .status
        .success());
    let tgt_file = tgt_dir
        .path()
        .join(name);
    let tgt_data = std::fs::read(tgt_file).unwrap();
    assert_eq!(tgt_data, data);
}

#[test]
fn send_recv_dir() {
    fn create_file(base: &Path, i: usize, j: usize, k: usize) -> (PathBuf, Vec<u8>) {
        let name = base
            .join(format!("dir-{i}"))
            .join(format!("subdir-{j}"))
            .join(format!("file-{k}"));
        let len = i * 100 + j * 10 + k;
        let data = vec![0u8; len];
        (name, data)
    }

    // create src and tgt dir, and src file
    let src_dir = tempfile::tempdir().unwrap();
    let tgt_dir = tempfile::tempdir().unwrap();
    let config_dir = tempfile::tempdir().unwrap();
    let src_data_dir = src_dir
        .path()
        .join("data");
    let tgt_data_dir = tgt_dir
        .path()
        .join("data");
    // create a complex directory structure
    for i in 0..5 {
        for j in 0..5 {
            for k in 0..5 {
                let (name, data) = create_file(&src_data_dir, i, j, k);
                std::fs::create_dir_all(
                    name.parent()
                        .unwrap(),
                )
                .unwrap();
                std::fs::write(&name, &data).unwrap();
            }
        }
    }
    let mut send_cmd = duct::cmd(
        ll_bin(),
        [
            "send",
            src_data_dir
                .as_os_str()
                .to_str()
                .unwrap(),
        ],
    )
    .dir(src_dir.path())
    .env("LAPLINK_CONFIG_DIR", config_dir.path())
    .env_remove("RUST_LOG") // disable tracing
    .stderr_to_stdout()
    .reader()
    .unwrap();
    let output = read_ascii_lines(3, &mut send_cmd).unwrap();
    let output = String::from_utf8(output).unwrap();
    let ticket = output
        .split_ascii_whitespace()
        .last()
        .unwrap();
    let ticket = BlobTicket::from_str(ticket).unwrap();
    let receive_output = duct::cmd(ll_bin(), ["receive", &ticket.to_string()])
        .dir(tgt_dir.path())
        .env("LAPLINK_CONFIG_DIR", config_dir.path())
        .env_remove("RUST_LOG") // disable tracing
        .stderr_to_stdout()
        .run()
        .unwrap();
    assert!(receive_output
        .status
        .success());
    // validate directory structure
    for i in 0..5 {
        for j in 0..5 {
            for k in 0..5 {
                let (name, data) = create_file(&tgt_data_dir, i, j, k);
                let tgt_data = std::fs::read(&name).unwrap();
                assert_eq!(tgt_data, data);
            }
        }
    }
}

#[test]
fn ll_remembers_ticket() {
    let name = "test_remember.bin";
    let data = vec![42u8; 50];
    let src_dir = tempfile::tempdir().unwrap();
    let tgt_dir1 = tempfile::tempdir().unwrap();
    let tgt_dir2 = tempfile::tempdir().unwrap();
    let config_dir = tempfile::tempdir().unwrap();

    let src_file = src_dir
        .path()
        .join(name);
    std::fs::write(&src_file, &data).unwrap();

    let mut send_cmd = duct::cmd(
        ll_bin(),
        [
            "send",
            src_file
                .as_os_str()
                .to_str()
                .unwrap(),
        ],
    )
    .dir(src_dir.path())
    .env("LAPLINK_CONFIG_DIR", config_dir.path())
    .env_remove("RUST_LOG")
    .stderr_to_stdout()
    .reader()
    .unwrap();

    let output = read_ascii_lines(3, &mut send_cmd).unwrap();
    let output = String::from_utf8(output).unwrap();
    let ticket = output
        .split_ascii_whitespace()
        .last()
        .unwrap();
    let ticket = BlobTicket::from_str(ticket).unwrap();

    // 1. Receive by explicitly specifying the ticket with `receive <ticket>`.
    let receive_output = duct::cmd(ll_bin(), ["receive", &ticket.to_string()])
        .dir(tgt_dir1.path())
        .env("LAPLINK_CONFIG_DIR", config_dir.path())
        .env_remove("RUST_LOG")
        .stderr_to_stdout()
        .run()
        .unwrap();
    assert!(receive_output
        .status
        .success());
    let tgt_data1 = std::fs::read(
        tgt_dir1
            .path()
            .join(name),
    )
    .unwrap();
    assert_eq!(tgt_data1, data);

    // 2. Receive in another directory WITHOUT specifying the ticket (`ll receive`).
    let receive_output2 = duct::cmd(ll_bin(), ["receive"])
        .dir(tgt_dir2.path())
        .env("LAPLINK_CONFIG_DIR", config_dir.path())
        .env_remove("RUST_LOG")
        .stderr_to_stdout()
        .run()
        .unwrap();
    assert!(receive_output2
        .status
        .success());
    let tgt_data2 = std::fs::read(
        tgt_dir2
            .path()
            .join(name),
    )
    .unwrap();
    assert_eq!(tgt_data2, data);

    // 3. In a fresh config dir with no remembered ticket, `ll receive` should fail.
    let empty_config = tempfile::tempdir().unwrap();
    let tgt_dir3 = tempfile::tempdir().unwrap();
    let receive_output3 = duct::cmd(ll_bin(), ["receive"])
        .dir(tgt_dir3.path())
        .env("LAPLINK_CONFIG_DIR", empty_config.path())
        .env_remove("RUST_LOG")
        .stderr_to_stdout()
        .unchecked()
        .run()
        .unwrap();
    assert!(!receive_output3
        .status
        .success());
}

#[test]
fn ll_tui_remembers_ticket() {
    let empty_config = tempfile::tempdir().unwrap();
    // With no ticket and no remembered ticket, ll-tui exits with error.
    let output = duct::cmd(ll_tui_bin(), Vec::<&str>::new())
        .env("LAPLINK_CONFIG_DIR", empty_config.path())
        .env_remove("RUST_LOG")
        .stderr_to_stdout()
        .unchecked()
        .run()
        .unwrap();
    assert!(!output
        .status
        .success());
}

#[test]
fn ll_serve_remembers_ticket() {
    let folder = tempfile::tempdir().unwrap();
    let config_dir = tempfile::tempdir().unwrap();
    let store_dir = folder
        .path()
        .join(".ll-serve-store");

    // 1. First run of ll-serve on the folder: ticket is generated and persisted.
    let mut child1 = std::process::Command::new(ll_serve_bin())
        .arg(folder.path())
        .env("LAPLINK_CONFIG_DIR", config_dir.path())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdout1 = child1
        .stdout
        .take()
        .unwrap();
    let output1 = read_ascii_lines(3, &mut stdout1).unwrap();
    let output1 = String::from_utf8(output1).unwrap();
    let ticket1 = output1
        .split_ascii_whitespace()
        .last()
        .unwrap()
        .to_string();

    child1
        .kill()
        .unwrap();
    let _ = child1.wait();

    // Verify it was saved in store_dir/ticket
    let loaded1 = laplink_p2p::ticket_storage::load_serve_ticket(&store_dir)
        .unwrap()
        .unwrap();
    assert_eq!(loaded1.to_string(), ticket1);

    // 2. Restart ll-serve on the same folder: ticket should be identical and stable.
    let mut child2 = std::process::Command::new(ll_serve_bin())
        .arg(folder.path())
        .env("LAPLINK_CONFIG_DIR", config_dir.path())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdout2 = child2
        .stdout
        .take()
        .unwrap();
    let output2 = read_ascii_lines(3, &mut stdout2).unwrap();
    let output2 = String::from_utf8(output2).unwrap();
    let ticket2 = output2
        .split_ascii_whitespace()
        .last()
        .unwrap()
        .to_string();

    child2
        .kill()
        .unwrap();
    let _ = child2.wait();

    assert_eq!(ticket1, ticket2);

    // 3. Start with explicit --ticket argument: overrides/remembers that ticket.
    let explicit_key = iroh::SecretKey::generate();
    let explicit_addr = iroh::EndpointAddr::from(explicit_key.public());
    let explicit_ticket = iroh_tickets::endpoint::EndpointTicket::new(explicit_addr);

    let mut child3 = std::process::Command::new(ll_serve_bin())
        .arg(folder.path())
        .arg("--ticket")
        .arg(explicit_ticket.to_string())
        .env("LAPLINK_CONFIG_DIR", config_dir.path())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdout3 = child3
        .stdout
        .take()
        .unwrap();
    let output3 = read_ascii_lines(3, &mut stdout3).unwrap();
    let output3 = String::from_utf8(output3).unwrap();
    let ticket3 = output3
        .split_ascii_whitespace()
        .last()
        .unwrap()
        .to_string();

    child3
        .kill()
        .unwrap();
    let _ = child3.wait();

    assert_eq!(ticket3, explicit_ticket.to_string());
    let loaded3 = laplink_p2p::ticket_storage::load_serve_ticket(&store_dir)
        .unwrap()
        .unwrap();
    assert_eq!(loaded3.to_string(), explicit_ticket.to_string());
}

#[test]
fn ll_serve_per_folder_persistence() {
    let folder = tempfile::tempdir().unwrap();
    let store_dir = folder
        .path()
        .join(".ll-serve-store");

    // Pre-create some file to serve
    std::fs::write(
        folder
            .path()
            .join("file.txt"),
        b"hello",
    )
    .unwrap();

    // Verify get_or_create_serve_secret creates persistent key in store_dir
    let (secret1, gen1) =
        laplink_p2p::ticket_storage::get_or_create_serve_secret(&store_dir).unwrap();
    assert!(gen1);
    assert!(store_dir
        .join("secret_key")
        .exists());

    let (secret2, gen2) =
        laplink_p2p::ticket_storage::get_or_create_serve_secret(&store_dir).unwrap();
    assert!(!gen2);
    assert_eq!(secret1.to_bytes(), secret2.to_bytes());

    // Save ticket and verify reload
    let addr = iroh::EndpointAddr::from(secret1.public());
    let ticket = iroh_tickets::endpoint::EndpointTicket::new(addr);
    laplink_p2p::ticket_storage::save_serve_ticket(&store_dir, &ticket).unwrap();

    let loaded = laplink_p2p::ticket_storage::load_serve_ticket(&store_dir)
        .unwrap()
        .unwrap();
    assert_eq!(loaded.to_string(), ticket.to_string());
}

#[test]
fn ll_serve_filesystem_monitoring() {
    let folder = tempfile::tempdir().unwrap();
    let config_dir = tempfile::tempdir().unwrap();
    let initial_file = folder
        .path()
        .join("initial.txt");
    std::fs::write(&initial_file, b"initial file content").unwrap();

    let mut child = std::process::Command::new(ll_serve_bin())
        .arg(folder.path())
        .env("LAPLINK_CONFIG_DIR", config_dir.path())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdout = child
        .stdout
        .take()
        .unwrap();
    let output = read_ascii_lines(3, &mut stdout).unwrap();
    let output = String::from_utf8(output).unwrap();
    let ticket_str = output
        .split_ascii_whitespace()
        .last()
        .unwrap();
    let ticket = iroh_tickets::endpoint::EndpointTicket::from_str(ticket_str).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (secret_key, _) = laplink_p2p::get_or_create_secret().unwrap();
        let lookup_by_dns = ticket
            .endpoint_addr()
            .addrs
            .is_empty();
        let endpoint =
            laplink_p2p::endpoint::build_endpoint(laplink_p2p::endpoint::EndpointConfig {
                secret_key: secret_key.clone(),
                alpns: vec![],
                relay: laplink_p2p::RelayModeOption::Default,
                magic_ipv4_addr: None,
                magic_ipv6_addr: None,
                publish_addr: false,
                lookup_by_dns,
            })
            .await
            .unwrap();

        // 1. Initial listing check
        let listing = laplink_p2p::listing::fetch_listing(&endpoint, &ticket)
            .await
            .unwrap();
        assert_eq!(
            listing
                .entries
                .len(),
            1
        );
        assert_eq!(listing.entries[0].path, "initial.txt");

        // 2. Add a new file dynamically
        let added_file = folder
            .path()
            .join("added.txt");
        std::fs::write(&added_file, b"dynamically added content").unwrap();

        // Wait for watcher to detect and update
        let mut listing = listing;
        for _ in 0..25 {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            if let Ok(l) = laplink_p2p::listing::fetch_listing(&endpoint, &ticket).await {
                if l.entries
                    .len()
                    == 2
                {
                    listing = l;
                    break;
                }
            }
        }
        assert_eq!(
            listing
                .entries
                .len(),
            2
        );
        let added_entry = listing
            .entries
            .iter()
            .find(|e| e.path == "added.txt")
            .expect("added.txt should exist");
        assert_eq!(added_entry.size, 25);

        // 3. Download the newly added file
        let download_dir = tempfile::tempdir().unwrap();
        let store_dir = download_dir
            .path()
            .join(".store");
        let export_path = download_dir
            .path()
            .join("added.txt");
        let cfg = laplink_p2p::endpoint::EndpointConfig {
            secret_key,
            alpns: vec![],
            relay: laplink_p2p::RelayModeOption::Default,
            magic_ipv4_addr: None,
            magic_ipv6_addr: None,
            publish_addr: false,
            lookup_by_dns: false,
        };
        laplink_p2p::receive::receive_single(
            added_entry
                .ticket
                .clone(),
            cfg,
            store_dir,
            export_path.clone(),
            None,
        )
        .await
        .unwrap();
        let downloaded = std::fs::read(&export_path).unwrap();
        assert_eq!(downloaded, b"dynamically added content");

        // 4. Delete the added file
        std::fs::remove_file(&added_file).unwrap();
        for _ in 0..25 {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            if let Ok(l) = laplink_p2p::listing::fetch_listing(&endpoint, &ticket).await {
                if l.entries
                    .len()
                    == 1
                {
                    listing = l;
                    break;
                }
            }
        }
        assert_eq!(
            listing
                .entries
                .len(),
            1
        );
        assert_eq!(listing.entries[0].path, "initial.txt");
    });

    child
        .kill()
        .unwrap();
    let _ = child.wait();
}

#[test]
fn ll_serve_subscription_stream() {
    let folder = tempfile::tempdir().unwrap();
    let config_dir = tempfile::tempdir().unwrap();
    let file1 = folder
        .path()
        .join("file1.txt");
    std::fs::write(&file1, b"first file content").unwrap();

    let mut child = std::process::Command::new(ll_serve_bin())
        .arg(folder.path())
        .env("LAPLINK_CONFIG_DIR", config_dir.path())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdout = child
        .stdout
        .take()
        .unwrap();
    let output = read_ascii_lines(3, &mut stdout).unwrap();
    let output = String::from_utf8(output).unwrap();
    let ticket_str = output
        .split_ascii_whitespace()
        .last()
        .unwrap();
    let ticket = iroh_tickets::endpoint::EndpointTicket::from_str(ticket_str).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (secret_key, _) = laplink_p2p::get_or_create_secret().unwrap();
        let lookup_by_dns = ticket
            .endpoint_addr()
            .addrs
            .is_empty();
        let endpoint =
            laplink_p2p::endpoint::build_endpoint(laplink_p2p::endpoint::EndpointConfig {
                secret_key,
                alpns: vec![],
                relay: laplink_p2p::RelayModeOption::Default,
                magic_ipv4_addr: None,
                magic_ipv6_addr: None,
                publish_addr: false,
                lookup_by_dns,
            })
            .await
            .unwrap();

        // 1. Subscribe to listing updates
        let mut stream = laplink_p2p::listing::subscribe_listing(&endpoint, &ticket)
            .await
            .unwrap();

        // 2. Initial listing snapshot pushed immediately
        let initial = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await
            .expect("timeout waiting for initial listing")
            .unwrap()
            .expect("stream should not be closed");
        assert_eq!(
            initial
                .entries
                .len(),
            1
        );
        assert_eq!(initial.entries[0].path, "file1.txt");

        // 3. Create a second file on the filesystem
        let file2 = folder
            .path()
            .join("file2.txt");
        std::fs::write(&file2, b"second file content").unwrap();

        // 4. Expect update pushed over the subscription stream
        let update1 = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await
            .expect("timeout waiting for update after file addition")
            .unwrap()
            .expect("stream should not be closed");
        assert_eq!(
            update1
                .entries
                .len(),
            2
        );
        assert_eq!(update1.entries[0].path, "file1.txt");
        assert_eq!(update1.entries[1].path, "file2.txt");

        // 5. Delete file1 on the filesystem
        std::fs::remove_file(&file1).unwrap();

        // 6. Expect update pushed over the subscription stream
        let update2 = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await
            .expect("timeout waiting for update after file deletion")
            .unwrap()
            .expect("stream should not be closed");
        assert_eq!(
            update2
                .entries
                .len(),
            1
        );
        assert_eq!(update2.entries[0].path, "file2.txt");
    });

    child
        .kill()
        .unwrap();
    let _ = child.wait();
}

#[test]
fn ll_serve_and_client_version_logging() {
    let folder = tempfile::tempdir().unwrap();
    let config_dir = tempfile::tempdir().unwrap();
    let test_file = folder.path().join("version_test.txt");
    std::fs::write(&test_file, b"version test content").unwrap();

    let mut child = std::process::Command::new(ll_serve_bin())
        .arg(folder.path())
        .env("LAPLINK_CONFIG_DIR", config_dir.path())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdout = child
        .stdout
        .take()
        .unwrap();
    let output = read_ascii_lines(3, &mut stdout).unwrap();
    let output = String::from_utf8(output).unwrap();
    let ticket_str = output
        .split_ascii_whitespace()
        .last()
        .unwrap();
    let ticket = iroh_tickets::endpoint::EndpointTicket::from_str(ticket_str).unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (secret_key, _) = laplink_p2p::get_or_create_secret().unwrap();
        let lookup_by_dns = ticket
            .endpoint_addr()
            .addrs
            .is_empty();
        let endpoint =
            laplink_p2p::endpoint::build_endpoint(laplink_p2p::endpoint::EndpointConfig {
                secret_key,
                alpns: vec![],
                relay: laplink_p2p::RelayModeOption::Default,
                magic_ipv4_addr: None,
                magic_ipv6_addr: None,
                publish_addr: false,
                lookup_by_dns,
            })
            .await
            .unwrap();

        // 1. Fetch listing and verify server version communicated
        let listing = laplink_p2p::listing::fetch_listing(&endpoint, &ticket)
            .await
            .unwrap();
        assert_eq!(
            listing.server_version(),
            Some(env!("CARGO_PKG_VERSION"))
        );

        // 2. Subscribe to listing and verify server version communicated
        let mut stream = laplink_p2p::listing::subscribe_listing(&endpoint, &ticket)
            .await
            .unwrap();
        let initial = stream
            .next()
            .await
            .unwrap()
            .expect("stream frame");
        assert_eq!(
            initial.server_version(),
            Some(env!("CARGO_PKG_VERSION"))
        );
    });

    child
        .kill()
        .unwrap();
    let out = child.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    let expected_serve_version = format!("ll-serve version: {}", env!("CARGO_PKG_VERSION"));
    assert!(
        stderr.contains(&expected_serve_version),
        "expected stderr to contain '{expected_serve_version}', got:\n{stderr}"
    );

    // 3. Test ll-tui logs its own version
    let empty_config = tempfile::tempdir().unwrap();
    let tui_output = std::process::Command::new(ll_tui_bin())
        .env("LAPLINK_CONFIG_DIR", empty_config.path())
        .output()
        .unwrap();
    let tui_stderr = String::from_utf8_lossy(&tui_output.stderr);
    let expected_tui_version = format!("ll-tui version: {}", env!("CARGO_PKG_VERSION"));
    assert!(
        tui_stderr.contains(&expected_tui_version),
        "expected ll-tui to output '{expected_tui_version}', got:\n{tui_stderr}"
    );
}
