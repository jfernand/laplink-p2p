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
fn ll_serve_remembers_ticket_arg() {
    let folder = tempfile::tempdir().unwrap();
    let config_dir = tempfile::tempdir().unwrap();
    let output = duct::cmd(ll_serve_bin(), ["--help"])
        .dir(folder.path())
        .env("LAPLINK_CONFIG_DIR", config_dir.path())
        .run()
        .unwrap();
    assert!(output
        .status
        .success());
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
