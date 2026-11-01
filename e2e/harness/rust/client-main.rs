//! Cross-language E2E probe: the `rmtt` Rust client against any rmtt server.
//!
//! Usage: rmtt-rust-e2e <scheme://host:port> [credential] [binarySize]
//!
//! Runs sequential, concurrent, binary and empty-payload round trips against an
//! echo server and prints CLIENT_E2E_PASS on success.
use std::sync::mpsc;
use std::time::Duration;

use rmtt::{Client, ClientOptions, TlsConfig};

const COUNT: usize = 5;

fn main() {
    let mut args = std::env::args().skip(1);
    let addr = args
        .next()
        .unwrap_or_else(|| "tcp://127.0.0.1:19990".to_string());
    let credential = args.next().unwrap_or_else(|| "rust-e2e".to_string());
    let binary_size: usize = args
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(256 * 1024);

    let mut opts = ClientOptions::default();
    opts.credential = credential;
    opts.heartbeat_seconds = 10;
    opts.auto_reconnect = false;
    opts.connect_timeout = Duration::from_secs(10);
    if addr.starts_with("tls://") {
        opts.tls = Some(TlsConfig {
            server_name: None,
            insecure: true,
        });
    }

    let client = match Client::connect(&addr, opts) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("connect failed: {e}");
            std::process::exit(1);
        }
    };
    println!("CLIENT_CONNECTED server_kp={}", client.server_keepalive());

    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    client.set_payload_handler(move |msg| {
        let _ = tx.send(msg.payload().to_vec());
    });

    let wait_echo = |want: &[u8], seconds: u64| -> bool {
        let deadline = std::time::Instant::now() + Duration::from_secs(seconds);
        while std::time::Instant::now() < deadline {
            match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(payload) => {
                    if payload == want {
                        return true;
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(_) => return false,
            }
        }
        false
    };

    for i in 0..COUNT {
        let message = format!("ping-{i}");
        if let Err(e) = client.push(message.as_bytes()) {
            eprintln!("push failed: {e}");
            std::process::exit(1);
        }
        if !wait_echo(format!("echo:{message}").as_bytes(), 10) {
            eprintln!("missing echo for {message}");
            std::process::exit(1);
        }
    }
    println!("CLIENT_SEQ_OK n={COUNT}");

    let mut pushed = 0;
    for i in 0..COUNT {
        if client.push(format!("conc-{i}").as_bytes()).is_ok() {
            pushed += 1;
        }
    }
    let mut pending: Vec<String> = (0..COUNT).map(|i| format!("echo:conc-{i}")).collect();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while !pending.is_empty() && std::time::Instant::now() < deadline {
        if let Ok(payload) = rx.recv_timeout(Duration::from_millis(200)) {
            let text = String::from_utf8_lossy(&payload).to_string();
            pending.retain(|item| *item != text);
        }
    }
    if !pending.is_empty() {
        eprintln!("missing concurrent echoes: {pending:?}");
        std::process::exit(1);
    }
    println!("CLIENT_CONCURRENT_OK n={pushed}");

    let blob: Vec<u8> = (0..binary_size).map(|i| (i % 256) as u8).collect();
    if let Err(e) = client.push(&blob) {
        eprintln!("binary push failed: {e}");
        std::process::exit(1);
    }
    let mut expected = b"echo:".to_vec();
    expected.extend_from_slice(&blob);
    if !wait_echo(&expected, 20) {
        eprintln!("binary echo mismatch");
        std::process::exit(1);
    }
    println!("CLIENT_BINARY_OK bytes={binary_size}");

    if let Err(e) = client.push(b"") {
        eprintln!("empty push failed: {e}");
        std::process::exit(1);
    }
    if !wait_echo(b"echo:", 10) {
        eprintln!("empty echo missing");
        std::process::exit(1);
    }
    println!("CLIENT_EMPTY_OK");

    client.disconnect();
    println!("CLIENT_E2E_PASS");
}
