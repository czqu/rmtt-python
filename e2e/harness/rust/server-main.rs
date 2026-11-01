//! Cross-language E2E probe: the `rmtt` Rust server from this repository
//! (e2e/rust-server) against the Go / Rust rmtt clients. Every upstream PUSH
//! is logged as `PUSH_FROM_CLIENT device=<id> payload=<text>` and echoed back
//! to the same device as `echo:<text>`, which the client verifies.
//!
//! Prints `E2E_SERVER_READY port=<port>` once bound, then stays alive until
//! the harness kills it.
use std::sync::Arc;
use std::time::Duration;

use rmtt::server::{AuthResult, KeepalivePolicy, Server, ServerOptions};

fn main() {
    let mut args = std::env::args().skip(1);
    let port = args.next().unwrap_or_else(|| "19990".to_string());
    let kp_min: u16 = args.next().and_then(|v| v.parse().ok()).unwrap_or(30);
    let kp_max: u16 = args.next().and_then(|v| v.parse().ok()).unwrap_or(600);
    let reject = args.next().unwrap_or_default();
    let addr = format!("0.0.0.0:{port}");

    let options = ServerOptions {
        keepalive: KeepalivePolicy {
            min_seconds: kp_min,
            max_seconds: kp_max,
            default_seconds: 60,
            allow_disable: false,
        },
        ..ServerOptions::default()
    };
    let server = Arc::new(Server::bind(&addr, options).expect("bind failed"));
    // Allow any credential and use it as the device id (like the Go server),
    // unless a credential is explicitly rejected.
    let rejected = reject.clone();
    server.set_authenticator(move |credential| {
        if !rejected.is_empty() && credential == rejected {
            AuthResult::reject()
        } else {
            AuthResult::allow(credential)
        }
    });

    let s = server.clone();
    server.set_message_handler(move |device_id, payload| {
        // Echo raw bytes (binary-safe) with the "echo:" prefix used by every
        // client probe in the cross-language matrix.
        println!("PUSH device={device_id} len={}", payload.len());
        let mut echoed = b"echo:".to_vec();
        echoed.extend_from_slice(payload);
        if let Err(e) = s.push(device_id, &echoed) {
            eprintln!("echo push failed for device={device_id}: {e}");
        }
    });

    server.set_on_connect(|device_id| println!("CONNECTED device={device_id}"));
    server.set_on_disconnect(|device_id, reason| {
        println!("DISCONNECTED device={device_id} reason={reason}")
    });

    println!("E2E_SERVER_READY port={port}");
    // Keep the process alive until the harness kills it.
    loop {
        std::thread::sleep(Duration::from_secs(60));
    }
}
