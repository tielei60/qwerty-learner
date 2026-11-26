// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

fn is_sync_port_open(port: u16) -> bool {
  let addr = SocketAddr::from(([127, 0, 0, 1], port));
  TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok()
}

fn find_sync_server_binary() -> Option<PathBuf> {
  let bin_name = if cfg!(windows) { "sync-server.exe" } else { "sync-server" };

  // 1. Same directory as the Tauri executable or macOS bundle Resources
  if let Ok(exe_path) = std::env::current_exe() {
    if let Some(dir) = exe_path.parent() {
      let candidate = dir.join(bin_name);
      if candidate.exists() {
        return Some(candidate);
      }
      let mac_res = dir.join("../Resources").join(bin_name);
      if mac_res.exists() {
        return Some(mac_res);
      }
    }
  }

  // 2. Workspace relative paths
  for rel in ["sync-server/sync-server", "../sync-server/sync-server"] {
    let candidate = PathBuf::from(rel);
    if candidate.exists() {
      return Some(candidate);
    }
  }

  None
}

/// Ensure the shared sync-server daemon is running on the local machine.
/// Left running as a detached background process when Tauri exits so the web client stays synced.
fn ensure_sync_server_running(port: u16, data_dir: Option<PathBuf>) {
  if is_sync_port_open(port) {
    return;
  }

  if let Some(bin) = find_sync_server_binary() {
    let mut cmd = Command::new(&bin);
    if let Some(parent) = bin.parent() {
      cmd.current_dir(parent);
    }
    if let Some(data_dir) = data_dir {
      if std::env::var_os("SYNC_DATA_DIR").is_none() { cmd.env("SYNC_DATA_DIR", data_dir); }
    }
    let _ = cmd
      .env("SYNC_WS_PORT", port.to_string())
      .stdin(Stdio::null())
      .stdout(Stdio::null())
      .stderr(Stdio::null())
      .spawn();
  }
}

fn main() {
  let ws_port: u16 = std::env::var("SYNC_WS_PORT")
    .ok()
    .and_then(|v| v.parse().ok())
    .unwrap_or(9322);

  tauri::Builder::default()
    .setup(move |app| {
      ensure_sync_server_running(ws_port, app.path_resolver().app_data_dir());
      Ok(())
    })
    .run(tauri::generate_context!())
    .expect("error while running tauri application");
}
