# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

`aa-proxy-rs` is a Rust daemon that bridges a wireless Android phone with a USB-connected car head unit, enabling wireless Android Auto. It runs on embedded Linux boards (Raspberry Pi, etc.) and acts as a protocol proxy between the phone (over Wi-Fi/TCP or wired USB) and the car head unit (over USB gadget/accessory mode).

## Build commands

The default Cargo target is `aarch64-unknown-linux-gnu` (set in `.cargo/config.toml`). The linker `aarch64-linux-gnu-gcc` must be installed.

```bash
cargo build --release                                        # cross-compile for aarch64 (default)
cargo build --release --target x86_64-unknown-linux-gnu     # build for local host
cargo check                                                  # fast type-check without linking
```

There are no automated tests. The main test workflow is the DHU (Desktop Head Unit) script.

## DHU development workflow

`build-and-run-with-dhu.sh` is the primary dev loop. It cross-compiles, deploys to a remote board at `192.168.1.97` via SSH/SCP, runs `aa-proxy-rs` there with a 15-second session timeout, and simultaneously runs DHU locally.

`aa-proxy-rs` can also be built and run locally on the development machine (x86_64) without a remote board. This requires enabling `dhu = true` and `wired = "VID:PID"` in the config, connecting an Android phone via USB, and launching DHU without arguments. The local binary is built with:

```bash
cargo build --release --target x86_64-unknown-linux-gnu
./target/x86_64-unknown-linux-gnu/release/aa-proxy-rs --config config.toml
```

```bash
./build-and-run-with-dhu.sh
# or run in a loop:
while ./runscriptloop; do watch -n 1 -g ls --time-style full-iso -l rerun; ./build-and-run-with-dhu.sh; done
```

The `rerun` file controls the test mode:
- `cluster` — DHU uses `cluster.ini`, proxy uses `config_nocluster.toml`
- `injected` — DHU uses default config, proxy uses `config_cluster.toml`
- empty/anything else — DHU default config, proxy uses `config_nocluster.toml`

Media taps are captured to `/tmp/main_tap.bin` (port 12345) and `/tmp/cluster_tap.bin` (port 12346). Logs are stored as `stored-log-<mode>-<date>.log` and `stored-dhu-log-<mode>-<date>.log`.

## Architecture

### Data flow

In **normal mode**: phone connects via TCP (port 5288) or USB → io_uring bidirectional copy → `/dev/usb_accessory` (car head unit).

In **MITM mode**: each side gets its own TLS connection. Frames are decrypted, parsed as protobuf, optionally modified, re-encrypted, and forwarded. This enables DPI override, display injection, EV routing data, etc.

In **DHU mode**: instead of USB accessory, the proxy connects to DHU's local TCP port (5277).

### Module map

| Module | Role |
|---|---|
| `main.rs` | Entry point; initializes all subsystems, orchestrates reconnect loop |
| `io_uring.rs` | Core I/O engine: bidirectional forwarding via io_uring between TCP (phone) and USB/DHU |
| `mitm.rs` | MITM protocol logic: TLS termination, protobuf frame parsing/injection, display injection, cluster stream |
| `media_tap.rs` | Taps raw H264/audio frames from the MITM stream and exposes them over TCP for capture |
| `mpegts.rs` | Wraps tapped media in MPEG-TS for VLC/ffmpeg consumption |
| `bluetooth.rs` | Powers BT adapter, registers AA profile and fake headset profile, sends `WifiStartRequest` |
| `btle.rs` | Bluetooth LE advertising |
| `usb_gadget.rs` | USB gadget mode switching (default/accessory) and uevent monitoring |
| `usb_stream.rs` | Async wrappers for USB accessory reads/writes |
| `aoa.rs` | Android Open Accessory protocol helpers |
| `ev.rs` | EV battery sensor data: receives REST data, injects protobuf sensor frames into AA stream |
| `web.rs` | Embedded axum HTTP server: config UI (`static/index.html`), log download, cert upload |
| `config.rs` / `config_types.rs` | TOML config parsing with serde; `SharedConfig = Arc<RwLock<AppConfig>>` |
| `led.rs` / `button.rs` | Hardware LED and button handling |
| `build.rs` | Generates Rust code from `.proto` files at build time |

### Protobuf

AndroidAuto protocol messages are generated at build time from `src/protos/` using `protobuf-codegen` + `protoc-bin-vendored`. Generated code is included with `include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"))`.

### Key config options

Config file default: `/etc/aa-proxy-rs/config.toml`. Notable options:
- `mitm` — enable MITM mode (requires SSL certs in `/etc/aa-proxy-rs/`)
- `dhu` — route USB side to DHU's TCP port instead of `/dev/usb_accessory`
- `wired` — wired USB phone mode with `VID:PID` string (e.g. `"4e8:6860"`)
- `inject_display_types` — inject cluster/aux display into ServiceDiscovery (e.g. `"DISPLAY_TYPE_CLUSTER"`)
- `legacy` — use legacy two-gadget USB switching for compatibility with some head units
- `connect` — BT MAC to auto-connect to (`00:00:00:00:00:00` = any paired device)

### Concurrency model

The entire application is async Tokio, but `io_uring` operations run in a dedicated `tokio_uring` runtime (not the main Tokio runtime). Communication between subsystems uses `tokio::sync::mpsc` channels. `SharedConfig = Arc<RwLock<AppConfig>>` is passed to all long-running tasks.

## MITM SSL certificates

MITM mode requires five PEM files in `/etc/aa-proxy-rs/`: `hu_key.pem`, `hu_cert.pem`, `md_key.pem`, `md_cert.pem`, `galroot_cert.pem`. These are not in the repo — see README for sources.
