#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Capture a synchronized debug session for aa-proxy-rs.

Usage:
  scripts/capture_session.sh [--label NAME] [--seconds N] [--dhu-cmd "CMD"] [--log-file PATH] [--tail-lines N] [--usb-iface NAME]

Options:
  --label NAME     Session label (default: session)
  --seconds N      Capture window in seconds (default: 120)
  --dhu-cmd CMD    Optional DHU command to run and capture output
  --log-file PATH  Proxy log file to follow (default: /var/log/aa-proxy-rs.log)
  --tail-lines N   How many existing log lines to include before follow (default: 0)
  --usb-iface IF   USB monitor interface for tshark (default: usbmon0)
  --help           Show this help

Examples:
  scripts/capture_session.sh --label scenarioC --seconds 150
  scripts/capture_session.sh --label scenarioA --seconds 120 --dhu-cmd "./desktop-head-unit --config config.ini"

Artifacts:
  artifacts/captures/<timestamp>-<label>/
    proxy.log.follow
    dhu.log                   (if --dhu-cmd used)
    usbmon.pcapng             (if tshark+permissions available)
    timeline.filtered.log
    timeline.summary.md
    capture.status
    metadata.txt
EOF
}

LABEL="session"
SECONDS_TO_CAPTURE=120
DHU_CMD=""
PROXY_LOG="/var/log/aa-proxy-rs.log"
TAIL_LINES=0
USB_IFACE="usbmon0"
SCRIPT_START_EPOCH="$(date +%s)"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --label)
      LABEL="$2"
      shift 2
      ;;
    --seconds)
      SECONDS_TO_CAPTURE="$2"
      shift 2
      ;;
    --dhu-cmd)
      DHU_CMD="$2"
      shift 2
      ;;
    --log-file)
      PROXY_LOG="$2"
      shift 2
      ;;
    --tail-lines)
      TAIL_LINES="$2"
      shift 2
      ;;
    --usb-iface)
      USB_IFACE="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if ! [[ "$SECONDS_TO_CAPTURE" =~ ^[0-9]+$ ]] || [[ "$SECONDS_TO_CAPTURE" -le 0 ]]; then
  echo "--seconds must be a positive integer" >&2
  exit 2
fi

if ! [[ "$TAIL_LINES" =~ ^[0-9]+$ ]]; then
  echo "--tail-lines must be a non-negative integer" >&2
  exit 2
fi

mkdir -p artifacts/captures
STAMP="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="artifacts/captures/${STAMP}-${LABEL}"
mkdir -p "$OUT_DIR"

echo "Capture output: $OUT_DIR"

PIDS=()
STATUS_FILE="$OUT_DIR/capture.status"

note_status() {
  echo "$(date -Is) $*" >> "$STATUS_FILE"
}

stop_pid() {
  local name="$1"
  local pid="$2"
  if ! kill -0 "$pid" 2>/dev/null; then
    note_status "$name pid=$pid already-exited"
    return
  fi

  kill -INT "$pid" 2>/dev/null || true
  for _ in {1..10}; do
    if ! kill -0 "$pid" 2>/dev/null; then
      note_status "$name pid=$pid stopped-by-int"
      return
    fi
    sleep 0.1
  done

  kill -TERM "$pid" 2>/dev/null || true
  for _ in {1..10}; do
    if ! kill -0 "$pid" 2>/dev/null; then
      note_status "$name pid=$pid stopped-by-term"
      return
    fi
    sleep 0.1
  done

  kill -KILL "$pid" 2>/dev/null || true
  note_status "$name pid=$pid stopped-by-kill"
}

cleanup() {
  local entry
  for entry in "${PIDS[@]:-}"; do
    local name="${entry%%:*}"
    local pid="${entry##*:}"
    stop_pid "$name" "$pid"
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT

note_status "capture-start label=$LABEL seconds=$SECONDS_TO_CAPTURE"

{
  echo "timestamp: $(date -Is)"
  echo "label: $LABEL"
  echo "seconds: $SECONDS_TO_CAPTURE"
  echo "proxy_log: $PROXY_LOG"
  echo "tail_lines: $TAIL_LINES"
  echo "usb_iface: $USB_IFACE"
  echo "cwd: $(pwd)"
  echo "uname: $(uname -a)"
  echo "start_epoch: $SCRIPT_START_EPOCH"
  echo "git_head: $(git rev-parse --short HEAD 2>/dev/null || echo n/a)"
  echo "git_dirty: $(git status --porcelain 2>/dev/null | wc -l | tr -d ' ') changed paths"
  if [[ -n "$DHU_CMD" ]]; then
    echo "dhu_cmd: $DHU_CMD"
  fi
} > "$OUT_DIR/metadata.txt"

if [[ -f "$PROXY_LOG" ]]; then
  stdbuf -oL tail -n"$TAIL_LINES" -F "$PROXY_LOG" > "$OUT_DIR/proxy.log.follow" 2>&1 &
  PIDS+=("proxy-tail:$!")
  note_status "started proxy-tail pid=$! file=$PROXY_LOG"
else
  echo "warning: proxy log file does not exist yet: $PROXY_LOG" | tee -a "$OUT_DIR/metadata.txt"
  note_status "warning missing-proxy-log path=$PROXY_LOG"
fi

USB_CAPTURE_STARTED=0
if command -v tshark >/dev/null 2>&1; then
  if stdbuf -oL tshark -i "$USB_IFACE" -w "$OUT_DIR/usbmon.pcapng" > "$OUT_DIR/usbmon.stderr.log" 2>&1 & then
    PIDS+=("usb-tshark:$!")
    USB_CAPTURE_STARTED=1
    note_status "started usb-tshark pid=$! iface=$USB_IFACE"
  else
    echo "warning: failed to start tshark capture on $USB_IFACE" | tee -a "$OUT_DIR/metadata.txt"
    note_status "warning tshark-start-failed iface=$USB_IFACE"
  fi
else
  echo "warning: tshark not found; USB pcap will not be captured" | tee -a "$OUT_DIR/metadata.txt"
  note_status "warning tshark-not-found"
fi

if [[ -n "$DHU_CMD" ]]; then
  # shellcheck disable=SC2086
  stdbuf -oL bash -lc "$DHU_CMD" > "$OUT_DIR/dhu.log" 2>&1 &
  PIDS+=("dhu:$!")
  note_status "started dhu pid=$!"
fi

echo "Capture running for ${SECONDS_TO_CAPTURE}s. Reproduce scenario now..."
sleep "$SECONDS_TO_CAPTURE"

cleanup
trap - EXIT

if [[ $USB_CAPTURE_STARTED -eq 1 ]]; then
  echo "usb capture: $OUT_DIR/usbmon.pcapng"
fi

FILTER='PING_REQUEST|PING_RESPONSE|AUDIO_FOCUS_REQUEST|runtime transmit timeout|runtime transmit error|startup guard failed|unexpected transfer stall|transfer monitor: no traffic|Connection error|SERVICE_DISCOVERY_RESPONSE|SDR input|SDR output|SERVICE_DISCOVERY_UPDATE|SDU:|MESSAGE_CHANNEL_OPEN_REQUEST|MESSAGE_CHANNEL_OPEN_RESPONSE|ChannelOpenRequest:|ChannelOpenResponse:|transparency: synthesized CHANNEL_OPEN_RESPONSE|dropping injected channel packet towards HU|divergence counters|SSL handshake|SSL init complete|timeout waiting on|timeout transmitting packet'

TIMELINE_SOURCE="$OUT_DIR/proxy.log.follow"
if [[ ! -f "$TIMELINE_SOURCE" ]]; then
  TIMELINE_SOURCE="$PROXY_LOG"
fi

if [[ -f "$TIMELINE_SOURCE" ]]; then
  if command -v rg >/dev/null 2>&1; then
    rg -n "$FILTER" "$TIMELINE_SOURCE" > "$OUT_DIR/timeline.filtered.log" || true
  else
    grep -En "$FILTER" "$TIMELINE_SOURCE" > "$OUT_DIR/timeline.filtered.log" || true
  fi
fi

if [[ -f "$OUT_DIR/timeline.filtered.log" ]]; then
  {
    echo "# Timeline Summary"
    echo
    echo "Source: $TIMELINE_SOURCE"
    echo
    echo "## Event Counts"
    for pat in \
      'SDR output' \
      'SDU:' \
      'MESSAGE_CHANNEL_OPEN_REQUEST|ChannelOpenRequest:' \
      'MESSAGE_CHANNEL_OPEN_RESPONSE|ChannelOpenResponse:' \
      'AUDIO_FOCUS_REQUEST' \
      'PING_REQUEST' \
      'PING_RESPONSE' \
      'runtime transmit timeout' \
      'runtime transmit error' \
      'unexpected transfer stall' \
      'divergence counters'; do
      count=$(grep -Ec "$pat" "$OUT_DIR/timeline.filtered.log" || true)
      printf -- "- %s: %s\n" "$pat" "$count"
    done
    echo
    echo "## First Error/Warning Lines"
    grep -En 'ERROR|WARN|runtime transmit timeout|runtime transmit error|unexpected transfer stall' "$OUT_DIR/timeline.filtered.log" | head -20 || true
  } > "$OUT_DIR/timeline.summary.md"
fi

SCRIPT_END_EPOCH="$(date +%s)"
{
  echo "end_epoch: $SCRIPT_END_EPOCH"
  echo "duration_s: $((SCRIPT_END_EPOCH - SCRIPT_START_EPOCH))"
} >> "$OUT_DIR/metadata.txt"

note_status "capture-end duration_s=$((SCRIPT_END_EPOCH - SCRIPT_START_EPOCH))"

echo "Done. Artifacts written to: $OUT_DIR"
echo "Next: attach $OUT_DIR/timeline.filtered.log, $OUT_DIR/timeline.summary.md, $OUT_DIR/proxy.log.follow, and usbmon.pcapng if present."
