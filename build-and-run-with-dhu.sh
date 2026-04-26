#!/bin/bash
# run this in a terminal where dhu works.
# while ./runscriptloop ; do watch -n 1 -g ls --time-style full-iso -l rerun ; ./build-and-run-with-dhu.sh ; done
# Modes are selected via the `rerun` file:
#   `cluster`  => DHU cluster.ini + proxy config_nocluster.toml
#   `injected` => DHU default config + proxy config_cluster.toml
#   anything else / empty => DHU default config + proxy config_nocluster.toml

# There is a maximum runtime of 15 seconds, after which aa-proxy-rs exits. Log files are named
# stored-log-<normal,cluster>-<date>.log for aa-proxy-rs and stored-dhu-log-<normal,cluster>-<date>.log for dhu.

set -ex

HWHOST="192.168.1.97"
max_runtime=15
cluster_capture_delay_secs="${CLUSTER_CAPTURE_DELAY_SECS:-6}"
main_capture_delay_secs="${MAIN_CAPTURE_DELAY_SECS:-0}"
capture_timeout_secs="${CAPTURE_TIMEOUT_SECS:-10}"
config_source="config_nocluster.toml"
breadcrumb_file="${BUILD_AND_RUN_WITH_DHU_BREADCRUMB_FILE:-$PWD/.build-and-run-with-dhu.breadcrumb.log}"

log_breadcrumb() {
    local stage="$1"
    shift || true
    printf '%s stage=%s pid=%s ppid=%s cwd=%s rerun=%s display=%s wayland=%s xauthority=%s %s\n' \
        "$(date --iso-8601=seconds)" \
        "$stage" \
        "$$" \
        "$PPID" \
        "$PWD" \
        "$(cat rerun 2>/dev/null || echo missing)" \
        "${DISPLAY:-}" \
        "${WAYLAND_DISPLAY:-}" \
        "${XAUTHORITY:-}" \
        "$*" >> "$breadcrumb_file" 2>/dev/null || true
}

trap 'log_breadcrumb exit "status=$?"' EXIT

if [ "$1" == "--help" ]; then
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  --help        Show this help message and exit"
    echo "Mode is selected via the rerun file: cluster, injected, or default"
    exit 0
fi
if [ ! -d "target" ]; then
    echo "Error: This script must be run from the root of the aa-proxy-rs project."
    exit 1
fi
if [ ! -d "$HOME/dhu" ]; then
    echo "Error: The desktop-head-unit directory was not found at $HOME/dhu."
    exit 1
fi
log_breadcrumb entered
dhu_args=()
run_cluster=$( [ -f rerun ] && [ "$(cat rerun)" == "cluster" ] && echo "true" || echo "false" )
injected_display_types=$( [ -f rerun ] && [ "$(cat rerun)" == "injected" ] && echo "true" || echo "false" )
if [ "$run_cluster" == "true" ]; then
    dhu_args+=("--config" "cluster.ini")
    log_filename="cluster"
elif [ "$injected_display_types" == "true" ]; then
    log_filename="injected"
    config_source="config_cluster.toml"
else
    log_filename="normal"
fi

if [ ! -f "$config_source" ]; then
    echo "Error: Expected config file '$config_source' was not found."
    exit 1
fi

if [ -z "${DISPLAY:-}" ]; then
    if [ -S /tmp/.X11-unix/X0 ]; then
        export DISPLAY=:0
    fi
fi
if [ -z "${XAUTHORITY:-}" ] && [ -f "${HOME}/.Xauthority" ]; then
    export XAUTHORITY="${HOME}/.Xauthority"
fi
if [ -z "${DISPLAY:-}" ] && [ -z "${WAYLAND_DISPLAY:-}" ]; then
    echo "Error: No graphical session detected (DISPLAY/WAYLAND_DISPLAY both unset)."
    echo "Set DISPLAY or WAYLAND_DISPLAY, or run this script from a graphical terminal session."
    exit 1
fi
log_breadcrumb pre_build "config_source=$config_source log_filename=${log_filename:-unset}"
echo "building aa-proxy-rs"
cargo build --release
log_breadcrumb post_build

#echo "starting desktop-head-unit $* in the background with a 5 second delay $(date)"
#(echo "starting dhu for 20 seconds after sleep $(date)";sleep 5; echo "starting dhu now $(date)"; ./desktop-head-unit "${dhu_args[@]}") > desktop-head-unit.log 2>&1 &


echo "Starting aa-proxy-rs in the foreground. Session timeout is ${max_runtime} seconds. $(date)"
echo "Using proxy config: ${config_source}"
# target/x86_64-unknown-linux-gnu/debug/aa-proxy-rs
log_breadcrumb pre_remote_prep
ssh root@$HWHOST sh -c 'killall aa-proxy-rs ; truncate -s0 /var/log/aa-proxy-rs.log'
log_breadcrumb post_remote_prep
scp -O "$config_source" root@$HWHOST:/tmp/config.toml
log_breadcrumb post_config_scp
scp -O target/aarch64-unknown-linux-gnu/release/aa-proxy-rs root@$HWHOST:/tmp/aa-proxy-rs
log_breadcrumb post_binary_scp
ssh root@$HWHOST /tmp/aa-proxy-rs --config /tmp/config.toml --session-timeout ${max_runtime} --connection-start-timeout ${max_runtime} &
log_breadcrumb post_remote_launch
pushd "$HOME/dhu"
rm /tmp/cluster_tap.bin || true
rm /tmp/main_tap.bin || true
echo "Starting delayed cluster & main tap capture after ${cluster_capture_delay_secs}/${main_capture_delay_secs}s (timeout ${capture_timeout_secs}s)"
(sleep "${cluster_capture_delay_secs}"; while ! nc -z $HWHOST 12346 >/dev/null 2>&1; do :; done; timeout "${capture_timeout_secs}"s nc $HWHOST 12346 > /tmp/cluster_tap.bin) &
(sleep "${main_capture_delay_secs}"; while ! nc -z $HWHOST 12345 >/dev/null 2>&1; do :; done; timeout "${capture_timeout_secs}"s nc $HWHOST 12345 > /tmp/main_tap.bin) &
date --iso-8601 seconds > desktop-head-unit.log
log_breadcrumb pre_dhu
./desktop-head-unit -u "${dhu_args[@]}" 2>&1 | tee -a desktop-head-unit.log && echo "desktop-head-unit exited with code $?" >> desktop-head-unit.log || echo "desktop-head-unit exited with error code $?" >> desktop-head-unit.log 
log_breadcrumb post_dhu
popd
echo "desktop-head-unit has exited. Rotating log files."

scp -O root@$HWHOST:/tmp/aa-proxy-rs.log ./
rotate_to="stored-log-${log_filename}-$(date --iso-8601 seconds).log"
cp -v aa-proxy-rs.log "${rotate_to}" && echo "" > aa-proxy-rs.log
cp -v "$HOME/dhu/desktop-head-unit.log" "stored-dhu-log-${log_filename}-$(date --iso-8601 seconds).log"
log_breadcrumb post_rotate "rotate_to=$rotate_to"