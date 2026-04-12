#!/bin/bash
# run this in a terminal where dhu works.
# while ./runscriptloop ; do watch -n 1 -g ls --time-style full-iso -l rerun ; ./build-and-run-with-dhu.sh ; done
# You can now test running the DHU yourself by doing either `touch rerun` to re-run aa-proxy-rs and dhu with the current configuration (whatever that is at that time), or `echo "cluster" > rerun` to run with dhu configured with cluster or `echo "" > rerun` to run with defaults (no cluster).

# There is a maximum runtime of 15 seconds, after which aa-proxy-rs exits. Log files are named
# stored-log-<normal,cluster>-<date>.log for aa-proxy-rs and stored-dhu-log-<normal,cluster>-<date>.log for dhu.

reset
set -e

max_runtime=15
cluster_capture_delay_secs="${CLUSTER_CAPTURE_DELAY_SECS:-6}"
main_capture_delay_secs="${MAIN_CAPTURE_DELAY_SECS:-0}"
capture_timeout_secs="${CAPTURE_TIMEOUT_SECS:-10}"

if [ "$1" == "--help" ]; then
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  --help        Show this help message and exit"
    echo "  cluster       Run desktop-head-unit with the cluster test configuration"
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
dhu_args=()
run_cluster=$( [ -f rerun ] && [ "$(cat rerun)" == "cluster" ] && echo "true" || echo "false" )
injected_display_types=$( [ -f rerun ] && [ "$(cat rerun)" == "injected" ] && echo "true" || echo "false" )
if [ "$run_cluster" == "true" ]; then
    dhu_args+=("--config" "cluster.ini")
    log_filename="cluster"
elif [ "$injected_display_types" == "true" ]; then
    log_filename="injected"
else
    log_filename="normal"
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
echo "building aa-proxy-rs"
cargo build --target x86_64-unknown-linux-gnu

#echo "starting desktop-head-unit $* in the background with a 5 second delay $(date)"
#(echo "starting dhu for 20 seconds after sleep $(date)";sleep 5; echo "starting dhu now $(date)"; ./desktop-head-unit "${dhu_args[@]}") > desktop-head-unit.log 2>&1 &


echo "Starting aa-proxy-rs in the foreground. Session timeout is ${max_runtime} seconds. $(date)"
target/x86_64-unknown-linux-gnu/debug/aa-proxy-rs --session-timeout ${max_runtime} --connection-start-timeout ${max_runtime} &
pushd "$HOME/dhu"
rm /tmp/cluster_tap.bin || true
rm /tmp/main_tap.bin || true
echo "Starting delayed cluster & main tap capture after ${cluster_capture_delay_secs}/${main_capture_delay_secs}s (timeout ${capture_timeout_secs}s)"
(sleep "${cluster_capture_delay_secs}"; while ! nc -z 127.0.0.1 12346 >/dev/null 2>&1; do :; done; timeout "${capture_timeout_secs}"s nc 127.0.0.1 12346 > /tmp/cluster_tap.bin) &
(sleep "${main_capture_delay_secs}"; while ! nc -z 127.0.0.1 12345 >/dev/null 2>&1; do :; done; timeout "${capture_timeout_secs}"s nc 127.0.0.1 12345 > /tmp/main_tap.bin) &
date --iso-8601 seconds > desktop-head-unit.log
./desktop-head-unit "${dhu_args[@]}" 2>&1 | tee -a desktop-head-unit.log && echo "desktop-head-unit exited with code $?" >> desktop-head-unit.log || echo "desktop-head-unit exited with error code $?" >> desktop-head-unit.log 
popd
echo "desktop-head-unit has exited. Rotating log files."

rotate_to="stored-log-${log_filename}-$(date --iso-8601 seconds).log"
cp -v aa-proxy-rs.log "${rotate_to}" && echo "" > aa-proxy-rs.log
cp -v "$HOME/dhu/desktop-head-unit.log" "stored-dhu-log-${log_filename}-$(date --iso-8601 seconds).log"