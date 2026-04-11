#!/bin/bash
# run this in a terminal where dhu works.
# while ./runscriptloop ; do watch -n 1 -g ls --time-style full-iso -l rerun ; ./build-and-run-with-dhu.sh ; done
# You can now test running the DHU yourself by doing either `touch rerun` to re-run aa-proxy-rs and dhu with the current configuration (whatever that is at that time), or `echo "cluster" > rerun` to run with dhu configured with cluster or `echo "" > rerun` to run with defaults (no cluster).

# There is a maximum runtime of 15 seconds, after which aa-proxy-rs exits. Log files are named
# stored-log-<normal,cluster>-<date>.log for aa-proxy-rs and stored-dhu-log-<normal,cluster>-<date>.log for dhu.

set -e
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
if [ ! -d "/home/joakim/dhu" ]; then
    echo "Error: The desktop-head-unit directory was not found at /home/joakim/dhu."
    exit 1
fi
dhu_args=()
run_cluster=$( [ -f rerun ] && [ "$(cat rerun)" == "cluster" ] && echo "true" || echo "false" )
if [ "$run_cluster" == "true" ]; then
    dhu_args+=("--config" "config/all_options_test.ini")
    log_filename="cluster"
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

max_runtime=10
echo "Starting aa-proxy-rs in the foreground. Session timeout is ${max_runtime} seconds. $(date)"
target/x86_64-unknown-linux-gnu/debug/aa-proxy-rs --session-timeout ${max_runtime} &
pushd /home/joakim/dhu
./desktop-head-unit "${dhu_args[@]}" > desktop-head-unit.log 2>&1
popd
echo "desktop-head-unit has exited. Rotating log files."
rotate_to="stored-log-${log_filename}-$(date --iso-8601 seconds).log"
cp -v aa-proxy-rs.log "${rotate_to}" && echo "" > aa-proxy-rs.log
cp -v /home/joakim/dhu/desktop-head-unit.log "stored-dhu-log-${log_filename}-$(date --iso-8601 seconds).log"