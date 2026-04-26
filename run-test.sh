#!/bin/bash
if [ "$1" == "--help" ]; then
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  cluster   Run with cluster configuration"
    echo "  injected  Run with injected display types"
    echo "  default   Run with default configuration (no cluster, no injected display types)"
    echo "Environment:"
    echo "  AA_PROXY_HOST         Remote host for aa-proxy-rs readiness checks"
    echo "  AA_PROXY_READY_PORTS  Space-separated remote ports to wait for (default: '5288 5277')"
    echo "  AA_PROXY_SSH_TARGET   Optional SSH target for remote diagnostics (defaults to AA_PROXY_HOST)"
    echo "  POLL_INTERVAL_SECS    Poll interval in seconds (default: 1)"
    exit 0
fi
if [ -f rerun ] && [ "$1" == "cluster" ]; then
    echo "cluster" > rerun
elif [ -f rerun ] && [ "$1" == "injected" ]; then
    echo "injected" > rerun
elif [ -f rerun ]; then
    echo "default" > rerun
fi

# Poll for exiting of desktop-head-unit and aa-proxy-rs.
# First detect when both have started, then start the exit timeout.
# Fail if both do not start within 30 seconds (startup timeout).
# Fail only if both do not exit within 20 seconds after both have started.
startup_timeout_secs=30
timeout_secs=20
poll_interval_secs="${POLL_INTERVAL_SECS:-1}"
AA_PROXY_HOST="${AA_PROXY_HOST:-192.168.1.97}"
proxy_host="${AA_PROXY_HOST:-}"
proxy_ready_ports="${AA_PROXY_READY_PORTS:-5288 5277}"
proxy_ssh_target="${AA_PROXY_SSH_TARGET:-$proxy_host}"

now_ms() {
    date +%s%3N
}

format_ms() {
    local ms="$1"
    local seconds=$(( ms / 1000 ))
    local millis=$(( ms % 1000 ))
    printf "%d.%03ds" "$seconds" "$millis"
}

start_ms=$(now_ms)
startup_deadline_ms=$(( start_ms + startup_timeout_secs * 1000 ))
deadline_ms=0

dhu_pattern='(^|/)desktop-head-unit([[:space:]]|$)'
proxy_pattern='(^|/)aa-proxy-rs([[:space:]]|$)'

probe_tcp_port() {
    local host="$1"
    local port="$2"

    if command -v nc >/dev/null 2>&1; then
        nc -z -w 1 "$host" "$port" >/dev/null 2>&1
        return $?
    fi

    timeout 1 bash -c "exec 3<>/dev/tcp/$host/$port" >/dev/null 2>&1
}

proxy_ready() {
    local port

    if [ -z "$proxy_host" ]; then
        pgrep -f "$proxy_pattern" >/dev/null
        return $?
    fi

    for port in $proxy_ready_ports; do
        if ! probe_tcp_port "$proxy_host" "$port"; then
            return 1
        fi
    done

    return 0
}

print_proxy_failure_diagnostics() {
    local ports_regex

    if [ -z "$proxy_host" ] || [ -z "$proxy_ssh_target" ]; then
        return 0
    fi

    ports_regex=$(printf '%s|' $proxy_ready_ports)
    ports_regex="${ports_regex%|}"

    echo "Remote aa-proxy-rs diagnostics from $proxy_ssh_target:"
    ssh "$proxy_ssh_target" "pgrep -af aa-proxy-rs || true; echo; ss -tln | grep -E ':(($ports_regex))([[:space:]]|$)' || true" 2>/dev/null || \
        echo "ssh diagnostics failed for $proxy_ssh_target"
}

dhu_started=0
proxy_started=0

dhu_start_ms=0
dhu_end_ms=0
proxy_start_ms=0
proxy_end_ms=0
timeout_start_ms=0

while true; do
    now=$(now_ms)
    if [ "$now" -gt "$startup_deadline_ms" ] && ([ "$dhu_started" -eq 0 ] || [ "$proxy_started" -eq 0 ]); then
        echo "Startup timeout: not all processes started within ${startup_timeout_secs}s"
        break
    fi
    if [ "$deadline_ms" -ne 0 ] && [ "$now" -gt "$deadline_ms" ]; then
        break
    fi

    dhu_running=0
    proxy_running=0

    if pgrep -f "$dhu_pattern" >/dev/null; then
        dhu_running=1
        if [ "$dhu_started" -eq 0 ]; then
            dhu_started=1
            dhu_start_ms="$now"
            echo "desktop-head-unit started at +$(format_ms $(( dhu_start_ms - start_ms )))"
        fi
    elif [ "$dhu_started" -eq 1 ] && [ "$dhu_end_ms" -eq 0 ]; then
        dhu_end_ms="$now"
    fi

    if proxy_ready; then
        proxy_running=1
        if [ "$proxy_started" -eq 0 ]; then
            proxy_started=1
            proxy_start_ms="$now"
            if [ -n "$proxy_host" ]; then
                echo "aa-proxy-rs ready on $proxy_host ports [$proxy_ready_ports] at +$(format_ms $(( proxy_start_ms - start_ms )))"
            else
                echo "aa-proxy-rs started at +$(format_ms $(( proxy_start_ms - start_ms )))"
            fi
        fi
    elif [ "$proxy_started" -eq 1 ] && [ "$proxy_end_ms" -eq 0 ]; then
        proxy_end_ms="$now"
    fi

    if [ "$dhu_started" -eq 1 ] && [ "$proxy_started" -eq 1 ]; then
        if [ "$timeout_start_ms" -eq 0 ]; then
            timeout_start_ms="$now"
            deadline_ms=$(( timeout_start_ms + timeout_secs * 1000 ))
            echo "Exit timeout started at +$(format_ms $(( timeout_start_ms - start_ms )))"
        fi

        if [ "$dhu_running" -eq 0 ] && [ "$proxy_running" -eq 0 ]; then
            echo "Both processes started and exited."
            echo "desktop-head-unit runtime: $(format_ms $(( dhu_end_ms - dhu_start_ms )))"
            echo "aa-proxy-rs runtime: $(format_ms $(( proxy_end_ms - proxy_start_ms )))"
            files=$(/home/joakim/.cargo/bin/eza --modified stored* --sort time -r | head -n2)
            echo "Most recent log files:"
            echo "$files"
            for file in $files; do
                if [[ "$file" == *"dhu"* ]]; then
                    :
                else
                    echo "MD media tap DATA from $file:" "$(grep -c 'MD media tap DATA' "$file")"
                fi
            done
            echo "cluster tap file size: $(stat -c%s /tmp/cluster_tap.bin || echo "not found") bytes" "$(ffmpeg -hide_banner -loglevel info -i /tmp/cluster_tap.bin -f h264 /dev/null -y 2>&1 |grep Duration || echo "ffmpeg failed to parse cluster tap")"
            
            echo "main tap file size: $(stat -c%s /tmp/main_tap.bin || echo "not found") bytes" "$(ffmpeg -hide_banner -loglevel info -i /tmp/main_tap.bin -f h264 /dev/null -y 2>&1 |grep Duration || echo "ffmpeg failed to parse main tap")"
            exit 0
        fi
    fi

    sleep "$poll_interval_secs"
done

if [ "$dhu_started" -eq 0 ]; then
    echo "desktop-head-unit never started"
else
    echo "desktop-head-unit did start"
fi

if [ "$proxy_started" -eq 0 ]; then
    if [ -n "$proxy_host" ]; then
        echo "aa-proxy-rs never became ready on $proxy_host ports [$proxy_ready_ports]"
        print_proxy_failure_diagnostics
    else
        echo "aa-proxy-rs never started"
    fi
else
    echo "aa-proxy-rs did start"
fi

if [ "$timeout_start_ms" -eq 0 ]; then
    echo "Exit timeout never started because both processes were not observed running at the same time"
elif pgrep -f "$dhu_pattern" >/dev/null; then
    echo "desktop-head-unit is still running after ${timeout_secs}s"
fi

if [ "$timeout_start_ms" -ne 0 ] && proxy_ready; then
    echo "aa-proxy-rs is still running after ${timeout_secs}s"
fi

now=$(now_ms)

if [ "$dhu_started" -eq 1 ]; then
    if pgrep -f "$dhu_pattern" >/dev/null; then
        echo "desktop-head-unit runtime so far: $(format_ms $(( now - dhu_start_ms )))"
    else
        if [ "$dhu_end_ms" -eq 0 ]; then
            dhu_end_ms="$now"
        fi
        echo "desktop-head-unit runtime: $(format_ms $(( dhu_end_ms - dhu_start_ms )))"
    fi
fi

if [ "$proxy_started" -eq 1 ]; then
    if proxy_ready; then
        echo "aa-proxy-rs runtime so far: $(format_ms $(( now - proxy_start_ms )))"
    else
        if [ "$proxy_end_ms" -eq 0 ]; then
            proxy_end_ms="$now"
        fi
        echo "aa-proxy-rs runtime: $(format_ms $(( proxy_end_ms - proxy_start_ms )))"
    fi
fi

/home/joakim/.cargo/bin/eza --modified stored* --sort time -r | head -n2
exit 1