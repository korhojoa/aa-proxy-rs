---
name: run-test-harness
description: 'Run and debug the aa-proxy-rs test harness via ./run-test.sh cluster|injected|default. Use when reproducing cluster startup failures, watcher-driven rerun issues, or verifying whether desktop-head-unit and remote aa-proxy-rs actually started. Do not run build-and-run-with-dhu.sh directly from the agent in this repo; use run-test.sh instead because the agent environment lacks the graphical/session access needed for the direct launcher.'
argument-hint: 'Mode to run: cluster, injected, or default'
---

# Run Test Harness

Use this skill when the task is to run or debug the repository's integration harness through `./run-test.sh`.

## Rules

- Use `./run-test.sh <mode>` as the entry point.
- Do not run `./build-and-run-with-dhu.sh` directly from the agent.
- Treat the launcher as watcher-driven and potentially external to the current terminal session.
- Preserve the user's current `rerun`-driven workflow; do not replace it with a different launch path.
- Do not use `ssh` for post-failure diagnosis unless the remote shell environment and required commands are already known to be available.

## Modes

- `cluster`
- `injected`
- `default`

## Procedure

1. Confirm the current state only as needed.
   - Check `rerun` contents.
   - Check whether a watcher process is present.
   - Check whether `desktop-head-unit` or `aa-proxy-rs` are already running.

2. Run the requested harness exactly.
   - Use `cd /home/joakim/aa-proxy-rs && ./run-test.sh <mode>`.

3. Interpret the result from `run-test.sh` before proposing causes.
   - If output says `desktop-head-unit never started` and `aa-proxy-rs never became ready`, then nothing was launched during the observation window.
   - If only old stored log files are listed afterward, no fresh session was produced.
   - If `rerun` changed but no new processes or logs appeared, the watcher path likely did not complete a launch.

4. Debug the watcher-driven failure without bypassing it.
   - Inspect the watcher process state.
   - Compare `rerun` modification time against current time.
   - Check whether the watcher process restarted or changed PID.
   - Inspect the newest DHU/proxy logs and determine whether they are fresh or stale.

## Post-Failure Commands

Use these commands after `./run-test.sh <mode>` finishes. Prefer the smallest set needed.

### 1. Check mode, freshness, and processes

```bash
cd /home/joakim/aa-proxy-rs && \
printf 'now: '; date --iso-8601=seconds && \
printf 'rerun: '; cat rerun && \
printf '\nrerun mtime: '; stat -c '%y' rerun && \
printf '\nprocesses:\n' && \
pgrep -af 'runscriptloop|watch -n 1 -g ls --time-style full-iso -l rerun|build-and-run-with-dhu.sh|desktop-head-unit|aa-proxy-rs' || true
```

Interpretation:
- `rerun` changed recently but there are no fresh launcher, DHU, or proxy processes: the watcher likely noticed the change but did not complete a launch.
- A different `watch` PID after rerun changed can indicate the surrounding loop advanced to the next iteration.

### 2. Check whether fresh logs were produced

```bash
cd /home/joakim/aa-proxy-rs && \
ls -1t stored-log-* stored-dhu-log-* 2>/dev/null | head -n 10
```

Interpretation:
- If only old logs are listed, no fresh session completed.
- If timestamps are stale relative to `rerun mtime`, the harness failure happened before log rotation.

### 3. Inspect latest rotated logs when they exist

```bash
cd /home/joakim/aa-proxy-rs && \
latest_proxy=$(ls -1t stored-log-* 2>/dev/null | head -n 1) && \
latest_dhu=$(ls -1t stored-dhu-log-* 2>/dev/null | head -n 1) && \
printf 'proxy log: %s\n' "$latest_proxy" && tail -n 80 "$latest_proxy" && \
printf '\ndhu log: %s\n' "$latest_dhu" && tail -n 80 "$latest_dhu"
```

Interpretation:
- Fresh proxy log but no fresh DHU log: proxy launched farther than DHU.
- Fresh DHU log with immediate disconnect: launch happened and failure moved beyond the watcher/no-launch stage.

### 4. Optional remote readiness check only when the remote shell is known-good

Only use this if you already know the remote host has the expected commands and shell behavior. Do not use it as a default diagnostic step.

Example:

```bash
ssh 192.168.1.97 "command -v pgrep >/dev/null 2>&1 && pgrep -af aa-proxy-rs || true"
```

Interpretation:
- Treat missing remote tooling as an invalid diagnostic path, not as evidence that the proxy failed to start.

## Failure Classification Heuristics

- `Harness timeout with no launch`
   - `run-test.sh` reports both DHU and proxy never started.
   - No fresh rotated logs appear.

- `Watcher reacted but launcher did not produce DHU/proxy`
   - `rerun` timestamp updates.
   - Watcher PID may change.
   - No fresh DHU/proxy processes or rotated logs appear afterward.

- `DHU started but proxy never became ready`
   - Fresh DHU output exists.
   - Optional remote checks, if valid in that environment, do not show readiness.

- `Both started and later failed at protocol/runtime stage`
   - Fresh rotated logs exist for both sides.
   - `run-test.sh` observed startup before later failure or timeout.

5. Report the exact failure class.
   - `Harness timeout with no launch`
   - `Watcher reacted but launcher did not produce DHU/proxy`
   - `DHU started but proxy never became ready`
   - `Both started and later failed at protocol/runtime stage`

## Notes For This Repo

- `run-test.sh` is the reliable agent entry point for this workflow.
- The direct launcher can fail in the agent environment for reasons unrelated to the user-visible harness failure.
- For this repository, incorrect use of the direct launcher can create misleading conclusions, so avoid it.