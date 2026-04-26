#!/usr/bin/env python3
import argparse
import datetime as dt
import math
import re
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Dict, List, Optional, Tuple

TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}),\s(\d{2}:\d{2}:\d{2}\.\d{3})")
ANSI_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")

EVENT_PATTERNS = {
    "VERSION_REQUEST": re.compile(r"MESSAGE_VERSION_REQUEST"),
    "VERSION_RESPONSE": re.compile(r"MESSAGE_VERSION_RESPONSE"),
    "SSL_STAGE": re.compile(r"SSL handshake:"),
    "SSL_COMPLETE": re.compile(r"SSL init complete"),
    "SDR_INPUT": re.compile(r"SDR input:"),
    "SDR_OUTPUT": re.compile(r"SDR output:"),
    "SDU": re.compile(r"SDU:|MESSAGE_SERVICE_DISCOVERY_UPDATE"),
    "CHANNEL_OPEN_REQUEST": re.compile(r"ChannelOpenRequest:|MESSAGE_CHANNEL_OPEN_REQUEST"),
    "CHANNEL_OPEN_RESPONSE": re.compile(r"MESSAGE_CHANNEL_OPEN_RESPONSE|synthesized CHANNEL_OPEN_RESPONSE"),
    "PING_REQUEST": re.compile(r"PING_REQUEST:|MESSAGE_PING_REQUEST"),
    "PING_RESPONSE": re.compile(r"PING_RESPONSE:|MESSAGE_PING_RESPONSE"),
    "RUNTIME_TIMEOUT": re.compile(r"runtime transmit timeout"),
}

SUBMIT_RE = re.compile(r"Submitted URB\s+(0x[0-9a-fA-F]+)\s+on ep\s+1\b")
COMPLETE_RE = re.compile(
    r"URB\s+(0x[0-9a-fA-F]+)\s+for ep\s+1 completed, status=([\-\d]+)\s+actual_length=(\d+)"
)


@dataclass
class Event:
    ts: dt.datetime
    kind: str
    line_no: int
    line: str


@dataclass
class EpLatency:
    urb: str
    submit_ts: dt.datetime
    complete_ts: dt.datetime
    status: int
    actual_length: int

    @property
    def latency_ms(self) -> float:
        return (self.complete_ts - self.submit_ts).total_seconds() * 1000.0


@dataclass
class ScenarioReport:
    name: str
    path: Path
    start_ts: Optional[dt.datetime]
    events: List[Event]
    ep_latencies: List[EpLatency]


def parse_ts(raw: str) -> Optional[dt.datetime]:
    m = TS_RE.match(raw)
    if not m:
        return None
    return dt.datetime.strptime(f"{m.group(1)} {m.group(2)}", "%Y-%m-%d %H:%M:%S.%f")


def strip_ansi(line: str) -> str:
    return ANSI_RE.sub("", line)


def percentile(sorted_values: List[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    idx = int(math.ceil((p / 100.0) * (len(sorted_values) - 1)))
    return sorted_values[idx]


def first_events_by_kind(events: List[Event]) -> Dict[str, Event]:
    out: Dict[str, Event] = {}
    for e in events:
        if e.kind not in out:
            out[e.kind] = e
    return out


def extract_ordered_signature(events: List[Event]) -> List[str]:
    # Keep the first occurrence of each kind in encounter order.
    seen = set()
    sig = []
    for e in events:
        if e.kind in seen:
            continue
        seen.add(e.kind)
        sig.append(e.kind)
    return sig


def analyze_log(name: str, path: Path, cut_at_first_timeout: bool) -> ScenarioReport:
    events: List[Event] = []
    pending_submit: Dict[str, dt.datetime] = {}
    ep_latencies: List[EpLatency] = []
    start_ts: Optional[dt.datetime] = None

    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line_no, raw in enumerate(f, start=1):
            line = strip_ansi(raw.rstrip("\n"))
            ts = parse_ts(line)
            if ts and start_ts is None:
                start_ts = ts

            if ts:
                submit = SUBMIT_RE.search(line)
                if submit:
                    pending_submit[submit.group(1).lower()] = ts

                complete = COMPLETE_RE.search(line)
                if complete:
                    urb = complete.group(1).lower()
                    submit_ts = pending_submit.pop(urb, None)
                    if submit_ts:
                        ep_latencies.append(
                            EpLatency(
                                urb=urb,
                                submit_ts=submit_ts,
                                complete_ts=ts,
                                status=int(complete.group(2)),
                                actual_length=int(complete.group(3)),
                            )
                        )

            for kind, pattern in EVENT_PATTERNS.items():
                if pattern.search(line):
                    if ts is None:
                        continue
                    events.append(Event(ts=ts, kind=kind, line_no=line_no, line=line))
                    if cut_at_first_timeout and kind == "RUNTIME_TIMEOUT":
                        return ScenarioReport(
                            name=name,
                            path=path,
                            start_ts=start_ts,
                            events=events,
                            ep_latencies=ep_latencies,
                        )
                    break

    return ScenarioReport(
        name=name,
        path=path,
        start_ts=start_ts,
        events=events,
        ep_latencies=ep_latencies,
    )


def fmt_rel_ms(start: Optional[dt.datetime], ts: dt.datetime) -> str:
    if start is None:
        return "n/a"
    delta_ms = (ts - start).total_seconds() * 1000.0
    return f"{delta_ms:.1f}ms"


def latency_stats(latencies: List[EpLatency]) -> Dict[str, float]:
    vals = sorted([l.latency_ms for l in latencies])
    ok_vals = sorted([l.latency_ms for l in latencies if l.status == 0])
    timeout_count = sum(1 for l in latencies if l.status != 0)

    return {
        "count": float(len(vals)),
        "ok_count": float(len(ok_vals)),
        "nonzero_status_count": float(timeout_count),
        "p50_ms": percentile(ok_vals, 50.0),
        "p95_ms": percentile(ok_vals, 95.0),
        "p99_ms": percentile(ok_vals, 99.0),
        "max_ms": max(ok_vals) if ok_vals else 0.0,
        "median_all_ms": median(vals) if vals else 0.0,
    }


def find_first_divergence(reports: List[ScenarioReport]) -> Optional[str]:
    if len(reports) < 2:
        return None

    base = reports[0]
    base_sig = extract_ordered_signature(base.events)
    out_lines = []

    for other in reports[1:]:
        other_sig = extract_ordered_signature(other.events)
        max_len = min(len(base_sig), len(other_sig))
        mismatch_idx = None
        for i in range(max_len):
            if base_sig[i] != other_sig[i]:
                mismatch_idx = i
                break
        if mismatch_idx is None:
            if len(base_sig) != len(other_sig):
                mismatch_idx = max_len

        if mismatch_idx is None:
            out_lines.append(f"- {other.name}: no divergence in ordered first-event signature")
            continue

        base_ev = base_sig[mismatch_idx] if mismatch_idx < len(base_sig) else "<end>"
        other_ev = other_sig[mismatch_idx] if mismatch_idx < len(other_sig) else "<end>"
        out_lines.append(
            f"- {other.name}: divergence index {mismatch_idx} vs {base.name} -> {base_ev} vs {other_ev}"
        )

    return "\n".join(out_lines)


def render_markdown(reports: List[ScenarioReport]) -> str:
    lines: List[str] = []
    lines.append("# Session Analysis Report")
    lines.append("")

    lines.append("## First-Divergence Summary")
    div = find_first_divergence(reports)
    lines.append(div if div else "- Not enough scenarios to compute divergence")
    lines.append("")

    lines.append("## Event Timelines")
    event_order = [
        "VERSION_REQUEST",
        "VERSION_RESPONSE",
        "SSL_STAGE",
        "SSL_COMPLETE",
        "SDR_INPUT",
        "SDR_OUTPUT",
        "SDU",
        "CHANNEL_OPEN_REQUEST",
        "CHANNEL_OPEN_RESPONSE",
        "PING_REQUEST",
        "PING_RESPONSE",
        "RUNTIME_TIMEOUT",
    ]

    for report in reports:
        lines.append(f"### {report.name}")
        lines.append(f"- Log: {report.path}")
        first = first_events_by_kind(report.events)
        for kind in event_order:
            ev = first.get(kind)
            if not ev:
                lines.append(f"- {kind}: not observed")
            else:
                rel = fmt_rel_ms(report.start_ts, ev.ts)
                lines.append(f"- {kind}: {ev.ts.isoformat(sep=' ')} ({rel})")
        lines.append("")

    lines.append("## USB EP 0x01 OUT Latency Summary")
    lines.append("| Scenario | Samples | OK | Non-zero status | p50 ms | p95 ms | p99 ms | max ms |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for report in reports:
        s = latency_stats(report.ep_latencies)
        lines.append(
            f"| {report.name} | {int(s['count'])} | {int(s['ok_count'])} | {int(s['nonzero_status_count'])} | {s['p50_ms']:.2f} | {s['p95_ms']:.2f} | {s['p99_ms']:.2f} | {s['max_ms']:.2f} |"
        )

    lines.append("")
    lines.append("## Notes")
    lines.append("- Non-zero EP 0x01 status counts indicate write completion anomalies that should be correlated with control-channel timeout windows.")
    lines.append("- Timeline events are first occurrence only; use full logs for repeated ping cadence or channel-open churn.")
    return "\n".join(lines)


def parse_scenario_arg(value: str) -> Tuple[str, Path]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("scenario format must be NAME=/path/to/log")
    name, path = value.split("=", 1)
    name = name.strip()
    p = Path(path.strip())
    if not name:
        raise argparse.ArgumentTypeError("scenario name must be non-empty")
    return name, p


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze aa-proxy-rs scenario logs for timeline and EP 0x01 latency stats")
    parser.add_argument(
        "--scenario",
        action="append",
        default=[],
        metavar="NAME=PATH",
        help="Scenario log input (repeat for A/B/C)",
    )
    parser.add_argument(
        "--out",
        default="artifacts/analysis/session_report.md",
        help="Output markdown report path",
    )
    parser.add_argument(
        "--keep-after-timeout",
        action="store_true",
        help="Continue parsing after first runtime timeout (default is to stop at first timeout)",
    )
    args = parser.parse_args()

    if not args.scenario:
        parser.error("provide at least one --scenario NAME=PATH")

    reports: List[ScenarioReport] = []
    for raw in args.scenario:
        name, path = parse_scenario_arg(raw)
        if not path.exists():
            parser.error(f"scenario log does not exist: {path}")
        reports.append(
            analyze_log(
                name,
                path,
                cut_at_first_timeout=not args.keep_after_timeout,
            )
        )

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render_markdown(reports), encoding="utf-8")
    print(f"Wrote report: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
