#!/usr/bin/env python
import argparse
import json
import statistics
import subprocess
import sys
import time
from datetime import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import requests

try:
    from rich.console import Console
except Exception:
    Console = None


@dataclass
class JobMetrics:
    thread_id: str
    start_ts: float
    first_running_ts: Optional[float] = None
    done_ts: Optional[float] = None
    status: str = "queued"
    timings: Dict[str, float] = field(default_factory=dict)

    def total_time(self) -> Optional[float]:
        if self.done_ts is None:
            return None
        return self.done_ts - self.start_ts

    def queue_time(self) -> Optional[float]:
        if self.first_running_ts is None:
            return None
        return self.first_running_ts - self.start_ts

    def run_time(self) -> Optional[float]:
        if self.first_running_ts is None or self.done_ts is None:
            return None
        return self.done_ts - self.first_running_ts


@dataclass
class ProcSample:
    ts: float
    cpu: Optional[float] = None
    mem: Optional[float] = None
    rss_mb: Optional[float] = None
    llm_procs: Optional[int] = None
    gemini_procs: Optional[int] = None


@dataclass
class RunResult:
    concurrency: int
    jobs: List[JobMetrics]
    wall_time_s: float
    proc_samples: List[ProcSample] = field(default_factory=list)


def request_json(
    method: str,
    url: str,
    *,
    timeout_s: float,
    retries: int,
    params: Optional[Dict[str, str]] = None,
) -> Dict[str, object]:
    attempt = 0
    while True:
        try:
            resp = requests.request(method, url, params=params, timeout=timeout_s)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict):
                return data
            return {}
        except Exception:
            if attempt >= retries:
                raise
            attempt += 1
            time.sleep(0.2 * attempt)


def fetch_thread_ids(base_url: str, limit: int, *, timeout_s: float, retries: int) -> List[str]:
    data = request_json(
        "GET",
        f"{base_url}/queue",
        params={"status": "unreviewed", "limit": str(limit), "offset": "0"},
        timeout_s=timeout_s,
        retries=retries,
    )
    return [row["thread_id"] for row in data.get("items", [])]


def post_judge(
    base_url: str,
    thread_id: str,
    *,
    dry_run: bool,
    timeout_s: float,
    retries: int,
) -> None:
    params = {"dry_run": "1"} if dry_run else None
    request_json(
        "POST",
        f"{base_url}/judge/{thread_id}",
        params=params,
        timeout_s=timeout_s,
        retries=retries,
    )


def get_status(
    base_url: str,
    thread_id: str,
    *,
    timeout_s: float,
    retries: int,
) -> Dict[str, str]:
    data = request_json(
        "GET",
        f"{base_url}/judge/status/{thread_id}",
        timeout_s=timeout_s,
        retries=retries,
    )
    return data  # type: ignore[return-value]


def get_metrics(
    base_url: str,
    thread_id: str,
    *,
    timeout_s: float,
    retries: int,
) -> Dict[str, float]:
    data = request_json(
        "GET",
        f"{base_url}/judge/metrics/{thread_id}",
        timeout_s=timeout_s,
        retries=retries,
    )
    if not isinstance(data, dict):
        return {}
    return data


def get_proc_sample(pid: Optional[int]) -> ProcSample:
    sample = ProcSample(ts=time.time())
    if pid is not None:
        try:
            out = subprocess.check_output(
                ["ps", "-o", "%cpu=,%mem=,rss=", "-p", str(pid)],
                text=True,
            ).strip()
        except Exception:
            out = ""
        if out:
            try:
                cpu_s, mem_s, rss_s = out.split()
                sample.cpu = float(cpu_s)
                sample.mem = float(mem_s)
                sample.rss_mb = float(rss_s) / 1024.0
            except Exception:
                pass
    try:
        sample.llm_procs = count_procs("llm_judge.py")
        sample.gemini_procs = count_procs("gemini_run.sh")
    except Exception:
        pass
    return sample


def find_server_pid(pattern: str) -> Optional[int]:
    try:
        out = subprocess.check_output(["pgrep", "-f", pattern], text=True).strip()
    except Exception:
        return None
    if not out:
        return None
    try:
        return int(out.splitlines()[0])
    except Exception:
        return None


def count_procs(pattern: str) -> int:
    try:
        out = subprocess.check_output(["pgrep", "-fc", pattern], text=True).strip()
    except Exception:
        return 0
    try:
        return int(out)
    except Exception:
        return 0


def run_level(
    base_url: str,
    thread_ids: List[str],
    poll_interval: float,
    pid: Optional[int],
    metrics_interval: float,
    dry_run: bool,
    timeout_s: float,
    retries: int,
    max_poll_errors: int,
) -> RunResult:
    jobs = [JobMetrics(thread_id=t, start_ts=time.time()) for t in thread_ids]
    for job in jobs:
        post_judge(base_url, job.thread_id, dry_run=dry_run, timeout_s=timeout_s, retries=retries)
    start = time.time()
    last_metrics = 0.0
    samples: List[ProcSample] = []

    remaining = {job.thread_id: job for job in jobs}
    poll_errors = 0
    while remaining:
        now = time.time()
        if pid is not None and (now - last_metrics) >= metrics_interval:
            samples.append(get_proc_sample(pid))
            last_metrics = now
        for thread_id in list(remaining.keys()):
            try:
                status = get_status(
                    base_url,
                    thread_id,
                    timeout_s=timeout_s,
                    retries=retries,
                )
            except Exception:
                poll_errors += 1
                if poll_errors >= max_poll_errors:
                    raise RuntimeError(
                        f"Too many polling errors ({poll_errors}). "
                        "Increase --request-timeout or check backend."
                    )
                continue
            st = status.get("status", "unknown")
            job = remaining[thread_id]
            job.status = st
            if st == "running" and job.first_running_ts is None:
                job.first_running_ts = time.time()
            if st in ("done", "error", "cancelled"):
                job.done_ts = time.time()
                try:
                    job.timings = get_metrics(
                        base_url,
                        thread_id,
                        timeout_s=timeout_s,
                        retries=retries,
                    )
                except Exception:
                    job.timings = {}
                remaining.pop(thread_id, None)
        if remaining:
            time.sleep(poll_interval)
    wall = time.time() - start
    if pid is not None:
        samples.append(get_proc_sample(pid))
    return RunResult(concurrency=len(thread_ids), jobs=jobs, wall_time_s=wall, proc_samples=samples)


def summarize_result(result: RunResult) -> Dict[str, float]:
    totals = [j.total_time() for j in result.jobs if j.total_time() is not None]
    run_times = [j.run_time() for j in result.jobs if j.run_time() is not None]
    queue_times = [j.queue_time() for j in result.jobs if j.queue_time() is not None]
    errors = sum(1 for j in result.jobs if j.status == "error")
    cancels = sum(1 for j in result.jobs if j.status == "cancelled")
    done = sum(1 for j in result.jobs if j.status == "done")

    def pct(values: List[float], p: float) -> float:
        if not values:
            return 0.0
        values = sorted(values)
        idx = int(round((p / 100.0) * (len(values) - 1)))
        return values[idx]

    cpu_vals = [s.cpu for s in result.proc_samples if s.cpu is not None]
    mem_vals = [s.mem for s in result.proc_samples if s.mem is not None]
    rss_vals = [s.rss_mb for s in result.proc_samples if s.rss_mb is not None]
    llm_proc_vals = [s.llm_procs for s in result.proc_samples if s.llm_procs is not None]
    gemini_proc_vals = [s.gemini_procs for s in result.proc_samples if s.gemini_procs is not None]

    llm_s = [j.timings.get("llm_s") for j in result.jobs if j.timings.get("llm_s") is not None]
    load_s = [j.timings.get("load_s") for j in result.jobs if j.timings.get("load_s") is not None]
    prompt_s = [j.timings.get("prompt_s") for j in result.jobs if j.timings.get("prompt_s") is not None]
    parse_s = [j.timings.get("parse_s") for j in result.jobs if j.timings.get("parse_s") is not None]
    total_s = [j.timings.get("total_s") for j in result.jobs if j.timings.get("total_s") is not None]
    process_s = [j.timings.get("process_s") for j in result.jobs if j.timings.get("process_s") is not None]
    spawn_s = [j.timings.get("spawn_s") for j in result.jobs if j.timings.get("spawn_s") is not None]

    return {
        "concurrency": result.concurrency,
        "jobs": len(result.jobs),
        "done": done,
        "errors": errors,
        "cancelled": cancels,
        "wall_time_s": result.wall_time_s,
        "throughput_jps": (len(result.jobs) / result.wall_time_s) if result.wall_time_s > 0 else 0.0,
        "mean_s": statistics.mean(totals) if totals else 0.0,
        "p50_s": pct(totals, 50),
        "p90_s": pct(totals, 90),
        "p95_s": pct(totals, 95),
        "mean_run_s": statistics.mean(run_times) if run_times else 0.0,
        "mean_queue_s": statistics.mean(queue_times) if queue_times else 0.0,
        "mean_llm_s": statistics.mean(llm_s) if llm_s else 0.0,
        "mean_load_s": statistics.mean(load_s) if load_s else 0.0,
        "mean_prompt_s": statistics.mean(prompt_s) if prompt_s else 0.0,
        "mean_parse_s": statistics.mean(parse_s) if parse_s else 0.0,
        "mean_llm_total_s": statistics.mean(total_s) if total_s else 0.0,
        "mean_process_s": statistics.mean(process_s) if process_s else 0.0,
        "mean_spawn_s": statistics.mean(spawn_s) if spawn_s else 0.0,
        "avg_cpu": statistics.mean(cpu_vals) if cpu_vals else 0.0,
        "max_cpu": max(cpu_vals) if cpu_vals else 0.0,
        "avg_mem": statistics.mean(mem_vals) if mem_vals else 0.0,
        "max_mem": max(mem_vals) if mem_vals else 0.0,
        "avg_rss_mb": statistics.mean(rss_vals) if rss_vals else 0.0,
        "max_rss_mb": max(rss_vals) if rss_vals else 0.0,
        "avg_llm_procs": statistics.mean(llm_proc_vals) if llm_proc_vals else 0.0,
        "max_llm_procs": max(llm_proc_vals) if llm_proc_vals else 0.0,
        "avg_gemini_procs": statistics.mean(gemini_proc_vals) if gemini_proc_vals else 0.0,
        "max_gemini_procs": max(gemini_proc_vals) if gemini_proc_vals else 0.0,
    }


def write_csv(rows: List[Dict[str, float]], path: str) -> None:
    if not rows:
        return
    keys = list(rows[0].keys())
    with open(path, "w", encoding="utf-8") as f:
        f.write(",".join(keys) + "\n")
        for row in rows:
            f.write(",".join(str(row[k]) for k in keys) + "\n")


def write_ascii_plot(rows: List[Dict[str, float]], path: str) -> None:
    if not rows:
        return
    max_tp = max(r.get("throughput_jps", 0.0) for r in rows) or 1.0
    lines = ["Throughput (jobs/s)"]
    for row in rows:
        tp = row.get("throughput_jps", 0.0)
        bar_len = int((tp / max_tp) * 40)
        bar = "#" * bar_len
        lines.append(f"{int(row['concurrency']):>2} | {bar} {tp:.2f}")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def write_png_plot(rows: List[Dict[str, float]], path: str) -> None:
    try:
        import matplotlib.pyplot as plt
    except Exception:
        print("matplotlib not available; skipping PNG plot.")
        return
    if not rows:
        return
    conc = [int(r["concurrency"]) for r in rows]
    tp = [r.get("throughput_jps", 0.0) for r in rows]
    p95 = [r.get("p95_s", 0.0) for r in rows]
    avg_cpu = [r.get("avg_cpu", 0.0) for r in rows]

    fig, ax1 = plt.subplots(figsize=(8, 4.5), dpi=140)
    ax1.plot(conc, tp, marker="o", label="throughput (jobs/s)")
    ax1.set_xlabel("concurrency")
    ax1.set_ylabel("throughput (jobs/s)")
    ax1.set_xticks(conc)

    ax2 = ax1.twinx()
    ax2.plot(conc, p95, marker="s", color="#d9534f", label="p95 latency (s)")
    ax2.plot(conc, avg_cpu, marker="^", color="#5cb85c", label="avg CPU (%)")
    ax2.set_ylabel("p95 latency (s) / avg CPU (%)")

    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines + lines2, labels + labels2, loc="upper left")

    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def write_jobs_jsonl(results: List[RunResult], path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for result in results:
            for job in result.jobs:
                payload = {
                    "concurrency": result.concurrency,
                    "thread_id": job.thread_id,
                    "status": job.status,
                    "queue_s": job.queue_time(),
                    "run_s": job.run_time(),
                    "total_s": job.total_time(),
                }
                for key in ("load_s", "prompt_s", "llm_s", "parse_s", "total_s", "process_s", "spawn_s"):
                    if job.timings.get(key) is not None:
                        payload[f"llm_{key}"] = job.timings.get(key)
                f.write(json.dumps(payload) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--concurrency", default="1,2,4,8,16")
    parser.add_argument("--poll-interval", type=float, default=0.5)
    parser.add_argument("--metrics-interval", type=float, default=1.0)
    parser.add_argument("--request-timeout", type=float, default=90.0)
    parser.add_argument("--request-retries", type=int, default=2)
    parser.add_argument("--max-poll-errors", type=int, default=50)
    parser.add_argument("--pid", type=int, default=None)
    parser.add_argument("--pgrep", default="uvicorn.*backend.app")
    parser.add_argument("--threads", type=int, default=64)
    parser.add_argument("--out", default="bench_results")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    console = Console() if Console else None

    def log(message: str) -> None:
        if console:
            console.print(message)
        else:
            print(message)

    pid = args.pid if args.pid else find_server_pid(args.pgrep)
    if pid is None:
        log("Warning: server PID not found; CPU/mem metrics will be empty.")

    levels = [int(x.strip()) for x in args.concurrency.split(",") if x.strip()]
    needed = sum(levels)
    thread_ids = fetch_thread_ids(
        args.base_url,
        max(args.threads, needed),
        timeout_s=args.request_timeout,
        retries=args.request_retries,
    )
    if len(thread_ids) < needed:
        log(f"Need {needed} thread_ids, only got {len(thread_ids)}. Increase --threads.")
        return 1

    cursor = 0
    results: List[RunResult] = []
    bench_start = time.time()
    for level in levels:
        batch = thread_ids[cursor : cursor + level]
        cursor += level
        level_start = time.time()
        start_ts = datetime.now().strftime("%H:%M:%S")
        log(f"[{start_ts}] Running concurrency={level} (jobs={len(batch)})...")
        result = run_level(
            args.base_url,
            batch,
            poll_interval=args.poll_interval,
            pid=pid,
            metrics_interval=args.metrics_interval,
            dry_run=args.dry_run,
            timeout_s=args.request_timeout,
            retries=args.request_retries,
            max_poll_errors=args.max_poll_errors,
        )
        results.append(result)
        summary = summarize_result(result)
        level_elapsed = time.time() - level_start
        bench_elapsed = time.time() - bench_start
        log(
            f"Done c={level} in {level_elapsed:.2f}s "
            f"(p95={summary['p95_s']:.2f}s tp={summary['throughput_jps']:.2f}/s "
            f"avg_cpu={summary['avg_cpu']:.1f}% avg_rss={summary['avg_rss_mb']:.1f}MB) "
            f"total_elapsed={bench_elapsed:.2f}s"
        )

    summary = [summarize_result(r) for r in results]
    write_csv(summary, f"{args.out}.csv")
    write_ascii_plot(summary, f"{args.out}.txt")
    write_png_plot(summary, f"{args.out}.png")
    write_jobs_jsonl(results, f"{args.out}_jobs.jsonl")
    with open(f"{args.out}.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    log("\nResults:")
    for row in summary:
        log(
            f"c={row['concurrency']:>2} jobs={row['jobs']} "
            f"wall={row['wall_time_s']:.2f}s tp={row['throughput_jps']:.2f}/s "
            f"p50={row['p50_s']:.2f}s p95={row['p95_s']:.2f}s "
            f"avg_cpu={row['avg_cpu']:.1f}% avg_rss={row['avg_rss_mb']:.1f}MB"
        )
    log(
        f"\nWrote: {args.out}.csv, {args.out}.json, {args.out}.txt, "
        f"{args.out}.png, {args.out}_jobs.jsonl"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
