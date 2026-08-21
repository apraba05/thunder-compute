#!/usr/bin/env python3
"""Fire concurrent simulated GPU workloads at vgpu-scheduler.

Usage:
  python submit_jobs.py [--url http://127.0.0.1:8080] [--jobs 12]
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import random
import sys
import urllib.error
import urllib.request


def post(url: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        url.rstrip("/") + path,
        data=data,
        headers={"Content-Type": "application/json"} if data else {},
        method="POST" if data is not None else "GET",
    )
    if path == "/metrics":
        req = urllib.request.Request(url.rstrip("/") + path, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        try:
            payload = json.loads(e.read().decode())
        except Exception:
            payload = {"error": str(e)}
        return e.code, payload


def get_metrics(url: str) -> dict:
    req = urllib.request.Request(url.rstrip("/") + "/metrics", method="GET")
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read().decode())


def allocate_one(url: str, job_id: str, compute: float, mem: float) -> dict:
    code, body = post(url, "/allocate", {"job_id": job_id, "compute": compute, "mem": mem})
    body["_http"] = code
    body["_job_id"] = job_id
    body["_ask"] = {"compute": compute, "mem": mem}
    return body


def run_wave(url: str, jobs: list[tuple[str, float, float]], label: str) -> dict:
    print(f"\n=== {label}: submitting {len(jobs)} concurrent jobs ===")
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(jobs)) as pool:
        futs = [
            pool.submit(allocate_one, url, jid, c, m) for jid, c, m in jobs
        ]
        results = [f.result() for f in concurrent.futures.as_completed(futs)]

    ok = [r for r in results if r.get("_http") == 200]
    bad = [r for r in results if r.get("_http") != 200]
    for r in sorted(ok, key=lambda x: x.get("_job_id", "")):
        print(
            f"  OK  {r['_job_id']:8} ask={r['_ask']['compute']:.0f}/{r['_ask']['mem']:.0f}"
            f"  → slot {r.get('slot')}"
        )
    for r in sorted(bad, key=lambda x: x.get("_job_id", "")):
        print(f"  REJ {r['_job_id']:8} ask={r['_ask']['compute']:.0f}/{r['_ask']['mem']:.0f}"
              f"  → {r.get('error', r)}")

    metrics = get_metrics(url)
    util = metrics["pool_utilization_pct"]
    print(f"\n--- /metrics ({metrics['mode']}) ---")
    print(f"pool utilization: {util}%  "
          f"(compute {metrics['pool_compute_pct']}% / mem {metrics['pool_mem_pct']}%)")
    print(f"accepted={metrics['accepted']}  rejected={metrics['rejected']}")
    for s in metrics["per_slot"]:
        lock = " [exclusive]" if s["exclusive_lock"] else ""
        bar_c = int(s["compute_pct"] // 5)
        print(
            f"  slot {s['slot']}: compute {s['compute_pct']:5.1f}%  "
            f"mem {s['mem_pct']:5.1f}%  jobs={s['jobs']}{lock}  "
            f"|{'█' * bar_c}{'·' * (20 - bar_c)}|"
        )
    return metrics


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:8080")
    ap.add_argument("--jobs", type=int, default=12)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    # Fragmented asks: each job wants a slice of a GPU, not a whole one.
    # The 26–32 range means any three jobs fit per slot. That keeps the result
    # reproducible even though concurrent requests can arrive in any order.
    jobs = []
    for i in range(args.jobs):
        c = rng.uniform(26, 32)
        m = rng.uniform(26, 32)
        jobs.append((f"job-{i+1:02d}", round(c, 1), round(m, 1)))

    # Baseline: naive 1:1
    post(args.url, "/mode", {"mode": "naive"})
    post(args.url, "/reset", {})
    naive = run_wave(args.url, jobs, "NAIVE (1 job = 1 GPU)")

    # Multiplexed: bin-pack into residual capacity
    post(args.url, "/mode", {"mode": "pack"})
    post(args.url, "/reset", {})
    packed = run_wave(args.url, jobs, "PACK (bin-pack onto shared pool)")

    print("\n========== BEFORE → AFTER ==========")
    print(f"  naive utilization : {naive['pool_utilization_pct']}%")
    print(f"  pack  utilization : {packed['pool_utilization_pct']}%")
    print(f"  accepted naive/pack: {naive['accepted']}/{packed['accepted']} of {args.jobs}")
    print("====================================")
    return 0


if __name__ == "__main__":
    sys.exit(main())
