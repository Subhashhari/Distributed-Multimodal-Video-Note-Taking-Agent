#!/usr/bin/env python3
"""Demo helper for the grader scenarios. Standard library only, so it works with any Python 3.10+.

  python scripts/demo.py health
  python scripts/demo.py submit https://youtu.be/VIDEO [--watch] [--fail-frames 30,90] [--unavailable]
  python scripts/demo.py watch JOB_ID
  python scripts/demo.py events JOB_ID
  python scripts/demo.py faults show | clear | set llm_429=2 notion_429=1 frame_fail_rate=0.5 stage_delay_sec=8
  python scripts/demo.py crash URL           # submit, wait for EXTRACTING_FRAMES, SIGKILL that worker, watch
  python scripts/demo.py burst URL [URL ...] # submit many quickly, watch them drain
  python scripts/demo.py dlq

Environment: GATEWAY (default http://localhost:8080), NOTION_TOKEN (any 20+ chars in stub mode),
ADMIN_TOKEN (read from .env if not set).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GATEWAY = os.environ.get("GATEWAY", "http://localhost:8080").rstrip("/")
TERMINAL = {"COMPLETED", "COMPLETED_WITH_WARNINGS", "FAILED", "DEAD_LETTERED"}


def env_file_value(key: str) -> str:
    try:
        for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
            if line.startswith(f"{key}="):
                return line.split("=", 1)[1].strip()
    except OSError:
        pass
    return ""


ADMIN = os.environ.get("ADMIN_TOKEN") or env_file_value("ADMIN_TOKEN")
NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "ntn_demo_token_for_stub_mode_000000")


def call(method: str, path: str, body: dict | None = None, admin: bool = False, headers: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{GATEWAY}{path}", data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if admin:
        req.add_header("Authorization", f"Bearer {ADMIN}")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"null")
        except json.JSONDecodeError:
            return e.code, None
    except urllib.error.URLError as e:
        sys.exit(f"Can't reach the gateway at {GATEWAY}: {e.reason}")


def submit(url: str, debug: dict | None = None) -> str:
    body = {"youtube_url": url, "notion_token": NOTION_TOKEN}
    if os.environ.get("NOTION_PARENT_ID"):
        body["notion_parent_id"] = os.environ["NOTION_PARENT_ID"]
    if debug:
        body["debug"] = debug
    status, data = call("POST", "/jobs", body, headers={"Idempotency-Key": str(uuid.uuid4())})
    if status != 202:
        sys.exit(f"Submit failed ({status}): {data}")
    print(f"submitted {data['job_id']}  video={data['video_id']}")
    return data["job_id"]


def fmt(j: dict) -> str:
    line = f"{j['status']:<24} {j['progress']:>3}%"
    if j.get("retry"):
        r = j["retry"]
        line += f"  waiting: {r['reason']} {r.get('api') or ''} attempt {r['attempt']}/{r['max_attempts']}"
    if j.get("error"):
        line += f"  error: {j['error'].get('code')}"
    if j.get("warnings"):
        line += f"  warnings: {len(j['warnings'])}"
    return line


def watch(job_id: str, quiet_after: str | None = None) -> dict:
    last = None
    while True:
        _, j = call("GET", f"/jobs/{job_id}")
        line = fmt(j)
        if line != last:
            print(f"  {time.strftime('%H:%M:%S')}  {line}")
            last = line
        if j["status"] in TERMINAL:
            _, res = call("GET", f"/jobs/{job_id}/results")
            print(json.dumps(res, indent=2))
            return j
        if quiet_after and j["status"] == quiet_after:
            return j
        time.sleep(1)


def worker_of(job_id: str) -> str | None:
    _, ev = call("GET", f"/admin/jobs/{job_id}/events", admin=True)
    ids = [e["worker_id"] for e in (ev or {}).get("events", []) if e.get("worker_id")]
    return ids[-1] if ids else None


def cmd_crash(url: str) -> None:
    call("POST", "/admin/faults", {"stage_delay_sec": 6}, admin=True)
    job = submit(url)
    print("waiting for EXTRACTING_FRAMES ...")
    watch(job, quiet_after="EXTRACTING_FRAMES")
    time.sleep(2)
    wid = worker_of(job)
    container = wid.rsplit("-", 1)[0] if wid else None
    print(f"SIGKILL worker {wid} (container {container}) mid-frame-extraction")
    subprocess.run(["docker", "kill", "-s", "KILL", container], check=False, capture_output=True)
    t0 = time.time()
    final = watch(job)
    call("DELETE", "/admin/faults", admin=True)
    print(f"recovered and finished {time.time() - t0:.1f}s after the kill; status={final['status']}")
    for e in call("GET", f"/admin/jobs/{job}/events", admin=True)[1]["events"]:
        print(f"  {e['at'][11:19]} {e['kind']:<16} {e['to'] or '':<20} {e['worker_id'] or ''}")
    print("Restart the killed worker with:  docker compose up -d worker")


def cmd_burst(urls: list[str]) -> None:
    jobs = [submit(u) for u in urls]
    print(f"submitted {len(jobs)} jobs; watching them drain ...")
    pending = set(jobs)
    while pending:
        rows = []
        for j in list(pending):
            _, s = call("GET", f"/jobs/{j}")
            rows.append(f"{j[:8]} {fmt(s)}")
            if s["status"] in TERMINAL:
                pending.discard(j)
        print(time.strftime("%H:%M:%S"), " | ".join(rows) if rows else "")
        time.sleep(3)
    print("all jobs finished. Check Grafana (docker compose --profile obs up -d): 'HTTP 429s' for llm stays 0.")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("health")
    s = sub.add_parser("submit")
    s.add_argument("url")
    s.add_argument("--watch", action="store_true")
    s.add_argument("--fail-frames", default="")
    s.add_argument("--unavailable", action="store_true", help="simulate a private/unavailable video (DLQ demo)")
    sub.add_parser("watch").add_argument("job_id")
    sub.add_parser("events").add_argument("job_id")
    f = sub.add_parser("faults")
    f.add_argument("action", choices=["show", "set", "clear"])
    f.add_argument("pairs", nargs="*")
    sub.add_parser("crash").add_argument("url")
    sub.add_parser("burst").add_argument("urls", nargs="+")
    sub.add_parser("dlq")
    a = p.parse_args()

    if a.cmd == "health":
        status, data = call("GET", "/healthz")
        print(status, json.dumps(data, indent=2))
    elif a.cmd == "submit":
        debug = {}
        if a.fail_frames:
            debug["fail_frames"] = [int(x) for x in a.fail_frames.split(",") if x.strip()]
        if a.unavailable:
            debug["unavailable"] = True
        job = submit(a.url, debug or None)
        if a.watch:
            watch(job)
    elif a.cmd == "watch":
        watch(a.job_id)
    elif a.cmd == "events":
        _, ev = call("GET", f"/admin/jobs/{a.job_id}/events", admin=True)
        for e in ev.get("events", []):
            print(f"{e['at'][11:19]} {e['kind']:<16} {e['to'] or '':<24} {e['worker_id'] or '':<22} "
                  f"{json.dumps(e['detail']) if e['detail'] else ''}")
    elif a.cmd == "faults":
        if a.action == "show":
            print(call("GET", "/admin/faults", admin=True)[1])
        elif a.action == "clear":
            print(call("DELETE", "/admin/faults", admin=True)[1])
        else:
            body = dict(kv.split("=", 1) for kv in a.pairs)
            print(call("POST", "/admin/faults", body, admin=True)[1])
    elif a.cmd == "crash":
        cmd_crash(a.url)
    elif a.cmd == "burst":
        cmd_burst(a.urls)
    elif a.cmd == "dlq":
        print(json.dumps(call("GET", "/admin/dlq", admin=True)[1], indent=2))


if __name__ == "__main__":
    main()
