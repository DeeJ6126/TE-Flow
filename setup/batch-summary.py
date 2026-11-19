#!/usr/bin/env python3
"""One-screen summary of a large TEflow batch (tf status prints every job).

usage: python setup/batch-summary.py [--settings settings.yaml] [--failed N] [--rate SECONDS]

Reads state/jobs.json without taking the registry lock (read-only snapshot).
--rate measures the server's network receive rate (eth0) over SECONDS.
"""
import argparse
import json
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

import yaml


def receive_bytes(interface="eth0"):
    for line in Path("/proc/net/dev").read_text().splitlines():
        name, _, data = line.partition(":")
        if name.strip() == interface:
            return int(data.split()[0])
    return None


def main():
    app = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--settings", default=str(app / "settings.yaml"))
    parser.add_argument("--failed", type=int, default=10, help="显示最近失败的项目数")
    parser.add_argument("--rate", type=int, default=0, help="测量网络接收速度的秒数（0 = 不测）")
    args = parser.parse_args()
    settings = yaml.safe_load(Path(args.settings).read_text(encoding="utf-8"))
    # Same rule as JobManager: state_dir relative to the settings file, default <app>/state.
    base = Path(args.settings).resolve().parent
    state = base / settings["state_dir"] if settings.get("state_dir") else base / "state"
    jobs = list(json.loads((state / "jobs.json").read_text(encoding="utf-8"))["jobs"].values())
    label = {("queued", "download"): "等待下载", ("running", "download"): "下载中", ("queued", "analyze"): "已下载待分析",
             ("running", "analyze"): "分析中", ("running", None): "运行中（项目内下载）", ("queued", None): "排队"}
    counts = Counter(label.get((j["status"], j.get("phase")), j["status"]) for j in jobs)
    print(datetime.now().strftime("%Y-%m-%d %H:%M"), f"共 {len(jobs)} 个任务")
    for key, value in counts.most_common():
        print(f"  {key}: {value}")
    done = [j for j in jobs if j["status"] == "succeeded" and j.get("finished_at")]
    recent = [j for j in done if (datetime.now().astimezone() - datetime.fromisoformat(j["finished_at"])).total_seconds() < 86400]
    print(f"  最近 24 小时完成: {len(recent)}")
    print("运行中:")
    for j in sorted((j for j in jobs if j["status"] == "running"), key=lambda j: (j.get("phase") or "", j["accession"])):
        print(f"  {j['accession']:<12} {j.get('phase') or '-':<8} {j.get('step', '')}")
    failed = sorted((j for j in jobs if j["status"] in ("failed", "interrupted")),
                    key=lambda j: j.get("finished_at") or j.get("updated_at") or "", reverse=True)
    if failed:
        print(f"失败 / 中断 {len(failed)} 个，最近 {min(args.failed, len(failed))} 个:")
        for j in failed[:args.failed]:
            print(f"  {j['accession']:<12} {(j.get('finished_at') or '')[5:16]} {(j.get('error') or '').splitlines()[0][:150]}")
    if args.rate > 0:
        first = receive_bytes()
        time.sleep(args.rate)
        second = receive_bytes()
        if first is not None and second is not None:
            print(f"网络接收（eth0）: {(second - first) / args.rate / 1024**2:.2f} MB/s")


if __name__ == "__main__":
    main()
