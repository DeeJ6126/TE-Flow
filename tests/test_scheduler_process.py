"""Real Linux scheduler/worker processes with a synthetic scientific executable.

This validates process scheduling and failure isolation, not biological results.
No download, reference loading or substantial memory allocation occurs.
"""

from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
import sys
import time

import psutil
import pytest
import yaml

from teflow import runtime


pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="Linux detached-process integration")


FAKE_SNAKEMAKE = r'''
import json
import os
from pathlib import Path
import sys
import time
import yaml

config = yaml.safe_load(Path(sys.argv[sys.argv.index("--configfile") + 1]).read_text())
project = config["ACCESSION"]
output = Path(config["OUTPUT_DIR"])
trace = output / "synthetic-timeline.json"
started = time.monotonic()
trace.write_text(json.dumps({"project": project, "start": started, "pid": os.getpid()}))
print("[TEFLOW_PROGRESS] " + json.dumps({"stage": "metadata", "step": "synthetic metadata"}), flush=True)
run = "SRR" + project.removeprefix("GSE")
raw = Path(config["SAMPLES_DIR"]) / run
raw.mkdir(parents=True, exist_ok=True)
reads = []
for mate in (1, 2):
    path = raw / f"{run}_{mate}.fastq.gz"
    path.write_bytes(b"synthetic test placeholder, not biological FASTQ")
    reads.append(path)
count = output / run / "abundance" / f"{run}.cntTable"
sample = {"r1": str(reads[0]), "r2": str(reads[1]), "count_path": str(count),
          "files": [{"name": path.name, "bytes": path.stat().st_size} for path in reads]}
(output / "samples.json").write_text(json.dumps({"accession": project, "samples": {run: sample}}))
print("[TEFLOW_PROGRESS] " + json.dumps({"stage": "download", "total_runs": 1,
      "downloaded_runs": 1}), flush=True)
print("[TEFLOW_PROGRESS] " + json.dumps({"stage": "analyze", "step": "synthetic work",
      "current_sample": run}), flush=True)

# Keep the first two synthetic processes alive together, even on a busy host.
if project in {"GSE1", "GSE2"}:
    peer = "GSE2" if project == "GSE1" else "GSE1"
    deadline = time.monotonic() + 6
    while not (output.parent / peer / "synthetic-timeline.json").exists():
        if time.monotonic() > deadline:
            raise RuntimeError("synthetic peer never started")
        time.sleep(0.02)
time.sleep({"GSE1": 0.6, "GSE2": 1.8, "GSE3": 0.35}[project])

failed = project == "GSE1"
if not failed:
    count.parent.mkdir(parents=True, exist_ok=True)
    count.write_text(f"gene\t{run}\nsynthetic_feature\t3\n")
    print("[TEFLOW_PROGRESS] " + json.dumps({"stage": "merge", "analyzed_runs": 1}), flush=True)
    (output / "merged.count").write_text(count.read_text())
trace.write_text(json.dumps({"project": project, "start": started, "end": time.monotonic(),
                            "pid": os.getpid(), "synthetic": True, "failed": failed}))
if failed:
    print("RuntimeError: intentional synthetic project failure", flush=True)
    raise SystemExit(7)
'''


def _read_registry(path: Path) -> dict:
    return json.loads(path.read_text()) if path.is_file() else {"jobs": {}}


def _remember_processes(registry: dict, owned: dict[int, str | None]) -> None:
    daemon = registry.get("scheduler", {})
    records = [daemon, *registry.get("jobs", {}).values()]
    for record in records:
        for key in ("pid", "snakemake_pid"):
            if record.get(key):
                owned[int(record[key])] = record.get(f"{key}_start")


def _cleanup_test_processes(registry_path: Path, settings_path: Path,
                            owned: dict[int, str | None]) -> None:
    """Stop only PIDs launched by this isolated registry, checking PID reuse."""
    with contextlib.suppress(OSError, ValueError):
        _remember_processes(_read_registry(registry_path), owned)
    # Stop the scheduler first so no queued worker can appear during cleanup.
    for pid, started in list(owned.items()):
        if not runtime.owned_pid_alive(pid, started):
            continue
        with contextlib.suppress(psutil.Error):
            process = psutil.Process(pid)
            command = process.cmdline()
            if "teflow.scheduler" not in command or str(settings_path) not in command:
                continue
            for child in process.children(recursive=True):
                owned[child.pid] = runtime.proc_start(child.pid)
            process.terminate()
            try:
                process.wait(timeout=0.5)
            except psutil.TimeoutExpired:
                process.kill()
                process.wait(timeout=0.5)
    with contextlib.suppress(OSError, ValueError):
        _remember_processes(_read_registry(registry_path), owned)
    processes = []
    for pid, started in owned.items():
        if not runtime.owned_pid_alive(pid, started):
            continue
        with contextlib.suppress(psutil.Error):
            process = psutil.Process(pid)
            # Every owned command names this test's settings or generated config.
            command = process.cmdline()
            if not any(str(settings_path.parent) in arg for arg in command):
                continue
            processes.extend(process.children(recursive=True))
            processes.append(process)
    unique = {process.pid: process for process in processes}
    for process in reversed(list(unique.values())):
        with contextlib.suppress(psutil.Error):
            process.terminate()
    _, alive = psutil.wait_procs(list(unique.values()), timeout=2)
    for process in alive:
        with contextlib.suppress(psutil.Error):
            process.kill()
    psutil.wait_procs(alive, timeout=1)


def test_real_detached_scheduler_parallel_budget_and_failure_release(tmp_path, monkeypatch):
    """Two actual workers overlap; a third waits and starts after failed work frees a slot."""
    monkeypatch.setattr(runtime, "check_network", lambda *args, **kwargs: None)
    environment = tmp_path / "synthetic-env"
    binary_dir = environment / "bin"
    binary_dir.mkdir(parents=True)
    (binary_dir / "python").symlink_to(sys.executable)
    executable = binary_dir / "snakemake"
    executable.write_text(f"#!{sys.executable}\n" + FAKE_SNAKEMAKE)
    executable.chmod(0o755)
    for program in ("iseq", "fastp", "STAR", "TEcount", "zcat"):
        stub = binary_dir / program
        stub.write_text("#!/bin/sh\nexit 0\n")
        stub.chmod(0o755)
    references = tmp_path / "synthetic-refs"
    references.mkdir()
    gene = references / "gene.gtf"
    repeat = references / "te.gtf"
    gene.write_text("synthetic reference placeholder\n")
    repeat.write_text("synthetic reference placeholder\n")
    template = tmp_path / "template"
    template.mkdir()
    (template / "config.yaml").write_text(yaml.safe_dump({
        "INDEX_PREFIX": str(references), "GENOME_ANNOTATION": str(gene),
        "TE_ANNOTATION": str(repeat),
    }))
    settings_path = tmp_path / "settings.yaml"
    settings_path.write_text(yaml.safe_dump({
        "environment": str(environment), "template_dir": str(template),
        "raw_root": str(tmp_path / "raw"), "output_root": str(tmp_path / "outputs"),
        "state_dir": str(tmp_path / "state"), "proxy_env": str(tmp_path / "no-proxy.env"),
        "threads": 10, "mem_mb": 256000, "scheduler_interval": 0.03,
    }))
    package_root = str(Path(runtime.__file__).resolve().parent.parent)
    monkeypatch.setenv("PYTHONPATH", package_root + os.pathsep + os.environ.get("PYTHONPATH", ""))
    # Reservations are synthetic accounting values; no corresponding RAM is allocated.
    monkeypatch.setattr(runtime, "detect_budget", lambda _: {
        "cores": 20, "mem_mb": 512000, "reservation": "whole_workflow",
    })
    manager = runtime.JobManager(settings_path)
    owned: dict[int, str | None] = {}
    observations = []
    try:
        submitted = manager.submit_many("GSE1,GSE2,GSE3", data_type="bulk")
        assert len(submitted) == 3
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            # Read the durable registry directly: no viewer tick is needed to launch the queue.
            registry = _read_registry(manager.registry_path)
            _remember_processes(registry, owned)
            jobs = registry["jobs"]
            running = [item for item in jobs.values() if item["status"] == "running"]
            assert len(running) <= 2
            assert sum(item["cores"] for item in running) <= 20
            assert sum(item["mem_mb"] for item in running) <= 512000
            observations.append({key: item["status"] for key, item in jobs.items()})
            if len(jobs) == 3 and all(item["status"] not in runtime.ACTIVE for item in jobs.values()):
                break
            time.sleep(0.03)
        else:
            logs = "\n".join(path.read_text(errors="replace") for path in tmp_path.rglob("*.log"))
            pytest.fail(f"Detached scheduler did not finish within 20 seconds.\n{observations[-1:]}\n{logs}")

        assert {key: item["status"] for key, item in jobs.items()} == {
            "GSE1": "failed", "GSE2": "succeeded", "GSE3": "succeeded",
        }
        assert any(snapshot == {"GSE1": "running", "GSE2": "running", "GSE3": "queued"}
                   for snapshot in observations), observations
        traces = {key: json.loads((Path(item["job_dir"]) / "synthetic-timeline.json").read_text())
                  for key, item in jobs.items()}
        first, second, third = (traces[key] for key in ("GSE1", "GSE2", "GSE3"))
        assert first["pid"] != second["pid"]
        assert max(first["start"], second["start"]) < min(first["end"], second["end"])
        assert third["start"] >= first["end"]
        assert third["start"] < second["end"], "Failed project must release its reservation while another is still running."
        assert jobs["GSE1"]["exit_code"] == 1
        assert "intentional synthetic project failure" in jobs["GSE1"]["error"]
        for key in ("GSE2", "GSE3"):
            assert jobs[key]["exit_code"] == 0
            assert Path(jobs[key]["result_path"]).read_text().endswith("synthetic_feature\t3\n")
    finally:
        _cleanup_test_processes(manager.registry_path, settings_path, owned)
