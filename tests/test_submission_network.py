import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from teflow import runtime
from teflow.network import NetworkUnavailable


@pytest.fixture
def manager(tmp_path, monkeypatch):
    settings = tmp_path / "settings.yaml"
    settings.write_text(yaml.safe_dump({"raw_root": str(tmp_path / "raw"),
        "output_root": str(tmp_path / "outputs"), "template_dir": str(tmp_path / "template"),
        "environment": str(tmp_path / "env"), "state_dir": str(tmp_path / "state"),
        "mem_mb": 256000, "threads": 10, "proxy_env": str(tmp_path / "none")}))
    monkeypatch.setattr(runtime, "detect_budget", lambda _: {"cores": 20, "mem_mb": 512000})
    monkeypatch.setattr(runtime.subprocess, "Popen", lambda *a, **kw: SimpleNamespace(pid=os.getpid()))
    return runtime.JobManager(settings)


def test_failed_preflight_creates_no_jobs_or_data_directories(manager, monkeypatch):
    def fail(*a, **kw):
        raise NetworkUnavailable("website_connection", "无法连接 NCBI GEO（www.ncbi.nlm.nih.gov）。")
    monkeypatch.setattr(runtime, "check_network", fail)
    with pytest.raises(NetworkUnavailable):
        manager.submit_many("GSE1,GSE2", data_type="bulk")
    assert not manager.registry_path.exists()
    assert not Path(manager.settings["raw_root"]).exists()
    assert not Path(manager.settings["output_root"]).exists()


def test_once_per_batch_and_active_job_reuse_skips_network(manager, monkeypatch):
    calls = []
    monkeypatch.setattr(runtime, "check_network", lambda *a, **kw: calls.append((a, kw)))
    manager.submit_many("GSE1,GSE2", data_type="bulk")
    assert len(calls) == 1 and calls[0][0][0] == ["GSE1", "GSE2"]
    monkeypatch.setattr(runtime, "check_network", lambda *a, **kw: pytest.fail("reused job checked network"))
    assert manager.submit_many("GSE1,GSE2", data_type="bulk")[0]["attempt"] == 1


def test_valid_local_raw_manifest_skips_network_for_resume(manager, monkeypatch):
    monkeypatch.setattr(runtime, "check_network", lambda *a, **kw: None)
    job = manager.submit("SRR1", data_type="bulk")
    raw = Path(job["raw_dir"])
    files = [raw / "SRR1_1.fastq.gz", raw / "SRR1_2.fastq.gz"]
    for file in files:
        file.write_bytes(b"fixture")
    sample = {"r1": str(files[0]), "r2": str(files[1]),
              "files": [{"name": file.name, "bytes": file.stat().st_size} for file in files]}
    (Path(job["job_dir"]) / "samples.json").write_text(json.dumps({"accession": "SRR1", "samples": {"SRR1": sample}}))
    manager.update("SRR1", status="failed")
    monkeypatch.setattr(runtime, "check_network", lambda *a, **kw: pytest.fail("offline analysis checked network"))
    assert manager.submit("SRR1", data_type="bulk")["attempt"] == 2


def test_preflight_releases_lock_and_reloads_racing_submission(manager, monkeypatch):
    calls = []
    def network(*a, **kw):
        calls.append(1)
        # A separate manager may submit while this check is in progress. Its
        # mutation must be reused, not overwritten or reserved a second time.
        monkeypatch.setattr(runtime, "check_network", lambda *a, **kw: None)
        runtime.JobManager(manager.settings_path).submit("GSE1", data_type="bulk")
    monkeypatch.setattr(runtime, "check_network", network)
    result = manager.submit("GSE1", data_type="bulk")
    assert len(calls) == 1 and result["attempt"] == 1
    assert len(manager.list_jobs()) == 1


@pytest.mark.parametrize("event,expected", [
    ({"code": "single_end", "message": "SRR1 是单端数据，当前分析流程要求双端数据。",
      "run": "SRR1", "detail": "explicit layout=SINGLE"}, "single_end"),
    ({"code": "unknown", "message": "invented reason"}, None),
    (None, None),
])
def test_worker_only_persists_valid_current_attempt_explanations(manager, monkeypatch, event, expected):
    monkeypatch.setattr(runtime, "check_network", lambda *a, **kw: None)
    job = manager.submit("GSE1", data_type="bulk")
    # A known event in an old attempt must never explain a new unknown failure.
    output = Path(job["job_dir"])
    (output / "run.log").write_text('[TEFLOW_FAILURE] {"code":"single_end","message":"old error"}\n', encoding="utf-8")
    lines = []
    if event:
        lines.append("[TEFLOW_FAILURE] " + json.dumps(event) + "\n")
    lines.append("RuntimeError: actual raw tool failure\n")
    class Process:
        pid = os.getpid()
        stdout = lines
        def wait(self, timeout=None):
            return 7
        def poll(self):
            return 7
    monkeypatch.setattr(runtime, "JobManager", lambda *a: manager)
    monkeypatch.setattr(runtime, "effective_config", lambda *a: {})
    monkeypatch.setattr(runtime.shutil, "which", lambda *a, **kw: "/synthetic/program")
    monkeypatch.setattr(runtime.subprocess, "Popen", lambda *a, **kw: Process())
    assert runtime.run_worker(str(manager.settings_path), "GSE1") == 1
    failed = manager.get_job("GSE1")
    assert "actual raw tool failure" in failed["error"]
    assert "Traceback" in (output / "run.log").read_text(encoding="utf-8")
    if expected:
        assert failed["failure"]["code"] == expected
        assert failed["error_detail"] == "explicit layout=SINGLE"
    else:
        assert failed.get("failure") is None
