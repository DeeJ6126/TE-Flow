from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from teflow import activity
from teflow.activity import ActivityRecorder, activity_snapshot, known_runs, log_snapshot


@pytest.fixture
def job(tmp_path):
    return {"accession": "GSE1", "job_dir": str(tmp_path), "raw_dir": str(tmp_path / "raw"),
            "status": "running", "stage": "analyze", "step": "比对", "attempt": 2,
            "current_sample": "SRR2"}


def rule(recorder, name, jobid, run=None):
    recorder.feed(f"localrule {name}:")
    recorder.feed(f"    jobid: {jobid}")
    if run:
        recorder.feed(f"    wildcards: sample={run}")


def journal(job):
    return ActivityRecorder(job, Path(job["job_dir"]) / ".teflow-activity.jsonl")


def test_interleaved_jobs_finish_their_own_run(job):
    recorder = journal(job)
    recorder.begin()
    rule(recorder, "star_align", 4, "SRR1")
    rule(recorder, "te_count", 5, "SRR2")
    recorder.feed("Finished jobid: 4 (Rule: star_align)")
    recorder.feed("Finished jobid: 5 (Rule: te_count)")
    done = [item for item in recorder.events if item["state"] == "succeeded"]
    assert [(item["run"], item["step"]) for item in done] == [("SRR1", "比对"), ("SRR2", "TE 计数")]
    assert activity_snapshot(job, "SRR2")["status"] == "succeeded"
    first = activity_snapshot(job, "SRR1")
    assert first["status"] == "queued" and "等待" in first["step"]


def test_failed_parallel_job_uses_error_jobid_without_wildcards(job):
    recorder = journal(job)
    rule(recorder, "te_count", 4, "SRR1")
    rule(recorder, "star_align", 5, "SRR2")
    recorder.feed("Error in rule te_count:")
    recorder.feed("    jobid: 4")
    recorder.finish("failed", "WorkflowError")
    job.update(status="failed", error="WorkflowError")
    assert activity_snapshot(job, "SRR1")["status"] == "failed"
    assert activity_snapshot(job, "SRR2")["status"] == "interrupted"
    errors = [item for item in recorder.events if item["state"] == "failed"]
    assert errors[0]["run"] == "SRR1"
    assert errors[-1]["run"] is None
    # The old registry may retain a peer's current_sample/error context.
    job["error"] = "SRR2: peer log context"
    assert "peer log context" not in str(activity_snapshot(job, "SRR1"))


def test_progress_download_and_duplicate_suppression(job):
    recorder = journal(job)
    message = '[TEFLOW_PROGRESS] {"stage":"download","step":"下载 FASTQ","current_sample":"SRR1","downloaded_runs":0}'
    recorder.feed(message)
    recorder.feed(message)
    assert len(recorder.events) == 1
    recorder.feed('[TEFLOW_PROGRESS] {"stage":"download","step":"FASTQ 已就绪","current_sample":"SRR1","downloaded_runs":1}')
    snap = activity_snapshot(job, "SRR1")
    assert snap["status"] == "queued"
    assert snap["events"][-1]["message"] == "FASTQ 下载完成"
    recorder.feed('[TEFLOW_PROGRESS] [broken]')


def test_attempt_scoping_hides_previous_terminal_events(job):
    old = dict(job, attempt=1)
    recorder = journal(old)
    recorder.finish("failed", "previous failure")
    recorder = journal(job)
    recorder.begin()
    assert all("previous" not in item["message"] for item in activity_snapshot(job)["events"])


def test_completed_run_remains_complete_despite_parent_or_peer_failure(job):
    count = Path(job["job_dir"]) / "SRR1/abundance/SRR1.cntTable"
    count.parent.mkdir(parents=True)
    count.write_text("gene\tSRR1\nA\t1\n")
    job.update(status="failed", error="SRR2: download failure", stage="download")
    snapshot = activity_snapshot(job, "SRR1")
    assert snapshot["status"] == "succeeded" and snapshot["step"] == "完成"
    assert "SRR2" not in str(snapshot)
    assert activity_snapshot(job, "SRR2")["status"] == "failed"
    assert activity_snapshot(job, "SRR3")["status"] == "interrupted"


def test_run_boundary_does_not_copy_peer_error(job):
    job.update(status="failed", stage="download", error="SRR12: connection reset")
    assert activity_snapshot(job, "SRR1")["status"] == "interrupted"


def test_partial_count_file_does_not_override_live_or_failed_count(job):
    count = Path(job["job_dir"]) / "SRR1/abundance/SRR1.cntTable"
    count.parent.mkdir(parents=True)
    count.write_text("gene\tSRR1\n")
    recorder = journal(job)
    rule(recorder, "te_count", 4, "SRR1")
    assert activity_snapshot(job, "SRR1")["status"] == "running"
    recorder.feed("Error in rule te_count:")
    recorder.feed("    jobid: 4")
    job.update(status="failed", error="WorkflowError")
    assert activity_snapshot(job, "SRR1")["status"] == "failed"


def test_elapsed_is_actual_step_start_and_unknown_stays_unknown(job):
    recorder = journal(job)
    start = datetime.now(timezone.utc) - timedelta(seconds=61)
    recorder.time = start.isoformat()
    recorder.emit("比对开始", run="SRR1", stage="analyze", step="比对")
    snap = activity_snapshot(job, "SRR1")
    assert 60 <= snap["elapsed_seconds"] <= 63
    assert activity_snapshot(job)["elapsed_seconds"] >= 60
    assert activity_snapshot(job, "SRR2")["elapsed_seconds"] is None
    recorder.time = ""
    recorder.emit("TE 计数开始", run="SRR1", stage="analyze", step="TE 计数")
    assert activity_snapshot(job, "SRR1")["elapsed_seconds"] is None


def test_historical_rule_timestamps_are_normalized_and_attempts_separated(job):
    log = Path(job["job_dir"]) / "run.log"
    log.write_text("=== 2026-10-13T19:00:00+08:00 attempt 1 ===\n"
                   "localrule star_align:\n jobid: 8\n wildcards: sample=SRR9\n"
                   "=== 2026-10-13T20:00:00+08:00 attempt 2 ===\n"
                   "[Tue Oct 13 12:00:00 2026]\nlocalrule star_align:\n jobid: 2\n wildcards: sample=SRR1\n"
                   "[Tue Oct 13 12:00:03 2026]\nFinished job 2.\n")
    snapshot = activity_snapshot(job, "SRR1")
    assert snapshot["events"][0]["time"] == "2026-10-13T20:00:00+08:00"
    assert snapshot["events"][-1]["message"] == "比对完成"
    assert "SRR9" not in str(activity_snapshot(job))


def test_stale_log_from_previous_attempt_is_not_active(job):
    log = Path(job["job_dir"]) / "run.log"
    log.write_text("localrule te_count:\n jobid: 2\n wildcards: sample=SRR1\n")
    started = datetime.now(timezone.utc)
    os.utime(log, (started.timestamp() - 120, started.timestamp() - 120))
    job["attempt_started_at"] = started.isoformat()
    assert activity_snapshot(job, "SRR1")["status"] == "queued"
    assert log_snapshot(job)["text"] == "暂无运行日志。"


def test_prepare_log_fallback_and_failed_mapping_without_manifest(job):
    base = Path(job["job_dir"])
    (base / "samples.tsv").write_text("GSE\tGSM\tRun\nGSE1\tGSM1\tSRR1\nGSE1\tGSM2\tSRR2\n")
    (base / "logs").mkdir()
    (base / "logs/prepare_samples.log").write_text("[TEFLOW] Run 1/2 SRR1: iSeq 下载（1/3）。\n")
    assert known_runs(job) == ["SRR1", "SRR2"]
    job.update(status="failed", stage="download", error="SRR1: download failed")
    assert activity_snapshot(job, "SRR1")["status"] == "failed"
    assert activity_snapshot(job, "SRR2")["status"] == "interrupted"


def test_log_run_scope_reads_actual_root_tools_and_nested_logs(job):
    base = Path(job["job_dir"])
    (base / "logs").mkdir()
    (base / "logs/SRR1.fastp.log").write_text("\x1b[31mfastp text\x1b[0m\x00\n")
    (base / "logs/SRR2.STAR.log").write_text("peer secret\n")
    nested = base / "SRR1/logs"
    nested.mkdir(parents=True)
    (nested / "SRR1.TEcount.log").write_text("count text\n")
    snap = log_snapshot(job, "SRR1")
    assert "fastp text" in snap["text"] and "count text" in snap["text"]
    assert "peer secret" not in snap["text"] and "\x1b" not in snap["text"] and "\x00" not in snap["text"]
    assert log_snapshot(job, "SRR1")["signature"] == snap["signature"]
    (nested / "SRR1.TEcount.log").write_text("count text changed\n")
    assert log_snapshot(job, "SRR1")["signature"] != snap["signature"]


def test_log_scope_before_tool_logs_filters_project_lines(job):
    (Path(job["job_dir"]) / "run.log").write_text("SRR1 downloading\nSRR2 downloading\nSRR12 downloading\n")
    text = log_snapshot(job, "SRR1")["text"]
    assert "SRR1 downloading" in text and "SRR2" not in text and "SRR12" not in text


def test_bounded_tail_skips_partial_prefix_and_does_not_load_whole_file(job, monkeypatch):
    base = Path(job["job_dir"])
    log = base / "run.log"
    log.write_bytes(b"secret prefix\n" + b"x" * 1000000 + b"\nlast line\n")
    reads = []
    original = Path.open
    class Reader:
        def __init__(self, handle): self.handle = handle
        def __enter__(self): return self
        def __exit__(self, *args): self.handle.close()
        def seek(self, *args): return self.handle.seek(*args)
        def read(self, maximum=-1):
            reads.append(maximum)
            assert 0 <= maximum <= activity.TAIL_BYTES
            return self.handle.read(maximum)
    def traced(path, *args, **kwargs):
        handle = original(path, *args, **kwargs)
        return Reader(handle) if args and args[0] == "rb" else handle
    monkeypatch.setattr(Path, "open", traced)
    snap = log_snapshot(job, lines=3)
    assert "last line" in snap["text"] and "secret prefix" not in snap["text"]
    assert len(snap["text"]) < 100
    first_count = len(reads)
    assert log_snapshot(job, lines=3) == snap
    assert len(reads) == first_count


def test_unknown_and_malformed_journal_lines_do_not_break_view(job):
    (Path(job["job_dir"]) / ".teflow-activity.jsonl").write_text("not JSON\n[]\n" + json.dumps({"attempt": 2, "message": "good"}) + "\n")
    assert activity_snapshot(job)["events"][-1]["message"] == "good"
    with pytest.raises(ValueError):
        log_snapshot(job, "../../other")
    with pytest.raises(ValueError):
        activity_snapshot(job, "SRR1/other")


def test_instrumentation_write_failure_cannot_stop_analysis(job, monkeypatch):
    recorder = journal(job)
    original = Path.open
    def denied(path, *args, **kwargs):
        if path.name == ".teflow-activity.jsonl":
            raise PermissionError("not writable")
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, "open", denied)
    recorder.begin()
    rule(recorder, "star_align", 1, "SRR1")
    recorder.finish("failed", "failure")
    assert recorder.events[-1]["state"] == "failed"


def test_historical_completed_tool_is_waiting_not_peer_stage(job):
    (Path(job["job_dir"]) / "logs").mkdir()
    (Path(job["job_dir"]) / "logs/SRR1.fastp.log").write_text("fastp v1.4.0, time used: 2 seconds\n")
    job.update(current_sample="SRR2", step="计数")
    snap = activity_snapshot(job, "SRR1")
    assert snap["status"] == "queued"
    assert "质控完成" in snap["step"] and "计数" not in snap["step"]
    assert snap["elapsed_seconds"] is None


def test_log_journal_and_manifest_symlinks_cannot_escape_job_root(job, tmp_path):
    root = tmp_path / "job"
    root.mkdir()
    job["job_dir"] = str(root)
    external = tmp_path / "external"
    external.mkdir()
    (external / "run.log").write_text("outside secret\n")
    (external / "activity").write_text(json.dumps({"attempt": 2, "message": "outside secret"}) + "\n")
    (external / "manifest").write_text(json.dumps({"samples": {"SRR999": {}}}))
    (external / "mapping").write_text("Run\nSRR999\n")
    try:
        (root / "run.log").symlink_to(external / "run.log")
        (root / ".teflow-activity.jsonl").symlink_to(external / "activity")
        (root / "samples.json").symlink_to(external / "manifest")
        (root / "samples.tsv").symlink_to(external / "mapping")
        (root / "logs").symlink_to(external, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"Symlink creation unavailable: {error}")
    assert known_runs(job) == []
    assert log_snapshot(job)["text"] == "暂无运行日志。"
    assert "outside secret" not in str(activity_snapshot(job))
    recorder = journal(job)
    recorder.begin()
    assert "开始解析项目" not in (external / "activity").read_text()


def test_internal_regular_symlink_remains_readable_and_cache_is_confined(job):
    base = Path(job["job_dir"])
    target = base / "target.log"
    target.write_text("safe internal log\n")
    link = base / "run.log"
    try:
        link.symlink_to(target)
    except OSError as error:
        pytest.skip(f"Symlink creation unavailable: {error}")
    assert "safe internal log" in log_snapshot(job)["text"]
    link.unlink()
    outside = base.parent / (base.name + ".outside")
    outside.write_text("outside secret\n")
    link.symlink_to(outside)
    assert log_snapshot(job)["text"] == "暂无运行日志。"


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO requires Unix")
def test_nonregular_files_are_never_opened(job, monkeypatch):
    base = Path(job["job_dir"])
    (base / "logs").mkdir()
    for name in ("run.log", ".teflow-activity.jsonl", "samples.json", "samples.tsv", "logs/SRR1.TEcount.log"):
        os.mkfifo(base / name)
    def forbidden(*args, **kwargs):
        raise AssertionError("A FIFO must not be opened")
    monkeypatch.setattr(Path, "open", forbidden)
    assert log_snapshot(job)["text"] == "暂无运行日志。"
    assert known_runs(job) == []
    assert activity_snapshot(job, "SRR1")["status"] == "queued"
    journal(job).begin()
