"""Shared progress sampled off-thread, with usable compact result previews."""
from __future__ import annotations

import asyncio
from dataclasses import replace
import threading
from rich.cells import cell_len

from textual.widgets import Static

from teflow.preview_widgets import SelectablePreview
from teflow.progress import ProgressSnapshot
from teflow.tui import TEFlowApp, LiveLog
from test_tui import FakeManager, eventually, select_node, visible
from test_tui_browser import fixture_job, open_file


def download_snapshot(**changes):
    snapshot = ProgressSnapshot(
        stage="download", status="running", label="下载数据",
        current_bytes=3_000_000_000, total_bytes=6_000_000_000,
        speed_bps=2_000_000, percent=50.0, eta_seconds=1500,
        completed_runs=1, total_runs=2, elapsed_seconds=120,
    )
    return replace(snapshot, **changes)


class ScopedTracker:
    def __init__(self, values):
        self.values, self.calls = values, []

    def snapshot(self, job, run=None):
        self.calls.append((job["accession"], run, threading.get_ident()))
        return self.values[run]


def current_activity(job, run=None, limit=5):
    return {"status": job["status"], "step": job.get("step") or job["stage"],
            "elapsed_seconds": 120,
            "events": [{"message": "最新下载事件", "run": run, "level": "info", "time": ""}]}


def test_initial_compact_project_without_selected_file_keeps_three_preview_rows(tmp_path):
    async def scenario():
        job, _ = fixture_job(tmp_path)
        app = TEFlowApp(manager=FakeManager([job]))
        async with app.run_test(size=(80, 24)) as pilot:
            await eventually(pilot, lambda: app.selected_accession == "GSE1")
            await pilot.pause()
            assert app.selected_run is None and app.selected_file is None
            assert not app.query_one("#preview-notice", Static).display
            preview = app.query_one("#preview-text", SelectablePreview)
            assert preview.region.height >= 3
            assert "选择项目 / Run" in preview.text
    asyncio.run(scenario())


def test_compact_sra_unknown_total_and_conversion_remain_readable(tmp_path, monkeypatch):
    monkeypatch.setattr("teflow.tui.activity_snapshot", current_activity)
    async def scenario():
        job, _ = fixture_job(tmp_path)
        job.update(stage="download", step="下载 FASTQ")
        tracker = ScopedTracker({None: download_snapshot(label="下载 SRA（SRR22521675）",
                                                          total_bytes=None, percent=None, eta_seconds=None)})
        app = TEFlowApp(manager=FakeManager([job]))
        app.progress_tracker = tracker
        async with app.run_test(size=(80, 24)) as pilot:
            await eventually(pilot, lambda: "总大小未知" in visible(app, "#activity"))
            activity = app.query_one("#activity", Static)
            assert all(cell_len(line) <= activity.size.width for line in visible(app, "#activity").splitlines())
            assert "2.00 MB/s" in visible(app, "#activity-title")
            assert "SRR22521675" in activity.tooltip
            tracker.values[None] = download_snapshot(label="转换 FASTQ", total_bytes=None, percent=None,
                                                     speed_bps=0, eta_seconds=None)
            app.action_refresh()
            await eventually(pilot, lambda: "转换 FASTQ" in visible(app, "#activity-title"))
            assert "B/s" not in visible(app, "#activity-title")
            await eventually(pilot, lambda: app.query_one("#preview-text", SelectablePreview).region.height >= 3)
            assert app.query_one("#preview-text", SelectablePreview).region.height >= 3
    asyncio.run(scenario())


def test_download_progress_uses_shared_values_and_selected_scope(tmp_path, monkeypatch):
    monkeypatch.setattr("teflow.tui.activity_snapshot", current_activity)

    async def scenario():
        job, _ = fixture_job(tmp_path)
        job.update(stage="download", step="下载 FASTQ")
        tracker = ScopedTracker({None: download_snapshot(), "SRR1": download_snapshot(percent=75.0, run="SRR1")})
        app = TEFlowApp(manager=FakeManager([job]))
        app.progress_tracker = tracker
        async with app.run_test(size=(170, 50)) as pilot:
            await eventually(pilot, lambda: "50.0%" in visible(app, "#activity"))
            details = visible(app, "#activity")
            assert "3.00 GB / 6.00 GB" in details
            assert "2.00 MB/s" in details and "25 分" in details
            assert "█" in details and "░" in details
            await select_node(app, pilot, "GSE1", "SRR1")
            await eventually(pilot, lambda: "75.0%" in visible(app, "#activity"))
            assert "50.0%" not in visible(app, "#activity")
            assert {scope for _, scope, _ in tracker.calls} == {None, "SRR1"}
            assert all(ident != threading.get_ident() for _, _, ident in tracker.calls)
    asyncio.run(scenario())


def test_compact_download_shows_percent_and_speed_without_shrinking_preview(tmp_path, monkeypatch):
    monkeypatch.setattr("teflow.tui.activity_snapshot", current_activity)

    async def scenario():
        job, count = fixture_job(tmp_path)
        job.update(stage="download", step="下载 FASTQ")
        app = TEFlowApp(manager=FakeManager([job]))
        app.progress_tracker = ScopedTracker({None: download_snapshot(), "SRR1": download_snapshot(run="SRR1")})
        async with app.run_test(size=(80, 24)) as pilot:
            await eventually(pilot, lambda: "50.0%" in visible(app, "#activity"))
            assert "2.00 MB/s" in visible(app, "#activity-title")
            assert "25 分" in visible(app, "#activity-title")
            await select_node(app, pilot, "GSE1", "SRR1")
            await open_file(app, pilot, count)
            await pilot.pause()
            assert app.query_one("#preview-table", SelectablePreview).region.height >= 3
            await pilot.press("f2")
            await eventually(pilot, lambda: app.logs_visible)
            assert app.query_one("#logs", LiveLog).region.height >= 3
            assert "3.00 GB / 6.00 GB" in app.query_one("#activity", Static).tooltip
    asyncio.run(scenario())


def test_analysis_reports_counts_elapsed_and_keeps_latest_event(tmp_path, monkeypatch):
    monkeypatch.setattr("teflow.tui.activity_snapshot", current_activity)

    async def scenario():
        job, _ = fixture_job(tmp_path)
        snapshot = download_snapshot(stage="analyze", label="分析数据", percent=None,
                                     speed_bps=None, eta_seconds=None, completed_runs=1)
        app = TEFlowApp(manager=FakeManager([job]))
        app.progress_tracker = ScopedTracker({None: snapshot})
        async with app.run_test(size=(170, 50)) as pilot:
            await eventually(pilot, lambda: "已完成 1/2 Run" in visible(app, "#activity"))
            details = visible(app, "#activity")
            assert "耗时 2 分" in details
            assert "%" not in details and "MB/s" not in details
            assert "最新下载事件" in details
    asyncio.run(scenario())


def test_verification_unknown_sizes_and_unknown_failures_are_honest(tmp_path, monkeypatch):
    monkeypatch.setattr("teflow.tui.activity_snapshot", current_activity)

    async def scenario():
        job, _ = fixture_job(tmp_path)
        job.update(stage="download", step="下载 FASTQ")
        tracker = ScopedTracker({None: download_snapshot(label="校验中", percent=100.0, speed_bps=None,
                                                         eta_seconds=None, current_bytes=6_000_000_000)})
        manager = FakeManager([job])
        app = TEFlowApp(manager=manager)
        app.progress_tracker = tracker
        async with app.run_test(size=(170, 50)) as pilot:
            await eventually(pilot, lambda: "校验中" in visible(app, "#activity"))
            tracker.values[None] = download_snapshot(total_bytes=None, percent=None, speed_bps=None, eta_seconds=None)
            app.action_refresh()
            await eventually(pilot, lambda: "总大小未知" in visible(app, "#activity"))
            assert "%" not in visible(app, "#activity")
            manager.jobs["GSE1"].update(status="failed", error="unclassified tool exception")
            app.action_refresh()
            await eventually(pilot, lambda: "失败" in visible(app, "#overview"))
            assert "%" not in visible(app, "#activity")
            assert app._activity_failure(manager.jobs["GSE1"], {"status": "failed"}) is None
    asyncio.run(scenario())


def test_friendly_failure_is_scoped_and_original_error_is_retained(tmp_path, monkeypatch):
    monkeypatch.setattr("teflow.tui.activity_snapshot", current_activity)

    async def scenario():
        job, _ = fixture_job(tmp_path, status="failed")
        original = "RuntimeError: original tool failure"
        reason = "SRR2 是单端数据，当前流程要求双端数据。"
        job.update(error=original, current_sample="SRR2",
                   failure={"code": "single_end", "message": reason, "run": "SRR2"})
        app = TEFlowApp(manager=FakeManager([job]))
        async with app.run_test(size=(170, 50)) as pilot:
            await eventually(pilot, lambda: reason in visible(app, "#overview"))
            assert reason in visible(app, "#activity")
            assert app.manager.jobs["GSE1"]["error"] == original
            await select_node(app, pilot, "GSE1", "SRR1")
            await eventually(pilot, lambda: app._last_activity[0]["accession"] == "GSE1")
            assert app._activity_failure(job, {"status": "failed"}) is None
            assert reason not in visible(app, "#activity")
            assert reason in visible(app, "#overview")
    asyncio.run(scenario())
