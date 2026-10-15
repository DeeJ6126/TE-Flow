"""Behavioral TUI tests; fake jobs never run downloads or analysis commands."""

from __future__ import annotations

import asyncio
import threading
from copy import deepcopy

import pytest
from textual.widgets import Button, Input, RichLog, Select, Static, TabbedContent, Tree
from teflow.result_tree import ResultFileTree

from teflow.runtime import normalize_accession
from teflow.tui import TEFlowApp, LiveLog


class FakeManager:
    def __init__(self, jobs: list[dict] | None = None) -> None:
        self.jobs = {job["accession"]: deepcopy(job) for job in jobs or []}
        self.submissions: list[str] = []
        self.batch_calls: list[tuple[str, str]] = []
        self.file_inputs: dict[str, list[str]] = {}
        self.submit_threads: list[int] = []
        self.read_threads: list[int] = []
        self.fail_submit: Exception | None = None
        self.logs: dict[str, str] = {}
        self.submit_started = threading.Event()
        self.submit_release: threading.Event | None = None
        self.log_reads: list[str] = []

    def submit_many(self, source: str, input_mode: str = "auto", cores: int | None = None, *, data_type=None) -> list[dict]:
        from teflow.data_types import validate_data_type
        validate_data_type(data_type)
        self.batch_calls.append((source, input_mode))
        if input_mode == "file":
            if source not in self.file_inputs:
                raise FileNotFoundError(f"找不到服务器 TXT 文件：{source}")
            inputs = self.file_inputs[source]
        else:
            inputs = source.split(",")
        accessions = list(dict.fromkeys(normalize_accession(item) for item in inputs))
        return [self.submit(accession) for accession in accessions]

    def submit(self, accession: str) -> dict:
        self.submit_threads.append(threading.get_ident())
        self.submissions.append(accession)
        self.submit_started.set()
        if self.submit_release is not None:
            if not self.submit_release.wait(timeout=5):
                raise RuntimeError("测试超时")
        if self.fail_submit:
            raise self.fail_submit
        if accession in self.jobs:
            return deepcopy(self.jobs[accession])
        job = make_job(accession)
        self.jobs[accession] = job
        self.logs[accession] = "iSeq download started\n等待下载完成"
        return deepcopy(job)

    def list_jobs(self) -> list[dict]:
        self.read_threads.append(threading.get_ident())
        return deepcopy(list(self.jobs.values()))

    def get_job(self, accession: str) -> dict | None:
        return deepcopy(self.jobs.get(accession))

    def log_tail(self, accession: str, lines: int = 100) -> str:
        self.log_reads.append(accession)
        return "\n".join(self.logs.get(accession, "").splitlines()[-lines:])


def make_job(accession: str, *, status: str = "running", stage: str = "download", created: str = "2026-10-13T12:00:00") -> dict:
    return {
        "accession": accession,
        "status": status,
        "stage": stage,
        "created_at": created,
        "pid": 123,
        "job_dir": f"/root/TEatlas/jobs/{accession}",
        "raw_dir": f"/root/nfsdata/TEatlas/bulk_rna/{accession}",
        "result_path": f"/root/TEatlas/bulk_rna/{accession}/merged.count",
        "error": None,
        "batch_id": "test-batch",
        "total_runs": 2,
        "downloaded_runs": 1,
        "analyzed_runs": 0,
        "step": "",
        "current_sample": None,
    }


def visible(app: TEFlowApp, widget_id: str) -> str:
    rendered = app.query_one(widget_id, Static).render()
    return getattr(rendered, "plain", str(rendered))


async def eventually(pilot, predicate, *, timeout: float = 3) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("界面未在限时内反映状态")
        await pilot.pause(0.02)


async def select_node(app, pilot, accession, run=None):
    key = ("run" if run else "project", accession, run, None)
    await eventually(pilot, lambda: key in app._project_nodes)
    tree = app.query_one("#projects", Tree)
    if run:
        app._project_nodes[("project", accession, None, None)].expand()
        await pilot.pause()
    tree.focus()
    tree.move_cursor(app._project_nodes[key])
    await pilot.press("enter")
    await eventually(pilot, lambda: app.selected_accession == accession and app.selected_run == run)


@pytest.mark.parametrize("size", [(80, 24), (120, 36), (170, 50)])
def test_submission_and_permanent_overview(size):
    async def scenario():
        manager = FakeManager()
        app = TEFlowApp(manager=manager)
        async with app.run_test(size=size) as pilot:
            app.query_one("#data-type", Select).value = "bulk"
            app.query_one("#accession", Input).value = " gse113777,gse52778,GSE113777 "
            await pilot.press("enter")
            await eventually(pilot, lambda: not app._submitting and len(app._project_nodes) == 2)
            assert manager.submissions == ["GSE113777", "GSE52778"]
            assert app.selected_accession == "GSE113777"
            assert app.query_one("#accession", Input).value == "GSE113777,GSE52778"
            assert "运行 2" in visible(app, "#batch-summary")
            assert "已下载 1/2 Run" in visible(app, "#overview")
            assert set(manager.submit_threads + manager.read_threads).isdisjoint({threading.get_ident()})
            await pilot.press("f2")
            await eventually(pilot, lambda: app.logs_visible)
            assert app.query_one("#files", ResultFileTree).display
            for widget in ("#overview", "#files-panel", "#activity-panel", "#viewer"):
                region = app.query_one(widget).region
                assert region.height >= 3 and region.bottom <= size[1] - 1, (widget, region)
            await pilot.press("f2")
            await eventually(pilot, lambda: not app.logs_visible)
            manager.jobs["GSE113777"].update(stage="analyze", step="align", current_sample="SRR42", analyzed_runs=1)
            app.action_refresh()
            await eventually(pilot, lambda: "SRR42" in visible(app, "#overview"))
            assert "● 分析数据" in visible(app, "#overview")
            await pilot.press("ctrl+q")
        assert manager.jobs["GSE113777"]["status"] == "running"
    asyncio.run(scenario())


def test_invalid_busy_and_missing_txt_feedback():
    async def scenario():
        manager = FakeManager()
        app = TEFlowApp(manager=manager)
        async with app.run_test() as pilot:
            await pilot.press("enter")
            assert "有效编号" in visible(app, "#feedback")
            app.query_one("#data-type", Select).value = "bulk"
            manager.fail_submit = RuntimeError("服务器资源暂不可用")
            app.query_one("#accession", Input).value = "GSE1"
            await pilot.click("#start")
            await eventually(pilot, lambda: not app._submitting)
            assert "服务器资源暂不可用" in visible(app, "#feedback")
            app.query_one("#input-mode", Select).value = "file"
            await pilot.pause()
            assert "服务器绝对路径" in app.query_one("#accession", Input).placeholder
            app.query_one("#accession", Input).value = "/root/missing.txt"
            await pilot.press("enter")
            await eventually(pilot, lambda: not app._submitting)
            assert "找不到服务器 TXT" in visible(app, "#feedback")
            assert not app.query_one("#start", Button).disabled
    asyncio.run(scenario())


def test_txt_mode_and_reused_batch_summary():
    async def scenario():
        reused = make_job("GSE1", status="succeeded", created="2025")
        unrelated = make_job("GSE99", status="failed", created="2025")
        manager = FakeManager([reused, unrelated])
        manager.file_inputs["/root/list.txt"] = ["GSE1", "GSE2"]
        app = TEFlowApp(manager=manager)
        async with app.run_test() as pilot:
            app.query_one("#data-type", Select).value = "bulk"
            app.query_one("#input-mode", Select).value = "file"
            app.query_one("#accession", Input).value = "/root/list.txt"
            await pilot.press("enter")
            await eventually(pilot, lambda: not app._submitting and len(manager.submissions) == 2)
            await eventually(pilot, lambda: "运行 1" in visible(app, "#batch-summary"))
            summary = visible(app, "#batch-summary")
            assert "完成 1" in summary and "失败 0" in summary
            assert manager.batch_calls == [("/root/list.txt", "file")]
            assert app.query_one("#accession", Input).value == "/root/list.txt"
            await select_node(app, pilot, "GSE99")
            await eventually(pilot, lambda: "失败 1" in visible(app, "#batch-summary"))
    asyncio.run(scenario())


def test_slow_submit_responsive_and_exit_does_not_cancel():
    async def scenario():
        manager = FakeManager()
        manager.submit_release = threading.Event()
        app = TEFlowApp(manager=manager)
        async with app.run_test(size=(80, 24)) as pilot:
            app.query_one("#data-type", Select).value = "bulk"
            app.query_one("#accession", Input).value = "GSE1"
            await pilot.press("enter")
            await eventually(pilot, manager.submit_started.is_set)
            assert app.query_one("#start", Button).disabled
            await pilot.press("enter")
            app.query_one("#accession", Input).value = "/root/se"
            await pilot.press("end")
            await pilot.press("q")
            assert app.query_one("#accession", Input).value == "/root/seq"
            assert manager.submissions == ["GSE1"]
            await pilot.press("ctrl+q")
        manager.submit_release.set()
        deadline = asyncio.get_running_loop().time() + 3
        while "GSE1" not in manager.jobs:
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(.02)
        assert manager.jobs["GSE1"]["status"] == "running"
    asyncio.run(scenario())


def test_legacy_accession_validation_matches_runtime():
    async def scenario():
        manager = FakeManager()
        app = TEFlowApp(manager=manager)
        ids = ["SRR1", "SRP1", "SRS1", "SRX1", "SRA1", "ERR1", "ERP1", "ERA1", "DRR1", "PRJNA1", "PRJEB1", "PRJDB1", "GSE1", "GSM1"]
        async with app.run_test() as pilot:
            app.query_one("#data-type", Select).value = "bulk"
            for accession in ids:
                app.query_one("#accession", Input).value = accession.lower()
                await pilot.press("enter")
                await eventually(pilot, lambda: not app._submitting and manager.submissions[-1] == accession)
            app.query_one("#accession", Input).value = "E-MTAB-2836"
            await pilot.press("enter")
            await eventually(pilot, lambda: not app._submitting)
            assert manager.submissions == ids
            assert "E-MTAB-2836" in visible(app, "#feedback")
            app.query_one("#projects", Tree).focus()
            await pilot.press("q")
    asyncio.run(scenario())


def test_type_is_explicit_and_single_cell_does_not_submit():
    async def scenario():
        manager = FakeManager()
        app = TEFlowApp(manager=manager)
        async with app.run_test() as pilot:
            app.query_one("#accession", Input).value = "GSE1"
            await pilot.press("enter")
            assert "请选择数据类型" in visible(app, "#feedback")
            assert not manager.submissions
            app.query_one("#data-type", Select).value = "scrna"
            await pilot.pause()
            await pilot.click("#start")
            assert "单细胞分析流程待配置" in visible(app, "#feedback")
            assert not manager.submissions
            app.query_one("#data-type", Select).value = "bulk"
            await pilot.pause(.2)
            await pilot.click("#start")
            await eventually(pilot, lambda: manager.submissions == ["GSE1"])
    asyncio.run(scenario())
