"""Directory mode switches files, while activity and logs keep their job scope."""
import asyncio
from pathlib import Path

import pytest
from textual.widgets import Button, TabbedContent, Tree

from teflow.preview_widgets import SelectablePreview
from teflow.result_tree import ResultFileTree
from teflow.tui import TEFlowApp, LiveLog
from test_tui import FakeManager, make_job, eventually, select_node, visible
from test_tui_browser import fixture_job, open_file


def input_fixture(tmp_path):
    job, count = fixture_job(tmp_path)
    raw = tmp_path / "raw" / "GSE1"
    run = raw / "SRR1"
    run.mkdir(parents=True)
    fastq = run / "SRR1_1.fastq.gz"
    fastq.write_bytes(b"compressed FASTQ")
    (run / "SRR1_2.fastq.gz").write_bytes(b"compressed FASTQ")
    metadata = raw / "download.metadata.tsv"
    metadata.write_text("run\tlayout\nSRR1\tPAIRED\n", encoding="utf-8")
    job["raw_dir"] = str(raw)
    return job, count, fastq, metadata


def test_default_output_toggle_input_and_restore_file_page(tmp_path):
    async def scenario():
        job, count, fastq, metadata = input_fixture(tmp_path)
        manager = FakeManager([job])
        original = dict(manager.jobs["GSE1"])
        app = TEFlowApp(manager=manager)
        async with app.run_test(size=(170, 50)) as pilot:
            await eventually(pilot, lambda: app.selected_accession == "GSE1")
            assert app.directory_source == "output"
            assert str(app.query_one("#directory-source", Button).label) == "输出目录 ⇄"
            await select_node(app, pilot, "GSE1", "SRR1")
            await open_file(app, pilot, count)
            await pilot.click("#next-page")
            await eventually(pilot, lambda: app.preview_page == 1 and "第 2 页" in visible(app, "#preview-title"))
            output_preview = app.query_one("#preview-table", SelectablePreview)
            output_preview.move_cursor((40, 0))
            output_preview.scroll_to(y=25, animate=False)
            await pilot.pause()
            previous_scroll = output_preview.scroll_y
            await pilot.click("#directory-source")
            await eventually(pilot, lambda: app.directory_source == "input" and str(fastq) in app.query_one("#files", ResultFileTree).file_nodes)
            assert app.selected_run == "SRR1" and app.selected_file is None
            assert not output_preview.text
            assert str(count) not in app.query_one("#files", ResultFileTree).file_nodes
            assert ("file", "GSE1", None, str(metadata)) in app._project_nodes
            await open_file(app, pilot, fastq)
            assert "预览不解压" in app.query_one("#preview-text", SelectablePreview).text
            await pilot.click("#directory-source")
            await eventually(pilot, lambda: app.directory_source == "output" and app.selected_file == count and app._preview_signature is not None)
            await pilot.pause()
            assert app.preview_page == 1
            assert output_preview.scroll_y == previous_scroll
            assert output_preview.cursor_location[0] == 40
            assert manager.jobs["GSE1"] == original and manager.submissions == []
            await pilot.click("#directory-source")
            await eventually(pilot, lambda: app.selected_file == fastq and app._preview_signature is not None)
            assert app.preview_page == 0
    asyncio.run(scenario())


def test_switch_keeps_project_tree_cursor_on_selected_run_across_newer_project(tmp_path):
    async def scenario():
        job, _, fastq, _ = input_fixture(tmp_path)
        newer = make_job("GSE9", created="2099")
        newer.update(job_dir=str(tmp_path/"newer-out"),raw_dir=str(tmp_path/"newer-raw"))
        app = TEFlowApp(manager=FakeManager([job,newer]))
        async with app.run_test(size=(120,36)) as pilot:
            await select_node(app,pilot,"GSE1","SRR1")
            await pilot.click("#directory-source")
            tree = app.query_one("#projects",Tree)
            expected = ("run","GSE1","SRR1",None)
            await eventually(pilot,lambda:app.directory_source=="input" and
                             str(fastq) in app.query_one("#files",ResultFileTree).file_nodes)
            await eventually(pilot,lambda:tree.cursor_node is not None and tree.cursor_node.data==expected)
            assert (app.selected_accession,app.selected_run)==("GSE1","SRR1")
    asyncio.run(scenario())


def test_input_project_files_logs_and_results_never_mix(tmp_path):
    async def scenario():
        job, count, fastq, metadata = input_fixture(tmp_path)
        app = TEFlowApp(manager=FakeManager([job]))
        async with app.run_test(size=(120, 36)) as pilot:
            await eventually(pilot, lambda: app.selected_accession == "GSE1")
            await pilot.click("#directory-source")
            await eventually(pilot, lambda: str(metadata) in app.query_one("#files", ResultFileTree).file_nodes)
            await open_file(app, pilot, metadata)
            assert app.query_one("#preview-table", SelectablePreview).text.splitlines()[1].split() == ["SRR1", "PAIRED"]
            assert "输入 · GSE1" in visible(app, "#files-title")
            await select_node(app, pilot, "GSE1", "SRR1")
            await pilot.press("f2")
            await eventually(pilot, lambda: app.logs_visible and bool(app.query_one("#logs", LiveLog).lines))
            assert "detailed count" in str(app.query_one("#logs", LiveLog).lines)
            assert app.query_one("#files", ResultFileTree).file_count == 2
            assert app.query_one("#files", ResultFileTree).display
            overview = visible(app, "#overview")
            logs_before = app.query_one("#logs", LiveLog).lines.copy()
            await pilot.click("#directory-source")
            await eventually(pilot, lambda: str(count) in app.query_one("#files", ResultFileTree).file_nodes)
            assert app.logs_visible and app.query_one("#viewer", TabbedContent).active == "log-pane"
            assert visible(app, "#overview") == overview
            assert app.query_one("#logs", LiveLog).lines == logs_before
    asyncio.run(scenario())


def test_input_preview_cannot_keep_old_output_on_scope_switch(tmp_path):
    async def scenario():
        job, count, fastq, metadata = input_fixture(tmp_path)
        app = TEFlowApp(manager=FakeManager([job]))
        async with app.run_test(size=(120, 36)) as pilot:
            await eventually(pilot, lambda: app.selected_accession == "GSE1")
            await select_node(app, pilot, "GSE1", "SRR1")
            await open_file(app, pilot, count)
            await pilot.click("#directory-source")
            await eventually(pilot, lambda: app.directory_source == "input" and "输入 · SRR1" in visible(app, "#files-title"))
            assert app.selected_file is None and app._preview_signature is None
            assert "gene" not in app.query_one("#preview-text", SelectablePreview).text
            assert app.query_one("#copy-page", Button).disabled
    asyncio.run(scenario())


@pytest.mark.parametrize("source", ["output", "input"])
def test_compact_directory_toggle_and_preview_remain_usable(tmp_path, source):
    async def scenario():
        job, count, fastq, metadata = input_fixture(tmp_path)
        app = TEFlowApp(manager=FakeManager([job]))
        async with app.run_test(size=(80, 24)) as pilot:
            await eventually(pilot, lambda: app.selected_accession == "GSE1")
            await select_node(app, pilot, "GSE1", "SRR1")
            if source == "input":
                await pilot.click("#directory-source")
                await eventually(pilot, lambda: app.directory_source == "input")
            await open_file(app, pilot, fastq if source == "input" else count)
            preview = app.query_one("#preview-text" if source == "input" else "#preview-table", SelectablePreview)
            button = app.query_one("#directory-source", Button)
            assert button.region.width >= 12 and button.region.height == 1
            assert button.region.bottom < 24 and preview.region.height >= 3
            await pilot.press("f2")
            await eventually(pilot, lambda: app.logs_visible)
            assert app.query_one("#logs", LiveLog).region.height >= 3
    asyncio.run(scenario())
