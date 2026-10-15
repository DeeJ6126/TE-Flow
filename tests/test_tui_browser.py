"""User interactions against real temporary result files; no scientific jobs."""
from __future__ import annotations

import asyncio
from pathlib import Path

from textual.widgets import Button, Static, TabbedContent, Tree
from teflow.result_tree import ResultFileTree
from teflow.preview_widgets import SelectablePreview

from teflow.tui import TEFlowApp, LiveLog
from test_tui import FakeManager, make_job, visible, eventually, select_node


def fixture_job(tmp_path, accession="GSE1", run="SRR1", status="running"):
    root = tmp_path / accession
    abundance = root / run / "abundance"
    abundance.mkdir(parents=True)
    count = abundance / f"{run}.cntTable"
    count.write_text("gene\t" + run + "\n" + "\n".join(f"gene{i}\t{i}" for i in range(250)) + "\n", encoding="utf-8")
    (root / "merged.count").write_text("gene\t" + run + "\nfeature\t9\n", encoding="utf-8")
    (root / "samples.tsv").write_text("run_accession\n" + run + "\n", encoding="utf-8")
    (root / "run.log").write_text("\n".join(f"project log {i}" for i in range(100)) + "\n", encoding="utf-8")
    (root / run / "logs").mkdir()
    (root / run / "logs" / f"{run}.TEcount.log").write_text("\n".join(f"{run} detailed count {i}" for i in range(100)) + "\n", encoding="utf-8")
    job = make_job(accession, status=status, stage="analyze", created="2026-10-14")
    job.update(job_dir=str(root), result_path=str(root / "merged.count"), current_sample=run, total_runs=1, downloaded_runs=1)
    return job, count


async def open_file(app, pilot, path, *, click=False):
    tree = app.query_one("#files", ResultFileTree)
    await eventually(pilot, lambda: str(path) in tree.file_nodes)
    tree.focus()
    tree.select_path(path)
    await pilot.pause()
    if click:
        await pilot.click("#files", offset=(8, tree.cursor_line - int(tree.scroll_y)))
    else:
        await pilot.press("enter")
    await eventually(pilot, lambda: app.selected_file == path and app._preview_signature is not None)


def test_tree_scopes_file_click_tabs_paging_and_scroll_retention(tmp_path):
    async def scenario():
        job, count = fixture_job(tmp_path)
        app = TEFlowApp(manager=FakeManager([job]))
        async with app.run_test(size=(170, 50)) as pilot:
            await eventually(pilot, lambda: len(app._project_nodes) >= 4)
            # A real mouse selection of the expanded folder is equivalent to Enter.
            await pilot.click("#projects", offset=(6, 1))
            await eventually(pilot, lambda: app.selected_run == "SRR1")
            await eventually(pilot, lambda: "SRR1 · 已完成" in visible(app, "#activity"))
            assert "● 分析数据" in visible(app, "#overview")
            await open_file(app, pilot, count, click=True)
            table = app.query_one("#preview-table", SelectablePreview)
            assert len(table.text.splitlines()) == 101
            assert app.query_one("#viewer", TabbedContent).active == "preview-pane"
            await pilot.click("#next-page")
            await eventually(pilot, lambda: app.preview_page == 1 and "第 2 页" in visible(app, "#preview-title"))
            assert table.text.splitlines()[1].split()[0] == "gene100"
            table.focus()
            table.move_cursor((60, 0))
            table.scroll_to(y=40, animate=False)
            await pilot.pause()
            scroll = table.scroll_y
            await pilot.press("f2")
            await eventually(pilot, lambda: app.logs_visible and bool(app.query_one("#logs", LiveLog).lines))
            assert app.query_one("#files", ResultFileTree).file_count >= 2
            assert app.query_one("#files", ResultFileTree).display
            await pilot.press("f2")
            await eventually(pilot, lambda: not app.logs_visible)
            app.action_refresh()
            await pilot.pause(.15)
            assert app.preview_page == 1 and table.scroll_y == scroll
            assert table.cursor_location[0] == 60
            oldsignature = app._preview_signature
            with count.open("a", encoding="utf-8") as handle:
                handle.write("appended\t251\n")
            app.action_refresh()
            await eventually(pilot, lambda: app._preview_signature != oldsignature)
            await pilot.pause()
            assert app.preview_page == 1 and table.scroll_y == scroll
            assert table.cursor_location[0] == 60 and app.selected_file == count
            await select_node(app, pilot, "GSE1")
            await eventually(pilot, lambda: app.selected_file is None and "GSE1" in visible(app, "#files-title"))
            assert not table.text
            merged = Path(job["job_dir"]) / "merged.count"
            node = app._project_nodes[("file", "GSE1", None, str(merged))]
            tree = app.query_one("#projects", Tree)
            app._project_nodes[("project", "GSE1", None, None)].expand()
            await pilot.pause()
            tree.focus()
            tree.move_cursor(node)
            await pilot.press("enter")
            await eventually(pilot, lambda: app.selected_file == merged and len(table.text.splitlines()) == 2)
            assert table.text.splitlines()[1].split()[0] == "feature"
            files = app.query_one("#files", ResultFileTree)
            assert files.cursor_node == files.file_nodes[str(merged)]
    asyncio.run(scenario())


def test_live_files_refresh_preserves_selection_and_hides_internals(tmp_path):
    async def scenario():
        job, count = fixture_job(tmp_path)
        hidden = Path(job["job_dir"]) / "SRR1" / ".snakemake" / "internal.log"
        hidden.parent.mkdir()
        hidden.write_text("internal", encoding="utf-8")
        app = TEFlowApp(manager=FakeManager([job]))
        async with app.run_test(size=(120, 36)) as pilot:
            await eventually(pilot, lambda: app.selected_accession == "GSE1")
            await select_node(app, pilot, "GSE1", "SRR1")
            await open_file(app, pilot, count)
            files = app.query_one("#files", ResultFileTree)
            assert str(hidden) not in files.file_nodes
            newfile = count.parent / "new.tsv"
            newfile.write_text("feature\tcount\na\t1\n", encoding="utf-8")
            oldrows = files.file_count
            app.action_refresh()
            await eventually(pilot, lambda: files.file_count == oldrows + 1 and files.cursor_node == files.file_nodes.get(str(count)))
            assert app.selected_file == count
            assert files.cursor_node == files.file_nodes[str(count)]
            tree = app.query_one("#projects", Tree)
            rootnode = app._project_nodes[("project", "GSE1", None, None)]
            rootnode.collapse()
            newroot = Path(job["job_dir"]) / "new-root.txt"
            newroot.write_text("root", encoding="utf-8")
            app.action_refresh()
            await eventually(pilot, lambda: ("file", "GSE1", None, str(newroot)) in app._project_nodes)
            assert not app._project_nodes[("project", "GSE1", None, None)].is_expanded
            await pilot.press("f3")
            await eventually(pilot, lambda: str(hidden) in files.file_nodes)
            assert app.selected_file == count
            assert not app._project_nodes[("project", "GSE1", None, None)].is_expanded
    asyncio.run(scenario())


def test_log_follow_pause_end_and_file_click_return(tmp_path):
    async def scenario():
        job, count = fixture_job(tmp_path)
        app = TEFlowApp(manager=FakeManager([job]))
        async with app.run_test(size=(120, 36)) as pilot:
            await eventually(pilot, lambda: app.selected_accession == "GSE1")
            await select_node(app, pilot, "GSE1", "SRR1")
            await pilot.press("f2")
            logs = app.query_one("#logs", LiveLog)
            await eventually(pilot, lambda: bool(logs.lines))
            logs.focus()
            await pilot.press("pageup")
            await eventually(pilot, lambda: not logs.following)
            oldsignature = app._last_log
            oldscroll = logs.scroll_y
            leaf = Path(job["job_dir"]) / "SRR1" / "logs" / "SRR1.TEcount.log"
            with leaf.open("a", encoding="utf-8") as handle:
                handle.write("newest count line\n")
            app.action_refresh()
            await pilot.pause(.15)
            assert app._last_log == oldsignature and logs.scroll_y == oldscroll
            assert "已暂停" in visible(app, "#log-state")
            await pilot.press("end")
            await eventually(pilot, lambda: logs.following and app._last_log != oldsignature)
            await open_file(app, pilot, count, click=True)
            await eventually(pilot, lambda: not app.logs_visible)
            assert app.query_one("#files", ResultFileTree).file_count >= 2
    asyncio.run(scenario())


def test_scope_change_never_leaks_old_preview_or_logs_and_empty_state(tmp_path):
    async def scenario():
        job, count = fixture_job(tmp_path)
        empty = make_job("GSE2", status="failed", created="2025")
        root = tmp_path / "GSE2"
        root.mkdir()
        (root / "samples.tsv").write_text("run_accession\nSRR9\n", encoding="utf-8")
        empty.update(job_dir=str(root), error="download failed")
        app = TEFlowApp(manager=FakeManager([job, empty]))
        async with app.run_test(size=(120, 36)) as pilot:
            await eventually(pilot, lambda: app.selected_accession == "GSE1")
            await select_node(app, pilot, "GSE1", "SRR1")
            await open_file(app, pilot, count)
            await pilot.press("f2")
            await eventually(pilot, lambda: bool(app.query_one("#logs", LiveLog).lines))
            app.query_one("#logs", LiveLog).pause_follow()
            await eventually(pilot, lambda: "已暂停" in visible(app, "#log-state"))
            await select_node(app, pilot, "GSE2", "SRR9")
            await eventually(pilot, lambda: "尚未生成" in visible(app, "#files-empty"))
            assert app.selected_file is None
            assert not app.query_one("#preview-table", SelectablePreview).text
            assert app.query_one("#files", ResultFileTree).file_count == 0
            assert app.logs_visible
            assert app.query_one("#logs", LiveLog).following
            assert "自动跟随最新" in visible(app, "#log-state")
            assert "GSE2" in visible(app, "#overview")
            await pilot.press("f2")
            await eventually(pilot, lambda: not app.logs_visible)
            assert "选择文件" in visible(app, "#preview-title")
    asyncio.run(scenario())


def test_binary_summary_and_deleted_file_error(tmp_path):
    async def scenario():
        job, count = fixture_job(tmp_path)
        bam = count.parent / "test.bam"
        bam.write_bytes(b"BAM\x01\x00binary")
        app = TEFlowApp(manager=FakeManager([job]))
        async with app.run_test(size=(120, 36)) as pilot:
            await eventually(pilot, lambda: app.selected_accession == "GSE1")
            await select_node(app, pilot, "GSE1", "SRR1")
            await open_file(app, pilot, bam)
            assert not app.query_one("#preview-table", SelectablePreview).display
            assert app.query_one("#preview-text", SelectablePreview).text
            await open_file(app, pilot, count)
            count.unlink()
            app.action_refresh()
            await eventually(pilot, lambda: app.query_one("#preview-text", SelectablePreview).display)
            assert not app.query_one("#preview-table", SelectablePreview).display
            assert app.query_one("#next-page", Button).disabled
    asyncio.run(scenario())


def test_compact_view_has_visible_rows_logs_and_latest_event(tmp_path, monkeypatch):
    monkeypatch.setattr("teflow.tui.activity_snapshot", lambda job, run=None, limit=5: {
        "status": "succeeded", "step": "完成", "elapsed_seconds": None,
        "events": [{"time": "", "message": label, "level": "info", "run": run}
                   for label in ["开始质控", "质控完成", "开始比对", "比对完成", "TEcount 完成"]],
    })
    async def scenario():
        job, count = fixture_job(tmp_path)
        app = TEFlowApp(manager=FakeManager([job]))
        async with app.run_test(size=(80, 24)) as pilot:
            await eventually(pilot, lambda: app.selected_accession == "GSE1")
            await select_node(app, pilot, "GSE1", "SRR1")
            await open_file(app, pilot, count)
            await pilot.pause()
            table = app.query_one("#preview-table", SelectablePreview)
            assert table.region.height >= 3, table.region
            assert table.region.bottom < 24
            assert "TEcount 完成" in visible(app, "#activity")
            assert "TEcount 完成" in visible(app, "#activity").splitlines()[0]
            assert "开始质控" not in visible(app, "#activity")
            assert not app.query_one("#preview-notice", Static).display
            await pilot.press("f2")
            await eventually(pilot, lambda: app.logs_visible)
            await pilot.pause()
            logs = app.query_one("#logs", LiveLog)
            assert logs.region.height >= 3, logs.region
            assert logs.region.bottom < 24
    asyncio.run(scenario())
