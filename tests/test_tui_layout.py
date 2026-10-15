"""Drag the actual dividers and check keyboard/mouse preview actions together."""
import asyncio

from textual import events
from textual.document._document import Selection
from textual.widgets import Button

from teflow.tui import TEFlowApp
from teflow.preview_widgets import SelectablePreview
from test_tui import FakeManager, eventually, select_node
from test_tui_browser import fixture_job, open_file


async def drag(pilot, app, selector, *, dx=0, dy=0):
    region = app.query_one(selector).region
    x, y = region.x, region.y
    await pilot._post_mouse_events([events.MouseDown], selector, button=1)
    await pilot._post_mouse_events([events.MouseMove], offset=(x + dx, y + dy), button=1)
    await pilot._post_mouse_events([events.MouseUp], offset=(x + dx, y + dy), button=1)
    await pilot.pause()


def test_all_dividers_resize_and_layout_survives_restart(tmp_path):
    async def scenario():
        preference = tmp_path / "ui-layout.json"
        app = TEFlowApp(manager=FakeManager(), layout_path=preference)
        async with app.run_test(size=(170, 50)) as pilot:
            await pilot.pause()
            entry = app.query_one("#entry").region
            widths = tuple(app.query_one(selector).size.width for selector in ("#history-panel", "#files-panel", "#right-panel"))
            await drag(pilot, app, "#history-splitter", dx=4)
            assert app.query_one("#history-panel").size.width == widths[0] + 4
            assert app.query_one("#files-panel").size.width == widths[1] - 4
            files = app.query_one("#files-panel").size.width
            await drag(pilot, app, "#files-splitter", dx=6)
            assert app.query_one("#files-panel").size.width == files + 6
            overview = app.query_one("#overview").size.height
            await drag(pilot, app, "#overview-splitter", dy=2)
            assert app.query_one("#overview").size.height == overview + 2
            activity = app.query_one("#activity-panel").size.height
            await drag(pilot, app, "#activity-splitter", dy=2)
            assert app.query_one("#activity-panel").size.height == activity + 2
            assert app.query_one("#entry").region == entry
            assert app.mouse_captured is None
            assert preference.is_file()
            saved = app.workspace_layout
        restored = TEFlowApp(manager=FakeManager(), layout_path=preference)
        assert restored.workspace_layout == saved
        async with restored.run_test(size=(80, 24)) as pilot:
            await pilot.pause()
            for selector in ("#history-panel", "#files-panel", "#right-panel", "#viewer"):
                region = restored.query_one(selector).region
                assert region.width > 0 and region.height > 0
                assert region.right <= 80 and region.bottom < 24
    asyncio.run(scenario())


def test_compact_default_overview_can_be_dragged_one_row(tmp_path):
    async def scenario():
        app = TEFlowApp(manager=FakeManager(), layout_path=tmp_path / "layout.json")
        async with app.run_test(size=(80, 24)) as pilot:
            await pilot.pause()
            assert app.query_one("#overview").region.height == 4
            await drag(pilot, app, "#overview-splitter", dy=1)
            assert app.query_one("#overview").region.height == 5
            assert app.workspace_layout.overview == 5
    asyncio.run(scenario())


def test_full_page_labels_copy_current_page_and_ctrl_c_selection(tmp_path):
    async def scenario():
        job, path = fixture_job(tmp_path)
        app = TEFlowApp(manager=FakeManager([job]))
        async with app.run_test(size=(120, 36)) as pilot:
            await eventually(pilot, lambda: app.selected_accession == "GSE1")
            await select_node(app, pilot, "GSE1", "SRR1")
            await open_file(app, pilot, path)
            assert str(app.query_one("#previous-page", Button).label) == "上一页"
            assert str(app.query_one("#next-page", Button).label) == "下一页"
            payloads = []
            notifications = []
            app.copy_to_clipboard = payloads.append
            app.notify = lambda *args, **kwargs: notifications.append((args, kwargs))
            await pilot.click("#copy-page")
            assert payloads[-1].startswith("gene\tSRR1\ngene0\t0\n")
            assert "gene100\t100" not in payloads[-1]
            preview = app.query_one("#preview-table", SelectablePreview)
            preview.focus()
            preview.selection = Selection((1, 0), (1, 5))
            await pilot.press("ctrl+c")
            assert payloads[-1] == "gene0" and app.is_running
            assert notifications == []
    asyncio.run(scenario())
