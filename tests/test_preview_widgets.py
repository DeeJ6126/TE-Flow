"""Mouse selection and exact, user-invoked clipboard payloads for file pages."""
from __future__ import annotations

import asyncio
import base64
import csv
from io import StringIO

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.document._document import Selection

from teflow.preview_widgets import SelectablePreview, table_page


class PreviewApp(App):
    BINDINGS = [Binding("ctrl+c", "quit", "Quit")]

    def __init__(self, content="first sentence\n第二行 alpha beta\nlast line"):
        super().__init__()
        self.preview = SelectablePreview(content, id="preview")
        self.copies = []

    def compose(self) -> ComposeResult:
        yield self.preview

    def on_mount(self):
        self.preview.focus()

    def on_selectable_preview_copied(self, message):
        self.copies.append((message.scope, message.characters))


def test_mouse_drag_copies_text_and_ctrl_c_never_quits():
    async def scenario():
        app = PreviewApp()
        async with app.run_test(size=(90, 20)) as pilot:
            preview = app.preview
            await pilot.mouse_down("#preview", offset=(1, 0))
            await pilot.hover("#preview", offset=(6, 0))
            await pilot.mouse_up("#preview", offset=(6, 0))
            await pilot.pause()
            assert preview.selected_text == "first"
            assert app.clipboard == ""  # Selecting alone doesn't write a clipboard.
            await pilot.press("ctrl+c")
            await pilot.pause()
            assert app.clipboard == "first"
            assert app.copies == [("selection", 5)]
            assert app.is_running
            preview.selection = Selection.cursor((0, 0))
            await pilot.press("ctrl+c")
            assert app.is_running and app.clipboard == "first"
    asyncio.run(scenario())


def test_unicode_mouse_selection_uses_terminal_cell_widths():
    async def scenario():
        app = PreviewApp("第二行 alpha beta")
        async with app.run_test(size=(70, 12)) as pilot:
            await pilot.mouse_down("#preview", offset=(1, 0))
            await pilot.hover("#preview", offset=(7, 0))
            await pilot.mouse_up("#preview", offset=(7, 0))
            await pilot.pause()
            assert app.preview.selected_text == "第二行"
            await pilot.press("ctrl+c")
            assert app.clipboard == "第二行"
    asyncio.run(scenario())


def test_table_selection_and_page_copy_are_distinct_and_page_is_exact_tsv():
    async def scenario():
        app = PreviewApp("")
        async with app.run_test(size=(100, 20)) as pilot:
            app.preview.set_table(["feature", "SRR1"], [["L1HS", "12"], ["ERVK", "7"]])
            await pilot.pause()
            await pilot.mouse_down("#preview", offset=(1, 1))
            await pilot.hover("#preview", offset=(5, 1))
            await pilot.mouse_up("#preview", offset=(5, 1))
            await pilot.pause()
            assert app.preview.selected_text == "L1HS"
            await pilot.press("ctrl+c")
            assert app.clipboard == "L1HS"
            assert app.preview.copy_page()
            await pilot.pause()
            assert app.clipboard == "feature\tSRR1\nL1HS\t12\nERVK\t7\n"
            assert app.copies[-1] == ("page", len(app.clipboard))
    asyncio.run(scenario())


def test_read_only_page_ignores_typing_delete_cut_and_paste():
    async def scenario():
        app = PreviewApp("original page")
        async with app.run_test(size=(80, 15)) as pilot:
            preview = app.preview
            preview.selection = Selection((0, 0), (0, 8))
            await pilot.press("x", "backspace", "delete", "ctrl+x")
            app.copy_to_clipboard("injected text")
            await pilot.press("ctrl+v")
            await pilot.pause()
            assert preview.text == "original page"
    asyncio.run(scenario())


def test_refresh_preserves_selection_scroll_and_no_automatic_copy():
    async def scenario():
        text = "\n".join(f"row {index} " + "x" * 150 for index in range(100))
        app = PreviewApp(text)
        async with app.run_test(size=(80, 15)) as pilot:
            preview = app.preview
            preview.selection = Selection((30, 0), (30, 6))
            preview.scroll_to(x=10, y=25, animate=False)
            await pilot.pause()
            original = preview.scroll_offset
            preview.set_page(text + "\nnew row", preserve_view=True)
            await pilot.pause()
            assert preview.selected_text == "row 30"
            assert preview.scroll_offset == original
            assert app.clipboard == "" and not app.copies
            preview.set_page("short", preserve_view=True)
            await pilot.pause()
            assert preview.selection.start == (0, 0)
            assert preview.selection.end == (0, 5)
            preview.set_page("replacement", preserve_view=False)
            assert preview.selected_text == ""
    asyncio.run(scenario())


def test_page_copy_sends_osc52_only_when_invoked():
    async def scenario():
        app = PreviewApp("file page with 中文\n")
        async with app.run_test(size=(80, 15)) as pilot:
            writes = []
            driver = app._driver
            original_write = driver.write
            driver.write = writes.append
            try:
                assert app.preview.copy_page()
                assert writes == ["\x1b]52;c;" + base64.b64encode("file page with 中文\n".encode()).decode() + "\a"]
                assert app.clipboard == "file page with 中文\n"
            finally:
                driver.write = original_write
            app.preview.set_page("")
            assert not app.preview.copy_page()
    asyncio.run(scenario())


def test_table_values_are_lossless_and_padding_bounded():
    cells = [["path" * 500, 'quoted\tvalue\nwith "text"'], ["中", "3"]]
    display, copied = table_page(["feature", "count"], cells)
    assert "path" * 500 in display
    assert '\\tvalue\\nwith "text"' in display
    assert list(csv.reader(StringIO(copied), delimiter="\t")) == [["feature", "count"], *cells]
    assert len(display.splitlines()[-1]) <= 65
    assert table_page([], []) == ("", "")
