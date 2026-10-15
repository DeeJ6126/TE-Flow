"""Selectable, read-only preview pages with explicit terminal clipboard actions.

Only the bounded page supplied by the result browser is held here.  Copying uses
Textual's OSC 52 support; the SSH client must permit terminal clipboard writes.
"""
from __future__ import annotations

import csv
from io import StringIO
from typing import Iterable, Sequence

from rich.cells import cell_len
from textual.binding import Binding
from textual.document._document import Selection
from textual.message import Message
from textual.widgets import TextArea


def _visible_cell(value: str) -> str:
    """Keep a table record on one display line without dropping cell content."""
    return value.replace("\\", "\\\\").replace("\r", "\\r").replace("\n", "\\n").replace("\t", "\\t")


def table_page(columns: Sequence[object], rows: Iterable[Sequence[object]]) -> tuple[str, str]:
    """Return an aligned display and valid TSV, both containing the current page.

    Alignment padding is limited to 60 cells per column, so a long path doesn't
    pad every other row to thousands of spaces.  Long values themselves remain
    intact; they can be read by scrolling horizontally.
    """
    records = [[str(value) for value in columns]]
    records.extend([str(value) for value in row] for row in rows)
    if not any(records):
        return "", ""
    visible = [[_visible_cell(value) for value in row] for row in records]
    count = max(map(len, visible), default=0)
    widths = [min(60, max((cell_len(row[index]) for row in visible if index < len(row)), default=0)) for index in range(count)]
    lines = []
    for row in visible:
        parts = []
        for index, value in enumerate(row):
            parts.append(value)
            if index < len(row) - 1:
                parts.append(" " * max(2, widths[index] - cell_len(value) + 2))
        lines.append("".join(parts))
    output = StringIO(newline="")
    writer = csv.writer(output, delimiter="\t", lineterminator="\n")
    writer.writerows(records)
    return "\n".join(lines), output.getvalue()


class SelectablePreview(TextArea):
    """A mouse-selectable file page which cannot edit its source or document.

    ``set_table`` aligns the parsed values for reading; ``copy_page`` copies valid
    TSV rather than layout padding.  ``Ctrl+C`` copies exactly the displayed
    selection.  Neither action sends content unless the user invokes it.
    """

    BINDINGS = [Binding("ctrl+c", "copy", "复制选中文字", show=False, priority=True)]
    DEFAULT_CSS = """
    SelectablePreview {
        border: none;
        padding: 0 1;
        height: 1fr;
    }
    SelectablePreview:focus { border: none; }
    """

    class Copied(Message):
        """A clipboard payload was sent; terminal acceptance is not observable."""

        def __init__(self, preview: SelectablePreview, scope: str, characters: int) -> None:
            super().__init__()
            self.preview, self.scope, self.characters = preview, scope, characters

        @property
        def control(self) -> SelectablePreview:
            return self.preview

    def __init__(self, text: str = "", **kwargs) -> None:
        # Read-only with the cursor retained allows keyboard selection as well as
        # mouse dragging.  Disabling the widget would disable selection entirely.
        kwargs.update(read_only=True, show_cursor=True, show_line_numbers=False, soft_wrap=False, highlight_cursor_line=False)
        super().__init__(text, **kwargs)
        self._page_copy_text = text
        self.cursor_blink = False

    def set_page(self, text: str, *, copy_text: str | None = None, preserve_view: bool = False) -> None:
        """Replace a page, optionally retaining its selection and scroll position."""
        self._page_copy_text = text if copy_text is None else copy_text
        if text == self.text:
            if not preserve_view:
                self.selection = Selection.cursor((0, 0))
                self.scroll_home(animate=False)
            return
        selection, scroll = self.selection, self.scroll_offset
        self.load_text(text)
        if preserve_view:
            def bound(location: tuple[int, int]) -> tuple[int, int]:
                row, column = location
                row = max(0, min(row, self.document.line_count - 1))
                return row, max(0, min(column, len(self.document.get_line(row))))

            self.selection = Selection(bound(selection.start), bound(selection.end))
            if self.is_mounted:
                self.call_after_refresh(lambda: self.scroll_to(x=scroll.x, y=scroll.y, animate=False))
        else:
            self.selection = Selection.cursor((0, 0))
            self.scroll_home(animate=False)

    def set_table(self, columns: Sequence[object], rows: Iterable[Sequence[object]], *, preserve_view: bool = False) -> None:
        display, tsv = table_page(columns, rows)
        self.set_page(display, copy_text=tsv, preserve_view=preserve_view)

    def action_copy(self) -> None:
        """Consume Ctrl+C and copy only a non-empty explicit selection."""
        text = self.selected_text
        if text:
            self.app.copy_to_clipboard(text)
            self.post_message(self.Copied(self, "selection", len(text)))

    def copy_page(self) -> bool:
        """Copy this bounded page, including table headers, on user request."""
        text = self._page_copy_text
        if not text:
            return False
        self.app.copy_to_clipboard(text)
        self.post_message(self.Copied(self, "page", len(text)))
        return True
