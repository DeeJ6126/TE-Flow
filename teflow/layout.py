"""Mouse dividers and small, separately stored workspace preferences."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import math
from pathlib import Path
import uuid

from rich.text import Text
from textual import events
from textual.message import Message
from textual.widgets import Static


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(value, max(minimum, maximum)))


@dataclass
class WorkspaceLayout:
    history: float = 0.16
    files: float = 0.20
    overview: int | None = None
    activity: float = 0.26

    @classmethod
    def load(cls, path: Path | None) -> WorkspaceLayout:
        if path is None:
            return cls()
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            result = cls(**{key: raw[key] for key in asdict(cls()) if key in raw})
            if not all(isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)
                       for key, v in asdict(result).items() if key != "overview" or v is not None):
                return cls()
            result.history = clamp(result.history, .001, .999)
            result.files = clamp(result.files, .001, .999)
            if result.overview is not None:
                result.overview = max(4, int(result.overview))
            result.activity = clamp(result.activity, .001, .999)
            return result
        except (OSError, ValueError, TypeError):
            return cls()

    def save(self, path: Path | None) -> None:
        if path is None:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        try:
            temporary.write_text(json.dumps(asdict(self), indent=2) + "\n", encoding="utf-8")
            temporary.chmod(0o664)
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
    def minimum_columns(width: int) -> tuple[int, int, int]:
        available = max(3, width - 2)
        if available >= 69:
            return 16, 18, 35
        # Keep every pane on screen when the terminal is temporarily narrowed.
        history = max(1, available * 16 // 69)
        files = max(1, available * 18 // 69)
        return history, files, max(1, available - history - files)

    def columns(self, width: int) -> tuple[int, int, int]:
        available = max(3, width - 2)
        mh, mf, mr = self.minimum_columns(width)
        history = int(clamp(round(available * self.history), mh, available - mf - mr))
        files = int(clamp(round(available * self.files), mf, available - history - mr))
        return history, files, available - history - files

    def rows(self, body_height: int, workspace_height: int, *, compact: bool = False) -> tuple[int, int]:
        overview = int(clamp(self.overview if self.overview is not None else 4 if compact else 5,
                             4, body_height - 12))
        activity = int(clamp(round(max(1, workspace_height - 1) * self.activity), 4, workspace_height - 7))
        return overview, activity

    def drag_columns(self, divider: str, delta: int, origin: tuple[int, int, int], width: int) -> None:
        h, f, r = origin
        mh, mf, mr = self.minimum_columns(width)
        if divider == "history-splitter":
            new_h = int(clamp(h + delta, mh, h + f - mf))
            h, f = new_h, h + f - new_h
        else:
            new_f = int(clamp(f + delta, mf, f + r - mr))
            f, r = new_f, f + r - new_f
        total = max(1, h + f + r)
        self.history, self.files = h / total, f / total


class Splitter(Static):
    """A one-cell divider captures mouse movement until the button is released."""
    DEFAULT_CSS = """
    Splitter { background: transparent; color: $text-muted; padding: 0; }
    Splitter.vertical { width: 1; height: 1fr; }
    Splitter.horizontal { height: 1; width: 1fr; }
    Splitter:hover, Splitter.dragging { color: $accent; }
    """

    class Dragged(Message):
        def __init__(self, splitter: Splitter, delta: int, *, beginning: bool = False) -> None:
            super().__init__()
            self.splitter, self.delta, self.beginning = splitter, delta, beginning

    class Finished(Message):
        pass

    def __init__(self, *, vertical: bool, id: str) -> None:
        super().__init__("│" if vertical else "─", id=id, classes="vertical" if vertical else "horizontal")
        self.vertical = vertical
        self._start: int | None = None
        self.tooltip = "按住鼠标拖动，调整相邻区域大小"

    def render(self) -> Text:
        if self.vertical:
            return Text("\n".join("│" for _ in range(self.size.height)), no_wrap=True)
        return Text("─" * self.size.width, no_wrap=True)

    def on_mouse_down(self, event: events.MouseDown) -> None:
        if event.button != 1:
            return
        event.stop()
        event.prevent_default()
        self._start = event.screen_x if self.vertical else event.screen_y
        self.capture_mouse()
        self.add_class("dragging")
        self.post_message(self.Dragged(self, 0, beginning=True))

    def on_mouse_move(self, event: events.MouseMove) -> None:
        if self._start is not None:
            event.stop()
            coordinate = event.screen_x if self.vertical else event.screen_y
            self.post_message(self.Dragged(self, coordinate - self._start))

    def on_mouse_up(self, event: events.MouseUp) -> None:
        if self._start is not None:
            event.stop()
            coordinate = event.screen_x if self.vertical else event.screen_y
            self.post_message(self.Dragged(self, coordinate - self._start))
            self._finish()

    def on_mouse_release(self, event: events.MouseRelease) -> None:
        if self._start is not None:
            self._finish()

    def _finish(self) -> None:
        self._start = None
        self.remove_class("dragging")
        self.release_mouse()
        self.post_message(self.Finished())

    def on_click(self, event: events.Click) -> None:
        event.stop()
