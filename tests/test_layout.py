from pathlib import Path

import pytest

from teflow.layout import WorkspaceLayout


@pytest.mark.parametrize("width", [50, 78, 100, 168, 240])
def test_divider_extremes_keep_all_panes_visible(width):
    for divider in ("history-splitter", "files-splitter"):
        for delta in (-1000, 1000):
            layout = WorkspaceLayout()
            layout.drag_columns(divider, delta, layout.columns(width), width)
            columns = layout.columns(width)
            assert sum(columns) == width - 2
            assert all(actual >= minimum for actual, minimum in zip(columns, layout.minimum_columns(width)))


def test_layout_roundtrip_and_bad_preferences(tmp_path):
    target = tmp_path / "ui-layout.json"
    layout = WorkspaceLayout(history=.24, files=.26, overview=6, activity=.36)
    layout.save(target)
    assert WorkspaceLayout.load(target) == layout
    for content in ('null', '[]', '{"history": "oops"}', '{"activity": NaN}', '{"overview": true}'):
        target.write_text(content)
        assert WorkspaceLayout.load(target) == WorkspaceLayout()
    assert not list(tmp_path.glob("*.tmp"))
    extreme = WorkspaceLayout(history=.88, files=.09, overview=40, activity=.9)
    extreme.save(target)
    assert WorkspaceLayout.load(target) == extreme
