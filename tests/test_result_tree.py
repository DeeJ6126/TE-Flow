"""Result-file navigation tests; no downloads or scientific processes."""
from __future__ import annotations

import asyncio
from pathlib import Path

from textual.app import App, ComposeResult

from teflow.result_tree import ResultFileTree
from teflow.results import ResultFile


def result(relative: str, *, size: int = 10, root: str = "/result/SRR1") -> ResultFile:
    return ResultFile(Path(root) / relative, relative, relative.rsplit("/", 1)[-1],
                      "text", size, size)


class TreeApp(App):
    def __init__(self, files):
        super().__init__()
        self.files = files
        self.selected = []
        self.raw_selections = []

    def compose(self) -> ComposeResult:
        yield ResultFileTree(id="files")

    def on_mount(self):
        self.query_one(ResultFileTree).update_files(self.files, scope_key="SRR1")

    def on_result_file_tree_file_selected(self, event):
        self.selected.append(event.entry)
        assert event.control is self.query_one(ResultFileTree)
        assert event.path == event.entry.path

    def on_tree_node_selected(self, event):
        self.raw_selections.append(event.node)


def test_directory_click_and_enter_expand_without_preview_then_file_emits():
    async def scenario():
        count = result("abundance/SRR1.cntTable")
        files = [result("align/a.log"), result("align/nested/detail.log"), result("align/nested/other.log"), count,
                 result("abundance/summary.tsv"),
                 result("logs/a.log"), result("qc/report.html"), result("run.log")]
        app = TreeApp(files)
        async with app.run_test(size=(80, 24)) as pilot:
            tree = app.query_one(ResultFileTree)
            assert [node.label.plain for node in tree.root.children] == ["abundance", "align", "logs/a.log", "qc/report.html", "run.log"]
            assert not any(node.is_expanded for node in tree.directory_nodes.values())
            await pilot.click("#files", offset=(5, 0))
            await pilot.pause()
            assert tree.directory_nodes["abundance"].is_expanded
            assert not app.selected
            await pilot.click("#files", offset=(8, 1))
            await pilot.pause()
            assert app.selected == [count]
            assert not app.raw_selections
            tree.move_cursor(tree.directory_nodes["abundance"])
            await pilot.press("enter")
            assert not tree.directory_nodes["abundance"].is_expanded
            assert app.selected == [count]
            tree.move_cursor(tree.directory_nodes["align"])
            await pilot.press("enter")
            assert tree.directory_nodes["align"].is_expanded
            assert not tree.directory_nodes["align/nested"].is_expanded
            # The disclosure indicator has its own Textual mouse action.
            await pilot.click("#files", offset=(0, 1))
            assert not tree.directory_nodes["align"].is_expanded
            assert app.selected == [count]
    asyncio.run(scenario())


def test_refresh_preserves_folders_cursor_and_scroll_then_scope_resets():
    async def scenario():
        files = [result(f"align/log{i:03}.out") for i in range(80)] + [result("qc/report.html")]
        app = TreeApp(files)
        async with app.run_test(size=(40, 15)) as pilot:
            tree = app.query_one(ResultFileTree)
            assert tree.select_path(files[50].path)
            await pilot.pause()
            assert tree.cursor_node.data.entry == files[50]
            assert tree.directory_nodes["align"].is_expanded
            tree.scroll_to(x=2, y=30, animate=False, force=True)
            await pilot.pause()
            old_scroll = (tree.scroll_x, tree.scroll_y)
            node = tree.file_nodes[str(files[50].path)]
            assert not tree.update_files(files, scope_key="SRR1")
            assert tree.file_nodes[str(files[50].path)] is node
            more = [*files, result("align/new.out")]
            assert tree.update_files(more, scope_key="SRR1")
            await pilot.pause()
            assert tree.directory_nodes["align"].is_expanded
            assert "qc" not in tree.directory_nodes
            assert tree.cursor_node.data.entry == files[50]
            assert (tree.scroll_x, tree.scroll_y) == old_scroll
            assert not app.selected
            tree.update_files([result("align/other.out", root="/result/SRR2"), result("align/second.out", root="/result/SRR2")], scope_key="SRR2")
            await pilot.pause()
            assert not tree.directory_nodes["align"].is_expanded
            assert tree.scroll_y == 0
            assert tree.file_count == 2
    asyncio.run(scenario())


def test_selected_path_reveals_ancestors_without_preview_or_refresh_reopen():
    async def scenario():
        leaf = result("align/nested/detail.log")
        files = [leaf, result("align/nested/other.log")]
        app = TreeApp(files)
        async with app.run_test() as pilot:
            tree = app.query_one(ResultFileTree)
            tree.update_files(files, selected_path=leaf.path, scope_key="SRR1")
            await pilot.pause()
            assert all(node.is_expanded for node in tree.directory_nodes.values())
            assert tree.cursor_node.data.entry == leaf
            assert not app.selected
            tree.directory_nodes["align"].collapse()
            tree.update_files([*files, result("align/new.log")], scope_key="SRR1")
            await pilot.pause()
            assert not tree.directory_nodes["align"].is_expanded
            assert tree.cursor_node is tree.directory_nodes["align"]
            assert not tree.select_path(Path("/missing"))
    asyncio.run(scenario())


def test_metadata_only_refresh_keeps_nodes_and_emits_latest_entry():
    async def scenario():
        original = result("align/a.log", size=10)
        changed = result("align/a.log", size=20)
        app = TreeApp([original])
        async with app.run_test() as pilot:
            tree = app.query_one(ResultFileTree)
            tree.select_path(original.path)
            await pilot.pause()
            node = tree.file_nodes[str(original.path)]
            assert not tree.update_files([changed], scope_key="SRR1")
            assert tree.file_nodes[str(changed.path)] is node
            tree.focus()
            await pilot.press("enter")
            assert app.selected == [changed]
    asyncio.run(scenario())


def test_removed_cursor_uses_parent_and_clear_rejects_stale_restore():
    async def scenario():
        leaf = result("align/a.log")
        sibling = result("align/sibling.log")
        app = TreeApp([leaf, sibling])
        async with app.run_test() as pilot:
            tree = app.query_one(ResultFileTree)
            tree.select_path(leaf.path)
            await pilot.pause()
            tree.update_files([sibling, result("align/b.log")], scope_key="SRR1")
            await pilot.pause()
            assert tree.cursor_node is tree.directory_nodes["align"]
            tree.update_files([leaf], scope_key="SRR1")
            tree.select_path(leaf.path)
            tree.clear_files()
            await pilot.pause()
            assert tree.file_count == 0
            assert not tree.directory_nodes
            assert not tree.root.children
            assert tree.cursor_node is None
    asyncio.run(scenario())


def test_labels_are_literal_and_tree_only_contains_given_entries():
    async def scenario():
        files = [result("[bold]/[red].log"), result("../outside.log"), result("/absolute.log")]
        app = TreeApp(files)
        async with app.run_test() as pilot:
            tree = app.query_one(ResultFileTree)
            assert tree.file_count == 1
            assert not tree.directory_nodes
            assert next(iter(tree.file_nodes.values())).label.plain == "[bold]/[red].log"
    asyncio.run(scenario())


def test_listing_and_explicit_selection_can_be_prepared_before_mount():
    leaf = result("align/nested/a.log")
    tree = ResultFileTree(id="files")
    tree.clear_files()
    tree.update_files([leaf, result("align/nested/other.log")], selected_path=leaf.path, scope_key="SRR1")

    class PreparedApp(App):
        def compose(self):
            yield tree

    async def scenario():
        async with PreparedApp().run_test() as pilot:
            await pilot.pause()
            assert tree.cursor_node.data.entry == leaf
            assert tree.directory_nodes["align/nested"].is_expanded
    asyncio.run(scenario())


def test_singleton_leaf_uses_relative_path_and_mouse_click_opens_directly():
    async def scenario():
        count = result("abundance/SRR1.cntTable")
        app = TreeApp([count])
        async with app.run_test() as pilot:
            tree = app.query_one(ResultFileTree)
            assert not tree.directory_nodes
            leaf = tree.file_nodes[str(count.path)]
            assert leaf.label.plain == "abundance/SRR1.cntTable"
            assert leaf.parent is tree.root and not leaf.allow_expand
            await pilot.click("#files", offset=(12, 0))
            assert app.selected == [count]
            assert not app.raw_selections
    asyncio.run(scenario())


def test_nested_singletons_are_relative_to_nearest_multifile_folder():
    async def scenario():
        nested = result("align/nested/deeper/detail.log")
        other = result("align/other.log")
        unrelated = result("qc/only/report.html")
        app = TreeApp([nested, other, unrelated])
        async with app.run_test() as pilot:
            tree = app.query_one(ResultFileTree)
            assert list(tree.directory_nodes) == ["align"]
            assert tree.file_nodes[str(nested.path)].label.plain == "nested/deeper/detail.log"
            assert tree.file_nodes[str(nested.path)].parent is tree.directory_nodes["align"]
            assert tree.file_nodes[str(unrelated.path)].label.plain == "qc/only/report.html"
            assert tree.select_path(nested.path)
            await pilot.pause()
            assert tree.directory_nodes["align"].is_expanded
            assert tree.cursor_node.data.entry == nested
            assert not app.selected
    asyncio.run(scenario())


def test_singleton_multifile_transitions_preserve_file_cursor_and_other_expansions():
    async def scenario():
        leaf = result("abundance/nested/one.cntTable")
        added = result("abundance/nested/two.cntTable")
        others = [result("align/a.log"), result("align/b.log"), result("qc/a.json"), result("qc/b.json")]
        app = TreeApp([leaf, *others])
        async with app.run_test() as pilot:
            tree = app.query_one(ResultFileTree)
            tree.directory_nodes["align"].expand()
            tree.select_path(leaf.path)
            await pilot.pause()
            assert tree.cursor_node.data.entry == leaf
            assert tree.file_nodes[str(leaf.path)].label.plain == "abundance/nested/one.cntTable"
            tree.update_files([leaf, added, *others], scope_key="SRR1")
            await pilot.pause()
            assert tree.cursor_node.data.entry == leaf
            assert tree.directory_nodes["abundance"].is_expanded
            assert tree.directory_nodes["abundance/nested"].is_expanded
            assert tree.file_nodes[str(leaf.path)].label.plain == "one.cntTable"
            assert tree.directory_nodes["align"].is_expanded
            assert not tree.directory_nodes["qc"].is_expanded
            tree.update_files([leaf, *others], scope_key="SRR1")
            await pilot.pause()
            assert tree.cursor_node.data.entry == leaf
            assert tree.file_nodes[str(leaf.path)].label.plain == "abundance/nested/one.cntTable"
            assert "abundance" not in tree.directory_nodes
            assert "abundance/nested" not in tree.directory_nodes
            assert tree.directory_nodes["align"].is_expanded
            assert not tree.directory_nodes["qc"].is_expanded
            assert not app.selected
    asyncio.run(scenario())


def test_shrinking_highlighted_folder_moves_cursor_to_its_single_file():
    async def scenario():
        leaf = result("abundance/one.cntTable")
        added = result("abundance/two.cntTable")
        app = TreeApp([result("align/a.log"), result("align/b.log"), leaf, added])
        async with app.run_test() as pilot:
            tree = app.query_one(ResultFileTree)
            tree.move_cursor(tree.directory_nodes["abundance"])
            tree.update_files([result("align/a.log"), result("align/b.log"), leaf], scope_key="SRR1")
            await pilot.pause()
            assert tree.cursor_node.data.entry == leaf
            assert not app.selected
    asyncio.run(scenario())
