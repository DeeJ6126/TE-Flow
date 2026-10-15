"""A directory tree over the result browser's already bounded file listing.

This widget never scans or opens paths. Only a file selection emits a preview
request; expanding a folder leaves the current preview alone.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable

from rich.text import Text
from textual.message import Message
from textual.widgets import Tree
from textual.widgets._tree import TreeNode

from .results import ResultFile


@dataclass(frozen=True)
class ResultNode:
    """Stable identity for restoring a node after a listing refresh."""

    kind: str
    key: str
    entry: ResultFile | None = None


class ResultFileTree(Tree[ResultNode]):
    """Keep result folders, selection and scroll stable during live refreshes.

    ``update_files`` consumes paths relative to the chosen job / Run scope.
    Folders containing multiple files begin collapsed. A folder containing just
    one listed descendant is folded into that file's relative-path label. Call
    ``select_path`` only for an explicit external file selection: it reveals the
    file without sending a second selection message, and therefore does not
    disturb preview tabs or paging on refresh.
    """

    DEFAULT_CSS = """
    ResultFileTree {
        height: 1fr;
        min-height: 1;
        scrollbar-size: 1 1;
    }
    """

    class FileSelected(Message):
        """A file was activated by mouse or Enter."""

        def __init__(self, tree: ResultFileTree, entry: ResultFile) -> None:
            self.tree = tree
            self.entry = entry
            self.path = entry.path
            super().__init__()

        @property
        def control(self) -> ResultFileTree:
            return self.tree

    def __init__(self, *, id: str | None = None, classes: str | None = None,
                 name: str | None = None, disabled: bool = False) -> None:
        super().__init__("结果", id=id, classes=classes, name=name, disabled=disabled)
        self.show_root = False
        self.show_guides = True
        self.auto_expand = False
        self.file_nodes: dict[str, TreeNode[ResultNode]] = {}
        self.directory_nodes: dict[str, TreeNode[ResultNode]] = {}
        self._listing_signature: tuple = ()
        self._scope_key: object = None
        self._view_revision = 0
        self._restore_pending: tuple | None = None
        self.root.expand()

    @property
    def file_count(self) -> int:
        return len(self.file_nodes)

    def clear_files(self, scope_key: object = None) -> None:
        self._scope_key = scope_key
        self._listing_signature = ()
        self._view_revision += 1
        self._restore_pending = None
        self.clear()
        self.root.expand()
        self.file_nodes.clear()
        self.directory_nodes.clear()
        self.move_cursor(None)
        if self.is_mounted:
            self.scroll_to(x=0, y=0, animate=False, force=True)

    @staticmethod
    def _parts(entry: ResultFile) -> tuple[str, ...]:
        relative = PurePosixPath(entry.relative)
        if relative.is_absolute() or ".." in relative.parts:
            return ()
        return relative.parts

    def update_files(self, files: Iterable[ResultFile],
                     selected_path: str | Path | None = None,
                     scope_key: object = None) -> bool:
        """Apply a listing and return whether its node structure changed.

        Metadata changes refresh node data without rebuilding the tree. Changes
        to the file set preserve expanded folders, the cursor and scroll within
        the same scope. A new scope starts with all folders collapsed.
        """
        unique = {str(item.path): item for item in files if self._parts(item)}
        entries = sorted(unique.values(), key=lambda item: (item.relative.casefold(), str(item.path)))
        signature = tuple((str(item.path), item.relative) for item in entries)
        same_scope = scope_key == self._scope_key
        if same_scope and signature == self._listing_signature:
            for item in entries:
                self.file_nodes[str(item.path)].data = ResultNode("file", str(item.path), item)
            if selected_path is not None:
                self.select_path(selected_path)
            return False

        expanded = {key for key, node in self.directory_nodes.items() if node.is_expanded} if same_scope else set()
        previous_directories = set(self.directory_nodes) if same_scope else set()
        cursor = self.cursor_node.data if same_scope and self.cursor_node else None
        scroll = (self.scroll_x, self.scroll_y) if same_scope else (0, 0)
        self._scope_key = scope_key
        self._listing_signature = signature
        self._view_revision += 1
        revision = self._view_revision
        self.clear()
        self.root.expand()
        self.move_cursor(None)
        self.file_nodes.clear()
        self.directory_nodes.clear()

        # Retain only directories grouping multiple listed descendant files.
        # A singleton chain becomes one leaf labelled relative to its nearest
        # retained parent, without requiring extra clicks to open that file.
        descendant_counts: dict[tuple[str, ...], int] = {}
        for item in entries:
            parts = self._parts(item)
            for depth in range(1, len(parts)):
                directory = parts[:depth]
                descendant_counts[directory] = descendant_counts.get(directory, 0) + 1
        directories = {parts for parts, count in descendant_counts.items() if count > 1}
        # Build directories first so every level lists folders before files.
        for parts in sorted(directories, key=lambda value: tuple(part.casefold() for part in value)):
            key = "/".join(parts)
            parent = self.root if len(parts) == 1 else self.directory_nodes["/".join(parts[:-1])]
            node = parent.add(Text(parts[-1]), ResultNode("directory", key), expand=key in expanded)
            self.directory_nodes[key] = node
        for item in entries:
            parts = self._parts(item)
            parent_depth = len(parts) - 1
            while parent_depth and parts[:parent_depth] not in directories:
                parent_depth -= 1
            parent = self.directory_nodes["/".join(parts[:parent_depth])] if parent_depth else self.root
            key = str(item.path)
            label = "/".join(parts[parent_depth:])
            self.file_nodes[key] = parent.add_leaf(Text(label), ResultNode("file", key, item))

        # If a singleton grows into a group while its file is highlighted,
        # reveal that file inside newly introduced folders. Existing folders
        # retain the user's expansion / collapse choices.
        if cursor is not None and cursor.kind == "file":
            node = self.file_nodes.get(cursor.key)
            ancestor = node.parent if node is not None else None
            while ancestor is not None and ancestor is not self.root:
                if ancestor.data is not None and ancestor.data.key not in previous_directories:
                    ancestor.expand()
                ancestor = ancestor.parent

        self._schedule_restore(cursor, scroll, revision)
        if selected_path is not None:
            self.select_path(selected_path)
        return True

    def _schedule_restore(self, cursor: ResultNode | None,
                          scroll: tuple[float, float] | None, revision: int) -> None:
        self._restore_pending = (cursor, scroll, revision)
        if self.is_mounted:
            self.call_after_refresh(self._restore_view, cursor, scroll, revision)

    def on_mount(self) -> None:
        if self._restore_pending is not None:
            self.call_after_refresh(self._restore_view, *self._restore_pending)

    def _restore_view(self, cursor: ResultNode | None,
                      scroll: tuple[float, float] | None, revision: int) -> None:
        if revision != self._view_revision:
            return
        self._restore_pending = None
        node = None
        if cursor is not None:
            nodes = self.file_nodes if cursor.kind == "file" else self.directory_nodes
            node = nodes.get(cursor.key)
            if node is None and cursor.kind == "directory":
                prefix = PurePosixPath(cursor.key).parts
                # When a multi-file folder shrinks to one file, keep the
                # cursor on that file rather than jumping to another branch.
                matches = [candidate for candidate in self.file_nodes.values()
                           if candidate.data is not None and candidate.data.entry is not None
                           and self._parts(candidate.data.entry)[:len(prefix)] == prefix]
                if len(matches) == 1:
                    node = matches[0]
            if node is None and cursor.entry is not None:
                parts = self._parts(cursor.entry)
                for depth in range(len(parts) - 1, 0, -1):
                    node = self.directory_nodes.get("/".join(parts[:depth]))
                    if node is not None:
                        break
        if node is None:
            node = next(iter(self.root.children), None)
        # A preserved cursor may now be hidden inside a manually closed folder.
        # Highlight its visible ancestor without reopening the user's folder.
        if node is not None:
            ancestor = node.parent
            while ancestor is not None and ancestor is not self.root:
                if not ancestor.is_expanded:
                    node = ancestor
                ancestor = ancestor.parent
        self.move_cursor(node)
        if scroll is not None:
            self.scroll_to(x=scroll[0], y=scroll[1], animate=False, force=True)

    def select_path(self, path: str | Path) -> bool:
        """Reveal and highlight a file without emitting ``FileSelected``."""
        node = self.file_nodes.get(str(path))
        if node is None:
            return False
        ancestor = node.parent
        while ancestor is not None:
            ancestor.expand()
            ancestor = ancestor.parent
        self._view_revision += 1
        self._schedule_restore(node.data, None, self._view_revision)
        return True

    def on_tree_node_selected(self, event: Tree.NodeSelected[ResultNode]) -> None:
        if event.control is not self:
            return
        event.stop()
        data = event.node.data
        if data is None:
            return
        if data.kind == "directory":
            event.node.toggle()
        elif data.entry is not None:
            self.post_message(self.FileSelected(self, data.entry))
