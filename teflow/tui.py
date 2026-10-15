"""Persistent workflow overview and a bounded, read-only results browser."""
from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from rich.text import Text
from textual import events, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.message import Message
from textual.widgets import Button, Footer, Header, Input, RichLog, Select, Static, TabbedContent, TabPane, Tree

from .activity import activity_snapshot, log_snapshot
from .layout import Splitter, WorkspaceLayout, clamp
from .presentation import batch_counts, job_progress, stage_track, status_label, value
from .results import ResultBrowser
from .result_tree import ResultFileTree
from .preview_widgets import SelectablePreview

if TYPE_CHECKING:
    from .runtime import JobManager


class LiveLog(RichLog):
    """Pause on upward navigation; End resumes the live tail."""
    BINDINGS = [Binding("end", "follow_tail", "跟随最新", show=False)]

    class FollowChanged(Message):
        def __init__(self, following: bool) -> None:
            super().__init__()
            self.following = following

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.following = True

    def pause_follow(self) -> None:
        if self.following:
            self.following = self.auto_scroll = False
            self.post_message(self.FollowChanged(False))

    def on_key(self, event: events.Key) -> None:
        if event.key in {"up", "pageup", "home", "ctrl+home"}:
            self.pause_follow()

    def on_mouse_scroll_up(self, event: events.MouseScrollUp) -> None:
        self.pause_follow()

    def action_follow_tail(self) -> None:
        self.following = self.auto_scroll = True
        self.scroll_end(animate=False)
        self.post_message(self.FollowChanged(True))


class TEFlowApp(App):
    """The viewer never owns a workflow process; closing leaves jobs running."""
    TITLE = "TE 工作流"
    SUB_TITLE = "项目结果 · 自动并行"
    ENABLE_COMMAND_PALETTE = False
    BINDINGS = [
        Binding("q", "quit", "退出", show=False),
        Binding("ctrl+q", "quit", "退出（任务继续）", priority=True),
        Binding("f2", "toggle_logs", "文件 / 日志"),
        Binding("f3", "toggle_internal", "内部文件"),
        Binding("f5", "refresh", "刷新"),
    ]
    CSS = """
    Screen { background: $background; }
    #entry { height: 3; margin: 1 1 0 1; }
    #input-mode { width: 20; margin-right: 1; }
    #data-type { width: 25; margin-right: 1; }
    #accession { width: 1fr; }
    #start { width: 8; min-width: 8; margin-left: 1; }
    #feedback { height: 1; padding: 0 1; color: $text-muted; }
    #feedback.error { color: $error; }
    #summary-row { height: 1; margin: 0 1; }
    #batch-summary { width: 1fr; }
    #toggle-internal { width: 20; min-width: 16; height: 1; min-height: 1; border: none; padding: 0; }
    #body { height: 1fr; margin: 0 1; }
    #overview { height: 5; border: double $accent; padding: 0 1; }
    #workspace { height: 1fr; }
    #history-panel { width: 27; border: solid $primary-background; }
    #files-panel { width: 34; border: solid $primary-background; }
    .panel-title { height: 1; padding: 0 1; color: $text-muted; }
    #projects, #files { height: 1fr; }
    #files-empty { height: auto; max-height: 3; padding: 0 1; color: $text-muted; }
    #right-panel { width: 1fr; }
    #activity-panel { height: 9; min-height: 4; border: solid $accent; padding: 0 1; }
    #activity { height: 1fr; }
    #viewer { height: 1fr; min-height: 5; border: solid $primary-background; }
    #viewer > ContentSwitcher { height: 1fr; }
    TabPane { padding: 0; }
    #preview-pane, #log-pane { height: 1fr; }
    #preview-tools { height: 1; padding: 0 1; }
    #preview-title { width: 1fr; height: 1; }
    #previous-page, #next-page { width: 9; min-width: 9; height: 1; min-height: 1; padding: 0; border: none; }
    #copy-page { width: 10; min-width: 10; height: 1; min-height: 1; padding: 0; border: none; }
    #preview-notice { height: auto; max-height: 2; padding: 0 1; color: $text-muted; }
    #preview-table, #preview-text, #logs { height: 1fr; }
    #preview-table { display: none; }
    #preview-text, #logs { padding: 0 1; }
    #log-state { height: 1; color: $text-muted; padding: 0 1; }
    Footer { height: 1; }
    Screen.compact #entry { margin-top: 0; }
    Screen.compact #input-mode { width: 17; }
    Screen.compact #data-type { width: 18; }
    Screen.compact #history-panel { width: 20; }
    Screen.compact #files-panel { width: 22; margin-left: 0; }
    Screen.compact #right-panel { margin-left: 0; }
    Screen.compact #activity-panel { height: 5; }
    Screen.compact #toggle-internal { width: 16; }
    Screen.compact #overview { height: 4; }
    Screen.compact #viewer > ContentTabs { height: 1; }
    Screen.compact #viewer Underline { display: none; }
    """

    def __init__(self, manager: JobManager | None = None, settings_path: str | Path | None = None,
                 layout_path: str | Path | None = None) -> None:
        super().__init__()
        if manager is None:
            from .runtime import JobManager
            manager = JobManager(settings_path=settings_path)
        self.manager, self.browser = manager, ResultBrowser()
        state_dir = getattr(manager, "state_dir", None)
        self.layout_path = Path(layout_path) if layout_path is not None else Path(state_dir) / "ui-layout.json" if state_dir else None
        self.workspace_layout = WorkspaceLayout.load(self.layout_path)
        self._drag_origin = None
        self.selected_accession: str | None = None
        self.selected_run: str | None = None
        self.selected_file: Path | None = None
        self.preview_page = 0
        self.show_internal = self.logs_visible = False
        self._submission_accessions: set[str] = set()
        self._submitting = self._refreshing = False
        self._scope_version = 0
        self._jobs: dict[str, dict] = {}
        self._tree_signature = self._file_signature = self._preview_signature = self._last_log = None
        self._project_nodes: dict[tuple, object] = {}
        self._files: dict[str, object] = {}
        self._preview_has_next = False
        self._preview_display = None
        self._preview_copy_text = ""
        self._last_activity = None

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal(id="entry"):
            yield Select([("Bulk RNA-seq", "bulk"), ("单细胞 · 待配置", "scrna")], prompt="请选择数据类型", id="data-type")
            yield Select([("编号列表", "accessions"), ("TXT 文件路径", "file")], value="accessions", allow_blank=False, id="input-mode")
            yield Input(placeholder="GSE113777,GSE…", id="accession")
            yield Button("开始", id="start", variant="primary")
        yield Static("输入编号或服务器 TXT 路径；回车提交，自动并行。", id="feedback", markup=False)
        with Horizontal(id="summary-row"):
            yield Static("批次：暂无任务", id="batch-summary", markup=False)
            yield Button("显示内部文件 [F3]", id="toggle-internal")
        with Vertical(id="body"):
            yield Static("尚未选择项目\n○ 解析项目 ─ ○ 下载数据 ─ ○ 分析数据 ─ ○ 汇总结果\n选择数据类型并输入编号即可开始。", id="overview", markup=False)
            yield Splitter(vertical=False, id="overview-splitter")
            yield from self._compose_workspace()
        yield Footer()

    def _compose_workspace(self) -> ComposeResult:
        with Horizontal(id="workspace"):
            with Vertical(id="history-panel"):
                yield Static("项目 / Run · 回车选择", classes="panel-title")
                yield Tree("项目", id="projects")
            yield Splitter(vertical=True, id="history-splitter")
            with Vertical(id="files-panel"):
                yield Static("结果文件", id="files-title", classes="panel-title", markup=False)
                yield ResultFileTree(id="files")
                yield Static("选择项目或 Run", id="files-empty", markup=False)
            yield Splitter(vertical=True, id="files-splitter")
            with Vertical(id="right-panel"):
                with Vertical(id="activity-panel"):
                    yield Static("实时概况 · 关键事件", id="activity-title", classes="panel-title", markup=False)
                    yield Static("尚未选择项目", id="activity", markup=False)
                yield Splitter(vertical=False, id="activity-splitter")
                with TabbedContent(initial="preview-pane", id="viewer"):
                    with TabPane("文件预览", id="preview-pane"):
                        with Horizontal(id="preview-tools"):
                            yield Static("选择文件查看内容", id="preview-title", markup=False)
                            yield Button("复制本页", id="copy-page", disabled=True)
                            yield Button("上一页", id="previous-page", disabled=True)
                            yield Button("下一页", id="next-page", disabled=True)
                        yield Static("", id="preview-notice", markup=False)
                        yield SelectablePreview(id="preview-table")
                        yield SelectablePreview(id="preview-text")
                    with TabPane("运行日志", id="log-pane"):
                        yield Static("自动跟随最新 · 上滚暂停 · End 跟随", id="log-state", markup=False)
                        yield LiveLog(id="logs", wrap=True, markup=False, highlight=False, max_lines=1000)

    def on_mount(self) -> None:
        self.query_one("#projects", Tree).show_root = False
        self.query_one("#accession", Input).focus()
        self.screen.set_class(self.size.width < 110 or self.size.height < 30, "compact")
        self.call_after_refresh(self._apply_layout)
        self.set_interval(1.0, self.refresh_jobs)
        self.refresh_jobs()

    def on_resize(self, event: events.Resize) -> None:
        self.screen.set_class(event.size.width < 110 or event.size.height < 30, "compact")
        self.call_after_refresh(self._apply_layout)

    def _apply_layout(self) -> None:
        if not self.is_mounted:
            return
        body, workspace = self.query_one("#body"), self.query_one("#workspace")
        if workspace.size.width < 5:
            return
        history, files, _ = self.workspace_layout.columns(workspace.content_size.width)
        self.query_one("#history-panel").styles.width = history
        self.query_one("#files-panel").styles.width = files
        overview, activity = self.workspace_layout.rows(body.content_size.height, workspace.content_size.height,
                                                       compact=self.screen.has_class("compact"))
        _, activity = self.workspace_layout.rows(body.content_size.height, body.content_size.height - overview - 1,
                                                compact=self.screen.has_class("compact"))
        self.query_one("#overview").styles.height = overview
        self.query_one("#activity-panel").styles.height = activity
        if self._last_activity is not None:
            self.call_after_refresh(lambda: self._show_activity(*self._last_activity))

    def on_splitter_dragged(self, event: Splitter.Dragged) -> None:
        workspace = self.query_one("#workspace")
        if event.beginning or self._drag_origin is None:
            self._drag_origin = (self.query_one("#history-panel").region.width,
                                 self.query_one("#files-panel").region.width,
                                 self.query_one("#right-panel").region.width,
                                 self.query_one("#overview").region.height,
                                 self.query_one("#activity-panel").region.height)
        h, f, r, overview, activity = self._drag_origin
        divider = event.splitter.id
        if divider in {"history-splitter", "files-splitter"}:
            self.workspace_layout.drag_columns(divider, event.delta, (h, f, r), workspace.content_size.width)
        elif divider == "overview-splitter":
            self.workspace_layout.overview = int(clamp(overview + event.delta, 4, self.query_one("#body").content_size.height - 12))
        else:
            available = max(1, workspace.content_size.height - 1)
            self.workspace_layout.activity = clamp(activity + event.delta, 4, available - 6) / available
        self._apply_layout()

    def on_splitter_finished(self, event: Splitter.Finished) -> None:
        self._drag_origin = None
        try:
            self.workspace_layout.save(self.layout_path)
        except OSError as exc:
            self._feedback(f"布局已调整，保存失败：{exc}", error=True)

    def _feedback(self, message: str, *, error: bool = False) -> None:
        widget = self.query_one("#feedback", Static)
        widget.update(Text(message))
        widget.set_class(error, "error")

    def on_select_changed(self, event: Select.Changed) -> None:
        if event.select.id == "data-type":
            self._feedback("单细胞分析流程待配置，暂不能运行。" if event.value == "scrna" else "已选择 Bulk RNA-seq；一次批量输入使用同一数据类型。" if event.value == "bulk" else "请选择数据类型。")
        if event.select.id == "input-mode":
            self.query_one("#accession", Input).placeholder = "服务器绝对路径，例如 /root/geo_ids.txt" if event.value == "file" else "GSE113777,GSE…"
            self._feedback("TXT 位于服务器，每行一个编号。" if event.value == "file" else "多个编号用逗号分隔；回车提交，自动并行。")

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "accession":
            self.action_start()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        action = {"start": self.action_start, "toggle-internal": self.action_toggle_internal, "copy-page": self.action_copy_page, "previous-page": lambda: self._change_page(-1), "next-page": lambda: self._change_page(1)}.get(event.button.id)
        if action:
            action()

    def action_start(self) -> None:
        if self._submitting:
            return
        source = self.query_one("#accession", Input).value.strip()
        if not source:
            self._feedback("请输入有效编号，或服务器 TXT 文件的绝对路径。", error=True)
            return
        from .data_types import validate_data_type
        try:
            selected_type = self.query_one("#data-type", Select).value
            data_type = validate_data_type(None if selected_type is Select.BLANK else selected_type)
        except ValueError as exc:
            self._feedback(str(exc), error=True)
            return
        self._submitting = True
        self.query_one("#start", Button).disabled = True
        self._feedback("正在读取输入并提交项目…")
        self._submit(source, str(self.query_one("#input-mode", Select).value), data_type)

    @work(group="submit", exclusive=True, exit_on_error=False)
    async def _submit(self, source: str, input_mode: str, data_type: str) -> None:
        try:
            jobs = await asyncio.to_thread(self.manager.submit_many, source, input_mode=input_mode, data_type=data_type)
            if not jobs:
                raise ValueError("输入中没有可提交的编号。")
            self._submission_accessions = {str(job["accession"]) for job in jobs}
            self._jobs.update({str(job["accession"]): job for job in jobs})
            if input_mode == "accessions":
                self.query_one("#accession", Input).value = ",".join(str(job["accession"]) for job in jobs)
            self._select_scope(str(jobs[0]["accession"]))
            completed = all(value(job.get("status")) in {"succeeded", "success", "completed"} for job in jobs)
            self._feedback(f"{len(jobs)} 个项目已完成，显示已有结果。" if completed else f"已提交 {len(jobs)} 个项目；退出界面后任务继续。")
        except Exception as exc:
            self._feedback(str(exc), error=True)
        finally:
            self._submitting = False
            if self.is_running:
                self.query_one("#start", Button).disabled = False
                self.query_one("#accession", Input).focus()
                self.refresh_jobs()

    def _select_scope(self, accession: str, run: str | None = None) -> None:
        if (accession, run) != (self.selected_accession, self.selected_run):
            self.selected_accession, self.selected_run = accession, run
            self._scope_version += 1
            self._file_signature = self._last_log = None
            self._clear_preview()
            self._files.clear()
            self.query_one("#files", ResultFileTree).clear_files(scope_key=(accession, run))
            self.query_one("#files-empty", Static).update("正在读取结果文件…")
            logs = self.query_one("#logs", LiveLog)
            logs.clear()
            logs.following = logs.auto_scroll = True
            self.query_one("#log-state", Static).update("自动跟随最新 · 上滚暂停 · End 跟随")
            self.query_one("#activity", Static).update("正在读取当前任务…")
        self.refresh_jobs()

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        if event.control.id == "projects" and event.node.data:
            kind, accession, run, path = event.node.data
            self._select_scope(accession, run)
            if kind == "file":
                self._open_file(Path(path))

    def on_result_file_tree_file_selected(self, event: ResultFileTree.FileSelected) -> None:
        self._open_file(event.path)

    def action_refresh(self) -> None:
        self.refresh_jobs()

    def action_toggle_internal(self) -> None:
        self.show_internal = not self.show_internal
        self.query_one("#toggle-internal", Button).label = "隐藏内部文件 [F3]" if self.show_internal else "显示内部文件 [F3]"
        self._tree_signature = self._file_signature = None
        self.refresh_jobs()

    def action_toggle_logs(self) -> None:
        self.query_one("#viewer", TabbedContent).active = "preview-pane" if self.logs_visible else "log-pane"

    def on_tabbed_content_tab_activated(self, event: TabbedContent.TabActivated) -> None:
        if event.tabbed_content.id == "viewer":
            self.logs_visible = event.pane.id == "log-pane"
            if self.logs_visible:
                self.query_one("#logs", LiveLog).focus()
                self.refresh_jobs()

    def on_live_log_follow_changed(self, event: LiveLog.FollowChanged) -> None:
        self.query_one("#log-state", Static).update("自动跟随最新 · 上滚暂停 · End 跟随" if event.following else "已暂停跟随 · 按 End 查看最新输出")
        if event.following:
            self._last_log = None
            self.refresh_jobs()

    def _snapshot(self, requested, run, show_internal, include_logs):
        jobs = sorted(self.manager.list_jobs(), key=lambda job: str(job.get("created_at") or ""), reverse=True)
        identifiers = {str(job["accession"]) for job in jobs}
        selected = requested if requested in identifiers else (str(jobs[0]["accession"]) if jobs else None)
        detail = self.manager.get_job(selected) if selected else None
        listings = {str(job["accession"]): self.browser.list_project(job, show_internal=show_internal) for job in jobs}
        if not detail:
            return jobs, selected, detail, listings, [], None, None
        selected_run = run if selected == requested else None
        try:
            files = self.browser.list_files(detail, run=selected_run, show_internal=show_internal)
        except (OSError, ValueError) as exc:
            files = []
            listings[selected].error = f"无法读取结果文件：{exc}"
        activity = activity_snapshot(detail, run=selected_run, limit=5)
        logs = log_snapshot(detail, run=selected_run, lines=300) if include_logs else None
        return jobs, selected, detail, listings, files, activity, logs

    @work(group="refresh", exit_on_error=False)
    async def refresh_jobs(self) -> None:
        if self._refreshing:
            return
        self._refreshing = True
        version = self._scope_version
        try:
            include_logs = self.logs_visible and self.query_one("#logs", LiveLog).following
            snapshot = await asyncio.to_thread(self._snapshot, self.selected_accession, self.selected_run, self.show_internal, include_logs)
            if version != self._scope_version:
                self.call_after_refresh(self.refresh_jobs)
                return
            jobs, selected, detail, listings, files, activity, logs = snapshot
            self._jobs = {str(job["accession"]): job for job in jobs}
            self.selected_accession = selected
            self._show_projects(jobs, listings)
            self._show_batch(jobs)
            if detail:
                self._show_overview(detail)
                self._show_files(files, listings.get(selected))
                self._show_activity(detail, activity)
                self._show_logs(logs)
                if self.selected_file is not None:
                    self._load_preview(self.selected_file, self.preview_page)
        except Exception as exc:
            self._feedback(f"无法读取任务状态：{exc}", error=True)
        finally:
            self._refreshing = False

    def _show_batch(self, jobs: list[dict]) -> None:
        selected = self._jobs.get(str(self.selected_accession), {})
        batch_id = selected.get("batch_id")
        if str(self.selected_accession) in self._submission_accessions:
            batch = [job for job in jobs if str(job["accession"]) in self._submission_accessions]
        else:
            batch = [job for job in jobs if job.get("batch_id") == batch_id] if batch_id else jobs
        counts = batch_counts(batch)
        self.query_one("#batch-summary", Static).update(Text(f"批次：运行 {counts['running']} · 等待 {counts['queued']} · 完成 {counts['succeeded']} · 失败 {counts['failed']}"))

    def _show_projects(self, jobs: list[dict], listings: dict) -> None:
        signature = tuple((job["accession"], value(job.get("status")), tuple(listings[job["accession"]].runs), tuple(str(file.path) for file in listings[job["accession"]].files)) for job in jobs)
        if signature == self._tree_signature:
            return
        tree = self.query_one("#projects", Tree)
        old_cursor = tree.cursor_node.data if tree.cursor_node else None
        expanded = {key for key, node in self._project_nodes.items() if node.is_expanded}
        scroll = (tree.scroll_x, tree.scroll_y)
        first = not self._project_nodes
        self._tree_signature = signature
        tree.clear()
        self._project_nodes.clear()
        for job in jobs:
            accession = str(job["accession"])
            key = ("project", accession, None, None)
            node = tree.root.add(Text(f"{accession} · {status_label(job)}"), data=key, expand=key in expanded or (first and accession == self.selected_accession))
            self._project_nodes[key] = node
            for run in listings[accession].runs:
                if str(run) == accession:
                    continue
                run_key = ("run", accession, str(run), None)
                self._project_nodes[run_key] = node.add_leaf(Text(str(run)), data=run_key)
            for file in listings[accession].files:
                key = ("file", accession, None, str(file.path))
                self._project_nodes[key] = node.add_leaf(Text(file.name), data=key)
        target = self._project_nodes.get(old_cursor) or self._project_nodes.get(("run" if self.selected_run else "project", self.selected_accession, self.selected_run, None))
        if target:
            tree.move_cursor(target)
        self.call_after_refresh(lambda: tree.scroll_to(x=scroll[0], y=scroll[1], animate=False))

    def _show_overview(self, job: dict) -> None:
        title = f"{job['accession']} · {status_label(job)}" + (f" · 查看 {self.selected_run}" if self.selected_run else "")
        compact = self.screen.has_class("compact")
        if compact:
            title += " · " + job_progress(job)
        overview = Text(title, style="bold")
        overview.append("\n")
        overview.append_text(stage_track(job))
        if not compact:
            overview.append("\n" + job_progress(job))
        overview.no_wrap = compact
        overview.overflow = "ellipsis"
        self.query_one("#overview", Static).update(overview)

    @staticmethod
    def _file_size(size: int) -> str:
        amount = float(size)
        for unit in ("B", "K", "M", "G", "T"):
            if amount < 1024 or unit == "T":
                return f"{amount:.0f}{unit}" if unit == "B" else f"{amount:.1f}{unit}"
            amount /= 1024
        return str(size)

    def _show_files(self, files: list, listing) -> None:
        self.query_one("#files-title", Static).update(Text(f"结果 · {self.selected_run or self.selected_accession}"))
        signature = tuple((str(file.path), file.size, file.modified_ns) for file in files)
        self._files = {str(file.path): file for file in files}
        empty = self.query_one("#files-empty", Static)
        empty.update(Text(str(getattr(listing, "error", "") or "尚未生成结果文件") if not files else ""))
        empty.display = not bool(files)
        if signature == self._file_signature:
            return
        self._file_signature = signature
        tree = self.query_one("#files", ResultFileTree)
        first = not tree.file_nodes
        tree.update_files(files, scope_key=(self.selected_accession, self.selected_run))
        if first and self.selected_file is not None:
            tree.select_path(self.selected_file)

    def _show_activity(self, job: dict, activity: dict | None) -> None:
        self._last_activity = job, activity
        activity = activity or {}
        scope = self.selected_run or str(job["accession"])
        elapsed = activity.get("elapsed_seconds")
        duration = f" · 已运行 {int(elapsed)//60}分{int(elapsed)%60:02d}秒" if elapsed is not None else ""
        activity_status = status_label({"status": activity.get("status", job.get("status"))})
        step = str(activity.get("step") or status_label(job))
        compact = self.screen.has_class("compact")
        title = f"{scope} · {activity_status} · {step}{duration}"
        if compact:
            title = f"{scope} · {step}" + (f" · {int(elapsed)//60}:{int(elapsed)%60:02d}" if elapsed is not None else "")
        available = self.query_one("#activity", Static).size.height
        tiny = available < 2
        self.query_one("#activity-title", Static).update(Text("实时 · " + title if tiny else "实时概况 · 关键事件", no_wrap=True, overflow="ellipsis"))
        text = Text("" if tiny else title + "\n", style="bold", no_wrap=True, overflow="ellipsis")
        available_events = max(1, min(5, available if tiny else available - 1))
        items = (activity.get("events") or [])[-available_events:]
        for index, event in enumerate(items):
            stamp = str(event.get("time") or "")
            if stamp:
                try:
                    stamp = datetime.fromisoformat(stamp).strftime("%H:%M")
                except ValueError:
                    stamp = stamp[-8:-3] if len(stamp) > 8 else stamp
            message, run = str(event.get("message") or ""), str(event.get("run") or "")
            if run and run not in message and not self.selected_run:
                message = run + " · " + message
            text.append((stamp + "  " if stamp else "") + message, style="red" if event.get("level") == "error" else "yellow" if event.get("level") == "warning" else "")
            if index < len(items) - 1:
                text.append("\n")
        if not items:
            text.append("等待关键事件；完整输出见运行日志。")
        self.query_one("#activity", Static).update(text)

    def _show_logs(self, snapshot: dict | None) -> None:
        if snapshot is None or not self.logs_visible:
            return
        logs = self.query_one("#logs", LiveLog)
        signature = (self.selected_accession, self.selected_run, snapshot.get("signature"))
        if not logs.following or signature == self._last_log:
            return
        self._last_log = signature
        logs.clear()
        logs.write(Text(snapshot.get("text") or "当前范围尚无日志。"), scroll_end=True)

    def _clear_preview(self) -> None:
        self.selected_file = None
        self.preview_page = 0
        self._preview_signature = None
        self._preview_display = None
        self._preview_has_next = False
        self.query_one("#preview-title", Static).update("选择文件查看内容")
        notice = self.query_one("#preview-notice", Static)
        notice.update("")
        notice.display = False
        table = self.query_one("#preview-table", SelectablePreview)
        table.set_page("")
        table.display = False
        text = self.query_one("#preview-text", SelectablePreview)
        text.display = True
        text.set_page("选择项目 / Run，再选择结果文件。")
        self._preview_copy_text = ""
        self.query_one("#copy-page", Button).disabled = True
        self.query_one("#previous-page", Button).disabled = True
        self.query_one("#next-page", Button).disabled = True

    def _open_file(self, path: Path) -> None:
        if path != self.selected_file:
            self.preview_page = 0
            self._preview_signature = None
        self.selected_file = path
        self.query_one("#files", ResultFileTree).select_path(path)
        self.query_one("#viewer", TabbedContent).active = "preview-pane"
        self._load_preview(path, self.preview_page)

    def _change_page(self, direction: int) -> None:
        if self.selected_file is None or (direction > 0 and not self._preview_has_next):
            return
        self.preview_page = max(0, self.preview_page + direction)
        self._load_preview(self.selected_file, self.preview_page)

    def action_copy_page(self) -> None:
        if self.selected_file is None or self.query_one("#copy-page", Button).disabled:
            return
        widget_id = "#preview-table" if self.query_one("#preview-table").display else "#preview-text"
        self.query_one(widget_id, SelectablePreview).copy_page()

    @work(group="preview", exclusive=True, exit_on_error=False)
    async def _load_preview(self, path: Path, page: int) -> None:
        scope = self._scope_version
        job = self._jobs.get(str(self.selected_accession))
        if not job:
            return
        try:
            result = await asyncio.to_thread(self.browser.preview, job, path, page=page)
            if scope != self._scope_version or path != self.selected_file or page != self.preview_page or result.signature == self._preview_signature:
                return
            same_page = self._preview_display == (path, result.page, result.kind)
            self.preview_page = result.page
            self._preview_signature, self._preview_has_next = result.signature, result.has_next
            preview_title = f"第 {result.page + 1} 页 · {result.title}" if self.screen.has_class("compact") else f"{result.title} · 第 {result.page + 1} 页"
            self.query_one("#preview-title", Static).update(Text(preview_title))
            notice = self.query_one("#preview-notice", Static)
            notice.update(Text(result.text if result.kind == "table" else ""))
            notice.display = bool(result.text) and result.kind == "table"
            self.query_one("#previous-page", Button).disabled = result.page == 0
            self.query_one("#next-page", Button).disabled = not result.has_next
            table, text = self.query_one("#preview-table", SelectablePreview), self.query_one("#preview-text", SelectablePreview)
            if result.kind == "table":
                text.display, table.display = False, True
                table.set_table(result.columns, result.rows, preserve_view=same_page)
            else:
                table.display, text.display = False, True
                text.set_page(result.text or "文件为空。", copy_text=result.text, preserve_view=same_page)
            self.query_one("#copy-page", Button).disabled = False
            self._preview_display = (path, result.page, result.kind)
        except Exception as exc:
            if scope == self._scope_version and path == self.selected_file:
                text = self.query_one("#preview-text", SelectablePreview)
                self.query_one("#preview-table", SelectablePreview).display = False
                text.display = True
                text.set_page(f"无法预览：{exc}")
                self.query_one("#copy-page", Button).disabled = True
                self.query_one("#previous-page", Button).disabled = True
                self.query_one("#next-page", Button).disabled = True
