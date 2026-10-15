"""Bounded, read-only activity and log snapshots for the result browser.

New workers append a small event journal. Older runs are reconstructed from the
bounded Snakemake tail and existing outputs; viewing never changes job state.
"""
from __future__ import annotations

import csv
import io
import json
import re
import stat
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ZONE = ZoneInfo("Asia/Shanghai")
RUN = re.compile(r"[SED]RR\d+")
ANSI = re.compile(r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|\x1b\[[0-?]*[ -/]*[@-~]|\x1b[@-_]")
CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")
RULES = {
    "prepare_samples": ("metadata", "解析项目"),
    "quality_control": ("analyze", "质控"),
    "quality_control_single": ("analyze", "质控"),
    "star_align": ("analyze", "比对"),
    "te_count": ("analyze", "TE 计数"),
    "merge_counts": ("merge", "汇总结果"),
}
TAIL_BYTES = 262144
_TAIL_CACHE: OrderedDict = OrderedDict()


def clean_text(text: str) -> str:
    return CONTROL.sub("", ANSI.sub("", text)).replace("\r", "")


def _stat(path: Path) -> tuple:
    try:
        stat = path.stat()
        return (str(path), stat.st_mtime_ns, stat.st_size)
    except OSError:
        return (str(path), None, None)


def _safe_stat(path: Path, root: Path):
    """Static confinement and file type checks before opening browser inputs."""
    try:
        target = path.resolve(strict=True)
        target.relative_to(root.resolve(strict=True))
        value = target.stat()
        return value if stat.S_ISREG(value.st_mode) else None
    except (OSError, ValueError, RuntimeError):
        return None


def _tail(path: Path, maximum: int = TAIL_BYTES, *, root: Path) -> str:
    if _safe_stat(path, root) is None:
        return ""
    signature = (*_stat(path), maximum)
    if signature in _TAIL_CACHE:
        _TAIL_CACHE.move_to_end(signature)
        return _TAIL_CACHE[signature]
    try:
        with path.open("rb") as handle:
            size = handle.seek(0, 2)
            offset = max(0, size - maximum)
            handle.seek(offset)
            raw = handle.read(maximum)
        if offset and b"\n" in raw:
            raw = raw.partition(b"\n")[2]
        text = clean_text(raw.decode("utf-8", errors="replace"))
    except OSError:
        text = ""
    _TAIL_CACHE[signature] = text
    while len(_TAIL_CACHE) > 64:
        _TAIL_CACHE.popitem(last=False)
    return text


def _iso(value=None) -> str:
    if value is None:
        return datetime.now(ZONE).isoformat(timespec="seconds")
    return str(value)


def _date(value) -> datetime | None:
    try:
        result = datetime.fromisoformat(str(value))
        return result if result.tzinfo else None
    except (ValueError, TypeError):
        return None


def _elapsed(value) -> int | None:
    parsed = _date(value)
    return max(0, int((datetime.now(ZONE) - parsed).total_seconds())) if parsed else None


class ActivityRecorder:
    """Translate real worker stdout into concise start/finish events.

    Snakemake job IDs retain their own rule and Run mapping, including when
    concurrent rules finish out of order. ``path=None`` parses history in memory.
    """
    def __init__(self, job: dict, path: Path | None = None, historical=False):
        self.job = job
        self.path = path
        self.historical = historical
        self.time = "" if historical else _iso()
        self.events: list[dict] = []
        self.jobs: dict[str, dict] = {}
        self.pending: dict | None = None
        self.last_progress: tuple | None = None

    def emit(self, message, *, run=None, stage="", step="", state="running", level="info", kind="step", at=None):
        item = {"time": self.time if at is None else at, "message": clean_text(str(message)),
                "level": level, "run": run, "stage": stage, "step": step,
                "state": state, "kind": kind, "attempt": self.job.get("attempt", 1)}
        self.events.append(item)
        if len(self.events) > 4096:
            del self.events[:2048]
        if self.path is not None:
            try:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                if self.path.exists() or self.path.is_symlink():
                    if _safe_stat(self.path, Path(self.job["job_dir"])) is None:
                        return item
                with self.path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(item, ensure_ascii=False) + "\n")
            except OSError:
                # Display instrumentation must never stop scientific execution.
                pass
        return item

    def begin(self):
        self.time = _iso()
        self.emit("开始解析项目", stage="metadata", step="解析项目", kind="attempt")

    def _flush_rule(self):
        item = self.pending
        if not item or item.get("emitted") or not item.get("jobid"):
            return
        rule = item["rule"]
        if rule not in RULES or (rule in {"quality_control", "quality_control_single", "star_align", "te_count"} and not item.get("run")):
            return
        stage, step = RULES[rule]
        self.jobs[item["jobid"]] = dict(item)
        state = "failed" if item.get("failed") else "running"
        self.emit(f"{step}{'失败' if state == 'failed' else '开始'}", run=item.get("run"), stage=stage,
                  step=step, state=state, level="error" if state == "failed" else "info")
        item["emitted"] = True

    def feed(self, line: str):
        line = clean_text(line).strip()
        if not self.historical:
            self.time = _iso()
        else:
            # Snakemake timestamps on this server are UTC. Event journals carry
            # explicit offsets; unrecognised historical time stays unknown.
            try:
                parsed = datetime.strptime(line, "[%a %b %d %H:%M:%S %Y]")
                self.time = parsed.replace(tzinfo=timezone.utc).astimezone(ZONE).isoformat(timespec="seconds")
                return
            except ValueError:
                pass
        progress = re.search(r"\[TEFLOW_PROGRESS\]\s*(\{.*\})", line)
        if progress:
            try:
                value = json.loads(progress.group(1))
                stage, step = value.get("stage", ""), str(value.get("step", ""))
                sample = value.get("current_sample")
                run = sample if RUN.fullmatch(str(sample or "")) else None
                key = (stage, step, sample, value.get("downloaded_runs"))
                if key != self.last_progress:
                    self.last_progress = key
                    at = "" if self.historical else None
                    if stage == "download" and step == "FASTQ 已就绪":
                        self.emit("FASTQ 下载完成", run=run, stage=stage, step="下载数据", state="succeeded", at=at)
                    elif stage == "download":
                        self.emit("开始下载 FASTQ" if run else "项目解析完成，准备下载", run=run,
                                  stage=stage, step="下载数据", at=at)
                    elif stage == "metadata":
                        self.emit(step or "解析项目", run=None, stage=stage, step="解析项目", at=at)
            except (ValueError, TypeError, AttributeError):
                pass
            return
        matched = re.search(r"(?:(?:localrule|localcheckpoint|rule|checkpoint)\s+|Error in rule\s+)(\w+):", line)
        if matched:
            self._flush_rule()
            self.pending = {"rule": matched.group(1), "failed": "Error in rule" in line}
            return
        finished = re.search(r"Finished job(?:id:)?\s*(\d+)", line)
        if finished:
            self._flush_rule()
            item = self.jobs.get(finished.group(1))
            if item and item["rule"] in RULES:
                stage, step = RULES[item["rule"]]
                if item["rule"] == "prepare_samples":
                    stage, step = "download", "下载数据"
                self.emit(f"{step}完成", run=item.get("run"), stage=stage, step=step, state="succeeded")
            return
        if self.pending:
            jobid = re.search(r"\bjobid:\s*(\d+)", line)
            if jobid:
                self.pending["jobid"] = jobid.group(1)
                if self.pending.get("failed") and jobid.group(1) in self.jobs:
                    self.pending["run"] = self.jobs[jobid.group(1)].get("run")
            sample = re.search(r"wildcards:\s*.*?\bsample=([SED]RR\d+)", line)
            if sample:
                self.pending["run"] = sample.group(1)
            self._flush_rule()
        if re.search(r"(?:^WARNING\b|\bWarning:|FATAL ERROR|^EXITING because)", line, re.I):
            self.emit(line[:400], level="warning" if "warn" in line.lower() else "error", kind="notice")

    def finish(self, status: str, error=""):
        self.time = "" if self.historical else _iso()
        self.emit("工作流完成" if status == "succeeded" else str(error or "工作流失败")[:1000],
                  stage="done" if status == "succeeded" else "", step="完成" if status == "succeeded" else "失败",
                  state=status, kind="terminal", level="info" if status == "succeeded" else "error")


def _attempt_tail(text: str, job: dict) -> str:
    markers = list(re.finditer(r"^=== .*? attempt (\d+) ===\s*$", text, re.M))
    if markers:
        last = markers[-1]
        return text[last.end():] if int(last.group(1)) == int(job.get("attempt", 1)) else ""
    # A bounded tail may omit the header. Historical output is safe only when
    # the file was updated during the active attempt; callers check its mtime.
    return text


def _current_file(path: Path, job: dict) -> bool:
    started = _date(job.get("attempt_started_at") or job.get("queued_at"))
    value = _safe_stat(path, Path(job["job_dir"]))
    return value is not None and (started is None or value.st_mtime >= started.timestamp() - 2)


def _records(job: dict) -> list[dict]:
    base = Path(job["job_dir"])
    events = []
    for line in _tail(base / ".teflow-activity.jsonl", root=base).splitlines():
        try:
            item = json.loads(line)
            if item.get("attempt") == job.get("attempt", 1) and isinstance(item.get("message"), str):
                events.append(item)
        except (ValueError, TypeError, AttributeError):
            pass
    if events:
        return events
    source = base / "run.log"
    recorder = ActivityRecorder(job, historical=True)
    if _current_file(source, job):
        for line in _attempt_tail(_tail(source, root=base), job).splitlines():
            recorder.feed(line)
    # Preparation writes a separate raw tool log. This also provides useful
    # scoped history when a download failed before samples.json was published.
    if not recorder.events:
        prepare = base / "logs" / "prepare_samples.log"
        if _current_file(prepare, job):
            for line in _tail(prepare, 65536, root=base).splitlines():
                match = re.search(r"\[TEFLOW\] Run \d+/\d+ ([SED]RR\d+): (.*)", line)
                if match:
                    run, detail = match.groups()
                    ready = "复用已完成 FASTQ" in detail
                    recorder.emit("FASTQ 已就绪" if ready else "下载 FASTQ", run=run,
                                  stage="download", step="下载数据", state="succeeded" if ready else "running")
    return recorder.events


def _count_file(job: dict, run: str) -> Path:
    base = Path(job["job_dir"])
    if str(job.get("accession", "")).startswith("GSE"):
        base /= run
    return base / "abundance" / f"{run}.cntTable"


def _nonempty(path: Path, root: Path) -> bool:
    value = _safe_stat(path, root)
    return value is not None and value.st_size > 0


def _mentions_run(text, run: str) -> bool:
    return bool(re.search(rf"(?<![A-Z0-9]){re.escape(run)}(?!\d)", str(text)))


def known_runs(job: dict) -> list[str]:
    """A bounded manifest/mapping read also covers failed download histories."""
    base = Path(job["job_dir"])
    manifest = base / "samples.json"
    try:
        # JSON must be complete, so refuse an oversized manifest instead of
        # parsing a misleading fragment. Mapping TSV is streamed separately.
        value = _safe_stat(manifest, base)
        if value is not None and value.st_size <= 2 * 1024 * 1024:
            with manifest.open(encoding="utf-8") as handle:
                samples = json.load(handle).get("samples", {})
            runs = [str(run) for run in samples if RUN.fullmatch(str(run))]
            if runs:
                return runs
    except (OSError, ValueError, TypeError, AttributeError):
        pass
    try:
        if _safe_stat(base / "samples.tsv", base) is None:
            return []
        with (base / "samples.tsv").open(encoding="utf-8", errors="replace") as handle:
            reader = csv.DictReader(io.StringIO(handle.read(2 * 1024 * 1024)), delimiter="\t")
            return list(dict.fromkeys(value for row in reader for value in row.values()
                                      if value and RUN.fullmatch(value)))
    except OSError:
        return []


def _tool_paths(job: dict, run: str) -> list[Path]:
    base = Path(job["job_dir"])
    directories = (base / "logs", base / run / "logs")
    return [directory / f"{run}.{suffix}.log" for suffix in ("fastp", "STAR", "TEcount") for directory in directories]


def activity_snapshot(job: dict, run: str | None = None, limit=5) -> dict:
    if run is not None and not RUN.fullmatch(str(run)):
        raise ValueError("Run 编号格式无效。")
    events = _records(job)
    scoped = [item for item in events if run is None or item.get("run") == run]
    status = str(job.get("status", "unknown"))
    step = {"metadata": "解析项目", "download": "下载数据", "merge": "汇总结果", "done": "完成"}.get(str(job.get("stage")), str(job.get("step") or "等待资源"))
    elapsed = None
    if run:
        count = _count_file(job, run)
        latest = scoped[-1] if scoped else None
        counting = bool(latest and latest.get("step") == "TE 计数" and latest.get("state") in {"running", "failed"})
        completed = bool(latest and latest.get("step") == "TE 计数" and latest.get("state") == "succeeded")
        if completed or (_nonempty(count, Path(job["job_dir"])) and not counting):
            status, step = "succeeded", "完成"
            if not completed:
                mtime = _stat(count)[1]
                scoped.append({"time": datetime.fromtimestamp(mtime / 1e9, ZONE).isoformat(timespec="seconds") if mtime else "",
                               "message": "TE 计数结果已生成", "level": "info", "run": run})
        elif latest:
            state = latest.get("state")
            step = latest.get("step") or "等待分析"
            if state == "failed":
                status = "failed"
            elif status in {"failed", "cancelled", "interrupted"}:
                specific = _mentions_run(job.get("error", ""), run)
                status = "failed" if specific and job.get("status") == "failed" else "interrupted"
                step += "失败" if status == "failed" else "已停止"
            elif state == "succeeded":
                status, step = "queued", "等待下一步分析"
            else:
                status = "running"
                elapsed = _elapsed(latest.get("time"))
        else:
            labels = {"fastp": "质控", "STAR": "比对", "TEcount": "TE 计数"}
            found = [(path, path.name.split(".")[-2]) for path in _tool_paths(job, run) if _current_file(path, job)]
            if found:
                path, suffix = max(found, key=lambda item: item[0].stat().st_mtime_ns)
                label = labels[suffix]
                detail = _tail(path, 16384, root=Path(job["job_dir"]))
                failed = bool(re.search(r"FATAL ERROR|EXITING because|(?:ValueError|RuntimeError|Error):", detail))
                completed = bool(re.search(r"fastp .*time used|finished successfully|Finished processing sample file", detail, re.I))
                if failed:
                    status, step = "failed", label + "失败"
                elif job.get("status") != "running":
                    status, step = "interrupted", label + "已停止"
                elif completed:
                    status, step = "queued", f"{label}完成，等待下一步"
                else:
                    status, step = "running", label
                scoped.append({"time": "", "message": step, "level": "error" if failed else "info", "run": run})
            else:
                status = "queued" if job.get("status") in {"queued", "running"} else "interrupted"
                step = "等待处理" if status == "queued" else "未完成（所属项目已结束）"
                if _mentions_run(job.get("error", ""), run) and job.get("status") == "failed":
                    status, step = "failed", "下载失败" if job.get("stage") == "download" else "处理失败"
        error_runs = RUN.findall(str(job.get("error", "")))
        if status == "failed" and job.get("error") and (not error_runs or run in error_runs):
            scoped.append({"time": job.get("finished_at", ""), "message": clean_text(str(job["error"]))[:1000], "level": "error", "run": run})
    else:
        if status == "succeeded":
            step = "完成"
        elif status in {"queued", "pending"}:
            step = "等待资源"
        elif status == "running":
            latest_by_scope = {}
            for event in scoped:
                if event.get("kind") != "notice":
                    key = (event.get("run"), event.get("stage"), event.get("step"))
                    latest_by_scope.pop(key, None)
                    latest_by_scope[key] = event
            phase = str(job.get("stage"))
            phase = "analyze" if phase in {"qc", "align", "count"} else phase
            active = [event for event in latest_by_scope.values() if event.get("state") == "running" and event.get("stage") == phase]
            if active:
                current = active[-1]
                step = current.get("step") or step
                elapsed = _elapsed(current.get("time"))
        if status in {"failed", "interrupted", "cancelled"} and job.get("error"):
            message = clean_text(str(job["error"]))[:1000]
            if not scoped or scoped[-1].get("message") != message:
                scoped.append({"time": job.get("finished_at", ""), "message": message, "level": "error", "run": None})
        if status == "succeeded" and (not scoped or scoped[-1].get("kind") != "terminal"):
            scoped.append({"time": job.get("finished_at", ""), "message": "工作流完成", "level": "info", "run": None})
    if not scoped:
        scoped = [{"time": "", "message": step, "level": "info", "run": run}]
    return {"status": status, "step": clean_text(step), "elapsed_seconds": elapsed,
            "events": [{key: clean_text(str(event.get(key, ""))) if key in {"time", "message", "level"} else event.get(key)
                        for key in ("time", "message", "level", "run")} for event in scoped[-max(1, min(int(limit), 100)):]]}


def log_snapshot(job: dict, run: str | None = None, lines=150) -> dict:
    if run is not None and not RUN.fullmatch(str(run)):
        raise ValueError("Run 编号格式无效。")
    base = Path(job["job_dir"])
    if run:
        paths = _tool_paths(job, run)
    else:
        paths = [base / "worker.log", base / "logs" / "prepare_samples.log", base / "logs" / "merge_counts.log", base / "run.log"]
    paths = [path for path in paths if _current_file(path, job)]
    signature = (job.get("accession"), run, job.get("attempt", 1), job.get("status"),
                 tuple(_stat(path) for path in paths), int(lines))
    chunks = []
    for path in paths:
        text = _tail(path, 65536, root=base)
        if path.name == "run.log":
            text = _attempt_tail(text, job)
        if text.strip():
            chunks.append(f"── {path.relative_to(base)} ──\n" + "\n".join(text.splitlines()[-max(1, min(int(lines), 2000)):]))
    if run and not chunks:
        sources = [base / "logs" / "prepare_samples.log", base / "run.log"]
        for source in sources:
            text = _tail(source, 65536, root=base) if _current_file(source, job) else ""
            if source.name == "run.log":
                text = _attempt_tail(text, job)
            matched = [line for line in text.splitlines() if _mentions_run(line, run)]
            if matched:
                chunks.append(f"── {source.relative_to(base)} 中与该 Run 相关的记录 ──\n" + "\n".join(matched[-max(1, min(int(lines), 2000)):]))
        signature += (tuple(_stat(source) for source in sources),)
    return {"text": "\n\n".join(chunks) or "暂无运行日志。", "signature": signature}
