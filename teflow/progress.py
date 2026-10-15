"""Read-only transfer progress shared by terminal frontends.

Sizes come only from the accession's mapped FASTQs. No network requests, hash
calculations or worker changes are needed, including for already running jobs.
"""
from __future__ import annotations

import csv
import io
import json
import math
import re
import stat
import time
from collections import OrderedDict, deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote, urlsplit

RUN = re.compile(r"[SED]RR\d+")
ACCESSION = re.compile(r"(?:GSE\d+|[SED]R[APXS]\d+|[SED]RR\d+|PRJ[END][A-Z]\d+|SAM[END][A-Z]?\d+|GSM\d+)")
FASTQ = re.compile(r"[^/\\]+\.(?:fastq|fq)\.gz", re.I)
MATE = re.compile(r"(?:^|[_.-])(?:R)?([12])(?:_\d+)?\.(?:fastq|fq)\.gz$", re.I)
MAX_METADATA_BYTES = 8 * 1024 * 1024
MAX_FILES = 4096
MAX_SCOPES = 128
ACTIVE = {"running"}


@dataclass(frozen=True)
class ProgressSnapshot:
    stage: str
    status: str
    label: str
    current_bytes: int
    total_bytes: int | None
    speed_bps: float | None
    percent: float | None
    eta_seconds: float | None
    completed_runs: int
    total_runs: int
    elapsed_seconds: int | None
    run: str | None = None


@dataclass(frozen=True)
class _File:
    run: str
    path: Path
    expected: int | None
    source: str = "ena-fastq"


@dataclass(frozen=True)
class _Plan:
    runs: tuple[str, ...] = ()
    files: tuple[_File, ...] = ()
    published: bool = False
    complete: bool = True


def _number(value, default=0):
    try:
        return max(0, int(value))
    except (ValueError, TypeError, OverflowError):
        return default


def _regular(path: Path, root: Path):
    try:
        target = path.resolve(strict=True)
        target.relative_to(root.resolve())
        value = target.stat()
        return value if stat.S_ISREG(value.st_mode) else None
    except (OSError, ValueError, RuntimeError):
        return None


def _read(path: Path, root: Path, maximum=MAX_METADATA_BYTES):
    value = _regular(path, root)
    if value is None or value.st_size > maximum:
        return None
    try:
        with path.open("rb") as handle:
            raw = handle.read(maximum + 1)
        return raw.decode("utf-8-sig") if len(raw) <= maximum else None
    except (OSError, UnicodeError):
        return None


def _json(path: Path, root: Path):
    try:
        value = json.loads(_read(path, root) or "null")
        return value if isinstance(value, dict) else {}
    except (ValueError, TypeError):
        return {}


def _signature(path: Path, root: Path):
    value = _regular(path, root)
    return (str(path), value.st_mtime_ns, value.st_size) if value else (str(path), None, None)


def _size(value):
    parsed = _number(value, None)
    return parsed if parsed and parsed > 0 else None


def _files_from_row(run: str, row: dict):
    row = row if isinstance(row, dict) else {}
    layout = str(row.get("library_layout", row.get("LibraryLayout", ""))).strip().upper()
    links = str(row.get("fastq_ftp") or "").split(";")
    sizes = str(row.get("fastq_bytes") or "").split(";")
    if layout == "SINGLE":
        links = [link.strip() for link in links if link.strip()]
        if not links:
            return [(f"{run}.fastq.gz", None, "sra-converted")]
        if len(links) != 1:
            return []
        try:
            name = Path(unquote(urlsplit(links[0] if "://" in links[0] else "https://" + links[0]).path)).name
        except ValueError:
            return []
        return [(name, _size(sizes[0]), "ena-fastq")] if FASTQ.fullmatch(name) else []
    if layout != "PAIRED":
        return []
    selected = {}
    for index, link in enumerate(links):
        try:
            name = Path(unquote(urlsplit(link if "://" in link else "https://" + link).path)).name
        except ValueError:
            continue
        match = MATE.search(name)
        if FASTQ.fullmatch(name) and match and match.group(1) not in selected:
            selected[match.group(1)] = (name, _size(sizes[index]) if index < len(sizes) else None, "ena-fastq")
    # SRA-converted names are defined by the pipeline; their compressed total is
    # unknowable before conversion. They must never get an invented percentage.
    return list(selected.values()) or [(f"{run}_{mate}.fastq.gz", None, "sra-converted") for mate in (1, 2)]


def _parse_plan(job: dict, raw: Path, output: Path) -> _Plan:
    accession = str(job.get("accession") or "")
    if not ACCESSION.fullmatch(accession):
        return _Plan(complete=False)
    geo = accession.startswith("GSE")
    manifest = _json(output / "samples.json", output)
    source = manifest.get("samples") if manifest.get("accession") == accession else None
    published = isinstance(source, dict) and bool(source)
    if not published:
        cache = _json(raw / f".teflow-{accession}.geo.complete.json", raw) if geo else {}
        source = cache.get("records") if cache.get("accession") == accession else None
    truncated = False
    if not isinstance(source, dict) or not source:
        text = _read(raw / f"{accession}.metadata.tsv", raw)
        source = {}
        if text and text.endswith(("\n", "\r")):
            try:
                reader = csv.DictReader(io.StringIO(text), delimiter="\t")
                for row in reader:
                    run = str(row.get("run_accession") or row.get("Run") or "").strip()
                    if RUN.fullmatch(run) and None not in row and all(value is not None for value in row.values()):
                        source[run] = {"row": row}
                    if len(source) * 2 >= MAX_FILES:
                        truncated = True
                        break
            except (csv.Error, ValueError):
                source = {}
    files, runs = [], []
    complete = not truncated
    for run, record in source.items():
        if not RUN.fullmatch(str(run)) or not isinstance(record, dict):
            complete = False
            continue
        if len(files) + 2 > MAX_FILES:
            complete = False
            break
        runs.append(str(run))
        directory = raw / run if geo else raw
        supplied = record.get("files")
        if isinstance(supplied, list) and supplied:
            names = [(item.get("name"), _size(item.get("bytes")), str(item.get("source") or "ena-fastq"))
                     for item in supplied if isinstance(item, dict)]
        else:
            names = _files_from_row(run, record.get("row", record))
        if not names:
            complete = False
        for name, expected, source_kind in names:
            if not isinstance(name, str) or not FASTQ.fullmatch(name) or Path(name).name != name:
                complete = False
                continue
            if len(files) >= MAX_FILES:
                complete = False
                break
            files.append(_File(str(run), directory / name, expected, source_kind))
    if len(runs) < _number(job.get("total_runs")):
        complete = False
    return _Plan(tuple(runs), tuple(files), published, complete)


def _elapsed(job, now):
    start = job.get("attempt_started_at") or job.get("created_at")
    finish = job.get("finished_at") if job.get("status") not in {"running", "queued"} else None
    try:
        beginning = datetime.fromisoformat(str(start)).timestamp()
        end = datetime.fromisoformat(str(finish)).timestamp() if finish else now
        return max(0, int(end - beginning))
    except (ValueError, TypeError, OSError, OverflowError):
        return None


def _conversion_log_phase(job: dict, output: Path, run: str) -> str | None:
    """Use iSeq's literal phase notices only within the current invocation.

    A later invocation marker resets old conversion notices from earlier tries.
    Growing wget output can evict the marker; file timestamps are then a fallback.
    """
    path = output / "logs" / "prepare_samples.log"
    value = _regular(path, output)
    if value is None:
        return None
    try:
        began = job.get("attempt_started_at")
        if began and value.st_mtime < datetime.fromisoformat(str(began)).timestamp():
            return None
        with path.open("rb") as handle:
            handle.seek(max(0, value.st_size - 65536))
            text = handle.read(65536).decode("utf-8", errors="replace")
    except (OSError, ValueError, TypeError, OverflowError):
        return None
    phase = None
    for line in text.splitlines():
        start = re.search(r"(?:\[TEFLOW\] Run \d+/\d+ |^\$ .* -i )([SED]RR\d+)(?=\s|:)", line)
        if start:
            phase = "download" if start[1] == run else None
        if re.search(rf"Converting {re.escape(run)} to fastq\b|Compressing {re.escape(run)}(?=[_.])", line):
            phase = "convert"
    return phase


def _sra_transfer(job: dict, plan: _Plan, raw: Path, output: Path,
                  selected_run: str | None, confirmed: set[str]):
    if str(job.get("stage")) != "download" or str(job.get("status")) not in ACTIVE:
        return None
    current = str(job.get("current_sample") or "")
    if current not in plan.runs:
        current = next((value for value in plan.runs if value not in confirmed), "")
    if not current or (selected_run is not None and selected_run != current) or current in confirmed:
        return None
    mapped = [item for item in plan.files if item.run == current]
    if not mapped or not all(item.source == "sra-converted" for item in mapped):
        return None
    directory = raw / current if str(job.get("accession")).startswith("GSE") else raw
    archive, value = None, None
    for name in (current, current + ".sra"):
        candidate = directory / name
        candidate_stat = _regular(candidate, raw)
        if candidate_stat is not None:
            archive, value = candidate, candidate_stat
            break
    phase = _conversion_log_phase(job, output, current)
    if phase is None:
        for item in mapped:
            # fasterq-dump writes the known uncompressed mates before pigz.
            # Their timestamps distinguish new conversion output from stale
            # files retained across a download retry.
            for candidate in (item.path.with_suffix(""), item.path):
                candidate_stat = _regular(candidate, raw)
                if candidate_stat is not None and (value is None or candidate_stat.st_mtime_ns >= value.st_mtime_ns):
                    phase = "convert"
                    break
            if phase == "convert":
                break
    if phase == "convert":
        return {"label": "转换 FASTQ", "sizes": (0,), "identity": ("convert", current), "active": False}
    # Do not combine archive bytes with converted FASTQ bytes from previous
    # Runs: they represent different encodings of the same sequencing reads.
    return {"label": f"下载 SRA（{current}）", "sizes": (value.st_size if value else 0,),
            "identity": ("archive", str(archive or directory / current)), "active": True}


class ProgressTracker:
    def __init__(self, *, clock=time.monotonic, wall_clock=time.time, window_seconds=5.0):
        self.clock, self.wall_clock = clock, wall_clock
        self.window = max(1.0, float(window_seconds))
        self._plans = OrderedDict()
        self._rates = OrderedDict()

    def _plan(self, job, raw, output):
        accession = str(job.get("accession") or "")
        key = (str(raw), str(output), accession, job.get("total_runs"),
               _signature(output / "samples.json", output),
               _signature(raw / f".teflow-{accession}.geo.complete.json", raw),
               _signature(raw / f"{accession}.metadata.tsv", raw))
        if key not in self._plans:
            self._plans[key] = _parse_plan(job, raw, output)
            while len(self._plans) > 32:
                self._plans.popitem(last=False)
        self._plans.move_to_end(key)
        return self._plans[key]

    def _rate(self, key, sizes, active):
        now = float(self.clock())
        previous = self._rates.get(key)
        stale = bool(previous and (now < previous[0][-1][0] or now - previous[0][-1][0] > self.window * 3))
        if previous is None or stale or not active or not previous[2] or len(sizes) != len(previous[1]) or any(a < b for a, b in zip(sizes, previous[1])):
            samples = deque([(now, sum(sizes))])
            speed = None if active else 0.0
        else:
            samples = previous[0]
            if now > samples[-1][0]:
                samples.append((now, sum(sizes)))
            # Keep one point on/before the rolling boundary for stable average.
            while len(samples) > 2 and samples[1][0] <= now - self.window:
                samples.popleft()
            span = now - samples[0][0]
            speed = max(0.0, (sum(sizes) - samples[0][1]) / span) if span >= 1 else None
            if len(samples) > 256:
                samples.popleft()
        self._rates[key] = (samples, sizes, active)
        self._rates.move_to_end(key)
        while len(self._rates) > MAX_SCOPES:
            self._rates.popitem(last=False)
        return speed

    def snapshot(self, job: dict, run: str | None = None) -> ProgressSnapshot:
        if run is not None and not RUN.fullmatch(str(run)):
            raise ValueError("Run 编号格式无效。")
        raw = Path(job.get("raw_dir") or job.get("job_dir") or ".").resolve()
        output = Path(job.get("job_dir") or ".").resolve()
        plan = self._plan(job, raw, output)
        files = [item for item in plan.files if run is None or item.run == run]
        runs = tuple(value for value in plan.runs if run is None or value == run)
        status = str(job.get("status") or "unknown")
        stage = str(job.get("stage") or "metadata")
        if stage in {"qc", "align", "count"}:
            stage = "analyze"
        if stage in {"preparing", "prepare"}:
            stage = "metadata"
        sizes, exact = [], {}
        for item in files:
            value = _regular(item.path, raw)
            actual = value.st_size if value else 0
            sizes.append(min(actual, item.expected) if item.expected else actual)
            exact[item.run] = exact.get(item.run, True) and bool(actual and (item.expected is None or actual == item.expected))
        current = sum(sizes)
        total = sum(item.expected for item in files) if files and all(item.expected for item in files) and (plan.complete or run in runs) else None
        # iSeq's success.log precedes TEflow's gzip validation. Only the
        # existing ready event or published manifest confirms the entire step.
        confirmed = set(plan.runs[:_number(job.get("downloaded_runs"))])
        if plan.published:
            confirmed.update(value for value in runs if exact.get(value))
        confirmed = {value for value in confirmed if exact.get(value)}
        download_done = (bool(runs) and len(confirmed.intersection(runs)) == len(runs)
                         and (run is not None or plan.complete))
        if download_done and current:
            # Converted gzip length becomes known once the ready event confirms
            # every mapped mate in this scope; it was unknowable beforehand.
            total = current
        transfer = _sra_transfer(job, plan, raw, output, run, confirmed)
        explicit_verify = (job.get("step") == "校验 FASTQ"
                           and (run is None or job.get("current_sample") == run))
        verify = bool(total is not None and current >= total and not download_done) or explicit_verify
        active = stage == "download" and status in ACTIVE and not verify and not download_done
        identity = tuple((str(item.path), item.expected) for item in files)
        if transfer:
            sizes = list(transfer["sizes"])
            current, total = sum(sizes), None
            active = active and transfer["active"]
            identity = transfer["identity"]
        key = (str(output), job.get("attempt", 1), job.get("attempt_started_at"), run,
               identity)
        speed = self._rate(key, tuple(sizes), active)
        percent = min(100.0, 100 * current / total) if stage == "download" and total else None
        eta = (total - current) / speed if active and total and speed and speed > 0 else None
        count = _number(job.get("analyzed_runs")) if stage in {"analyze", "merge", "done"} else len(confirmed.intersection(runs))
        total_runs = (1 if run in runs else 0) if run else max(len(plan.runs), _number(job.get("total_runs")))
        if run and stage in {"analyze", "merge", "done"}:
            # Reuse the same per-Run status interpretation as the result browser.
            from .activity import activity_snapshot
            scope = activity_snapshot(job, run, limit=1)
            count = int(scope["status"] == "succeeded")
        labels = {"metadata": "解析项目", "download": "下载数据", "analyze": "分析数据", "merge": "汇总结果", "done": "已完成"}
        label = labels.get(stage, "处理中")
        if stage == "download" and status in ACTIVE:
            label = "校验中" if verify else "下载完成" if download_done else transfer["label"] if transfer else label
        if status in {"queued", "pending"}:
            label = "等待资源"
        return ProgressSnapshot(stage, status, label, current, total, speed, percent, eta,
                                min(count, total_runs) if total_runs else count, total_runs,
                                _elapsed(job, self.wall_clock()), run)


def format_bytes(value: int | float | None) -> str:
    if value is None:
        return "未知"
    number = max(0.0, float(value))
    units = ("B", "KB", "MB", "GB", "TB", "PB")
    index = 0
    while number >= 1000 and index < len(units) - 1:
        number /= 1000
        index += 1
    return f"{number:.2f} {units[index]}"


def format_speed(value: float | None) -> str:
    return "速度计算中" if value is None else format_bytes(value) + "/s"


def format_duration(value: float | None) -> str:
    if value is None or not math.isfinite(value):
        return "未知"
    seconds = max(0, math.ceil(value))
    if seconds < 60:
        return f"{seconds} 秒"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes} 分 {seconds:02d} 秒"
    hours, minutes = divmod(minutes, 60)
    return f"{hours} 时 {minutes:02d} 分"


def progress_lines(snapshot: ProgressSnapshot, width=20) -> tuple[str, ...]:
    width = max(3, min(80, int(width)))
    if snapshot.label == "转换 FASTQ":
        elapsed = f" · 总耗时 {format_duration(snapshot.elapsed_seconds)}" if snapshot.elapsed_seconds is not None else ""
        return (snapshot.label + elapsed,)
    if snapshot.stage == "download":
        if snapshot.percent is None:
            first = f"{snapshot.label} [{'░' * width}] 总大小未知"
        else:
            filled = min(width, int(width * snapshot.percent / 100))
            first = f"{snapshot.label} {'█' * filled}{'░' * (width - filled)} {snapshot.percent:.1f}%"
        second = f"{format_bytes(snapshot.current_bytes)} / {format_bytes(snapshot.total_bytes)} · {format_speed(snapshot.speed_bps)}"
        if snapshot.eta_seconds is not None:
            second += f" · 剩余 {format_duration(snapshot.eta_seconds)}"
        return first, second
    count = f" · 已完成 {snapshot.completed_runs}/{snapshot.total_runs} Run" if snapshot.total_runs and snapshot.stage in {"analyze", "merge", "done"} else ""
    elapsed = f" · 总耗时 {format_duration(snapshot.elapsed_seconds)}" if snapshot.elapsed_seconds is not None else ""
    return (snapshot.label + count + elapsed,)
