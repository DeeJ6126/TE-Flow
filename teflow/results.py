"""Read-only, bounded result-directory browsing for the terminal interface.

The browser never follows a result path outside the selected job directory.
Pages are indexed by byte offsets, not by repeatedly reading the entire file.
Scientific outputs are not modified by this module.
"""
from __future__ import annotations

from collections import OrderedDict
import csv
from dataclasses import dataclass, field
import io
import json
import os
from pathlib import Path
import re
import stat
import threading
from typing import BinaryIO


PAGE_ROWS = 100
MAX_CELL_CHARS = 2_000
MAX_COLUMNS = 60
MAX_RECORD_BYTES = 65_536
MAX_READ_BYTES = 2 * 1024 * 1024
MAX_METADATA_BYTES = 2 * 1024 * 1024
MAX_LIST_FILES = 10_000
MAX_CACHED_FILES = 32
MAX_SCAN_PAGES = 8
APPEND_EDGE_BYTES = 4096
APPEND_ANCHOR_BYTES = 128
MAX_APPEND_ANCHORS = 128
RUN_RE = re.compile(r"[SED]RR\d+\Z", re.IGNORECASE)


@dataclass(frozen=True)
class ResultFile:
    path: Path
    relative: str
    name: str
    kind: str
    size: int
    modified_ns: int


@dataclass
class ProjectListing:
    runs: list[str] = field(default_factory=list)
    files: list[ResultFile] = field(default_factory=list)
    signature: tuple = ()
    error: str = ""


@dataclass
class PreviewPage:
    kind: str
    title: str
    path: Path
    page: int = 0
    columns: list[str] = field(default_factory=list)
    rows: list[list[str]] = field(default_factory=list)
    text: str = ""
    has_next: bool = False
    signature: tuple = ()


@dataclass
class _FileIndex:
    signature: tuple
    kind: str
    delimiter: str = "\t"
    columns: list[str] = field(default_factory=list)
    notice: str = ""
    offsets: dict[int, int] = field(default_factory=dict)
    pages: OrderedDict[int, PreviewPage] = field(default_factory=OrderedDict)
    prefix: bytes = b""
    tail: bytes = b""
    tail_start: int = 0
    anchors: OrderedDict[int, bytes] = field(default_factory=OrderedDict)


class _ReadLimit(Exception):
    pass


class _Budget:
    def __init__(self):
        self.remaining = MAX_READ_BYTES

    def line(self, handle: BinaryIO) -> bytes:
        if self.remaining <= 0:
            raise _ReadLimit
        chunk = handle.readline(min(8192, self.remaining))
        self.remaining -= len(chunk)
        return chunk

    def read(self, handle: BinaryIO, count: int) -> bytes:
        if count > self.remaining:
            raise _ReadLimit
        data = handle.read(count)
        self.remaining -= len(data)
        return data


def _read_at(handle: BinaryIO, offset: int, count: int, budget: _Budget) -> bytes:
    position = handle.tell()
    try:
        handle.seek(offset)
        return budget.read(handle, count)
    finally:
        handle.seek(position)


def _remember_edges(handle: BinaryIO, index: _FileIndex, budget: _Budget):
    size = index.signature[2]
    index.prefix = _read_at(handle, 0, min(APPEND_EDGE_BYTES, size), budget)
    index.tail_start = max(0, size - APPEND_EDGE_BYTES)
    index.tail = _read_at(handle, index.tail_start, size - index.tail_start, budget)


def _remember_anchor(handle: BinaryIO, index: _FileIndex, offset: int, budget: _Budget):
    size = index.signature[2]
    start = max(0, offset - APPEND_ANCHOR_BYTES // 2)
    count = min(APPEND_ANCHOR_BYTES, max(0, size - start))
    if not count:
        return
    index.anchors[start] = _read_at(handle, start, count, budget)
    index.anchors.move_to_end(start)
    while len(index.anchors) > MAX_APPEND_ANCHORS:
        index.anchors.popitem(last=False)


def _is_verified_append(handle: BinaryIO, index: _FileIndex,
                        signature: tuple, budget: _Budget) -> bool:
    old = index.signature
    if old[0] != signature[0] or old[3] != signature[3] or signature[2] <= old[2]:
        return False
    # Literal edge and page-boundary comparisons are bounded (<25 KiB). They
    # distinguish an append from replacement, truncation and ordinary rewrites
    # without scanning the file, hashing data or discarding current-page offsets.
    probes = [(0, index.prefix), (index.tail_start, index.tail), *index.anchors.items()]
    return all(_read_at(handle, offset, len(expected), budget) == expected
               for offset, expected in probes)


def _signature(path: Path, info: os.stat_result) -> tuple:
    return (str(path), info.st_mtime_ns, info.st_size, info.st_ino)


def _root(job: dict) -> Path:
    value = job.get("job_dir")
    if not value:
        raise ValueError("该项目尚未指定结果目录。")
    return Path(value).expanduser().resolve()


def _inside(root: Path, candidate: Path) -> Path:
    path = candidate.resolve()
    if not path.is_relative_to(root):
        raise ValueError("该文件不在当前项目的结果目录内。")
    return path


def _regular(path: Path) -> os.stat_result:
    info = path.stat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("只能预览普通结果文件。")
    return info


def _open_regular(path: Path):
    # O_NONBLOCK prevents a replaced FIFO from blocking the UI between stat/open.
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError("只能预览普通结果文件。")
        return os.fdopen(descriptor, "rb")
    except BaseException:
        os.close(descriptor)
        raise


def _hidden(name: str) -> bool:
    low = name.lower()
    return (low.startswith(".") or low in {"tmp", "temp", "__pycache__"}
            or "_startmp" in low or low.endswith((".tmp", ".temp")))


def _kind(path: Path) -> str:
    name = path.name.lower()
    if name.endswith((".cnttable", ".count", ".tsv", ".csv")):
        return "table"
    if name.endswith(".bam"):
        return "bam"
    if name.endswith((".gz", ".bgz", ".bz2", ".xz", ".zip", ".sra")):
        return "compressed"
    if name.endswith((".html", ".htm")):
        return "html"
    if name.endswith((".png", ".jpg", ".jpeg", ".pdf", ".tif", ".tiff", ".bai", ".csi")):
        return "binary"
    return "text"


def _file(root: Path, path: Path, relative_root: Path) -> ResultFile | None:
    try:
        resolved = _inside(root, path)
        info = _regular(resolved)
        relative = (path.relative_to(relative_root) if path.is_relative_to(relative_root)
                    else path.relative_to(root))
        return ResultFile(resolved, relative.as_posix(), path.name, _kind(path),
                          info.st_size, info.st_mtime_ns)
    except (OSError, ValueError):
        return None


def _bounded_bytes(root: Path, path: Path) -> bytes:
    resolved = _inside(root, path)
    _regular(resolved)
    with _open_regular(resolved) as handle:
        data = handle.read(MAX_METADATA_BYTES + 1)
    if len(data) > MAX_METADATA_BYTES:
        raise ValueError("项目元数据超过预览读取上限。")
    return data


def _clip(value: str) -> str:
    value = value.replace("\x00", "�")
    if len(value) > MAX_CELL_CHARS:
        return value[:MAX_CELL_CHARS] + "…［单元格已截断］"
    return value


def _record(handle: BinaryIO, delimiter: str | None, budget: _Budget):
    """Read one bounded logical CSV record (or one physical text line).

    Keep only the record prefix while skipping an oversized record. Quote state
    is tracked across physical lines/chunks, so a quoted newline does not move
    the next page into the middle of a CSV row. A hard per-request budget also
    bounds work on pathological multi-gigabyte lines.
    """
    stored = bytearray()
    oversized = False
    quoted = False
    quote_pending = False
    field_start = True
    delimiter_byte = ord(delimiter) if delimiter else None
    saw_data = False
    while True:
        at_start = handle.tell() == 0
        chunk = budget.line(handle)
        if not chunk:
            return (bytes(stored), oversized) if saw_data else None
        saw_data = True
        room = MAX_RECORD_BYTES - len(stored)
        stored.extend(chunk[:max(room, 0)])
        oversized |= len(chunk) > room
        if delimiter is not None:
            scan = chunk[3:] if at_start and chunk.startswith(b"\xef\xbb\xbf") else chunk
            for char in scan:
                if quoted:
                    if quote_pending:
                        if char == 34:
                            quote_pending = False
                            continue
                        quoted = False
                        quote_pending = False
                    elif char == 34:
                        quote_pending = True
                        continue
                    else:
                        continue
                if char == delimiter_byte:
                    field_start = True
                elif char == 34 and field_start:
                    quoted = True
                    field_start = False
                elif char not in (10, 13):
                    field_start = False
        if chunk.endswith(b"\n"):
            if delimiter is None or not quoted or quote_pending:
                return bytes(stored), oversized


def _csv_values(data: bytes, delimiter: str) -> list[str]:
    text = data.decode("utf-8-sig", errors="replace")
    return next(csv.reader(io.StringIO(text, newline=""), delimiter=delimiter), [])


class ResultBrowser:
    def __init__(self):
        self._indices: OrderedDict[str, _FileIndex] = OrderedDict()
        self._lock = threading.RLock()

    def list_project(self, job: dict, show_internal: bool = False) -> ProjectListing:
        try:
            root = _root(job)
            if not root.exists():
                return ProjectListing(error="结果目录尚未创建。")
            if not root.is_dir():
                return ProjectListing(error="结果路径不是文件夹。")
            runs: set[str] = set()
            metadata_signature = []
            for name in ("samples.json", "samples.tsv"):
                path = root / name
                try:
                    data = _bounded_bytes(root, path)
                    metadata_signature.append(_signature(path, path.stat()))
                    if name.endswith("json"):
                        document = json.loads(data.decode("utf-8-sig"))
                        samples = document.get("samples", {})
                        if isinstance(samples, dict):
                            runs.update(str(key).upper() for key in samples if RUN_RE.fullmatch(str(key)))
                    else:
                        reader = csv.DictReader(io.StringIO(data.decode("utf-8-sig")), delimiter="\t")
                        for row in reader:
                            for key, value in row.items():
                                if key and key.lower() in {"run", "run_accession", "sra_run"}:
                                    run = (value or "").strip().upper()
                                    if RUN_RE.fullmatch(run):
                                        runs.add(run)
                except (OSError, ValueError, csv.Error, UnicodeError, TypeError, AttributeError):
                    # The manifest may be absent or still being published. The
                    # other metadata source and physical directories remain useful.
                    pass
            files = []
            for position, entry in enumerate(root.iterdir()):
                if position >= MAX_LIST_FILES:
                    break
                if not show_internal and _hidden(entry.name):
                    continue
                try:
                    path = _inside(root, entry)
                    if path.is_dir() and RUN_RE.fullmatch(entry.name):
                        runs.add(entry.name.upper())
                    elif (item := _file(root, entry, root)) is not None:
                        files.append(item)
                except (OSError, ValueError):
                    continue
            current = str(job.get("current_sample") or "").upper()
            if RUN_RE.fullmatch(current):
                runs.add(current)
            accession = str(job.get("accession") or "").upper()
            if RUN_RE.fullmatch(accession):
                runs.add(accession)
            files.sort(key=lambda item: item.relative.lower())
            ordered_runs = sorted(runs)
            signature = (str(root), tuple(ordered_runs), tuple(metadata_signature),
                         tuple((str(item.path), item.size, item.modified_ns) for item in files))
            return ProjectListing(ordered_runs, files, signature)
        except PermissionError:
            return ProjectListing(error="没有权限读取该项目的结果目录。")
        except (OSError, ValueError) as exc:
            return ProjectListing(error=f"无法读取结果目录：{exc}")

    def list_files(self, job: dict, run: str | None = None,
                   show_internal: bool = False) -> list[ResultFile]:
        root = _root(job)
        if run is not None and not RUN_RE.fullmatch(run):
            raise ValueError("无效的 Run 编号。")
        legacy = run and str(job.get("accession", "")).upper() == run.upper()
        scope = root if not run or legacy else _inside(root, root / run)
        exists = scope.exists()
        if exists and not scope.is_dir():
            raise ValueError("选中的结果路径不是文件夹。")
        files: dict[Path, ResultFile] = {}
        scanned = 0
        pending = [scope] if exists else []
        # scandir streams directory entries. os.walk first allocates every name
        # in a directory, which is unnecessary for a bounded UI listing.
        while pending and scanned < MAX_LIST_FILES:
            current = pending.pop()
            with os.scandir(current) as entries:
                for entry in entries:
                    scanned += 1
                    if scanned > MAX_LIST_FILES:
                        break
                    if not show_internal and _hidden(entry.name):
                        continue
                    path = Path(entry.path)
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            if not (not run and RUN_RE.fullmatch(entry.name)):
                                pending.append(_inside(root, path))
                        elif (item := _file(root, path, scope)) is not None:
                            files[item.path] = item
                    except FileNotFoundError:
                        # Writers can remove temporary outputs between refreshes.
                        continue
        if run and not legacy:
            log_root = root / "logs"
            try:
                resolved_logs = _inside(root, log_root)
                if resolved_logs.is_dir():
                    for position, path in enumerate(log_root.iterdir()):
                        if position >= MAX_LIST_FILES or len(files) >= MAX_LIST_FILES:
                            break
                        if not show_internal and _hidden(path.name):
                            continue
                        if re.match(re.escape(run) + r"[._-]", path.name, re.IGNORECASE):
                            item = _file(root, path, root)
                            if item:
                                files[item.path] = item
            except (OSError, ValueError):
                pass
        return sorted(files.values(), key=lambda item: item.relative.lower())

    def preview(self, job: dict, file_path: str | Path, page: int = 0) -> PreviewPage:
        path = Path(file_path)
        try:
            if not isinstance(page, int) or page < 0:
                raise ValueError("页码必须是非负整数。")
            root = _root(job)
            path = _inside(root, path if path.is_absolute() else root / path)
            info = _regular(path)
            signature = _signature(path, info)
            kind = _kind(path)
            if kind in {"bam", "compressed", "html", "binary"}:
                return self._summary(path, info, kind, page, signature)
            with self._lock, _open_regular(path) as handle:
                # Stat again after opening: an atomic replacement must not reuse
                # the previous file's page offsets.
                signature = _signature(path, os.fstat(handle.fileno()))
                index = self._indices.get(str(path))
                budget = _Budget()
                reset_notice = ""
                if index is not None and index.signature != signature:
                    if _is_verified_append(handle, index, signature, budget):
                        index.signature = signature
                        index.pages.clear()
                        _remember_edges(handle, index, budget)
                    else:
                        index = None
                        if page:
                            page = 0
                            reset_notice = "文件已修改、截断或替换，预览已返回第 1 页。"
                if index is None or index.signature != signature:
                    index = _FileIndex(signature, kind, "," if path.suffix.lower() == ".csv" else "\t")
                    if kind == "table":
                        header = _record(handle, index.delimiter, budget)
                        if header:
                            values = _csv_values(header[0], index.delimiter)
                            index.columns = [_clip(value) or f"列 {i + 1}"
                                             for i, value in enumerate(values[:MAX_COLUMNS])]
                            if header[1]:
                                index.notice = "表头超过 64 KiB，已截断。"
                            if len(values) > MAX_COLUMNS:
                                index.notice += f" 仅显示前 {MAX_COLUMNS} 列。"
                            if any(len(value) > MAX_CELL_CHARS for value in values):
                                index.notice += " 过长表头已截断。"
                        index.offsets[0] = handle.tell()
                    else:
                        index.offsets[0] = 0
                    _remember_edges(handle, index, budget)
                    self._indices[str(path)] = index
                    if len(self._indices) > MAX_CACHED_FILES:
                        self._indices.popitem(last=False)
                self._indices.move_to_end(str(path))
                if page in index.pages:
                    return index.pages[page]
                nearest = max(number for number in index.offsets if number <= page)
                if page - nearest > MAX_SCAN_PAGES:
                    raise ValueError("该页尚未建立索引，请从已加载页面依次翻页。")
                handle.seek(index.offsets[nearest])
                for number in range(nearest, page + 1):
                    result = self._read_page(path, index, handle, number, budget)
                    index.pages[number] = result
                    while len(index.pages) > 4:
                        index.pages.popitem(last=False)
                    if not result.has_next and number < page:
                        return PreviewPage(kind, path.name, path, page,
                                           columns=index.columns.copy(), text="已到文件末尾。",
                                           signature=(*signature, page))
                if reset_notice:
                    result.text = reset_notice + ("\n" + result.text if result.text else "")
                return result
        except FileNotFoundError:
            message = "文件尚未生成，或已被移动/删除。"
        except PermissionError:
            message = "没有权限读取该文件。"
        except _ReadLimit:
            message = "本次预览达到 2 MiB 读取上限；该文件的单行或表头过长。"
        except (OSError, ValueError, csv.Error) as exc:
            message = f"无法预览：{exc}"
        return PreviewPage("error", path.name, path, page, text=message)

    def _read_page(self, path: Path, index: _FileIndex, handle: BinaryIO,
                   page: int, budget: _Budget) -> PreviewPage:
        start = handle.tell()
        result = PreviewPage(index.kind, path.name, path, page,
                             columns=index.columns.copy(), text=index.notice,
                             signature=(*index.signature, page))
        lines = []
        notices = []
        for _ in range(PAGE_ROWS):
            record = _record(handle, index.delimiter if index.kind == "table" else None, budget)
            if record is None:
                break
            data, oversized = record
            if index.kind == "table":
                if oversized:
                    values = ["［该行超过 64 KiB，已省略］"]
                    notices.append("过长数据行已省略。")
                else:
                    values = _csv_values(data, index.delimiter)
                if len(values) > MAX_COLUMNS:
                    notices.append(f"仅显示前 {MAX_COLUMNS} 列。")
                if any(len(value) > MAX_CELL_CHARS for value in values):
                    notices.append("过长单元格已截断。")
                width = len(index.columns) or min(len(values), MAX_COLUMNS)
                if not result.columns and width:
                    result.columns = [f"列 {i + 1}" for i in range(width)]
                if len(values) > width:
                    notices.append("部分数据列超过表头列数，已省略。")
                result.rows.append([_clip(value) for value in values[:width]]
                                   + [""] * max(0, width - len(values)))
            else:
                text = data.decode("utf-8-sig" if handle.tell() == len(data) else "utf-8",
                                   errors="replace").rstrip("\r\n")
                if b"\x00" in data:
                    return PreviewPage("summary", path.name, path, page,
                                       text=f"此文件包含二进制内容，不直接显示。\n路径：{path}",
                                       signature=(*index.signature, page))
                lines.append(text + ("…［本行超过 64 KiB，已截断］" if oversized else ""))
        position = handle.tell()
        result.has_next = position < os.fstat(handle.fileno()).st_size
        if result.has_next:
            index.offsets[page + 1] = position
        _remember_anchor(handle, index, start, budget)
        _remember_anchor(handle, index, position, budget)
        if index.kind == "text":
            result.text = "\n".join(lines) if lines else "（空文件）"
        else:
            if not result.columns and not result.rows:
                notices.append("（空表格）")
            result.text = " ".join(filter(None, [index.notice, *dict.fromkeys(notices)]))
        return result

    def _summary(self, path: Path, info: os.stat_result, kind: str,
                 page: int, signature: tuple) -> PreviewPage:
        descriptions = {"bam": "BAM 比对结果（二进制）", "compressed": "压缩/归档数据文件",
                        "html": "HTML 报告", "binary": "二进制结果文件"}
        text = f"{descriptions[kind]}\n大小：{info.st_size:,} 字节\n路径：{path}"
        if kind == "html":
            with _open_regular(path) as handle:
                prefix = handle.read(16_384).decode("utf-8", errors="replace")
            title = re.search(r"<title\b[^>]*>(.*?)</title\s*>", prefix, re.I | re.S)
            if title:
                from html import unescape
                text += "\n报告标题：" + _clip(unescape(re.sub(r"<[^>]+>", "", title.group(1))).strip())
            text += "\n请在浏览器中打开此报告。"
        elif kind == "bam":
            text += "\n内容包括比对记录；可用 samtools 或基因组浏览器查看。"
        elif kind == "compressed":
            text += "\n预览不解压该文件。"
        return PreviewPage("summary", path.name, path, page, text=text,
                           signature=(*signature, page))
