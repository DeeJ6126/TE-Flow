"""Shared, side-effect-free progress text for the terminal interfaces."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from rich.text import Text


STAGES = (
    ("metadata", "解析项目"),
    ("download", "下载数据"),
    ("analyze", "分析数据"),
    ("merge", "汇总结果"),
)
SUCCESS = {"succeeded", "success", "completed"}
ACTIVE = {"queued", "pending", "running"}
STATUS_LABELS = {
    "queued": "等待资源", "pending": "等待资源", "running": "运行中",
    "succeeded": "已完成", "success": "已完成", "completed": "已完成",
    "failed": "失败", "cancelled": "已取消", "interrupted": "已中断",
}
DETAIL_LABELS = {
    "quality_control": "质控", "qc": "质控", "fastp": "质控",
    "star_align": "比对", "align": "比对", "star": "比对",
    "te_count": "TE 计数", "count": "TE 计数", "tecount": "TE 计数",
}


def value(item: Any) -> str:
    return str(getattr(item, "value", item) or "").lower()


def stage_key(job: Mapping[str, Any]) -> str:
    stage = value(job.get("stage"))
    if value(job.get("status")) in SUCCESS:
        return "done"
    return {
        "preparing": "metadata", "prepare": "metadata", "resolve": "metadata",
        "qc": "analyze", "align": "analyze", "count": "analyze",
        "quality_control": "analyze", "star_align": "analyze", "te_count": "analyze",
        "summarize": "merge",
    }.get(stage, stage if stage in {key for key, _ in STAGES} | {"done"} else "metadata")


def stage_label(job: Mapping[str, Any]) -> str:
    stage = stage_key(job)
    return dict(STAGES).get(stage, "完成" if stage == "done" else "解析项目")


def status_label(job: Mapping[str, Any]) -> str:
    status = value(job.get("status"))
    return STATUS_LABELS.get(status, status or "未知")


def _number(item: Any) -> int:
    try:
        return max(0, int(item or 0))
    except (TypeError, ValueError, OverflowError):
        return 0


def job_progress(job: Mapping[str, Any]) -> str:
    status = value(job.get("status"))
    if status in SUCCESS:
        return "已完成"
    if status in {"queued", "pending"}:
        return "等待资源（空出后自动开始）"
    if status not in ACTIVE:
        return status_label(job)
    stage = stage_key(job)
    total = _number(job.get("total_runs"))
    if stage == "metadata":
        detail = str(job.get("step") or "").strip()
        if value(detail) in {"", "metadata", "preparing", "解析项目", "准备解析项目", "等待资源"}:
            detail = "正在获取样本和 Run 清单"
        sample = str(job.get("current_sample") or "").strip()
        return f"{detail} · {sample}" if sample else detail
    if stage == "download":
        return f"已下载 {_number(job.get('downloaded_runs'))}/{total} Run" if total else "正在下载"
    if stage == "merge":
        return "正在合并计数表"
    if stage == "done":
        return "正在确认结果"
    parts = [f"已完成 {_number(job.get('analyzed_runs'))}/{total} Run"] if total else []
    sample = str(job.get("current_sample") or "")
    step = value(job.get("step") or job.get("stage"))
    detail = DETAIL_LABELS.get(step, str(job.get("step") or ""))
    if sample:
        parts.append(sample)
    if detail:
        parts.append(detail)
    return " · ".join(parts) or "正在分析"


def job_summary(job: Mapping[str, Any]) -> str:
    status = value(job.get("status"))
    pieces = [str(job.get("accession", ""))]
    if status == "running":
        pieces.extend((stage_label(job), job_progress(job)))
    elif status in {"queued", "pending"}:
        pieces.append(job_progress(job))
    else:
        pieces.append(status_label(job))
        if status not in SUCCESS:
            pieces.append(stage_label(job))
    return " · ".join(pieces)


def batch_counts(jobs: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    counts = dict.fromkeys(("total", "running", "queued", "succeeded", "failed"), 0)
    for job in jobs:
        counts["total"] += 1
        status = value(job.get("status"))
        key = "succeeded" if status in SUCCESS else "queued" if status in {"queued", "pending"} else "running" if status == "running" else "failed"
        counts[key] += 1
    return counts


def batch_summary(jobs: Iterable[Mapping[str, Any]]) -> str:
    counts = batch_counts(jobs)
    return (f"共 {counts['total']} 个项目 · 运行中 {counts['running']} · 等待 {counts['queued']}"
            f" · 完成 {counts['succeeded']} · 失败 {counts['failed']}")


def stage_track(job: Mapping[str, Any]) -> Text:
    stage = stage_key(job)
    current = 4 if stage == "done" else next(index for index, (key, _) in enumerate(STAGES) if key == stage)
    active = value(job.get("status"))
    text = Text()
    for index, (_, label) in enumerate(STAGES):
        if index:
            text.append(" ━━ " if index <= current else " ── ", style="dim")
        if index < current:
            text.append(f"✓ {label}", style="green")
        elif index == current and active in {"queued", "pending"}:
            text.append(f"○ {label}", style="bold yellow")
        elif index == current:
            text.append(f"{'●' if active in ACTIVE else '✗'} {label}", style="bold cyan" if active in ACTIVE else "bold red")
        else:
            text.append(f"○ {label}", style="dim")
    return text


def progress_signature(job: Mapping[str, Any]) -> tuple:
    """Fields that deserve a new line in a redirected CLI log."""
    return tuple(str(job.get(key) or "") for key in (
        "status", "stage", "step", "total_runs", "downloaded_runs", "analyzed_runs",
        "current_sample", "queue_wait_reason", "error", "result_path",
    ))
