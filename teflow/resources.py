"""Conservative, shared per-workflow reservations for independent processes."""
import math
import os
from pathlib import Path

import psutil


def _integer_file(path: str) -> int | None:
    try:
        value = Path(path).read_text().strip()
        return int(value) if value != "max" else None
    except (OSError, ValueError):
        return None


def detect_budget(settings: dict) -> dict:
    cpus = os.cpu_count() or 1
    try:
        cpus = min(cpus, len(psutil.Process().cpu_affinity()))
    except (AttributeError, psutil.Error):
        pass
    try:
        quota, period = Path("/sys/fs/cgroup/cpu.max").read_text().split()
        if quota != "max":
            cpus = min(cpus, max(1, math.floor(int(quota) / int(period))))
    except (OSError, ValueError):
        pass
    available = psutil.virtual_memory().available
    maximum = _integer_file("/sys/fs/cgroup/memory.max")
    used = _integer_file("/sys/fs/cgroup/memory.current")
    if maximum is not None and used is not None:
        available = min(available, max(0, maximum - used))
    fraction = float(settings.get("resource_fraction", 0.8))
    if not 0 < fraction <= 1:
        raise ValueError("resource_fraction 必须在 0 到 1 之间。")
    configured_cores = settings.get("total_cores", "auto")
    configured_memory = settings.get("total_mem_mb", "auto")
    cores = max(1, math.floor(cpus * fraction)) if configured_cores == "auto" else int(configured_cores)
    memory = math.floor(available / 1024**2 * fraction) if configured_memory == "auto" else int(configured_memory)
    if cores < 1 or memory < 1:
        raise ValueError("总 CPU 和内存预算必须大于零。")
    if cores > cpus:
        raise ValueError(f"总 CPU 预算 {cores} 超过当前可用 CPU {cpus}。")
    if memory > available // 1024**2:
        raise ValueError("总内存预算超过当前可用内存。")
    return {"cores": cores, "mem_mb": memory, "detected_cores": cpus,
            "detected_available_mem_mb": available // 1024**2,
            "reservation": "whole_workflow"}


def available_capacity(budget: dict, jobs: list[dict]) -> tuple[int, int]:
    running = [job for job in jobs if job["status"] == "running"]
    return (budget["cores"] - sum(int(job["cores"]) for job in running),
            budget["mem_mb"] - sum(int(job["mem_mb"]) for job in running))
