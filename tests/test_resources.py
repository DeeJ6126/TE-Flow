from types import SimpleNamespace

import pytest

from teflow import resources


def test_available_capacity_accounts_for_both_cpu_and_memory():
    budget = {"cores": 20, "mem_mb": 512000}
    jobs = [{"status": "running", "cores": 10, "mem_mb": 256000},
            {"status": "queued", "cores": 10, "mem_mb": 256000},
            {"status": "failed", "cores": 10, "mem_mb": 256000}]
    assert resources.available_capacity(budget, jobs) == (10, 256000)


def test_auto_budget_respects_cgroup_and_headroom(monkeypatch):
    monkeypatch.setattr(resources.os, "cpu_count", lambda: 64)
    monkeypatch.setattr(resources.psutil, "Process", lambda: SimpleNamespace(cpu_affinity=lambda: list(range(32))))
    monkeypatch.setattr(resources.psutil, "virtual_memory", lambda: SimpleNamespace(available=1024**4))
    original = resources.Path.read_text
    def contents(path, *args, **kwargs):
        return {"/sys/fs/cgroup/cpu.max": "800000 100000",
                "/sys/fs/cgroup/memory.max": str(512000 * 1024**2),
                "/sys/fs/cgroup/memory.current": str(12000 * 1024**2)}.get(str(path), "max")
    monkeypatch.setattr(resources.Path, "read_text", contents)
    budget = resources.detect_budget({"resource_fraction": 0.8})
    assert budget["cores"] == 6
    assert budget["mem_mb"] == 400000
    with pytest.raises(ValueError, match="CPU"):
        resources.detect_budget({"total_cores": 9})
    with pytest.raises(ValueError, match="总内存"):
        resources.detect_budget({"total_mem_mb": 500001})
