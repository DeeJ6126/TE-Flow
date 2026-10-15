import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from teflow import runtime
from teflow.inputs import parse_inputs
from teflow.runtime import JobManager, effective_config, normalize_accession, scheduler_tick, worker_environment


@pytest.fixture
def manager(tmp_path, monkeypatch):
    template = tmp_path / "templates"
    template.mkdir()
    reference = tmp_path / "index"
    reference.mkdir()
    (reference / "gene.gtf").write_text("gene\n")
    (reference / "te.gtf").write_text("te\n")
    (template / "config.yaml").write_text(yaml.safe_dump({
        "INDEX_PREFIX": "~/index", "GENOME_ANNOTATION": "~/index/gene.gtf",
        "TE_ANNOTATION": "~/index/te.gtf", "THREADS": 10,
    }))
    settings = tmp_path / "settings.yaml"
    settings.write_text(yaml.safe_dump({
        "raw_root": str(tmp_path / "raw"), "output_root": str(tmp_path / "outputs"),
        "template_dir": str(template), "template_home": str(tmp_path),
        "environment": str(tmp_path / "environment"), "threads": 10,
        "mem_mb": 256000, "state_dir": str(tmp_path / "state"),
        "proxy_env": str(tmp_path / "proxy.env"),
    }))
    monkeypatch.setattr(runtime.subprocess, "Popen", lambda *args, **kwargs: SimpleNamespace(pid=os.getpid()))
    monkeypatch.setattr(runtime, "detect_budget", lambda settings: {"cores": 20, "mem_mb": 512000})
    return JobManager(settings)


@pytest.mark.parametrize("value", ["SRR22521673", "SRP013565", "PRJNA523380", "SRA000001", "ERP123456", "GSE123456"])
def test_accessions(value):
    assert normalize_accession(" " + value.lower() + " ") == value


@pytest.mark.parametrize("value", ["", "../SRR1", "SRR1; rm -rf /", "E-MTAB-2836", "SRR1/other"])
def test_unsafe_or_unsupported_input(value):
    with pytest.raises(ValueError):
        normalize_accession(value)


def test_input_comma_order_dedup_and_atomic_validation(manager):
    assert parse_inputs("gse1,GSE2,GSE1") == ["GSE1", "GSE2"]
    with pytest.raises(ValueError):
        manager.submit_many("GSE1,invalid,GSE2", data_type="bulk")
    assert manager.list_jobs() == []


@pytest.mark.parametrize("data_type,message", [
    (None, "请选择数据类型"), ("", "请选择数据类型"),
    ("scrna", "单细胞分析流程待配置"), ("other", "不支持的数据类型"),
    (123, "数据类型必须是"),
])
def test_invalid_or_unavailable_data_type_has_no_submission_side_effects(manager, monkeypatch, data_type, message):
    monkeypatch.setattr(runtime.subprocess, "Popen", lambda *a, **kw: pytest.fail("invalid type launched a process"))
    with pytest.raises(ValueError, match=message):
        manager.submit_many("GSE1,GSE2", data_type=data_type)
    with pytest.raises(ValueError, match=message):
        manager.submit("GSE1", data_type=data_type)
    assert not manager.registry_path.exists()
    assert list(manager.state_dir.iterdir()) == []
    assert not Path(manager.settings["output_root"]).exists()
    assert not Path(manager.settings["raw_root"]).exists()


def test_bulk_type_is_canonical_and_persisted(manager):
    jobs = manager.submit_many("GSE1,GSE2", data_type=" Bulk ")
    assert [job["data_type"] for job in jobs] == ["bulk", "bulk"]
    assert all(job["data_type"] == "bulk" for job in json.loads(manager.registry_path.read_text())["jobs"].values())
    assert all(json.loads((Path(job["job_dir"]) / "job.json").read_text())["data_type"] == "bulk" for job in jobs)


def test_legacy_job_without_type_remains_bulk_and_reuses_attempt(manager):
    first = manager.submit("GSE1", data_type="bulk")
    registry = json.loads(manager.registry_path.read_text())
    registry["jobs"]["GSE1"].pop("data_type")
    manager.registry_path.write_text(json.dumps(registry))
    assert manager.get_job("GSE1")["data_type"] == "bulk"
    again = manager.submit("GSE1", data_type="bulk")
    assert again["data_type"] == "bulk" and again["attempt"] == first["attempt"]


def test_scrna_validation_precedes_file_access(manager):
    with pytest.raises(ValueError, match="单细胞分析流程待配置"):
        manager.submit_many("/does/not/exist.txt", input_mode="file", data_type="scrna")
    assert not manager.registry_path.exists()


def test_server_txt_absolute_path_bom_and_line_errors(tmp_path):
    source = tmp_path / "list.txt"
    source.write_text("\ufeffgse1\n\nGSE2\nGSE1\n", encoding="utf-8")
    assert parse_inputs(str(source)) == ["GSE1", "GSE2"]
    source.write_text("GSE1\nGSE2,GSE3\n")
    with pytest.raises(ValueError, match="2"):
        parse_inputs(str(source), "file")
    with pytest.raises(ValueError, match="绝对路径"):
        parse_inputs("relative.txt", "file")
    source.write_text(" \n")
    with pytest.raises(ValueError, match="为空"):
        parse_inputs(str(source))
    with pytest.raises(ValueError, match="无法读取"):
        parse_inputs(str(tmp_path / "absent.txt"))


def test_duplicate_submission_only_spawns_one_scheduler(manager, monkeypatch):
    calls = []
    monkeypatch.setattr(runtime.subprocess, "Popen", lambda *args, **kwargs: (calls.append(args) or SimpleNamespace(pid=os.getpid())))
    first = manager.submit("SRR22521673", data_type="bulk")
    second = manager.submit("srr22521673", data_type="bulk")
    assert len(calls) == 1
    assert first["batch_id"] == second["batch_id"]
    assert first["pid"] is None and first["status"] == "queued"
    third = manager.submit("GSE113777", data_type="bulk")
    assert third["status"] == "queued" and len(calls) == 1


def test_auto_parallel_shared_budget_and_failure_isolation(manager):
    jobs = manager.submit_many("GSE1,GSE2,GSE3", data_type="bulk")
    assert scheduler_tick(manager)
    statuses = {j["accession"]: j["status"] for j in manager.list_jobs()}
    assert statuses == {"GSE3": "queued", "GSE2": "running", "GSE1": "running"}
    assert manager.get_job("GSE3")["queue_wait_reason"].startswith("等待资源")
    manager.update("GSE1", status="failed", error="controlled failure")
    assert scheduler_tick(manager)
    assert manager.get_job("GSE2")["status"] == "running"
    assert manager.get_job("GSE3")["status"] == "running"
    manager.update("GSE2", status="succeeded")
    manager.update("GSE3", status="succeeded")
    assert scheduler_tick(manager) is False


def test_second_manager_joins_same_queue_without_extra_reservations(manager):
    manager.submit_many("GSE1,GSE2", data_type="bulk")
    other = JobManager(manager.settings_path)
    assert len(other.submit_many("GSE2,GSE3", data_type="bulk")) == 2
    assert len(other.list_jobs()) == 3
    scheduler_tick(other)
    running = [j for j in manager.list_jobs() if j["status"] == "running"]
    assert len(running) == 2
    assert sum(j["mem_mb"] for j in running) <= 512000


def test_success_reuses_cnttable_and_missing_result_resumes(manager, monkeypatch):
    job = manager.submit("SRR22521673", data_type="bulk")
    raw = Path(job["raw_dir"])
    first, second = raw / "run_1.fastq.gz", raw / "run_2.fastq.gz"
    first.write_bytes(b"test")
    second.write_bytes(b"test")
    (Path(job["job_dir"]) / "samples.json").write_text(json.dumps({
        "accession": job["accession"], "samples": {job["accession"]: {
            "r1": str(first), "r2": str(second), "files": [
                {"name": first.name, "bytes": 4}, {"name": second.name, "bytes": 4}]}}}))
    result = Path(job["result_path"])
    result.parent.mkdir(parents=True)
    result.write_text("gene\tSRR22521673\nA\t1\n")
    manager.update(job["accession"], status="succeeded", stage="done")
    monkeypatch.setattr(runtime, "detect_budget", lambda settings: (_ for _ in ()).throw(ValueError("no free memory")))
    assert manager.submit(job["accession"], data_type="bulk")["status"] == "succeeded"
    monkeypatch.setattr(runtime, "detect_budget", lambda settings: {"cores": 20, "mem_mb": 512000})
    result.unlink()
    assert manager.submit(job["accession"], data_type="bulk")["attempt"] == 2


def test_missing_raw_manifest_uses_nested_record_paths(manager):
    job = manager.submit("GSE1", data_type="bulk")
    raw = Path(job["raw_dir"]) / "SRR1"
    raw.mkdir()
    first, second = raw / "run_1.fastq.gz", raw / "run_2.fastq.gz"
    first.write_bytes(b"test")
    second.write_bytes(b"test")
    document = {"accession": "GSE1", "samples": {"SRR1": {
        "r1": str(first), "r2": str(second), "files": [
            {"name": first.name, "bytes": 4}, {"name": second.name, "bytes": 4}]}}}
    (Path(job["job_dir"]) / "samples.json").write_text(json.dumps(document))
    assert runtime.raw_manifest_valid(job)
    second.unlink()
    assert not runtime.raw_manifest_valid(job)


def test_queued_jobs_survive_without_worker_pid_and_dead_running_interrupts(manager, monkeypatch):
    manager.submit("GSE1", data_type="bulk")
    monkeypatch.setattr(runtime, "pid_alive", lambda pid: False)
    monkeypatch.setattr(manager, "_ensure_scheduler", lambda registry: None)
    assert manager.get_job("GSE1")["status"] == "queued"
    manager.update("GSE1", status="running", pid=999999)
    assert manager.get_job("GSE1")["status"] == "interrupted"


def test_scheduler_launch_failure_does_not_break_history_view(manager, monkeypatch):
    def fail(*args, **kwargs):
        raise FileNotFoundError("missing environment")
    monkeypatch.setattr(runtime.subprocess, "Popen", fail)
    manager.submit("GSE1", data_type="bulk")
    job = manager.get_job("GSE1")
    assert job["status"] == "failed" and "调度器启动失败" in job["error"]


def test_worker_launch_failure_releases_resources_for_other_projects(manager, monkeypatch):
    manager.submit_many("GSE1,GSE2", data_type="bulk")
    calls = []
    def spawn(*args, **kwargs):
        calls.append(args)
        if len(calls) == 1:
            raise OSError("controlled spawn failure")
        return SimpleNamespace(pid=os.getpid())
    monkeypatch.setattr(runtime.subprocess, "Popen", spawn)
    scheduler_tick(manager)
    assert manager.get_job("GSE1")["status"] == "failed"
    assert manager.get_job("GSE2")["status"] == "running"


def test_legacy_running_reservations_are_normalized(manager):
    job = manager.submit("GSE1", data_type="bulk")
    manager.update("GSE1", status="running", pid=os.getpid())
    with manager._lock():
        registry = manager._load()
        registry["jobs"]["GSE1"].pop("cores")
        registry["jobs"]["GSE1"].pop("mem_mb")
        manager._save(registry)
    assert manager.get_job("GSE1")["cores"] == 10
    scheduler_tick(manager)


def test_reference_paths_and_allocated_cores(manager):
    job = manager.submit_many("GSE1", cores=5, data_type="bulk")[0]
    config = effective_config(manager, job)
    assert config["ACCESSION"] == "GSE1"
    assert config["THREADS"] == 5
    assert Path(config["INDEX_PREFIX"]).name == "index" and "~" not in config["INDEX_PREFIX"]


def test_request_above_global_budget_has_no_partial_batch(manager):
    with pytest.raises(ValueError, match="超过总预算"):
        manager.submit_many("GSE1,GSE2", cores=21, data_type="bulk")
    assert manager.list_jobs() == []


def test_proxy_parsing_is_not_shell_execution(manager, monkeypatch):
    monkeypatch.setenv("HOME", "original-home")
    Path(manager.settings["proxy_env"]).write_text("export HTTPS_PROXY='http://127.0.0.1:17890'\nHOME=/other\nexport NO_PROXY=localhost,127.0.0.1\n")
    environment = worker_environment(manager.settings)
    assert environment["HTTPS_PROXY"] == "http://127.0.0.1:17890"
    assert environment["HOME"] == "original-home"
    assert environment["NO_PROXY"] == "localhost,127.0.0.1"


def test_logs_retain_corrupt_utf8_without_crashing(manager):
    job = manager.submit("SRR22521673", data_type="bulk")
    (Path(job["job_dir"]) / "run.log").write_bytes(b"line1\nline2\xff\n")
    assert "line2" in manager.log_tail(job["accession"])


def test_scientific_error_prefers_reason_over_snakemake_footer(manager):
    job = manager.submit("GSE1", data_type="bulk")
    output = Path(job["job_dir"])
    (output / "run.log").write_text("ValueError: SRR1: 当前模板需要双端，实际 SINGLE。\nWorkflowError:\nAt least one job did not complete successfully.\n")
    assert "实际 SINGLE" in runtime._failure_message(output, 1, job)


def test_nested_projects_read_actual_project_tool_logs(manager):
    job = manager.submit("GSE1", data_type="bulk")
    output = Path(job["job_dir"])
    (output / "logs").mkdir()
    (output / "logs/SRR1.STAR.log").write_text("EXITING because FATAL ERROR: example\n")
    job = manager.update("GSE1", stage="analyze", step="比对", current_sample="SRR1")
    assert "FATAL ERROR" in manager.log_tail("GSE1")
    assert "FATAL ERROR" in runtime._failure_message(output, 1, job)
