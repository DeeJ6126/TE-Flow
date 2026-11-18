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
    monkeypatch.setattr(runtime, "check_network", lambda *args, **kwargs: None)
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


def test_single_raw_manifest_accepts_one_read_and_rejects_conflicting_layout(manager):
    job = manager.submit("SRR1", data_type="bulk")
    read = Path(job["raw_dir"]) / "SRR1.fastq.gz"
    read.write_bytes(b"read")
    sample = {"library_layout": "SINGLE", "r1": str(read), "r2": None,
              "files": [{"name": read.name, "bytes": 4}]}
    manifest = Path(job["job_dir"]) / "samples.json"
    document = {"accession": "SRR1", "samples": {"SRR1": sample}}
    manifest.write_text(json.dumps(document))
    assert runtime.raw_manifest_valid(job)
    sample["r2"] = str(read)
    manifest.write_text(json.dumps(document))
    assert not runtime.raw_manifest_valid(job)
    sample.update(r2=None, library_layout="UNKNOWN")
    manifest.write_text(json.dumps(document))
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


def test_selection_csv_submits_one_pooled_job_per_gsm_and_serializes_cleanup(manager, tmp_path):
    source = tmp_path / "selection.csv"
    source.write_text("gsm,target_runs\nGSM1,SRR3;SRR2\nGSM2,SRR1\n")
    jobs = manager.submit_many(str(source), data_type="bulk", discard_reads=True)
    assert [(j["accession"], j["gsm"], j["runs"]) for j in jobs] == [("GSM1", "GSM1", ["SRR3", "SRR2"]),
                                                                     ("GSM2", "GSM2", ["SRR1"])]
    assert jobs[0]["result_path"].endswith(str(Path("GSM1") / "abundance" / "GSM1.cntTable"))
    config = effective_config(manager, jobs[0])
    assert config["DISCARD_READS"] and config["RUNS"] == ["SRR3", "SRR2"] and config["STRANDED"] == "auto"
    scheduler_tick(manager)
    assert [manager.get_job(j["accession"])["status"] for j in jobs] == ["running", "queued"]
    manager.update("GSM1", status="failed")
    scheduler_tick(manager)
    assert manager.get_job("GSM2")["status"] == "running"


def test_selection_csv_rejects_duplicate_gsm_and_shared_run(manager, tmp_path):
    source = tmp_path / "selection.csv"
    source.write_text("gsm,target_runs\nGSM1,SRR1\nGSM1,SRR2\n")
    with pytest.raises(ValueError, match="重复"):
        manager.submit_many(str(source), data_type="bulk")
    source.write_text("gsm,target_runs\nGSM1,SRR1\nGSM2,SRR1\n")
    with pytest.raises(ValueError, match="多个 GSM"):
        manager.submit_many(str(source), data_type="bulk")


def test_resubmitting_gsm_with_other_runs_counts_again(manager, tmp_path):
    source = tmp_path / "selection.csv"
    source.write_text("gsm,target_runs\nGSM1,SRR1\n")
    first = manager.submit_many(str(source), data_type="bulk")[0]
    manager.update("GSM1", status="succeeded")
    source.write_text("gsm,target_runs\nGSM1,SRR1;SRR2\n")
    second = manager.submit_many(str(source), data_type="bulk")[0]
    assert second["runs"] == ["SRR1", "SRR2"] and second["attempt"] == first["attempt"] + 1


def test_pooled_manifest_with_cleaned_receipt_is_reusable(manager, tmp_path):
    job = {"accession": "GSM7", "job_dir": str(tmp_path / "GSM7"), "discard_reads": True}
    table = Path(job["job_dir"]) / "abundance" / "GSM7.cntTable"
    table.parent.mkdir(parents=True)
    table.write_text("gene\tcount\nA\t1\n")
    (Path(job["job_dir"]) / "samples.json").write_text(json.dumps({
        "accession": "GSM7", "pooled": {"sample": "GSM7", "runs": ["SRR8"], "library_layout": "SINGLE"},
        "samples": {"SRR8": {"r1": str(tmp_path / "gone.fastq.gz"), "library_layout": "SINGLE"}}}))
    assert not runtime.raw_manifest_valid(job)
    (table.parent / ".GSM7.cleaned").write_text("GSM7\n")
    assert runtime.raw_manifest_valid(job)


def test_stranded_setting_reaches_workflow_config(manager):
    manager.settings["stranded"] = "reverse"
    job = manager.submit("SRR1", data_type="bulk")
    assert effective_config(manager, job)["STRANDED"] == "reverse"


def test_parallel_jobs_default_auto_keeps_previous_behaviour(manager, tmp_path):
    assert manager.settings["max_parallel_jobs"] == "auto"
    source = tmp_path / "selection.csv"
    source.write_text("gsm,target_runs\nGSM1,SRR1\nGSM2,SRR2\n")
    jobs = manager.submit_many(str(source), data_type="bulk", discard_reads=True)
    assert all(j["parallel_jobs"] == "auto" for j in jobs)
    scheduler_tick(manager)
    assert [manager.get_job(j["accession"])["status"] for j in jobs] == ["running", "queued"]
    assert manager.get_job("GSM2")["queue_wait_reason"] == "等待本批上一个任务结束。"


def test_parallel_jobs_caps_running_projects_per_batch(manager, tmp_path):
    source = tmp_path / "selection.csv"
    source.write_text("gsm,target_runs\nGSM1,SRR1\nGSM2,SRR2\nGSM3,SRR3\n")
    manager.submit_many(str(source), data_type="bulk", discard_reads=True, parallel_jobs=2, cores=5)
    scheduler_tick(manager)
    statuses = [manager.get_job(g)["status"] for g in ("GSM1", "GSM2", "GSM3")]
    assert statuses == ["running", "running", "queued"]
    assert "最多 2 个" in manager.get_job("GSM3")["queue_wait_reason"]
    manager.update("GSM1", status="succeeded")
    scheduler_tick(manager)
    assert manager.get_job("GSM3")["status"] == "running"


def test_parallel_jobs_one_serialises_even_without_cleanup(manager):
    manager.submit_many("GSE1,GSE2", data_type="bulk", parallel_jobs=1, cores=5)
    scheduler_tick(manager)
    assert [manager.get_job(a)["status"] for a in ("GSE1", "GSE2")] == ["running", "queued"]


def test_parallel_jobs_setting_is_validated(manager):
    for bad in (0, -1, "two", True, "1.5"):
        with pytest.raises(ValueError, match="max_parallel_jobs"):
            runtime.parse_parallel_jobs(bad)
    assert runtime.parse_parallel_jobs("AUTO") == "auto" and runtime.parse_parallel_jobs("3") == 3
    settings = yaml.safe_load(manager.settings_path.read_text())
    settings["max_parallel_jobs"] = 3
    manager.settings_path.write_text(yaml.safe_dump(settings))
    assert JobManager(manager.settings_path).settings["max_parallel_jobs"] == 3
