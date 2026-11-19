"""2.7: separate download / analysis scheduling and download options."""
import pytest

from teflow import cli, runtime
from teflow.runtime import JobManager, effective_config, scheduler_tick

from test_runtime import manager  # noqa: F401  (shared fixture)


def selection(tmp_path, count):
    source = tmp_path / "selection.csv"
    source.write_text("gsm,target_runs\n" + "".join(f"GSM{i},SRR{i}\n" for i in range(1, count + 1)))
    return str(source)


def finish_download(manager, accession):
    """What run_worker records after a successful download phase."""
    manager.update(accession, status="queued", phase="analyze", pid=None, pid_start=None,
                   snakemake_pid=None, snakemake_pid_start=None, reserved_cores=None, reserved_mem_mb=None)


def test_defaults_keep_previous_behaviour(manager, tmp_path):
    settings = manager.settings
    assert (settings["download_source"], settings["download_connections"],
            settings["max_parallel_downloads"], settings["network_wait_minutes"]) == ("ena", 1, 0, 0.0)
    job = manager.submit_many(selection(tmp_path, 1), data_type="bulk", discard_reads=True)[0]
    assert job["phase"] is None and job["download_jobs"] == 0
    config = effective_config(manager, job)
    assert (config["DOWNLOAD_SOURCE"], config["DOWNLOAD_CONNECTIONS"], config["NETWORK_WAIT"]) == ("ena", 1, 0.0)
    scheduler_tick(manager)
    running = manager.get_job("GSM1")
    assert running["status"] == "running" and running["reserved_cores"] == 10 and running["reserved_mem_mb"] == 256000


def test_download_phase_reserves_little_and_is_capped(manager, tmp_path):
    manager.submit_many(selection(tmp_path, 3), data_type="bulk", discard_reads=True, parallel_jobs=1,
                        cores=4, download_source="aws", connections=16, downloads=2, network_wait=720)
    scheduler_tick(manager)
    jobs = {g: manager.get_job(g) for g in ("GSM1", "GSM2", "GSM3")}
    assert [jobs[g]["status"] for g in jobs] == ["running", "running", "queued"]
    assert all(jobs[g]["phase"] == "download" for g in jobs)
    assert jobs["GSM1"]["reserved_cores"] == 4 and jobs["GSM1"]["reserved_mem_mb"] == runtime.DOWNLOAD_MEM_MB
    assert "最多 2 个同时下载" in jobs["GSM3"]["queue_wait_reason"]
    config = effective_config(manager, jobs["GSM1"])
    assert (config["DOWNLOAD_SOURCE"], config["DOWNLOAD_CONNECTIONS"], config["NETWORK_WAIT"]) == ("aws", 16, 43200.0)


def test_downloaded_project_is_analysed_with_full_reservation_while_next_downloads(manager, tmp_path):
    manager.submit_many(selection(tmp_path, 3), data_type="bulk", discard_reads=True, parallel_jobs=1,
                        cores=4, download_source="aws", downloads=2)
    scheduler_tick(manager)
    finish_download(manager, "GSM1")
    scheduler_tick(manager)
    first, third = manager.get_job("GSM1"), manager.get_job("GSM3")
    assert first["status"] == "running" and first["phase"] == "analyze"
    assert first["reserved_cores"] == 4 and first["reserved_mem_mb"] == 256000
    assert third["status"] == "running" and third["phase"] == "download"
    # Analysis is limited per batch (-j 1); downloads are not counted against it.
    finish_download(manager, "GSM2")
    scheduler_tick(manager)
    assert manager.get_job("GSM2")["status"] == "queued"
    assert manager.get_job("GSM2")["queue_wait_reason"] == "等待本批上一个任务结束。"
    manager.update("GSM1", status="succeeded")
    scheduler_tick(manager)
    assert manager.get_job("GSM2")["status"] == "running" and manager.get_job("GSM2")["phase"] == "analyze"


def test_downloads_pause_when_downloaded_backlog_is_full(manager, tmp_path):
    manager.submit_many(selection(tmp_path, 4), data_type="bulk", discard_reads=True, parallel_jobs=1,
                        cores=4, downloads=1)
    scheduler_tick(manager)
    finish_download(manager, "GSM1")
    scheduler_tick(manager)            # GSM1 analyses, GSM2 downloads
    finish_download(manager, "GSM2")
    scheduler_tick(manager)            # GSM2 waits for analysis; backlog of 1 blocks GSM3
    assert manager.get_job("GSM3")["status"] == "queued"
    assert "暂缓下载" in manager.get_job("GSM3")["queue_wait_reason"]


def test_download_options_are_validated(manager):
    assert runtime.parse_download_options("AWS", "16", 8, "720") == ("aws", 16, 8, 720.0)
    for bad in (("ftp", 1, 0, 0), ("aws", 0, 0, 0), ("aws", 1, -1, 0), ("aws", 1, 0, -5), ("aws", "x", 0, 0)):
        with pytest.raises(ValueError):
            runtime.parse_download_options(*bad)
    with pytest.raises(ValueError):
        manager.submit_many("GSE1", data_type="bulk", download_source="ftp")


def test_settings_can_enable_staged_downloads(manager):
    import yaml
    settings = yaml.safe_load(manager.settings_path.read_text())
    settings.update(download_source="aws", download_connections=12, max_parallel_downloads=6, network_wait_minutes=60)
    manager.settings_path.write_text(yaml.safe_dump(settings))
    job = JobManager(manager.settings_path).submit_many("GSE1", data_type="bulk")[0]
    assert (job["download_source"], job["download_connections"], job["download_jobs"], job["phase"]) == \
        ("aws", 12, 6, "download")


def test_shortcut_forwards_download_options(monkeypatch):
    forwarded = []
    monkeypatch.setattr(cli, "main", lambda argv: (forwarded.append(argv) or 0))
    assert cli.shortcut_main(["-f", "/root/sel.csv", "-t", "bulk", "--source", "aws", "--connections", "16",
                              "--downloads", "8", "--wait-network", "720"]) == 0
    argv = forwarded[0]
    for flag, value in (("--source", "aws"), ("--connections", "16"), ("--downloads", "8"), ("--wait-network", "720")):
        assert argv[argv.index(flag) + 1] == value
    assert argv[-2:] == ["--", "/root/sel.csv"]
    parsed = cli.build_parser().parse_args(argv)
    assert (parsed.download_source, parsed.connections, parsed.downloads, parsed.network_wait) == ("aws", 16, 8, 720)


def test_download_options_without_source_are_rejected(monkeypatch):
    monkeypatch.setattr(cli, "main", lambda argv: 0)
    with pytest.raises(SystemExit):
        cli.shortcut_main(["--downloads", "4"])


def test_registry_is_compact_and_job_records_stay_readable(manager):
    job = manager.submit_many("GSE1", data_type="bulk")[0]
    registry = manager.registry_path.read_text(encoding="utf-8")
    assert "\n  " not in registry and '"GSE1"' in registry
    assert "\n  " in (runtime.Path(job["job_dir"]) / "job.json").read_text(encoding="utf-8")
