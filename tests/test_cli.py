import io
import pytest
from rich.console import Console

from teflow import cli


def job(accession="GSE113777", status="queued", stage="metadata", **extra):
    return {"accession": accession, "status": status, "stage": stage,
            "job_dir": f"/root/TEatlas/bulk_rna/{accession}",
            "result_path": f"/root/TEatlas/bulk_rna/{accession}/merged.count", **extra}


class FakeManager:
    def __init__(self, initial, snapshots=()):
        self.initial = initial
        self.snapshots = iter(snapshots)
        self.current = initial
        self.submissions = []
        self.log_calls = []
        self.list_calls = 0

    def submit_many(self, source, input_mode="auto", cores=None, *, data_type=None):
        self.submissions.append((source, input_mode, cores, data_type))
        return self.initial

    def list_jobs(self):
        self.list_calls += 1
        self.current = next(self.snapshots)
        return self.current

    def log_tail(self, accession, lines=100):
        self.log_calls.append(accession)
        return f"tool noise {accession}"


def output_console(**kwargs):
    stream = io.StringIO()
    return Console(file=stream, width=120, **kwargs), stream


def test_redirected_progress_only_emits_changes_and_final_path(monkeypatch):
    initial = job()
    download = job(status="running", stage="download", total_runs=2, downloaded_runs=1)
    analyze = job(status="running", stage="analyze", total_runs=2, analyzed_runs=1, step="align")
    done = job(status="succeeded", stage="done")
    manager = FakeManager([initial], [[download], [{**download, "updated_at": "later"}], [analyze], [done]])
    monkeypatch.setattr(cli.time, "sleep", lambda _: None)
    console, stream = output_console()
    assert cli.watch_jobs(manager, [initial], console=console) == 0
    text = stream.getvalue()
    assert text.count("已下载 1/2 Run") == 1
    assert "分析数据" in text and "汇总结果" in text
    assert done["result_path"] in text
    assert "tool noise" not in text and manager.log_calls == []


@pytest.mark.parametrize("source", ["GSE113777,GSE52778", "/root/geo_ids.txt"])
def test_run_accepts_batch_source_without_parallel_option(source, monkeypatch):
    manager = FakeManager([job(status="succeeded", stage="done")])
    monkeypatch.setattr(cli, "JobManager", lambda settings: manager)
    assert cli.main(["run", "-t", "bulk", source]) == 0
    assert manager.submissions == [(source, "auto", None, "bulk")]


@pytest.mark.parametrize("arguments", [
    ["--settings", "/root/teflow/settings.yaml", "run", "-t", "bulk", "GSE1", "--cores", "4"],
    ["run", "-t", "bulk", "GSE1", "--cores", "4", "--settings", "/root/teflow/settings.yaml"],
])
def test_settings_before_or_after_subcommand(arguments, monkeypatch):
    selected = []
    manager = FakeManager([job(status="succeeded")])
    monkeypatch.setattr(cli, "JobManager", lambda settings: (selected.append(settings) or manager))
    assert cli.main(arguments) == 0
    assert selected == ["/root/teflow/settings.yaml"]
    assert manager.submissions == [("GSE1", "auto", 4, "bulk")]


def test_failures_do_not_stop_viewing_other_projects(monkeypatch):
    failed = job("GSE1", status="failed", error="metadata 不完整\nlong tool traceback")
    waiting = job("GSE2")
    done = job("GSE2", status="succeeded")
    manager = FakeManager([failed, waiting], [[failed, done]])
    monkeypatch.setattr(cli.time, "sleep", lambda _: None)
    console, stream = output_console()
    assert cli.watch_jobs(manager, manager.initial, console=console) == 1
    text = stream.getvalue()
    assert text.count("GSE1 错误：metadata 不完整") == 1
    assert "long tool traceback" not in text
    assert done["result_path"] in text
    assert manager.list_calls == 1


@pytest.mark.parametrize("status", ["failed", "cancelled", "interrupted"])
def test_non_success_terminal_status_returns_one(status):
    initial = [job(status=status)]
    console, _ = output_console()
    assert cli.watch_jobs(FakeManager(initial), initial, console=console) == 1


def test_invalid_input_returns_two_with_error(monkeypatch, capsys):
    manager = FakeManager([])
    def invalid(*args, **kwargs):
        raise ValueError("请输入有效编号")
    manager.submit_many = invalid
    monkeypatch.setattr(cli, "JobManager", lambda _: manager)
    assert cli.main(["run", "-t", "bulk", "bad"]) == 2
    assert "请输入有效编号" in capsys.readouterr().err


def test_missing_file_returns_two(monkeypatch):
    manager = FakeManager([])
    def missing(*args, **kwargs):
        raise FileNotFoundError("/root/missing.txt")
    manager.submit_many = missing
    monkeypatch.setattr(cli, "JobManager", lambda _: manager)
    assert cli.main(["run", "-t", "bulk", "/root/missing.txt"]) == 2


def test_nonpositive_cores_rejected_before_submission(monkeypatch):
    monkeypatch.setattr(cli, "JobManager", lambda _: pytest.fail("invalid argument reached runtime"))
    with pytest.raises(SystemExit) as error:
        cli.main(["run", "-t", "bulk", "GSE1", "--cores", "0"])
    assert error.value.code == 2


def test_ctrl_c_closes_viewer_without_cancelling_jobs(monkeypatch):
    initial = [job()]
    manager = FakeManager(initial)
    def interrupt(_):
        raise KeyboardInterrupt
    monkeypatch.setattr(cli.time, "sleep", interrupt)
    console, stream = output_console()
    assert cli.watch_jobs(manager, initial, console=console) == 130
    assert "后台任务继续运行" in stream.getvalue()
    assert manager.initial[0]["status"] == "queued"


def test_verbose_shows_log_tail_once_when_unchanged(monkeypatch):
    initial = [job()]
    manager = FakeManager(initial, [[job(status="succeeded")]])
    monkeypatch.setattr(cli.time, "sleep", lambda _: None)
    console, stream = output_console()
    assert cli.watch_jobs(manager, initial, console=console, verbose=True) == 0
    assert stream.getvalue().count("tool noise GSE113777") == 1


def test_log_delta_handles_rolling_tail_and_appended_lines():
    assert cli._log_delta("one\ntwo\n", "one\ntwo\nthree\n") == "three\n"
    assert cli._log_delta("one\ntwo", "two\nthree") == "three"
    assert cli._log_delta("one", "one") == ""


def test_live_dashboard_has_four_stage_overview_and_result():
    initial = [job(status="succeeded", stage="done")]
    console, stream = output_console(force_terminal=True, color_system=None)
    assert cli.watch_jobs(FakeManager(initial), initial, console=console) == 0
    text = stream.getvalue()
    assert "解析项目 → 下载数据 → 分析数据 → 汇总结果" in text
    assert initial[0]["result_path"] in text


def test_maintenance_commands_preserved(monkeypatch, capsys):
    manager = FakeManager([job(status="succeeded")])
    manager.submit = lambda accession, *, data_type: job(accession, status="running")
    manager.get_job = lambda accession: job(accession)
    monkeypatch.setattr(cli, "JobManager", lambda _: manager)
    assert cli.main(["submit", "SRR1", "-t", "bulk"]) == 0
    assert '"accession": "SRR1"' in capsys.readouterr().out
    assert cli.main(["status", "SRR1"]) == 0
    assert '"status": "queued"' in capsys.readouterr().out
    assert cli.main(["logs", "SRR1"]) == 0
    assert "tool noise SRR1" in capsys.readouterr().out


@pytest.mark.parametrize("arguments,source,mode", [
    (["-a", "GSE219739"], "GSE219739", "accessions"),
    (["--accession", "GSE1,GSE2"], "GSE1,GSE2", "accessions"),
    (["-f", "/root/geo_ids.txt"], "/root/geo_ids.txt", "file"),
    (["--file", "/root/lists/geo ids.txt"], "/root/lists/geo ids.txt", "file"),
])
def test_shortcut_routes_single_batch_and_server_file(arguments, source, mode, monkeypatch):
    manager = FakeManager([job(status="succeeded", stage="done")])
    monkeypatch.setattr(cli, "JobManager", lambda _: manager)
    assert cli.shortcut_main([*arguments, "-t", "bulk"]) == 0
    assert manager.submissions == [(source, mode, None, "bulk")]


def test_shortcut_preserves_settings_resources_and_verbose(monkeypatch):
    paths = []
    manager = FakeManager([job(status="succeeded", stage="done")])
    monkeypatch.setattr(cli, "JobManager", lambda settings: (paths.append(settings) or manager))
    assert cli.shortcut_main(["--settings", "/root/settings custom.yaml", "-a", "GSE219739",
                              "--cores", "6", "-v", "-t", "bulk"]) == 0
    assert paths == ["/root/settings custom.yaml"]
    assert manager.submissions == [("GSE219739", "accessions", 6, "bulk")]
    assert manager.log_calls == ["GSE113777"]


@pytest.mark.parametrize("arguments", [[], ["--settings", "/root/teflow/settings.yaml"]])
def test_shortcut_no_source_delegates_to_same_tui_entry(arguments, monkeypatch):
    forwarded = []
    monkeypatch.setattr(cli, "main", lambda argv: (forwarded.append(argv) or 0))
    assert cli.shortcut_main(arguments) == 0
    expected = [] if not arguments else ["--settings=/root/teflow/settings.yaml"]
    assert forwarded == [expected]


@pytest.mark.parametrize("arguments", [
    ["-a", "GSE1", "-f", "/root/list.txt"],
    ["-a", "GSE1", "--cores", "0"],
    ["-a", "GSE1", "--cores", "-2"],
    ["--cores", "2"],
])
def test_shortcut_rejects_conflicts_and_invalid_cores_before_runtime(arguments, monkeypatch):
    monkeypatch.setattr(cli, "JobManager", lambda _: pytest.fail("Invalid shortcut reached runtime"))
    with pytest.raises(SystemExit) as error:
        cli.shortcut_main(arguments)
    assert error.value.code == 2


def test_shortcut_propagates_scientific_failure_exit_code(monkeypatch):
    manager = FakeManager([job(status="failed", error="intentional test failure")])
    monkeypatch.setattr(cli, "JobManager", lambda _: manager)
    assert cli.shortcut_main(["-a", "GSE219739", "-t", "bulk"]) == 1


def test_shortcut_preserves_leading_hyphen_source_as_data(monkeypatch):
    manager = FakeManager([job(status="succeeded")])
    monkeypatch.setattr(cli, "JobManager", lambda _: manager)
    assert cli.shortcut_main(["-a=-not-an-accession", "-t", "bulk"]) == 0
    assert manager.submissions == [("-not-an-accession", "accessions", None, "bulk")]


def test_shortcut_help_explains_inputs_without_constructing_runtime(monkeypatch, capsys):
    monkeypatch.setattr(cli, "JobManager", lambda _: pytest.fail("Help reached runtime"))
    with pytest.raises(SystemExit) as error:
        cli.shortcut_main(["--help"])
    assert error.value.code == 0
    help_text = capsys.readouterr().out
    assert "usage: tf" in help_text
    assert "--accession" in help_text and "--file" in help_text
    assert "--type" in help_text and "scrna" in help_text
    assert "绝对路径" in help_text and "TUI" in help_text


@pytest.mark.parametrize("entry,arguments", [
    (cli.main, ["run", "GSE1"]),
    (cli.main, ["submit", "GSE1"]),
    (cli.shortcut_main, ["-a", "GSE1"]),
    (cli.shortcut_main, ["-f", "/root/list.txt"]),
])
def test_explicit_type_required_before_runtime(entry, arguments, monkeypatch):
    monkeypatch.setattr(cli, "JobManager", lambda _: pytest.fail("missing type constructed runtime"))
    with pytest.raises(SystemExit) as error:
        entry(arguments)
    assert error.value.code == 2


@pytest.mark.parametrize("entry,arguments", [
    (cli.main, ["run", "GSE1", "-t", "scrna"]),
    (cli.main, ["submit", "GSE1", "-t", "scrna"]),
    (cli.shortcut_main, ["-a", "GSE1,GSE2", "-t", "scrna"]),
    (cli.shortcut_main, ["-f", "/root/missing-list.txt", "-t", "scrna"]),
])
def test_unconfigured_scrna_rejected_before_runtime(entry, arguments, monkeypatch, capsys):
    monkeypatch.setattr(cli, "JobManager", lambda _: pytest.fail("scrna constructed runtime"))
    assert entry(arguments) == 2
    assert "单细胞分析流程待配置" in capsys.readouterr().err


@pytest.mark.parametrize("entry,arguments", [
    (cli.main, ["run", "GSE1", "-t", "other"]),
    (cli.main, ["submit", "GSE1", "-t", "other"]),
    (cli.shortcut_main, ["-a", "GSE1", "-t", "other"]),
    (cli.shortcut_main, ["-t", "bulk"]),
])
def test_invalid_type_or_type_without_source_rejected_before_runtime(entry, arguments, monkeypatch):
    monkeypatch.setattr(cli, "JobManager", lambda _: pytest.fail("invalid type constructed runtime"))
    with pytest.raises(SystemExit) as error:
        entry(arguments)
    assert error.value.code == 2


@pytest.mark.parametrize("arguments", [
    ["status"], ["status", "SRR1"], ["logs", "SRR1"],
    ["--settings", "/root/custom.yaml", "status", "SRR1"],
    ["--settings=/root/custom.yaml", "logs", "SRR1"],
])
def test_tf_maintenance_dispatch_keeps_settings_and_needs_no_type(arguments, monkeypatch):
    calls = []
    monkeypatch.setattr(cli, "main", lambda argv: calls.append(argv) or 0)
    assert cli.shortcut_main(arguments) == 0
    assert calls == [arguments]


def test_tf_status_and_logs_use_real_command_parser(monkeypatch, capsys):
    manager = FakeManager([job(status="succeeded")])
    manager.list_jobs = lambda: manager.initial
    manager.get_job = lambda accession: job(accession)
    monkeypatch.setattr(cli, "JobManager", lambda _: manager)
    assert cli.shortcut_main(["status"]) == 0
    assert '"accession": "GSE113777"' in capsys.readouterr().out
    assert cli.shortcut_main(["status", "SRR1"]) == 0
    assert '"accession": "SRR1"' in capsys.readouterr().out
    assert cli.shortcut_main(["logs", "SRR1"]) == 0
    assert "tool noise SRR1" in capsys.readouterr().out


def test_tf_subcommand_help_identifies_tf(monkeypatch, capsys):
    monkeypatch.setattr(cli, "JobManager", lambda _: pytest.fail("help reached runtime"))
    with pytest.raises(SystemExit) as error:
        cli.shortcut_main(["status", "--help"])
    assert error.value.code == 0
    assert "usage: tf status" in capsys.readouterr().out


def test_cli_hints_use_tf_after_ctrl_c(monkeypatch):
    manager = FakeManager([job(status="running", stage="download")])
    def interrupt(*args):
        raise KeyboardInterrupt
    monkeypatch.setattr(cli.time, "sleep", interrupt)
    console, stream = output_console()
    assert cli.watch_jobs(manager, manager.initial, console=console) == 130
    assert "tf status" in stream.getvalue()
    assert "teflow status" not in stream.getvalue()
