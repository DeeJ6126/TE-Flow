import io
from pathlib import Path

from rich.console import Console

from teflow import cli
from teflow.progress import ProgressSnapshot


def test_live_project_row_contains_shared_percentage_and_transfer_units():
    job = {"accession": "GSE1", "status": "running", "stage": "download",
           "total_runs": 2, "downloaded_runs": 0}
    snapshot = ProgressSnapshot(stage="download", status="running", label="下载数据",
                                current_bytes=4_000_000, total_bytes=10_000_000,
                                speed_bps=2_000_000, percent=40.0, eta_seconds=3.0,
                                completed_runs=0, total_runs=2, elapsed_seconds=2.0, run=None)
    stream = io.StringIO()
    Console(file=stream, width=150).print(cli.render_batch([job], {"GSE1": snapshot}))
    text = stream.getvalue()
    assert "40.0%" in text and "2.00 MB/s" in text
    assert "GSE1" in text and "tf logs" in text
    assert "teflow logs" not in text


def test_unknown_failure_keeps_original_text_and_log_entry():
    stream = io.StringIO()
    job = {"accession": "GSE1", "status": "failed", "stage": "analyze",
           "job_dir": "/results/GSE1", "error": "FATAL original tool error\nMore context",
           "failure": {"code": "unknown", "message": "Do not guess this cause"}}
    cli._print_result(Console(file=stream, width=150), job)
    assert "FATAL original tool error" in stream.getvalue()
    assert "Do not guess" not in stream.getvalue()
    assert str(Path(job["job_dir"]) / "run.log") in stream.getvalue()


def test_startup_connection_error_is_plain_and_verbose_retains_detail(monkeypatch, capsys):
    from teflow.failures import KnownFailure
    class Manager:
        def submit_many(self, *args, **kwargs):
            raise KnownFailure("website_connection", "无法连接 NCBI GEO（www.ncbi.nlm.nih.gov）。",
                               detail="ConnectionRefusedError: 127.0.0.1:17890")
    monkeypatch.setattr(cli, "JobManager", lambda *_: Manager())
    assert cli.shortcut_main(["-a", "GSE1", "-t", "bulk"]) == 1
    output = capsys.readouterr().err
    assert "无法连接 NCBI GEO" in output and "127.0.0.1" not in output
    assert cli.shortcut_main(["-a", "GSE1", "-t", "bulk", "--verbose"]) == 1
    assert "ConnectionRefusedError: 127.0.0.1:17890" in capsys.readouterr().err
