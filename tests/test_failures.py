import json
import os
from pathlib import Path
import sys
import urllib.error

import pytest

from teflow.failures import KnownFailure, friendly_failure, known_failure

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "workflow" / "scripts"))
import prepare_samples as pipeline


def test_known_single_end_is_explicit_not_unknown_layout():
    with pytest.raises(KnownFailure) as captured:
        pipeline.paired_files({"library_layout": "SINGLE"}, "SRR1")
    assert captured.value.failure["run"] == "SRR1"
    assert captured.value.failure["code"] == "single_end"
    with pytest.raises(ValueError) as unknown:
        pipeline.paired_files({"library_layout": ""}, "SRR1")
    assert not isinstance(unknown.value, KnownFailure)


def test_connection_exception_labels_site_and_retains_original_cause(monkeypatch):
    original = urllib.error.URLError(ConnectionRefusedError("proxy closed"))
    def fail(*a, **kw):
        raise original
    monkeypatch.setattr(pipeline.urllib.request, "urlopen", fail)
    with pytest.raises(KnownFailure) as captured:
        pipeline._request_text("https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE1")
    assert captured.value.failure["website"] == "NCBI GEO"
    assert captured.value.__cause__ is original
    assert "proxy closed" not in str(captured.value)
    assert "proxy closed" in captured.value.detail


def test_http_status_is_not_invented_as_connection_failure(monkeypatch):
    original = urllib.error.HTTPError("https://www.ebi.ac.uk/ena/", 403, "Forbidden", None, None)
    def fail(*a, **kw):
        raise original
    monkeypatch.setattr(pipeline.urllib.request, "urlopen", fail)
    with pytest.raises(urllib.error.HTTPError) as captured:
        pipeline._request_text(original.url)
    assert known_failure(captured.value) is None


def test_filesystem_permission_error_is_not_called_integrity_failure(tmp_path, monkeypatch):
    path = tmp_path / "SRR1_1.fastq.gz"
    path.write_bytes(b"data")
    def denied(*a, **kw):
        raise PermissionError("not permitted")
    monkeypatch.setattr(pipeline.gzip, "open", denied)
    with pytest.raises(PermissionError) as captured:
        pipeline.validate_fastq(path, {"bytes": 4, "source": "ena-fastq"})
    assert known_failure(captured.value) is None


def test_wrapped_known_cause_only_and_unknown_log_text_unchanged():
    explicit = KnownFailure("single_end", "单端数据", run="SRR1")
    wrapper = RuntimeError("outer tool failure")
    wrapper.__cause__ = explicit
    assert known_failure(wrapper) is explicit
    recovered_context = RuntimeError("unrelated later failure")
    recovered_context.__context__ = explicit
    assert known_failure(recovered_context) is None
    assert friendly_failure({"status": "failed", "error": "MD5 mismatch for: something"}) is None
    assert friendly_failure({"status": "failed", "failure": explicit.failure}) == "单端数据"
    assert friendly_failure({"status": "running", "failure": explicit.failure}) is None
    assert friendly_failure({"status": "failed", "failure": {"code": "unknown", "message": "guess"}}) is None


def test_event_only_published_when_known_error_escapes(monkeypatch, capsys):
    def known(*a, **kw):
        raise KnownFailure("single_end", "单端数据", run="SRR1")
    monkeypatch.setattr(pipeline, "prepare", known)
    with pytest.raises(KnownFailure):
        pipeline.run_preparation()
    event = capsys.readouterr().out.strip()
    assert json.loads(event.removeprefix("[TEFLOW_FAILURE] "))["code"] == "single_end"
    def unknown(*a, **kw):
        raise RuntimeError("an unknown failure")
    monkeypatch.setattr(pipeline, "prepare", unknown)
    with pytest.raises(RuntimeError):
        pipeline.run_preparation()
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("current,old,known", [
    ("MD5 mismatch for: SRR1_1.fastq.gz\n", "", False),
    ("SRR1 md5 check failed after trying 3 times, save Run ID in fail.log\n", "", True),
    ("Error: download failed\n", "MD5 mismatch for: SRR1_1.fastq.gz\n", False),
    ("Error: disk write failed\n", "", False),
])
def test_iseq_integrity_requires_explicit_diagnostic_in_current_invocation(tmp_path, monkeypatch, current, old, known):
    log_path = tmp_path / "download.log"
    log_path.write_text(old, encoding="utf-8")
    class Process:
        pid = os.getpid()
        def wait(self, timeout=None):
            return 1
    def start(*a, **kw):
        with log_path.open("a", encoding="utf-8") as writer:
            writer.write(current)
        return Process()
    monkeypatch.setattr(pipeline.subprocess, "Popen", start)
    monkeypatch.setattr(pipeline.os, "killpg", lambda *a: None, raising=False)
    monkeypatch.setattr(pipeline.signal, "SIGKILL", 9, raising=False)
    with log_path.open("a", encoding="utf-8") as handle:
        with pytest.raises((RuntimeError, KnownFailure)) as captured:
            pipeline.run_iseq(["iseq", "-i", "SRR1"], tmp_path, handle)
    assert isinstance(captured.value, KnownFailure) is known
    if known:
        assert captured.value.failure["run"] == "SRR1"
