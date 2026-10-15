"""Input browsing stays read-only and uses its own raw-data security root."""
from pathlib import Path

import pytest

from teflow.results import ResultBrowser


@pytest.fixture
def roots(tmp_path):
    output, raw = tmp_path / "output" / "GSE1", tmp_path / "raw" / "GSE1"
    output.mkdir(parents=True)
    raw.mkdir(parents=True)
    (output / "samples.tsv").write_text("run_accession\nSRR1\nSRR2\n", encoding="utf-8")
    (output / "merged.count").write_text("gene\tSRR1\na\t1\n", encoding="utf-8")
    run = raw / "SRR1"
    run.mkdir()
    (run / "SRR1_1.fastq.gz").write_bytes(b"gzip data")
    (run / "SRR1_2.fastq.gz").write_bytes(b"gzip data")
    (raw / "input.metadata.tsv").write_text("run\tlayout\nSRR1\tPAIRED\n", encoding="utf-8")
    return {"accession": "GSE1", "job_dir": str(output), "raw_dir": str(raw)}


def test_default_output_and_explicit_input_roots(roots):
    browser = ResultBrowser()
    output, raw = Path(roots["job_dir"]), Path(roots["raw_dir"])
    assert {file.name for file in browser.list_project(roots).files} == {"samples.tsv", "merged.count"}
    listing = browser.list_project(roots, source="input")
    assert listing.runs == ["SRR1", "SRR2"]  # Planned Run before its files arrive.
    assert {file.name for file in listing.files} == {"input.metadata.tsv"}
    files = browser.list_files(roots, "SRR1", source="input")
    assert {file.relative for file in files} == {"SRR1_1.fastq.gz", "SRR1_2.fastq.gz"}
    assert all(file.path.is_relative_to(raw) for file in files)
    assert browser.list_files(roots, "SRR2", source="input") == []
    assert browser.preview(roots, raw / "input.metadata.tsv", source="input").rows == [["SRR1", "PAIRED"]]
    assert browser.preview(roots, output / "merged.count", source="input").kind == "error"
    assert browser.preview(roots, raw / "input.metadata.tsv").kind == "error"


def test_input_has_no_fallback_into_output_or_result_logs(roots):
    output = Path(roots["job_dir"])
    (output / "logs").mkdir()
    (output / "logs" / "SRR2.STAR.log").write_text("result log", encoding="utf-8")
    assert ResultBrowser().list_files(roots, "SRR2", source="input") == []
    raw = Path(roots["raw_dir"])
    for item in raw.glob("SRR1/*"):
        item.unlink()
    (raw / "SRR1").rmdir()
    (raw / "input.metadata.tsv").unlink()
    raw.rmdir()
    listing = ResultBrowser().list_project(roots, source="input")
    assert listing.runs == ["SRR1", "SRR2"]
    assert listing.files == [] and "输入目录尚未创建" in listing.error


def test_direct_run_input_scope_and_native_sra_are_summaries(roots, monkeypatch):
    from teflow import results
    roots["accession"] = "SRR1"
    raw = Path(roots["raw_dir"])
    archive = raw / "SRR1" / "SRR1"
    archive.write_bytes(b"NCBI.sra\x00" + b"x" * 10_000)
    browser = ResultBrowser()
    for run in (None, "SRR1"):
        files = browser.list_files(roots, run, source="input")
        assert {file.name for file in files} == {"SRR1", "SRR1_1.fastq.gz", "SRR1_2.fastq.gz"}
        assert next(file.kind for file in files if file.name == "SRR1") == "compressed"
    monkeypatch.setattr(results, "_open_regular", lambda path: pytest.fail("Archive contents must not be opened"))
    assert browser.preview(roots, archive, source="input").kind == "summary"
    assert browser.preview(roots, raw / "SRR1" / "SRR1_1.fastq.gz", source="input").kind == "summary"


def test_input_rejects_escaped_symlink_and_nonregular_file(roots, tmp_path):
    raw = Path(roots["raw_dir"])
    outside = tmp_path / "secret.txt"
    outside.write_text("secret", encoding="utf-8")
    link = raw / "escaped.txt"
    try:
        link.symlink_to(outside)
    except OSError:
        pytest.skip("Symlink creation is unavailable")
    browser = ResultBrowser()
    assert str(link) not in {str(file.path) for file in browser.list_files(roots, source="input")}
    assert browser.preview(roots, link, source="input").kind == "error"
    assert browser.preview(roots, raw / "SRR1", source="input").kind == "error"


def test_input_requires_its_own_directory_and_valid_source(roots):
    browser = ResultBrowser()
    rawfile = Path(roots["raw_dir"]) / "input.metadata.tsv"
    assert browser.preview(roots, rawfile, source="unknown").kind == "error"
    roots.pop("raw_dir")
    assert "尚未指定输入目录" in browser.preview(roots, rawfile, source="input").text
