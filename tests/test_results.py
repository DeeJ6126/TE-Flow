"""Result browsing is read-only, scoped and bounded even for large outputs."""
import csv
import io
import json
import os
from pathlib import Path

import pytest

from teflow import results
from teflow.results import PAGE_ROWS, ResultBrowser


@pytest.fixture
def project(tmp_path):
    root = tmp_path / "GSE123"
    root.mkdir()
    return {"accession": "GSE123", "job_dir": str(root), "current_sample": ""}


def put(job, relative, text):
    path = Path(job["job_dir"]) / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="")
    return path


def test_project_combines_manifest_mapping_and_physical_folders(project):
    put(project, "samples.json", json.dumps({"samples": {"SRR1": {}, "not-a-run": {}}}))
    put(project, "samples.tsv", "gse\tgsm\texperiment\trun\nGSE123\tGSM1\tSRX2\tSRR2\n")
    Path(project["job_dir"], "ERR3").mkdir()
    put(project, "merged.count", "gene\tSRR1\nx\t1\n")
    put(project, ".snakemake/metadata/internal", "hidden")
    project["current_sample"] = "DRR4"
    listing = ResultBrowser().list_project(project)
    assert listing.runs == ["DRR4", "ERR3", "SRR1", "SRR2"]
    assert {file.name for file in listing.files} == {"samples.json", "samples.tsv", "merged.count"}
    assert listing.signature and not listing.error


def test_mapping_runs_exist_before_download_completes(project):
    put(project, "samples.tsv", "gse\tgsm\texperiment\trun\nGSE123\tGSM1\tSRX1\tSRR1\n")
    browser = ResultBrowser()
    assert browser.list_project(project).runs == ["SRR1"]
    assert browser.list_files(project, "SRR1") == []


def test_project_and_run_scopes_keep_common_logs_but_exclude_other_runs(project):
    put(project, "merged.count", "gene\tSRR1\nx\t1\n")
    put(project, "SRR1/abundance/SRR1.cntTable", "gene\tcount\nx\t1\n")
    put(project, "SRR2/abundance/SRR2.cntTable", "gene\tcount\nx\t2\n")
    put(project, "logs/SRR1.STAR.log", "run1")
    put(project, "logs/SRR10.STAR.log", "other")
    put(project, "logs/SRR2.TEcount.log", "run2")
    browser = ResultBrowser()
    assert not any(item.relative.startswith(("SRR1/", "SRR2/"))
                   for item in browser.list_files(project))
    selected = browser.list_files(project, "SRR1")
    assert {item.relative for item in selected} == {
        "abundance/SRR1.cntTable", "logs/SRR1.STAR.log"}


def test_run_root_log_can_be_read_before_result_folder_exists(project):
    put(project, "logs/SRR1.fastp.log", "working")
    assert [file.relative for file in ResultBrowser().list_files(project, "SRR1")] == ["logs/SRR1.fastp.log"]


def test_legacy_direct_run_root(project):
    project["accession"] = "SRR1"
    put(project, "abundance/SRR1.cntTable", "gene\tcount\nx\t1\n")
    browser = ResultBrowser()
    assert browser.list_project(project).runs == ["SRR1"]
    assert browser.list_files(project, "SRR1")[0].relative == "abundance/SRR1.cntTable"


def test_hidden_files_and_temporary_directories_have_explicit_opt_in(project):
    for name in (".snakemake/metadata/blob", ".teflow-state.json", "tmp/x", "SRR1_STARtmp/x", "log.tmp"):
        put(project, name, "internal")
    put(project, "config.yaml", "gene: human")
    browser = ResultBrowser()
    assert [file.relative for file in browser.list_files(project)] == ["config.yaml"]
    all_files = {file.relative for file in browser.list_files(project, show_internal=True)}
    assert {".snakemake/metadata/blob", "tmp/x", "SRR1_STARtmp/x", "log.tmp"}.issubset(all_files)


def test_table_pages_are_seek_cached_not_rescanned(project, monkeypatch):
    path = put(project, "merged.count", "gene\tSRR1\n" + "".join(f"g{i}\t{i}\n" for i in range(350)))
    browser = ResultBrowser()
    first = browser.preview(project, path)
    second = browser.preview(project, path, 1)
    assert first.kind == "table" and first.columns == ["gene", "SRR1"]
    assert len(first.rows) == PAGE_ROWS and first.rows[0] == ["g0", "0"]
    assert second.rows[0] == ["g100", "100"] and second.has_next
    calls = 0
    old_record = results._record

    def count_records(*args, **kwargs):
        nonlocal calls
        calls += 1
        return old_record(*args, **kwargs)

    monkeypatch.setattr(results, "_record", count_records)
    assert browser.preview(project, path, 1) is second
    third = browser.preview(project, path, 2)
    assert calls == PAGE_ROWS
    assert third.rows[0] == ["g200", "200"]
    assert not browser.preview(project, path, 3).has_next


def test_csv_quotes_bom_and_multiline_rows_survive_page_boundaries(project):
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(["基因\n名称", "count"])
    for index in range(105):
        writer.writerow([f'基因,{index}\n"第二行"', str(index)])
    path = put(project, "counts.csv", "\ufeff" + output.getvalue())
    browser = ResultBrowser()
    first = browser.preview(project, path)
    second = browser.preview(project, path, 1)
    assert first.columns == ["基因\n名称", "count"]
    assert first.rows[-1] == ['基因,99\n"第二行"', "99"]
    assert second.rows[0] == ['基因,100\n"第二行"', "100"]
    assert len(second.rows) == 5 and not second.has_next


@pytest.mark.parametrize("suffix", [".json", ".yaml", ".log", ".txt", ".out"])
def test_text_config_and_log_pages(project, suffix):
    path = put(project, "result" + suffix, "\ufeff" + "\n".join(f"行 {i}" for i in range(105)))
    browser = ResultBrowser()
    first = browser.preview(project, path)
    second = browser.preview(project, path, 1)
    assert first.kind == "text" and first.text.startswith("行 0\n")
    assert first.has_next and second.text.startswith("行 100\n")
    assert not second.has_next


@pytest.mark.parametrize("name,expected", [("x.bam", "BAM"), ("x.fastq.gz", "不解压"),
                                           ("x.pdf", "二进制"), ("x.html", "报告标题：fastp & QC")])
def test_binary_compressed_and_html_are_safe_summaries(project, name, expected):
    path = put(project, name, '<html><title>fastp &amp; QC</title><script>bad()</script></html>')
    result = ResultBrowser().preview(project, path)
    assert result.kind == "summary" and expected in result.text
    assert str(path) in result.text and not result.has_next
    assert "<script>" not in result.text


def test_unknown_binary_is_not_decoded_as_output(project):
    path = put(project, "data.unknown", "abc\x00def")
    result = ResultBrowser().preview(project, path)
    assert result.kind == "summary" and "二进制" in result.text


def test_changed_and_truncated_file_invalidates_cached_offsets(project):
    path = put(project, "merged.count", "gene\tcount\n" + "x\t1\n" * 201)
    browser = ResultBrowser()
    before = browser.preview(project, path)
    assert browser.preview(project, path, 1).has_next
    path.write_text("gene\tcount\ny\t2\n", encoding="utf-8")
    after = browser.preview(project, path)
    assert after.rows == [["y", "2"]]
    assert after.signature != before.signature and not after.has_next
    assert browser.preview(project, path, 1).rows == []


@pytest.mark.parametrize("name", ["growing.tsv", "growing.log"])
def test_append_retains_deep_page_offsets_without_full_rescan(project, name, monkeypatch):
    table = name.endswith("tsv")
    header = "gene\tcount\n" if table else ""
    data = "".join(f"gene{i:04d}\t{i}\n" for i in range(1300))
    path = put(project, name, header + data)
    browser = ResultBrowser()
    for number in range(13):
        before = browser.preview(project, path, number)
        assert before.kind == ("table" if table else "text")
    assert not before.has_next
    with path.open("a", encoding="utf-8", newline="") as handle:
        handle.write("".join(f"gene{i:04d}\t{i}\n" for i in range(1300, 1450)))
    calls = 0
    original = results._record

    def measured(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(results, "_record", measured)
    refreshed = browser.preview(project, path, 12)
    assert refreshed.kind == before.kind and refreshed.page == 12 and refreshed.has_next
    assert refreshed.signature != before.signature
    if table:
        assert refreshed.rows[0] == ["gene1200", "1200"]
    else:
        assert refreshed.text.startswith("gene1200\t1200\n")
    assert calls == PAGE_ROWS  # No re-read of the previous 1,200 records.
    following = browser.preview(project, path, 13)
    assert following.page == 13 and following.has_next
    if table:
        assert following.rows[0] == ["gene1300", "1300"]
    else:
        assert following.text.startswith("gene1300\t1300\n")


def test_truncate_on_deep_page_returns_first_page_with_reset_notice(project):
    path = put(project, "changing.tsv", "gene\tcount\n" + "x\t1\n" * 1300)
    browser = ResultBrowser()
    for number in range(13):
        assert browser.preview(project, path, number).kind == "table"
    path.write_text("gene\tcount\nnew\t2\n", encoding="utf-8")
    page = browser.preview(project, path, 12)
    assert page.page == 0 and page.rows == [["new", "2"]]
    assert "返回第 1 页" in page.text


def test_rewrite_and_growth_resets_offset_when_boundary_anchor_changes(project):
    header = "gene\tcount\n"
    data = "".join(f"gene{i:04d}\t{'a' * 60}\n" for i in range(1300))
    path = put(project, "rewritten.tsv", header + data)
    browser = ResultBrowser()
    for number in range(13):
        assert browser.preview(project, path, number).kind == "table"
    # Keep byte lengths and old file edges unchanged, modify an indexed middle
    # boundary, then grow the file. This must not be mistaken for pure append.
    path.write_text(header + data.replace("gene0600", "CHANGED!", 1) + "last\t2\n",
                    encoding="utf-8", newline="")
    page = browser.preview(project, path, 12)
    assert page.page == 0 and "返回第 1 页" in page.text


def test_atomic_replacement_with_longer_content_resets_index(project):
    path = put(project, "replaced.tsv", "gene\tcount\n" + "old\t1\n" * 1300)
    browser = ResultBrowser()
    for number in range(13):
        browser.preview(project, path, number)
    replacement = put(project, "replacement.tmp", "gene\tcount\n" + "replacement\t2\n" * 1400)
    replacement.replace(path)
    page = browser.preview(project, path, 12)
    assert page.page == 0 and page.rows[0] == ["replacement", "2"]
    assert "返回第 1 页" in page.text


def test_preview_is_bounded_and_huge_cells_columns_have_notices(project):
    header = "\t".join(f"h{i}" for i in range(results.MAX_COLUMNS + 10))
    values = ["a" * (results.MAX_CELL_CHARS + 100)] + ["1"] * (results.MAX_COLUMNS + 9)
    path = put(project, "wide.tsv", header + "\n" + "\t".join(values) + "\n")
    page = ResultBrowser().preview(project, path)
    assert len(page.columns) == results.MAX_COLUMNS
    assert len(page.rows[0]) == results.MAX_COLUMNS
    assert len(page.rows[0][0]) < results.MAX_CELL_CHARS + 50
    assert "截断" in page.text and "列" in page.text


def test_oversized_quoted_record_is_skipped_at_correct_csv_boundary(project):
    path = put(project, "large.csv", 'gene,count\n"' + "x" * 80_000 + '\nmore",1\nnormal,2\n')
    page = ResultBrowser().preview(project, path)
    assert page.kind == "table"
    assert "省略" in page.rows[0][0] and page.rows[1] == ["normal", "2"]


def test_pathological_single_line_has_hard_read_budget(project, monkeypatch):
    path = put(project, "huge.log", "a" * (results.MAX_READ_BYTES + 100))
    read = 0
    original = results._Budget.line

    def measured(self, handle):
        nonlocal read
        chunk = original(self, handle)
        read += len(chunk)
        return chunk

    monkeypatch.setattr(results._Budget, "line", measured)
    result = ResultBrowser().preview(project, path)
    assert result.kind == "error" and "读取上限" in result.text
    assert read <= results.MAX_READ_BYTES


def test_large_file_reads_only_first_page(project, monkeypatch):
    path = put(project, "large.tsv", "gene\tcount\n" + "x\t1\n" * 101)
    with path.open("r+b") as handle:
        handle.truncate(128 * 1024**2)
    read = 0
    original = results._Budget.line

    def measured(self, handle):
        nonlocal read
        chunk = original(self, handle)
        read += len(chunk)
        return chunk

    monkeypatch.setattr(results._Budget, "line", measured)
    result = ResultBrowser().preview(project, path)
    assert len(result.rows) == 100 and result.has_next
    assert read < 2_000
    assert ResultBrowser().preview(project, path, 99999).kind == "error"


@pytest.mark.parametrize("name", ["empty.txt", "empty.tsv"])
def test_empty_files_are_meaningful(project, name):
    page = ResultBrowser().preview(project, put(project, name, ""))
    assert page.kind in {"text", "table"} and "空" in page.text
    assert not page.has_next


def test_header_only_table_and_missing_file(project):
    browser = ResultBrowser()
    page = browser.preview(project, put(project, "header.tsv", "gene\tcount\n"))
    assert page.columns == ["gene", "count"] and page.rows == [] and not page.has_next
    missing = browser.preview(project, Path(project["job_dir"]) / "missing.txt")
    assert missing.kind == "error" and "尚未生成" in missing.text


def test_paths_are_confined_and_run_injection_rejected(project, tmp_path):
    outside = tmp_path / "secret.txt"
    outside.write_text("secret", encoding="utf-8")
    browser = ResultBrowser()
    for path in (outside, "../secret.txt"):
        page = browser.preview(project, path)
        assert page.kind == "error" and "不在当前项目" in page.text
        assert "secret" != page.text
    with pytest.raises(ValueError, match="无效"):
        browser.list_files(project, "../secret")


def test_symlink_escape_is_excluded(project, tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_text("secret", encoding="utf-8")
    link = Path(project["job_dir"]) / "escape"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("Symlinks unavailable to this account")
    file_link = Path(project["job_dir"]) / "escape.txt"
    file_link.symlink_to(outside / "secret.txt")
    browser = ResultBrowser()
    assert browser.list_files(project) == []
    assert browser.list_project(project).files == []
    assert browser.preview(project, link / "secret.txt").kind == "error"
    assert browser.preview(project, file_link).kind == "error"


def test_permission_failure_and_nonregular_path_have_clear_errors(project, monkeypatch):
    path = put(project, "private.txt", "private")

    def denied(_path):
        raise PermissionError("denied")

    monkeypatch.setattr(results, "_open_regular", denied)
    assert "没有权限" in ResultBrowser().preview(project, path).text
    directory = Path(project["job_dir"]) / "folder"
    directory.mkdir()
    assert "普通结果文件" in ResultBrowser().preview(project, directory).text


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO is a POSIX file type")
def test_fifo_is_rejected_without_opening_or_blocking(project):
    path = Path(project["job_dir"]) / "pipe.log"
    os.mkfifo(path)
    assert "普通结果文件" in ResultBrowser().preview(project, path).text


def test_missing_project_and_invalid_page(project):
    browser = ResultBrowser()
    assert browser.list_project({"job_dir": str(Path(project["job_dir"]) / "not-yet")}).error
    assert browser.preview(project, put(project, "x.txt", "x"), -1).kind == "error"
