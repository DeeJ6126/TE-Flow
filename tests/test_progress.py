import csv
import json
import os
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from teflow.progress import (
    ProgressTracker, format_bytes, format_duration, format_speed, progress_lines,
)


class Clock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        return self.value

    def advance(self, seconds=1):
        self.value += seconds


def sparse(path, size):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("ab") as handle:
        handle.truncate(size)


def setup_job(tmp_path, *, geo=True, runs=("SRR1", "SRR2"), sizes=(100, 100)):
    raw, output = tmp_path / "raw", tmp_path / "output"
    raw.mkdir()
    output.mkdir()
    accession = "GSE1" if geo else runs[0]
    rows = [{"run_accession": run, "library_layout": "PAIRED",
             "fastq_ftp": ";".join(f"ftp.sra.ebi.ac.uk/{run}_{mate}.fastq.gz" for mate in (1, 2)),
             "fastq_bytes": ";".join("" if value is None else str(value) for value in sizes)} for run in runs]
    with (raw / f"{accession}.metadata.tsv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0].keys(), delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    job = {"accession": accession, "job_dir": str(output), "raw_dir": str(raw),
           "status": "running", "stage": "download", "total_runs": len(runs),
           "downloaded_runs": 0, "analyzed_runs": 0, "attempt": 1}
    return job, raw, output


def fastq(raw, run, mate=1, geo=True):
    return (raw / run if geo else raw) / f"{run}_{mate}.fastq.gz"


def test_geo_project_aggregates_only_mapped_files_and_reads_existing_worker(tmp_path):
    job, raw, output = setup_job(tmp_path)
    sparse(fastq(raw, "SRR1"), 100)
    sparse(fastq(raw, "SRR1", 2), 50)
    sparse(fastq(raw, "SRR2"), 25)
    sparse(raw / "SRR999" / "SRR999_1.fastq.gz", 10_000)
    sparse(output / "unrelated.fastq.gz", 99_999)
    snap = ProgressTracker().snapshot(job)
    assert snap.current_bytes == 175
    assert snap.total_bytes == 400
    assert snap.percent == 43.75
    assert snap.completed_runs == 0
    assert snap.speed_bps is None


def test_flat_run_and_metadata_Run_column(tmp_path):
    job, raw, _ = setup_job(tmp_path, geo=False, runs=("SRR1",))
    metadata = raw / "SRR1.metadata.tsv"
    metadata.write_text(metadata.read_text().replace("run_accession", "Run"))
    sparse(fastq(raw, "SRR1", geo=False), 60)
    snap = ProgressTracker().snapshot(job)
    assert snap.current_bytes == 60 and snap.total_bytes == 200 and snap.percent == 30


def test_geo_completed_cache_can_supply_plan_before_samples_manifest(tmp_path):
    job, raw, _ = setup_job(tmp_path)
    (raw / "GSE1.metadata.tsv").unlink()
    cache = {"accession": "GSE1", "records": {
        run: {"files": [{"name": f"{run}_{mate}.fastq.gz", "bytes": 100} for mate in (1, 2)]}
        for run in ("SRR1", "SRR2")}}
    (raw / ".teflow-GSE1.geo.complete.json").write_text(json.dumps(cache))
    sparse(fastq(raw, "SRR2"), 100)
    snap = ProgressTracker().snapshot(job)
    assert snap.current_bytes == 100 and snap.total_bytes == 400


def test_rolling_rate_eta_growth_stall_and_resumption(tmp_path):
    job, raw, _ = setup_job(tmp_path, geo=False, runs=("SRR1",), sizes=(1000, 1000))
    clock = Clock()
    tracker = ProgressTracker(clock=clock)
    assert tracker.snapshot(job).speed_bps is None
    clock.advance(2)
    sparse(fastq(raw, "SRR1", geo=False), 400)
    snap = tracker.snapshot(job)
    assert snap.speed_bps == 200 and snap.eta_seconds == 8
    clock.advance(2)
    sparse(fastq(raw, "SRR1", geo=False), 800)
    assert tracker.snapshot(job).speed_bps == 200
    clock.advance(5)
    tracker.snapshot(job)
    clock.advance(1)
    stalled = tracker.snapshot(job)
    assert stalled.speed_bps == 0 and stalled.eta_seconds is None
    clock.advance(1)
    sparse(fastq(raw, "SRR1", geo=False), 900)
    assert tracker.snapshot(job).speed_bps > 0


def test_file_removal_retry_resets_rate_even_if_other_file_increases(tmp_path):
    job, raw, _ = setup_job(tmp_path, geo=False, runs=("SRR1",), sizes=(1000, 1000))
    clock = Clock()
    tracker = ProgressTracker(clock=clock)
    sparse(fastq(raw, "SRR1", geo=False), 500)
    tracker.snapshot(job)
    clock.advance()
    sparse(fastq(raw, "SRR1", geo=False), 200)
    sparse(fastq(raw, "SRR1", 2, geo=False), 400)
    snap = tracker.snapshot(job)
    assert snap.current_bytes == 600 and snap.speed_bps is None


def test_scope_switching_retains_independent_samples(tmp_path):
    job, raw, _ = setup_job(tmp_path)
    clock = Clock()
    tracker = ProgressTracker(clock=clock)
    tracker.snapshot(job)
    tracker.snapshot(job, "SRR1")
    clock.advance(2)
    sparse(fastq(raw, "SRR1"), 20)
    sparse(fastq(raw, "SRR2"), 40)
    assert tracker.snapshot(job).speed_bps == 30
    assert tracker.snapshot(job, "SRR1").speed_bps == 10
    assert tracker.snapshot(job, "SRR2").speed_bps is None


def test_attempt_change_resets_transfer_rate(tmp_path):
    job, raw, _ = setup_job(tmp_path)
    clock = Clock()
    tracker = ProgressTracker(clock=clock)
    tracker.snapshot(job)
    clock.advance(2)
    sparse(fastq(raw, "SRR1"), 40)
    assert tracker.snapshot(job).speed_bps == 20
    assert tracker.snapshot({**job, "attempt": 2}).speed_bps is None


def test_all_bytes_present_are_verifying_until_download_confirmation(tmp_path):
    job, raw, _ = setup_job(tmp_path)
    for run in ("SRR1", "SRR2"):
        for mate in (1, 2):
            sparse(fastq(raw, run, mate), 100)
    tracker = ProgressTracker()
    snap = tracker.snapshot(job)
    assert snap.percent == 100 and snap.label == "校验中"
    assert snap.speed_bps == 0 and snap.completed_runs == 0
    job["downloaded_runs"] = 2
    done = tracker.snapshot(job)
    assert done.label == "下载完成" and done.completed_runs == 2


def test_native_success_marker_does_not_skip_workflow_validation(tmp_path):
    job, raw, _ = setup_job(tmp_path)
    for mate in (1, 2):
        sparse(fastq(raw, "SRR1", mate), 100)
    (raw / "SRR1" / "success.log").write_text("SRR1\n")
    snap = ProgressTracker().snapshot(job, "SRR1")
    assert snap.label == "校验中" and snap.completed_runs == 0
    job["downloaded_runs"] = 1
    snap = ProgressTracker().snapshot(job, "SRR1")
    assert snap.label == "下载完成" and snap.completed_runs == 1
    sparse(fastq(raw, "SRR1"), 20)
    snap = ProgressTracker().snapshot(job, "SRR1")
    assert snap.label == "下载数据" and snap.completed_runs == 0


def test_published_manifest_confirms_old_worker_downloads(tmp_path):
    job, raw, output = setup_job(tmp_path, geo=False, runs=("SRR1",))
    for mate in (1, 2):
        sparse(fastq(raw, "SRR1", mate, geo=False), 100)
    (output / "samples.json").write_text(json.dumps({"accession": "SRR1", "samples": {
        "SRR1": {"files": [{"name": f"SRR1_{mate}.fastq.gz", "bytes": 100} for mate in (1, 2)]}}}))
    snap = ProgressTracker().snapshot(job)
    assert snap.label == "下载完成" and snap.completed_runs == 1


def test_unknown_compressed_size_never_guesses_percentage(tmp_path):
    job, raw, _ = setup_job(tmp_path, geo=False, runs=("SRR1",), sizes=(None, None))
    sparse(fastq(raw, "SRR1", geo=False), 99)
    snap = ProgressTracker().snapshot(job)
    assert snap.current_bytes == 99 and snap.total_bytes is None
    assert snap.percent is None and snap.eta_seconds is None
    assert "总大小未知" in progress_lines(snap)[0]


def test_analysis_uses_run_counts_and_elapsed_without_made_up_percentage(tmp_path):
    job, _, _ = setup_job(tmp_path)
    job.update(stage="align", analyzed_runs=1, attempt_started_at="2026-10-15T12:00:00+08:00")
    end = datetime.fromisoformat("2026-10-15T12:00:15+08:00").timestamp()
    snap = ProgressTracker(wall_clock=lambda: end).snapshot(job)
    assert snap.stage == "analyze" and snap.percent is None and snap.speed_bps == 0
    assert snap.elapsed_seconds == 15 and snap.completed_runs == 1
    assert "已完成 1/2 Run" in progress_lines(snap)[0]


def test_terminal_elapsed_and_speed_are_frozen(tmp_path):
    job, _, _ = setup_job(tmp_path)
    job.update(status="failed", attempt_started_at="2026-10-15T12:00:00+08:00", finished_at="2026-10-15T12:01:00+08:00")
    snap = ProgressTracker(wall_clock=lambda: 10000000000).snapshot(job)
    assert snap.elapsed_seconds == 60 and snap.speed_bps == 0


def test_half_written_or_missing_metadata_is_graceful_and_recovers(tmp_path):
    job, raw, _ = setup_job(tmp_path)
    metadata = raw / "GSE1.metadata.tsv"
    good = metadata.read_text()
    metadata.write_text("run_accession\tfastq_bytes\nSRR1")
    tracker = ProgressTracker()
    assert tracker.snapshot(job).total_bytes is None
    metadata.write_text(good)
    assert tracker.snapshot(job).total_bytes == 400
    metadata.unlink()
    assert tracker.snapshot(job).total_bytes is None
    assert ProgressTracker().snapshot({"accession": "SRR9", "job_dir": str(tmp_path / "missing")}).total_bytes is None


def test_metadata_mapping_cache_refreshes_after_atomic_replacement(tmp_path):
    job, raw, _ = setup_job(tmp_path)
    tracker = ProgressTracker()
    assert tracker.snapshot(job).total_bytes == 400
    metadata = raw / "GSE1.metadata.tsv"
    updated = raw / "new.tsv"
    updated.write_text(metadata.read_text().replace("100;100", "250;250"))
    updated.replace(metadata)
    assert tracker.snapshot(job).total_bytes == 1000


def test_wrong_accession_manifest_does_not_override_metadata(tmp_path):
    job, raw, output = setup_job(tmp_path)
    (output / "samples.json").write_text(json.dumps({"accession": "GSE99", "samples": {"SRR99": {"files": [{"name": "SRR99_1.fastq.gz", "bytes": 999}]}}}))
    assert ProgressTracker().snapshot(job).total_bytes == 400


def test_incomplete_project_mapping_cannot_claim_total(tmp_path):
    job, _, _ = setup_job(tmp_path)
    job["total_runs"] = 3
    tracker = ProgressTracker()
    assert tracker.snapshot(job).total_bytes is None
    assert tracker.snapshot(job, "SRR1").total_bytes == 200


def test_oversized_files_are_capped_but_not_marked_valid(tmp_path):
    job, raw, _ = setup_job(tmp_path, geo=False, runs=("SRR1",))
    sparse(fastq(raw, "SRR1", geo=False), 150)
    sparse(fastq(raw, "SRR1", 2, geo=False), 100)
    snap = ProgressTracker().snapshot({**job, "downloaded_runs": 1})
    assert snap.percent == 100 and snap.current_bytes == 200
    assert snap.completed_runs == 0 and snap.label == "校验中"


@pytest.mark.parametrize("value,result", [(0, "0.00 B"), (1234, "1.23 KB"), (2_500_000, "2.50 MB"), (3_000_000_000, "3.00 GB"), (None, "未知")])
def test_decimal_units(value, result):
    assert format_bytes(value) == result


def test_formatting_and_unknown_values(tmp_path):
    assert format_speed(2_500_000) == "2.50 MB/s"
    assert format_speed(None) == "速度计算中"
    assert format_duration(61) == "1 分 01 秒"
    assert format_duration(3661) == "1 时 01 分"
    assert format_duration(None) == "未知"
    job, _, _ = setup_job(tmp_path)
    snap = replace(ProgressTracker().snapshot(job), current_bytes=200, percent=50, speed_bps=10, eta_seconds=20)
    lines = progress_lines(snap, width=10)
    assert len(lines) == 2 and "█████░░░░░ 50.0%" in lines[0]
    assert "10.00 B/s" in lines[1] and "剩余 20 秒" in lines[1]


def test_symlink_escape_and_fifo_are_never_opened(tmp_path):
    job, raw, _ = setup_job(tmp_path, geo=False, runs=("SRR1",))
    outside = tmp_path / "outside.fastq.gz"
    sparse(outside, 100)
    target = fastq(raw, "SRR1", geo=False)
    try:
        target.symlink_to(outside)
    except OSError:
        pytest.skip("symlinks unavailable")
    assert ProgressTracker().snapshot(job).current_bytes == 0


def test_invalid_run_scope_rejected(tmp_path):
    job, _, _ = setup_job(tmp_path)
    with pytest.raises(ValueError):
        ProgressTracker().snapshot(job, "../../SRR1")


def test_metadata_reads_are_bounded_and_do_not_show_partial_project_percent(tmp_path):
    import teflow.progress as progress
    job, raw, _ = setup_job(tmp_path)
    sparse(raw / "GSE1.metadata.tsv", progress.MAX_METADATA_BYTES + 1)
    assert ProgressTracker().snapshot(job).total_bytes is None


def test_file_count_limit_makes_project_size_unknown(tmp_path, monkeypatch):
    import teflow.progress as progress
    job, _, _ = setup_job(tmp_path)
    job["total_runs"] = 0
    monkeypatch.setattr(progress, "MAX_FILES", 2)
    assert ProgressTracker().snapshot(job).total_bytes is None


def test_bad_metadata_urls_and_missing_row_values_are_graceful(tmp_path):
    job, raw, _ = setup_job(tmp_path)
    cache = {"accession": "GSE1", "records": {"SRR1": {"row": {"fastq_ftp": "https://["}}, "SRR2": {"row": None}}}
    (raw / ".teflow-GSE1.geo.complete.json").write_text(json.dumps(cache))
    snap = ProgressTracker().snapshot(job)
    assert snap.total_bytes is None and snap.percent is None


def test_reopening_old_scope_does_not_average_over_inactive_minutes(tmp_path):
    job, raw, _ = setup_job(tmp_path)
    clock = Clock()
    tracker = ProgressTracker(clock=clock)
    tracker.snapshot(job)
    clock.advance(120)
    sparse(fastq(raw, "SRR1"), 50)
    assert tracker.snapshot(job).speed_bps is None


def test_orphan_fastq_not_in_paired_workflow_is_excluded(tmp_path):
    job, raw, _ = setup_job(tmp_path, geo=False, runs=("SRR1",))
    metadata = raw / "SRR1.metadata.tsv"
    text = metadata.read_text().replace("SRR1_2.fastq.gz", "SRR1_2.fastq.gz;ftp.sra.ebi.ac.uk/SRR1.fastq.gz").replace("100;100", "100;100;999")
    metadata.write_text(text)
    sparse(raw / "SRR1.fastq.gz", 999)
    snap = ProgressTracker().snapshot(job)
    assert snap.current_bytes == 0 and snap.total_bytes == 200


def test_same_count_mapping_change_resets_speed_samples(tmp_path):
    job, raw, _ = setup_job(tmp_path, geo=False, runs=("SRR1",))
    clock = Clock()
    tracker = ProgressTracker(clock=clock)
    sparse(fastq(raw, "SRR1", geo=False), 50)
    tracker.snapshot(job)
    metadata = raw / "SRR1.metadata.tsv"
    metadata.write_text(metadata.read_text().replace("SRR1_1.fastq.gz", "SRR1_R1.fastq.gz"))
    sparse(raw / "SRR1_R1.fastq.gz", 80)
    clock.advance(2)
    snap = tracker.snapshot(job)
    assert snap.current_bytes == 80 and snap.speed_bps is None


def test_selected_run_percent_is_not_whole_project_percent(tmp_path):
    job, raw, _ = setup_job(tmp_path)
    sparse(fastq(raw, "SRR1"), 100)
    tracker = ProgressTracker()
    assert tracker.snapshot(job).percent == 25
    assert tracker.snapshot(job, "SRR1").percent == 50
    assert tracker.snapshot(job, "SRR2").percent == 0
    sparse(fastq(raw, "SRR1", 2), 100)
    sparse(fastq(raw, "SRR2"), 50)
    job["downloaded_runs"] = 1
    assert tracker.snapshot(job).percent == 62.5
    assert tracker.snapshot(job, "SRR1").label == "下载完成"
    assert tracker.snapshot(job, "SRR2").percent == 25


def test_explicit_current_run_validation_pauses_rate_before_project_download_is_complete(tmp_path):
    job, raw, _ = setup_job(tmp_path)
    clock = Clock()
    tracker = ProgressTracker(clock=clock)
    tracker.snapshot(job)
    clock.advance(2)
    sparse(fastq(raw, "SRR1"), 80)
    assert tracker.snapshot(job).speed_bps == 40
    sparse(fastq(raw, "SRR1"), 100)
    sparse(fastq(raw, "SRR1", 2), 100)
    job.update(step="校验 FASTQ", current_sample="SRR1")
    project = tracker.snapshot(job)
    assert project.percent == 50 and project.label == "校验中"
    assert project.speed_bps == 0 and project.eta_seconds is None
    assert tracker.snapshot(job, "SRR1").label == "校验中"
    assert tracker.snapshot(job, "SRR2").label != "校验中"
    job.update(step="下载 FASTQ", current_sample="SRR2", downloaded_runs=1)
    clock.advance()
    assert tracker.snapshot(job).speed_bps is None


def sra_job(tmp_path, *, geo=True):
    job, raw, output = setup_job(tmp_path, geo=geo, runs=("SRR1", "SRR2") if geo else ("SRR1",))
    metadata = raw / f"{job['accession']}.metadata.tsv"
    lines = metadata.read_text().splitlines()
    rows = list(csv.DictReader(lines, delimiter="\t"))
    for row in rows:
        row.update(fastq_ftp="", fastq_bytes="")
    with metadata.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]), delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    job["current_sample"] = "SRR2" if geo else "SRR1"
    job["downloaded_runs"] = 1 if geo else 0
    return job, raw, output


def test_sra_archive_growth_tracks_actual_transfer_without_mixing_completed_fastq(tmp_path):
    job, raw, _ = sra_job(tmp_path)
    sparse(raw / "SRR1" / "SRR1", 10_000)
    for mate in (1, 2):
        sparse(fastq(raw, "SRR1", mate), 3000)
    archive = raw / "SRR2" / "SRR2"
    sparse(archive, 1000)
    clock = Clock()
    tracker = ProgressTracker(clock=clock)
    first = tracker.snapshot(job)
    assert first.label == "下载 SRA（SRR2）" and first.current_bytes == 1000
    assert first.total_bytes is None and first.percent is None and first.speed_bps is None
    assert tracker.snapshot(job, "SRR2").current_bytes == 1000
    other = tracker.snapshot(job, "SRR1")
    assert other.current_bytes == 6000 and other.label == "下载完成"
    assert other.total_bytes == 6000 and other.percent == 100
    assert other.speed_bps == 0
    clock.advance(2)
    sparse(archive, 1400)
    project = tracker.snapshot(job)
    scoped = tracker.snapshot(job, "SRR2")
    assert project.speed_bps == scoped.speed_bps == 200
    assert project.current_bytes == scoped.current_bytes == 1400
    assert project.percent is None and project.eta_seconds is None
    assert "总大小未知" in progress_lines(project)[0]


def test_sra_extension_archive_and_phase_notice_before_fastq_creation(tmp_path):
    job, raw, output = sra_job(tmp_path, geo=False)
    job["attempt_started_at"] = "2026-10-15T12:00:00+08:00"
    archive = raw / "SRR1.sra"
    sparse(archive, 1000)
    tracker = ProgressTracker(wall_clock=lambda: datetime.fromisoformat("2026-10-15T12:01:00+08:00").timestamp())
    assert tracker.snapshot(job).current_bytes == 1000
    logs = output / "logs"
    logs.mkdir()
    (logs / "prepare_samples.log").write_text("$ /bin/iseq -i SRR1 -g\nConverting SRR1 to fastq files using 8 threads\n")
    snap = tracker.snapshot(job)
    assert snap.label == "转换 FASTQ" and snap.current_bytes == 0
    assert snap.speed_bps == 0 and snap.percent is None and snap.eta_seconds is None
    lines = progress_lines(snap)
    assert len(lines) == 1 and "转换 FASTQ" in lines[0] and "1 分" in lines[0]
    assert "B/s" not in lines[0] and "%" not in lines[0]


def test_sra_uncompressed_fastq_marks_conversion_and_native_retry_resets_it(tmp_path):
    job, raw, output = sra_job(tmp_path, geo=False)
    archive = raw / "SRR1"
    sparse(archive, 1000)
    sparse(raw / "SRR1_1.fastq", 50)
    tracker = ProgressTracker()
    assert tracker.snapshot(job).label == "转换 FASTQ"
    (output / "logs").mkdir()
    log = output / "logs" / "prepare_samples.log"
    log.write_text("Converting SRR1 to fastq files using 8 threads\n$ /bin/iseq -i SRR1 -g\n")
    assert tracker.snapshot(job).label == "下载 SRA（SRR1）"
    log.write_text(log.read_text() + "Compressing SRR1_1.fastq to SRR1_1.fastq.gz using 8 threads\n")
    assert tracker.snapshot(job).label == "转换 FASTQ"


def test_sra_stale_fastq_does_not_hide_new_archive_download(tmp_path):
    job, raw, _ = sra_job(tmp_path, geo=False)
    sparse(raw / "SRR1_1.fastq", 50)
    os.utime(raw / "SRR1_1.fastq", (1, 1))
    sparse(raw / "SRR1", 1000)
    assert ProgressTracker().snapshot(job).label == "下载 SRA（SRR1）"


def test_sra_verify_event_overrides_conversion_phase(tmp_path):
    job, raw, _ = sra_job(tmp_path, geo=False)
    sparse(raw / "SRR1", 1000)
    sparse(raw / "SRR1_1.fastq", 50)
    job["step"] = "校验 FASTQ"
    snap = ProgressTracker().snapshot(job)
    assert snap.label == "校验中" and snap.speed_bps == 0 and snap.percent is None


def test_sra_cache_file_source_controls_archive_fallback(tmp_path):
    job, raw, _ = sra_job(tmp_path)
    cache = {"accession": "GSE1", "records": {
        run: {"files": [{"name": f"{run}_{mate}.fastq.gz", "bytes": None, "source": "sra-converted"} for mate in (1, 2)]}
        for run in ("SRR1", "SRR2")}}
    (raw / ".teflow-GSE1.geo.complete.json").write_text(json.dumps(cache))
    sparse(raw / "SRR2" / "SRR2", 1000)
    assert ProgressTracker().snapshot(job).label == "下载 SRA（SRR2）"


def test_ena_unknown_byte_size_does_not_consume_unrelated_sra_archive(tmp_path):
    job, raw, _ = setup_job(tmp_path, geo=False, runs=("SRR1",), sizes=(None, None))
    job["current_sample"] = "SRR1"
    sparse(raw / "SRR1", 1000)
    snap = ProgressTracker().snapshot(job)
    assert snap.current_bytes == 0 and snap.label == "下载数据"


def test_all_sra_runs_confirmed_reveal_actual_final_compressed_total(tmp_path):
    job, raw, _ = sra_job(tmp_path)
    for run in ("SRR1", "SRR2"):
        for mate in (1, 2):
            sparse(fastq(raw, run, mate), 3000)
    tracker = ProgressTracker()
    partial = tracker.snapshot(job)
    assert partial.total_bytes is None and partial.percent is None
    job["downloaded_runs"] = 2
    completed = tracker.snapshot(job)
    assert completed.total_bytes == completed.current_bytes == 12000
    assert completed.percent == 100 and completed.label == "下载完成"


def test_confirmed_partial_mapping_does_not_claim_whole_project_complete(tmp_path):
    job, raw, _ = setup_job(tmp_path)
    metadata = raw / "GSE1.metadata.tsv"
    metadata.write_text("\n".join(metadata.read_text().splitlines()[:2]) + "\n")
    for mate in (1, 2):
        sparse(fastq(raw, "SRR1", mate), 100)
    job["downloaded_runs"] = 1
    snap = ProgressTracker().snapshot(job)
    assert snap.total_bytes is None and snap.percent is None and snap.label != "下载完成"
