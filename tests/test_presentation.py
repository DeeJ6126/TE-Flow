from rich.text import Text

from teflow.presentation import (
    batch_counts, job_progress, progress_signature, stage_key, stage_label, stage_track,
)


def test_legacy_history_uses_same_four_stages():
    for old in ("qc", "align", "count"):
        assert stage_key({"stage": old}) == "analyze"
        assert stage_label({"stage": old}) == "分析数据"
    assert stage_key({"stage": "preparing"}) == "metadata"


def test_stage_track_highlights_current_stage_and_all_completed():
    track = stage_track({"stage": "analyze", "status": "running"})
    assert isinstance(track, Text)
    assert track.plain == "✓ 解析项目 ━━ ✓ 下载数据 ━━ ● 分析数据 ── ○ 汇总结果"
    done = stage_track({"stage": "merge", "status": "succeeded"})
    assert done.plain.count("✓") == 4
    failed = stage_track({"stage": "download", "status": "failed"})
    assert "✗ 下载数据" in failed.plain


def test_queue_has_no_false_started_marker():
    track = stage_track({"stage": "metadata", "status": "queued"})
    assert track.plain.startswith("○ 解析项目")
    assert "●" not in track.plain
    assert job_progress({"status": "queued", "queue_wait_reason": "等待内存"}) == "等待资源（空出后自动开始）"


def test_queued_mainline_hides_detailed_resource_accounting():
    for status in ("queued", "pending"):
        job = {"status": status, "queue_wait_reason": "需 10 CPU / 256000 MB；当前余量 0 CPU / 1 MB。"}
        assert job_progress(job) == "等待资源（空出后自动开始）"
        assert "256000" not in job_progress(job)


def test_progress_reports_run_counts_and_analysis_step():
    assert job_progress({"status": "running", "stage": "download", "total_runs": 5,
                         "downloaded_runs": 2}) == "已下载 2/5 Run"
    assert job_progress({"status": "running", "stage": "analyze", "total_runs": 2,
                         "analyzed_runs": 1, "step": "star_align", "current_sample": "SRR9"}) == "已完成 1/2 Run · SRR9 · 比对"
    assert "质控" in job_progress({"status": "running", "stage": "qc"})
    assert job_progress({"status": "running", "stage": "download", "total_runs": "bad"}) == "正在下载"


def test_batch_counts_terminal_failures_and_legacy_success():
    counts = batch_counts({"status": status} for status in (
        "running", "queued", "pending", "success", "succeeded", "failed", "interrupted", "cancelled"))
    assert counts == {"total": 8, "running": 1, "queued": 2, "succeeded": 2, "failed": 3}


def test_progress_signature_ignores_clock_updates_but_captures_counts():
    job = {"stage": "download", "status": "running", "downloaded_runs": 1}
    assert progress_signature(job) == progress_signature({**job, "updated_at": "later"})
    assert progress_signature(job) != progress_signature({**job, "downloaded_runs": 2})


def test_metadata_progress_explains_sample_resolution_changes():
    metadata = {"status": "running", "stage": "metadata", "step": "解析项目"}
    assert job_progress(metadata) == "正在获取样本和 Run 清单"
    first = {**metadata, "step": "解析样本 1/2", "current_sample": "GSM1"}
    second = {**metadata, "step": "解析样本 2/2", "current_sample": "GSM2"}
    assert job_progress(first) == "解析样本 1/2 · GSM1"
    assert job_progress(second) == "解析样本 2/2 · GSM2"
