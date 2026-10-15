# TEflow implementation plan — 2026-10-14

## Scope

Implement requirements 1, 2, 3 and 5. Requirement 4 (single-cell analysis) stays pending and is not implemented in this release.

1. A supported CLI `run` command sharing the runtime and scientific workflow with the TUI.
2. Resolve GEO Series through its own GSM samples to all associated sequencing Runs. Store FASTQ under `<raw_root>/<GSE>/<SRR>/` and scientific outputs under `<output_root>/<GSE>/<SRR>/`. Keep per-Run TEcount `.cntTable`; generate `merged.count` only at the project level. Save the GSE/GSM/experiment/Run mapping.
3. Accept comma-separated accessions and an absolute server TXT path (one accession per line). Deduplicate and validate before submission. Automatically run independent projects concurrently within a shared CPU/memory budget; persist queues and isolate failures.
4. Pending: single-cell workflow and analysis-method specification.
5. Four visible stages: resolve project → download → analyze → summarize. Persistent highlighted TUI stage line, batch summary, opt-in detailed logs, concise CLI reporting and useful errors.

Deployment root changes from `/root/TEatlas/teflow` to `/root/teflow`. Existing raw data, scientific output directories, the original template, reference indices and historical job records are retained.

## Execution steps

- [x] Audit existing source, deployment, jobs, resource limits and network.
- [x] Implement and test GEO-scoped resolution and nested workflow outputs.
- [x] Implement shared persistent automatic batch scheduling and input parsing.
- [x] Implement the foreground CLI and progress reporting.
- [x] Implement TUI batch inputs, persistent four-stage overview and log toggle.
- [x] Migrate deployment to `/root/teflow`; update launcher and environment entry point.
- [x] Run integration tests, real metadata resolution and meaningful workflow/UI checks.
- [x] Record evidence, usage, remaining limitations and delivery files in this document and README.

## Runtime interface for parallel implementation

`JobManager.submit_many(source, input_mode='auto', cores=None)` returns a list of project job dictionaries. `submit(accession)` remains available. `list_jobs()`, `get_job(accession)`, and `log_tail(accession, lines=100)` remain available. Batch input parsing is exposed as `parse_inputs(source, input_mode='auto') -> list[str]`.

Jobs expose `accession`, `status`, `stage` (`metadata/download/analyze/merge/done`), `step` (detail), `total_runs`, `downloaded_runs`, `analyzed_runs`, `current_sample`, `job_dir`, `raw_dir`, `result_path`, `error`, `created_at`, `batch_id`, and allocated `cores`/`mem_mb`. Older stage values are supported for history display. Queued jobs have no worker PID yet. Closing a viewer leaves the persistent scheduler and workers running.

## Results

Requirements **1, 2, 3 and 5 are complete**. Requirement **4 remains pending**. Current deployment: `/root/teflow`, TEflow 2.1.0 in the existing independent TE/Python 3.12 environment. Initial 2.0 acceptance and subsequent improvements are recorded below.

### Deployment and acceptance results

- Version 2.0.0 is installed from `/root/teflow` in the existing `TE` environment. The v1 snapshot is `/root/teflow-backups/20261014-v1.tar.gz`.
- Both historical jobs were retained. Existing raw data, result directories, references and template remain in their original locations.
- Final Linux suite (including the short CLI follow-up): **119 tests and 3 subtests passed**. Includes real detached scheduler/worker processes, shared CPU/memory accounting, resource queueing, failure release, exact GEO scope, Snakemake DAGs, CLI exit codes, and Textual Pilot tests at 80×24 and 100×30.
- Real GEO resolution found `GSE219739`, exactly two human paired Runs (`SRR22521672`, `SRR22521673`), totaling 48,129,144 compressed FASTQ bytes. Its broad SRA study contains thousands of unrelated Runs, making this a useful regression for precise Series scope.
- Real CLI TXT submission includes `GSE219739` and deliberately unsupported single-end `GSE219738`, with one duplicated GSE line. Queue validation deduplicated to two projects. First snapshot records two distinct worker processes and Snakemake processes running concurrently under one shared budget.
- Real scientific workflow completed on **2026-10-14 at 11:35:14 Asia/Shanghai**, first attempt successful. Both FASTQ pairs, sorted BAMs and per-Run TEcount tables are valid.
- Final project table: **37,921 feature rows**, columns `gene`, `SRR22521672`, `SRR22521673`; 489,617 bytes. Every merged count was independently compared to its input table. Neither Run subdirectory contains a redundant merged.count.
- Deliberately unsupported `GSE219738` failed before downloading its SINGLE Run; `GSE219739` continued to completion. TXT deduplication produced exactly two project jobs. The mixed positive/negative batch returned CLI exit code 1 as expected; completed-project comma reuse returned 0 without increasing attempts.
- TUI verification used the real JobManager and existing jobs: server TXT input, four-stage overview, folded logs, F2 expansion, visible error reason, and viewer exit without duplicate workers or retries. Screenshots were inspected; a duplicated error-log path was corrected and regression-tested.
- `pip check` passed. Launcher/setup Bash syntax checked. Root CLI and subsequent codex state access verified, preserving existing ACLs. Scientific STAR/TEcount parameters and template references remain unchanged.

### Files and usage

- Application and server record: `/root/teflow/IMPLEMENTATION_PLAN.md` and `/root/teflow/README.md`.
- TUI: `/root/teflow/launch.sh`.
- CLI: `/root/teflow/launch.sh run GSE219739,GSE113777` or `/root/teflow/launch.sh run /root/geo_ids.txt`.
- Result: `/root/TEatlas/bulk_rna/GSE219739/merged.count`.
- Evidence: `verification/v2/tests.log`, `end-to-end.json`, `first-batch-snapshot.json`, `tui-real.json`, CLI logs and screenshots.
- Local source: `D:/TE/teflow`; delivery archive: `D:/TE/teflow-v2-delivery.zip` (source, environment records, evidence and example merged table; raw FASTQ/BAM excluded).

### Remaining scope

Single-cell method, tools, parameters and output standard remain unspecified (requirement 4). This release continues the supplied human paired-end bulk template. Automatic parallelism uses conservative whole-workflow CPU/memory reservations; each project is internally scheduled by Snakemake. Existing proxy/tunnel connectivity is required for new network retrievals.

### Follow-up: short CLI entry (2026-10-14)

- Requested interface: `tf -a GSE219739`; long option `--accession`. Batch comma-separated IDs use the same flag; `tf -f /absolute/list.txt` selects a server-side TXT list.
- Implementation: a thin parser delegates to the existing run command. No changes to scientific analysis or scheduler behavior. `tf` alone opens the TUI.
- Installation: TE environment console entry plus `/usr/local/bin/tf` pointing to `/root/teflow/tf.sh`; wrapper loads the existing environment and proxy. Existing commands must not be overwritten.
- Complete: no pre-existing tf executable or root-shell command was found; installed `/usr/local/bin/tf -> /root/teflow/tf.sh` and the TE environment console entry.
- Verified real single accession, comma list, server TXT and root login-shell commands with exit code 0; all reused completed tasks without increasing attempts. Conflicting -a/-f flags return 2. Shell syntax checks passed.
- CLI unit suite: 31 passed. Final integrated suite: 119 passed plus 3 subtests. Evidence: `verification/v2/tf-shortcut.json`, `tf-cli-tests.log`, `tf-command-*.log`, and updated `tests.log`.
- README, deployment source and `D:/TE/teflow-v2-delivery.zip` refreshed. Existing full-form teflow commands remain supported.

### Follow-up: TUI results browser (2026-10-14)

Agreed layout: permanent four-stage workflow overview; left expandable GSE/SRR result tree; middle result-file list; right upper concise live events with elapsed time; right lower tabs for file preview and detailed running logs. F2 switches tabs, file selection returns to preview, and changing GSE/SRR scopes events/logs/files together. File list remains present while viewing logs.

- [x] Implement bounded directory listing and paged previews; hide internal artifacts by default. Text/count/config/log previews; FASTQ/BAM/HTML summaries. Preserve selection and scroll while refreshing.
- [x] Implement concise per-project/per-Run activity and bounded detailed log access, including existing history without new activity files.
- [x] Implement the tree/file-list/events/preview-and-log TUI with mouse and keyboard control, paused log following on upward scroll and End to resume.
- [x] Run filesystem, live-output, CLI regression and Pilot tests, inspect rendered UI at useful terminal sizes, deploy and verify against real existing results without rerunning analysis.
- [x] Update usage, evidence and delivery package with completion results.

Filesystem/browser API: `ResultBrowser.list_project(job, show_internal=False) -> ProjectListing` (runs and project files), `list_files(job, run=None, show_internal=False) -> list[ResultFile]`, `preview(job, file_path, page=0) -> PreviewPage`. ResultFile has path, relative, name, kind, size, modified_ns. PreviewPage has kind (table/text/summary/error), title, path, page, columns, rows, text, has_next, signature. All access stays within the project's result directory; pagination/read sizes are bounded.

Activity API: `activity_snapshot(job, run=None, limit=5) -> dict` with status, step, elapsed_seconds, events (list of dicts containing time/message/level/run). `log_snapshot(job, run=None, lines=150) -> dict` returns text and signature with bounded tail reads. Existing running workers and historical results remain viewable through log/manifest fallback; new workers may write structured activity without scientific-method changes.

#### Completion results

- Released TEflow **2.1.0** at `/root/teflow`. Previous source snapshot: `/root/teflow-backups/20261014-before-result-browser.tar.gz` (state excluded; live state retained).
- `results.py`: bounded, scoped directory browsing and 100-record pages; cached byte offsets survive verified appends using literal prefix/boundary comparisons, without hashing. Truncation/replacement resets to a valid page with a notice. Table previews cap columns/cells explicitly. Only regular in-project files are read; FIFO, devices and escaping symlinks are rejected.
- `activity.py`: job-ID-aware start/finish/failure events and current-step elapsed time, exact per-Run filtering, existing-history fallback, bounded log tails and ANSI cleanup. `runtime.py` adds observer calls only; scientific commands and scheduling behavior remain intact.
- `tui.py`: project/Run tree, persistent file list, compact live events, preview/log tabs, paging, F2/F3/F5, selection/scroll preservation, paused log following, and narrow-terminal layout. Final polish resets the follow-status label when changing scope.
- Full Linux regression: **174 tests + 3 subtests passed**. Final UI-specific checks after the follow-label polish: **13 passed**. Real result browsing verified source data on page 1/2, two-column per-Run table, three-column project merge, HTML summary, hidden-file toggle, empty planned Run, tab/page/scroll retention and log follow controls.
- Screenshots inspected at **170×48 and 80×24**. Compact view retains at least three visible data rows / log lines and shows the latest event. A test initially navigated to a collapsed tree child; corrected the smoke test to expand it first, matching user navigation.
- No scientific tasks were restarted. Existing job status/attempt/PID records were compared before and after the real UI smoke and stayed unchanged. `tf`/CLI entry remains compatible; single-cell requirement 4 remains pending.
- Evidence: `verification/browser/tests.log`, `tui-final-tests.log`, `real-ui.json`, `smoke-real-ui.py`, result/merged/log/compact screenshots. Updated usage is in README.
- Local source: `D:/TE/teflow`; release delivery: `D:/TE/teflow-v2.1-delivery.zip`. Original scientific result: `/root/TEatlas/bulk_rna/GSE219739/merged.count`.

### Follow-up: adjustable workspace and explicit data types (2026-10-14)

Approved scope: draggable workspace dividers with minimum sizes and saved layout; middle result tree grouped by directories; selectable, read-only file previews and copy-page action; full previous/next-page labels; explicit bulk/scrna selection shared by CLI/TUI. The top input area stays fixed. Single-cell analysis remains pending and must be rejected before submitting scientific work.

- [x] Add and test draggable workspace splitters, layout persistence and narrow-terminal bounds.
- [x] Replace the flat result list with a directory tree, preserving navigation on refresh.
- [x] Enable preview text selection/copy and reserve enough width for full pagination labels.
- [x] Require explicit data type in TUI and CLI (`-t bulk` / `-t scrna`), with shared validation and legacy-job compatibility.
- [x] Run regression and interaction checks; deploy and browse existing results without restarting analysis.
- [x] Update usage, completion evidence and delivery archive.

#### Completion results

- Released **2.2.0** to `/root/teflow` in the dedicated TE environment. Backup: `/root/teflow-backups/20261014-before-adjustable-workspace.tar.gz`. Validation staging: `/root/teflow-staging/20261014-workspace`.
- All four dividers capture the mouse while dragging, clamp adjacent panes to usable sizes, and retain the fixed input area. Preferences live separately in `state/ui-layout.json`. Saved ratios adapt to a different terminal size. Explicit heights are retained independently of the compact layout's automatic default.
- The middle `ResultFileTree` groups directories, expands on mouse/Enter, and retains cursor, expansions and scroll when outputs arrive. Clicking a folder leaves the preview intact; activating a file opens it.
- Read-only TextArea previews support mouse and keyboard selection, Chinese text, Ctrl+C, and copying the current bounded page. Tables remain aligned for display and copy as valid TSV with headers. Clipboard payload/OSC52 emission was verified; acceptance by the user's SSH client requires its clipboard permission and cannot be confirmed by the server.
- Pagination buttons now reserve sufficient width for **上一页 / 下一页**. Small activity panes put scope/status in their title so the latest event remains visible.
- New submissions require explicit `bulk` or `scrna` in every CLI/TUI entry. `scrna` is reserved and rejected before creating a job/downloading data until its scientific workflow is configured. Existing jobs with no recorded type are read as bulk. Scheduling and scientific parameters remain the supplied bulk workflow.
- Linux full regression: **183 passed**. Final layout/persistence checks after preference polish: **9 passed**. Isolated Windows component/interaction checks and independent read-only reviews also completed.
- Real GSE219739 checks: result folders, source-matching first/second count pages, exact copied TSV, mouse resizing, log tab, and full page labels. Inspected rendered screenshots at **170×48 / 80×24**; compact preview retains three widget rows and the latest activity event.
- Production command checks: single accession, comma list and TXT list all returned **0** with `-t bulk`; missing type and `-t scrna` returned **2**. Package/source versions both report 2.2.0; `pip check` passed. Existing job attempt counts and worker/Snakemake PIDs stayed unchanged, including the already running GSE113777 attempt 2.
- Evidence: `/root/teflow/verification/workspace/` and local `D:/TE/teflow/verification/workspace/` (tests, CLI reports/logs, real UI assertions and screenshots). Usage updated in README; delivery archive `D:/TE/teflow-v2.2-delivery.zip`.

### Follow-up: small TUI polish (2.2.1, 2026-10-14)

- Removed copy notifications for both selected-text and current-page copy; clipboard behavior is retained.
- Directories containing exactly one listed descendant file now appear as a single path leaf, e.g. `abundance/SRR22521672.cntTable`. Multi-file groups remain foldable. File/folder cursor transitions and unrelated expansion states are retained as results arrive or disappear.
- All draggable dividers use thin box-drawing lines on a transparent background, including hover/drag; horizontal dividers no longer fill an entire row with color. Drag hit areas and stored layout are retained.
- Validation: **35 related UI/component tests passed on Linux**, plus real GSE219739 preview/copy/page/drag checks. Inspected the rendered thin dividers and direct singleton path. Package/source imports and tf help verified after deployment; version **2.2.1** is installed in TE at `/root/teflow`.
- Backup: `/root/teflow-backups/20261014-before-tui-polish.tar.gz`. Evidence: `verification/workspace/polish-tests.log`, `polish-real-ui.json`, `polished-preview.svg/png`. Delivery: `D:/TE/teflow-v2.2.1-delivery.zip`.
