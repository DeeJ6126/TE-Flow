# Confirmed follow-up work

Current baseline: TEflow 2.2.1. The items below were agreed on 2026-10-15 and have not yet been implemented.

## 1. Unified tf commands

- [ ] Keep `tf` for opening the TUI and `tf -a/-f ... -t bulk` for submission.
- [ ] Add `tf status [accession]` and `tf logs <accession>`.
- [ ] Update every user-facing command hint to the corresponding `tf` spelling.

## 2. Shared CLI/TUI progress bars

- [ ] Show actual downloaded bytes, total size, percentage, recent average speed, and estimated remaining time.
- [ ] Choose B/s, KB/s, MB/s or GB/s automatically and show two decimal places.
- [ ] Sum project download progress over all its Runs; follow the selected project/Run in the TUI.
- [ ] Display verification explicitly and use actual tool progress when available. Otherwise show the analysis step, completed Runs and elapsed time.
- [ ] Use the same progress data in CLI and TUI; allow one row per active CLI project.

## 3. Bounded connection checks

- [ ] Check only websites required by new network work; completed-result reuse and local-only analysis need no check.
- [ ] Run lightweight checks concurrently once per batch, with a proposed total budget of five seconds.
- [ ] Report the unreachable website in plain language, e.g. NCBI GEO, rather than exposing proxy implementation details.
- [ ] Observe actual request failures during execution and try an available fallback source when appropriate.

## 4. Explain only confirmed failures

- [ ] Explain explicit single-end/paired-end mismatches, website connection failures, and exhausted download-integrity retries.
- [ ] Derive explanations from structured checks/exceptions with clear evidence.
- [ ] Keep existing behavior for unknown errors: failed status, original error and detailed-log access. Do not guess a cause from the last log line or label arbitrary excerpts as the root cause.

## Pending scientific input

Single-cell analysis remains unimplemented. The `scrna` type is reserved and rejected before submission until the method, example data, required references, parameters and outputs are supplied.

Other experience suggestions have not been approved for implementation and are not part of this list.
