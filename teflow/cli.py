"""Foreground CLI viewer over the same durable jobs used by the TUI."""

from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Sequence
from pathlib import Path

from rich.console import Console, Group
from rich.live import Live
from rich.table import Table
from rich.text import Text

from .data_types import DATA_TYPES, validate_data_type
from .presentation import (
    ACTIVE, STAGES, SUCCESS, batch_summary, job_progress, job_summary,
    progress_signature, stage_label, status_label, value,
)
from .runtime import JobManager


POLL_SECONDS = 1.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TE 工作流：GEO/SRA 到合并计数表；不带命令打开 TUI。")
    parser.add_argument("--settings", help="应用 settings.yaml 路径")
    commands = parser.add_subparsers(dest="command")
    run = commands.add_parser("run", help="批量运行并查看进度，自动按资源并行")
    run.add_argument("source", help="逗号分隔编号，或服务器 TXT 文件的绝对路径（每行一个编号）")
    run.add_argument("-t", "--type", dest="data_type", choices=DATA_TYPES, required=True,
                     help="数据类型：bulk；scrna 单细胞流程待配置")
    run.add_argument("--input-mode", choices=("auto", "accessions", "file"), default="auto",
                     help="输入方式，默认自动识别")
    run.add_argument("--cores", type=_positive_integer, help="每个项目使用的 CPU 数，默认读取配置")
    run.add_argument("--verbose", "-v", action="store_true", help="显示详细工具日志")
    submit = commands.add_parser("submit", help="提交单个编号后立即返回（维护入口）")
    submit.add_argument("accession")
    submit.add_argument("-t", "--type", dest="data_type", choices=DATA_TYPES, required=True,
                        help="数据类型：bulk；scrna 单细胞流程待配置")
    status = commands.add_parser("status", help="以 JSON 查看任务状态")
    status.add_argument("accession", nargs="?")
    logs = commands.add_parser("logs", help="查看详细日志")
    logs.add_argument("accession")
    for child in (run, submit, status, logs):
        child.add_argument("--settings", default=argparse.SUPPRESS, help="应用 settings.yaml 路径")
    return parser


def shortcut_main(argv: Sequence[str] | None = None) -> int:
    """Short `tf` spelling; retain one implementation of submission and viewing."""
    parser = argparse.ArgumentParser(
        prog="tf",
        description="TE 工作流快捷入口；不带参数打开 TUI。多个项目自动按共享资源并行。",
        epilog="示例：tf -a GSE219739 -t bulk；tf -a GSE1,GSE2 -t bulk；tf -f /root/geo_ids.txt -t bulk",
    )
    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument("-a", "--accession", help="单个编号或逗号分隔的多个编号")
    source_group.add_argument("-f", "--file", help="服务器 TXT 文件的绝对路径，每行一个编号")
    parser.add_argument("-t", "--type", dest="data_type", choices=DATA_TYPES,
                        help="运行时必须指定数据类型：bulk；scrna 单细胞流程待配置")
    parser.add_argument("--settings", help="应用 settings.yaml 路径")
    parser.add_argument("--cores", type=_positive_integer, help="每个项目使用的 CPU 数，默认读取配置")
    parser.add_argument("--verbose", "-v", action="store_true", help="显示详细工具日志")
    args = parser.parse_args(argv)
    forwarded = [f"--settings={args.settings}"] if args.settings is not None else []
    if args.accession is None and args.file is None:
        if args.cores is not None or args.verbose or args.data_type is not None:
            parser.error("请使用 -a 指定编号或 -f 指定 TXT；直接运行 tf 打开 TUI。")
        return main(forwarded)
    if args.data_type is None:
        parser.error("请使用 -t bulk 或 -t scrna 明确指定数据类型。")
    source = args.accession if args.accession is not None else args.file
    mode = "accessions" if args.accession is not None else "file"
    forwarded.extend(["run", "--input-mode", mode, "--type", args.data_type])
    if args.cores is not None:
        forwarded.extend(["--cores", str(args.cores)])
    if args.verbose:
        forwarded.append("--verbose")
    # An explicit positional boundary also preserves values beginning with '-'.
    forwarded.extend(["--", source])
    return main(forwarded)


def _positive_integer(raw: str) -> int:
    try:
        result = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("必须是正整数") from exc
    if result < 1:
        raise argparse.ArgumentTypeError("必须是正整数")
    return result


def render_batch(jobs: list[dict]) -> Group:
    """A compact live dashboard without scientific-tool output."""
    table = Table.grid(padding=(0, 2), expand=True)
    table.add_column(no_wrap=True)
    table.add_column(no_wrap=True)
    table.add_column(ratio=1, overflow="fold")
    for job in jobs:
        status = value(job.get("status"))
        style = "green" if status in SUCCESS else "cyan" if status == "running" else "yellow" if status in ACTIVE else "red"
        label = stage_label(job) if status == "running" else status_label(job)
        table.add_row(Text(str(job["accession"]), style="bold"),
                      Text(label, style=style),
                      Text(job_progress(job) if status in ACTIVE else stage_label(job) if status not in SUCCESS else ""))
    return Group(
        Text(" → ".join(label for _, label in STAGES), style="bold cyan"),
        Text(batch_summary(jobs)), Text(), table,
        Text("Ctrl+C 退出查看，后台任务继续。详细日志：--verbose 或 teflow logs <编号>", style="dim"),
    )


def _log_delta(previous: str, current: str) -> str:
    if not current or current == previous:
        return ""
    if not previous:
        return current
    if current.startswith(previous):
        return current[len(previous):].lstrip("\n")
    old_lines, new_lines = previous.splitlines(), current.splitlines()
    for overlap in range(min(len(old_lines), len(new_lines)), 0, -1):
        if old_lines[-overlap:] == new_lines[:overlap]:
            return "\n".join(new_lines[overlap:])
    return current


def _latest_jobs(manager: JobManager, accessions: list[str]) -> list[dict]:
    snapshots = {str(job["accession"]): job for job in manager.list_jobs()}
    return [snapshots.get(accession) or {
        "accession": accession, "status": "failed", "stage": "metadata",
        "error": "找不到任务记录，请检查 state/jobs.json。",
    } for accession in accessions]


def _print_result(console: Console, job: dict) -> None:
    accession = str(job["accession"])
    if value(job.get("status")) in SUCCESS:
        console.print(Text(f"{accession} 结果：{job.get('result_path') or '未记录结果路径'}", style="green"), soft_wrap=True)
    else:
        message = str(job.get("error") or status_label(job)).splitlines()[0]
        console.print(Text(f"{accession} 错误：{message}", style="red"), soft_wrap=True)
        if job.get("job_dir"):
            console.print(Text(f"{accession} 日志：{Path(job['job_dir']) / 'run.log'}"), soft_wrap=True)


def watch_jobs(manager: JobManager, submitted: list[dict], *, verbose: bool = False,
               console: Console | None = None) -> int:
    """View submitted projects until terminal; closing this viewer never cancels jobs."""
    console = console or Console()
    jobs = submitted
    accessions = [str(job["accession"]) for job in jobs]
    signatures: dict[str, tuple] = {}
    log_tails: dict[str, str] = {}
    shown_results: set[str] = set()
    use_live = console.is_terminal and not console.is_dumb_terminal
    live = Live(render_batch(jobs), console=console, auto_refresh=False, transient=False) if use_live else None
    try:
        if live:
            live.start()
        else:
            console.print(Text("流程：" + " → ".join(label for _, label in STAGES)))
            console.print(Text(batch_summary(jobs)))
        while True:
            if live:
                live.update(render_batch(jobs), refresh=True)
            for job in jobs:
                accession = str(job["accession"])
                signature = progress_signature(job)
                if not live and signatures.get(accession) != signature:
                    console.print(Text(job_summary(job)), soft_wrap=True)
                signatures[accession] = signature
                if verbose:
                    tail = manager.log_tail(accession, lines=100)
                    delta = _log_delta(log_tails.get(accession, ""), tail)
                    if delta:
                        console.print(Text(f"[{accession}]\n{delta}"), soft_wrap=True)
                    log_tails[accession] = tail
                if value(job.get("status")) not in ACTIVE and accession not in shown_results:
                    _print_result(console, job)
                    shown_results.add(accession)
            if all(value(job.get("status")) not in ACTIVE for job in jobs):
                break
            time.sleep(POLL_SECONDS)
            jobs = _latest_jobs(manager, accessions)
    except KeyboardInterrupt:
        if live:
            live.stop()
            live = None
        console.print(Text("已退出进度查看，后台任务继续运行。可用 teflow status 或重新打开 TUI 查看。", style="yellow"))
        return 130
    finally:
        if live:
            live.stop()
    console.print(Text(batch_summary(jobs)))
    return 0 if jobs and all(value(job.get("status")) in SUCCESS for job in jobs) else 1


def main(argv: Sequence[str] | None = None) -> int:
    # os.umask exists on supported Linux servers and test hosts, but is optional on other hosts.
    if hasattr(os, "umask"):
        os.umask(0o002)
    args = build_parser().parse_args(argv)
    console = Console()
    try:
        if args.command in {"run", "submit"}:
            validate_data_type(args.data_type)
        manager = JobManager(args.settings)
        if args.command == "run":
            jobs = manager.submit_many(args.source, input_mode=args.input_mode, cores=args.cores,
                                       data_type=args.data_type)
            return watch_jobs(manager, jobs, verbose=args.verbose, console=console)
        if args.command == "submit":
            print(json.dumps(manager.submit(args.accession, data_type=args.data_type), ensure_ascii=False, indent=2))
        elif args.command == "status":
            print(json.dumps(manager.get_job(args.accession) if args.accession else manager.list_jobs(), ensure_ascii=False, indent=2))
        elif args.command == "logs":
            print(manager.log_tail(args.accession))
        else:
            from .tui import TEFlowApp
            TEFlowApp(manager=manager).run()
        return 0
    except (ValueError, OSError) as exc:
        Console(stderr=True).print(Text(f"输入或配置错误：{exc}", style="red"))
        return 2
    except RuntimeError as exc:
        Console(stderr=True).print(Text(f"运行失败：{exc}", style="red"))
        return 1
    except KeyboardInterrupt:
        Console(stderr=True).print(Text("已退出，已提交的后台任务继续运行。", style="yellow"))
        return 130
