"""Validate an entire batch before any job is submitted."""
from pathlib import Path
import csv
import re


def normalize_accession(value: str) -> str:
    accession = str(value).strip().upper()
    if not re.fullmatch(r"(?:[SED]R[APRSX]|PRJ(?:NA|EB|DB)|GS[EM])\d+", accession):
        raise ValueError(f"编号无效：{value!r}。请输入 GSE、SRR、SRP 或 PRJNA 等编号。")
    return accession


def parse_inputs(source: str, input_mode: str = "auto") -> list[str]:
    value = str(source).strip()
    if not value:
        raise ValueError("请输入编号列表或 TXT 文件的绝对路径。")
    mode = {"direct": "accessions", "text": "accessions"}.get(input_mode, input_mode)
    if mode not in {"auto", "accessions", "file"}:
        raise ValueError("输入方式必须是 accessions、file 或 auto。")
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        value = value[1:-1]
    if mode == "auto":
        mode = "file" if ("/" in value or "\\" in value or value.lower().endswith(".txt")) else "accessions"
    if mode == "file":
        path = Path(value)
        if not path.is_absolute():
            raise ValueError("TXT 请使用运行工作流的服务器上的绝对路径。")
        try:
            content = path.read_text(encoding="utf-8-sig")
        except (OSError, UnicodeError) as exc:
            raise ValueError(f"无法读取 UTF-8 TXT 文件 {path}：{exc}") from exc
        items = []
        for number, line in enumerate(content.splitlines(), 1):
            if not line.strip():
                continue
            try:
                items.append(normalize_accession(line))
            except ValueError as exc:
                raise ValueError(f"{path} 第 {number} 行：每行应只有一个编号。{exc}") from exc
    else:
        items = [normalize_accession(part) for part in value.split(",")]
    result = list(dict.fromkeys(items))
    if not result:
        raise ValueError("编号清单为空。")
    return result

def parse_batch(source, input_mode):
    """Return (accessions, runs_by_gsm).

    A selection CSV (columns gsm, target_runs) yields one job per GSM; its Runs are
    pooled into a single sample before QC, alignment and counting.
    """
    path = Path(str(source).strip().strip("'\""))
    if input_mode not in {"auto", "file"} or path.suffix.lower() != ".csv":
        return parse_inputs(source, input_mode), {}
    if not path.is_absolute():
        raise ValueError("选样 CSV 需要服务器绝对路径。")
    runs_by_gsm, owner = {}, {}
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not {"gsm", "target_runs"}.issubset(reader.fieldnames or []):
            raise ValueError("选样 CSV 需包含 gsm 和 target_runs 列。")
        for row in reader:
            gsm = (row["gsm"] or "").strip().upper()
            if not re.fullmatch(r"GSM\d+", gsm):
                raise ValueError(f"第 {reader.line_num} 行 GSM 无效。")
            if gsm in runs_by_gsm:
                raise ValueError(f"第 {reader.line_num} 行：{gsm} 在清单中重复出现。")
            runs = []
            for item in (row["target_runs"] or "").split(";"):
                run = item.strip().upper()
                if not re.fullmatch(r"[SED]RR\d+", run):
                    raise ValueError(f"第 {reader.line_num} 行 Run 无效。")
                if run in owner and owner[run] != gsm:
                    raise ValueError(f"{run} 对应多个 GSM，请核对清单。")
                owner[run] = gsm
                if run not in runs:
                    runs.append(run)
            runs_by_gsm[gsm] = runs
    if not runs_by_gsm:
        raise ValueError("编号清单为空。")
    return list(runs_by_gsm), runs_by_gsm
