"""Validate an entire batch before any job is submitted."""
from pathlib import Path
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
