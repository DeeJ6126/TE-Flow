"""Prepare a complete, metadata-mapped SINGLE/PAIRED FASTQ manifest through iSeq.

Network clients inherit the active proxy environment. Downloads are sequential,
so iSeq's success/failure logs are never modified by simultaneous iSeq calls.
"""
import csv
import gzip
import hashlib
import io
import json
import os
import re
import shutil
import signal
import ssl
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
import zlib
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from teflow.failures import KnownFailure, known_failure, website_failure

try:
    import aws_download
except ImportError:  # Snakemake runs a copy of this script; its directory is added below.
    aws_download = None

ACCESSION_PATTERN = re.compile(
    r"(?:[SED]R[APXS]\d+|[SED]RR\d+|PRJ[END][A-Z]\d+|SAM[END][A-Z]?\d+|GSE\d+|GSM\d+)"
)
RUN_PATTERN = re.compile(r"[SED]RR\d+")
FASTQ_PATTERN = re.compile(r"[^/\\]+\.(?:fastq|fq)\.gz", re.IGNORECASE)
MATE_PATTERN = re.compile(r"(?:^|[_.-])(?:R)?([12])(?:_\d+)?\.(?:fastq|fq)\.gz$", re.IGNORECASE)
ENA_FIELDS = (
    "study_accession,secondary_study_accession,sample_accession,secondary_sample_accession,"
    "experiment_accession,run_accession,submission_accession,tax_id,scientific_name,"
    "instrument_platform,instrument_model,library_name,nominal_length,library_layout,"
    "library_strategy,library_source,library_selection,read_count,base_count,center_name,"
    "first_public,last_updated,experiment_title,study_title,study_alias,experiment_alias,"
    "run_alias,fastq_bytes,fastq_md5,fastq_ftp,fastq_aspera,fastq_galaxy,submitted_bytes,"
    "submitted_md5,submitted_ftp,submitted_aspera,submitted_galaxy,submitted_format,sra_bytes,"
    "sra_md5,sra_ftp,sra_aspera,sra_galaxy,sample_alias,broker_name,sample_title,nominal_sdev,"
    "first_created,bam_ftp,bam_bytes,bam_md5"
)
ENA_COLUMNS = ["run_accession", *(name for name in ENA_FIELDS.split(",") if name != "run_accession")]


class MetadataFormatError(ValueError):
    """A failed/interrupted metadata request may leave a reusable bad cache."""


class RunSetMismatch(MetadataFormatError):
    """A trustworthy independent query contradicts the cached metadata scope."""


def canonical_metadata_fields(fields: list[str]) -> list[str]:
    if "run_accession" in fields:
        # iSeq reads Run at column 1, bytes at 28 and FASTQ URL at 30. ENA search
        # and filereport endpoints need not return the same requested ordering.
        return ENA_COLUMNS + [name for name in fields if name not in ENA_COLUMNS]
    return ["Run", *(name for name in fields if name != "Run")]


def write_metadata(path: Path, fields: list[str], rows):
    temporary = path.with_name(path.name + ".teflow.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=canonical_metadata_fields(fields),
                                    delimiter="\t", lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def normalize_accession(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("编号必须是字符串。")
    accession = value.strip().upper()
    if not ACCESSION_PATTERN.fullmatch(accession):
        raise ValueError("编号格式无效，请输入 SRR、SRP、PRJNA 或 SRA 等真实数据库编号。")
    return accession


def atomic_json(path: Path, document: dict):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    try:
        temporary.write_text(json.dumps(document, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def log_message(message: str, handle=None):
    line = f"[TEFLOW] {message}"
    print(line, flush=True)
    if handle is not None:
        handle.write(line + "\n")
        handle.flush()


def progress(stage: str, **values):
    """Small machine-readable events; never mix tool stdout into this record."""
    print("[TEFLOW_PROGRESS] " + json.dumps({"stage": stage, **values}, ensure_ascii=False), flush=True)


def _safe_fastq_name(url: str) -> str:
    parsed = urllib.parse.urlsplit(url if "://" in url else "https://" + url)
    name = Path(urllib.parse.unquote(parsed.path)).name
    if not FASTQ_PATTERN.fullmatch(name) or name in (".", ".."):
        raise ValueError(f"元数据中的 FASTQ 文件名无效: {url}")
    return name


def fastq_files(row: dict, run: str) -> list[dict]:
    """Select each Run's reads from its authoritative layout and metadata names."""
    layout = str(row.get("library_layout", row.get("LibraryLayout", ""))).strip().upper()
    if layout not in {"SINGLE", "PAIRED"}:
        raise ValueError(f"{run}: 无法识别单双端类型，实际 layout={layout or '未知'}。")
    links = [link.strip() for link in str(row.get("fastq_ftp", "")).split(";") if link.strip()]
    if not links:
        # iSeq's SRA fallback converts the downloaded archive to these filenames.
        if layout == "SINGLE":
            return [{"name": f"{run}.fastq.gz", "bytes": None, "mate": 1, "source": "sra-converted"}]
        return [{"name": f"{run}_{mate}.fastq.gz", "bytes": None, "mate": mate, "source": "sra-converted"}
                for mate in (1, 2)]
    sizes = str(row.get("fastq_bytes", "")).split(";")
    if len(sizes) != len(links):
        raise ValueError(f"{run}: 元数据 FASTQ 链接与大小数量不一致。")
    if layout == "SINGLE":
        if len(links) != 1:
            raise ValueError(f"{run}: 单端 Run 对应多份 FASTQ，无法唯一确定输入，需明确文件含义。")
        name = _safe_fastq_name(links[0])
        if not sizes[0].isdigit() or int(sizes[0]) <= 0:
            raise ValueError(f"{run}: 元数据 FASTQ 大小无效: {name}")
        return [{"name": name, "bytes": int(sizes[0]), "mate": 1, "source": "ena-fastq"}]
    by_mate = {}
    all_names = set()
    for link, size in zip(links, sizes):
        name = _safe_fastq_name(link)
        if name in all_names:
            raise ValueError(f"{run}: 元数据 FASTQ 文件名重复: {name}")
        all_names.add(name)
        if not size.isdigit() or int(size) <= 0:
            raise ValueError(f"{run}: 元数据 FASTQ 大小无效: {name}")
        match = MATE_PATTERN.search(name)
        if not match:
            # ENA may also supply an unpaired orphan-read file. Only explicit
            # R1/R2 files enter this paired-end scientific workflow.
            continue
        mate = int(match.group(1))
        if mate in by_mate:
            raise ValueError(f"{run}: 一个 Run 对应多份 R{mate} 文件，无法唯一确定配对。")
        by_mate[mate] = {"name": name, "bytes": int(size), "mate": mate, "source": "ena-fastq"}
    if set(by_mate) != {1, 2}:
        raise ValueError(f"{run}: 元数据中缺少明确的 R1/R2 配对文件。")
    return [by_mate[1], by_mate[2]]


def paired_files(row: dict, run: str) -> list[dict]:
    """Compatibility name for callers; selection now supports both layouts."""
    return fastq_files(row, run)


def parse_metadata(path: Path, allow_duplicate_names=False) -> tuple[list[str], dict[str, dict]]:
    with Path(path).open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        fields = list(reader.fieldnames or [])
        run_field = "run_accession" if "run_accession" in fields else "Run"
        if run_field not in fields:
            raise MetadataFormatError(f"元数据缺少 Run 编号列: {path}")
        if not {"library_layout", "LibraryLayout"}.intersection(fields):
            raise MetadataFormatError(f"元数据缺少单双端 layout 列: {path}")
        records = {}
        names = {}
        for row in reader:
            if None in row or any(value is None for value in row.values()):
                raise MetadataFormatError(f"元数据表格列数不一致: {path}")
            run = row[run_field].strip()
            if not RUN_PATTERN.fullmatch(run):
                raise MetadataFormatError(f"元数据包含无效 Run 编号: {run}")
            files = fastq_files(row, run)
            layout = str(row.get("library_layout", row.get("LibraryLayout", ""))).strip().upper()
            record = {"row": row, "files": files, "library_layout": layout}
            if run in records and records[run] != record:
                raise ValueError(f"元数据包含相互冲突的 Run 记录: {run}")
            if run in records:
                continue
            for item in files:
                other = names.get(item["name"])
                if other and other != run and not allow_duplicate_names:
                    raise ValueError(f"不同 Run 使用相同 FASTQ 文件名: {other}, {run}, {item['name']}")
                names[item["name"]] = run
            records[run] = record
    if not records:
        raise MetadataFormatError(f"未找到可分析的 Run，编号可能无效或数据尚未公开: {path.stem}")
    return fields, records


def _request_text(url: str) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": "TEflow/1.0"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.read().decode("utf-8-sig")
    except urllib.error.HTTPError:
        raise
    except (urllib.error.URLError, TimeoutError, ConnectionError, ssl.SSLError) as exc:
        raise website_failure(url, exc) from exc


def fetch_ncbi_runinfo(accession: str) -> tuple[list[str], list[dict]]:
    params = urllib.parse.urlencode({"db": "sra", "term": f"{accession}[All Fields]",
                                    "usehistory": "y", "retmax": 0})
    result = ET.fromstring(_request_text("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?" + params))
    count = int(result.findtext("Count", "0"))
    webenv, querykey = result.findtext("WebEnv"), result.findtext("QueryKey")
    if count == 0 or not webenv or not querykey:
        raise ValueError(f"{accession}: 未找到公开的测序 Run。")
    all_rows, fieldnames = [], None
    for start in range(0, count, 10000):
        time.sleep(0.4)
        params = urllib.parse.urlencode({"db": "sra", "rettype": "runinfo", "retmode": "text",
                                        "WebEnv": webenv, "query_key": querykey,
                                        "retstart": start, "retmax": 10000})
        reader = csv.DictReader(io.StringIO(_request_text(
            "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?" + params)))
        if not reader.fieldnames or "Run" not in reader.fieldnames:
            raise ValueError("NCBI 没有返回有效 RunInfo。")
        if fieldnames is not None and fieldnames != reader.fieldnames:
            raise ValueError("NCBI 分页 RunInfo 列定义不一致。")
        fieldnames = reader.fieldnames
        page_rows = list(reader)
        if not page_rows or any(None in row or any(value is None for value in row.values()) for row in page_rows):
            raise ValueError("NCBI RunInfo 分页为空或表格不完整。")
        all_rows.extend(page_rows)
    return fieldnames, all_rows


def _metadata_request(url: str) -> str:
    last_error = None
    for attempt in range(3):
        try:
            return _request_text(url)
        except (OSError, ValueError) as exc:
            last_error = exc
            if attempt < 2:
                explicit = known_failure(exc)
                if explicit and explicit.failure["code"] == "website_connection":
                    progress("metadata", step=f"连接 {explicit.failure['website']} 失败，正在重试（{attempt + 1}/2）")
                time.sleep(attempt + 1)
    raise ValueError(f"元数据请求失败: {url}: {last_error}") from last_error


def geo_soft(accession: str) -> str:
    params = urllib.parse.urlencode({"acc": accession, "targ": "self", "form": "text", "view": "full"})
    payload = _metadata_request("https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?" + params)
    entity = "SERIES" if accession.startswith("GSE") else "SAMPLE"
    if not re.search(r"^\^" + entity + r"\s*=\s*" + re.escape(accession) + r"\s*$", payload, re.MULTILINE):
        raise ValueError(f"{accession}: GEO 未返回该编号的公开 SOFT 元数据。")
    return payload


def _ncbi_row_to_ena(row: dict) -> dict:
    """Use one iSeq-compatible schema even when an experiment needed NCBI."""
    names = {"run_accession": "Run", "experiment_accession": "Experiment", "study_accession": "SRAStudy",
             "sample_accession": "BioSample", "secondary_sample_accession": "Sample",
             "library_layout": "LibraryLayout", "library_strategy": "LibraryStrategy",
             "library_source": "LibrarySource", "library_selection": "LibrarySelection",
             "scientific_name": "ScientificName", "tax_id": "TaxID", "instrument_platform": "Platform",
             "instrument_model": "Model", "read_count": "spots", "base_count": "bases"}
    return {field: str(row.get(names.get(field, ""), "")) for field in ENA_COLUMNS}


def experiment_metadata(experiment: str) -> dict[str, dict]:
    params = urllib.parse.urlencode({"accession": experiment, "result": "read_run", "fields": ENA_FIELDS,
                                    "format": "tsv", "limit": 0})
    ena_error = None
    try:
        reader = csv.DictReader(io.StringIO(_metadata_request(
            "https://www.ebi.ac.uk/ena/portal/api/filereport?" + params)), delimiter="\t")
        rows = list(reader)
        if not rows or "run_accession" not in (reader.fieldnames or []):
            raise ValueError("ENA 未返回 Run 元数据。")
        if any(None in row or any(value is None for value in row.values()) for row in rows):
            raise ValueError("ENA Run 元数据表格不完整。")
    except (ValueError, OSError) as exc:
        ena_error = exc
        try:
            _, ncbi_rows = fetch_ncbi_runinfo(experiment)
            rows = [_ncbi_row_to_ena(row) for row in ncbi_rows]
        except Exception as ncbi_error:
            raise ValueError(f"{experiment}: 无法获取公开 Run。ENA: {ena_error}；NCBI: {ncbi_error}") from ncbi_error
    records = {}
    for row in rows:
        run = row.get("run_accession", "").strip()
        if not RUN_PATTERN.fullmatch(run) or row.get("experiment_accession", "").strip() != experiment:
            raise ValueError(f"{experiment}: 返回的 Run {run!r} 不属于请求的实验，拒绝扩大 GEO 范围。")
        strategy = row.get("library_strategy", "").strip().upper()
        if strategy and strategy not in {"RNA-SEQ", "RNA_SEQ"}:
            raise ValueError(f"{experiment}/{run}: 当前 bulk 模板需要 RNA-Seq，实际为 {strategy}。")
        layout = str(row.get("library_layout", row.get("LibraryLayout", ""))).strip().upper()
        record = {"row": row, "files": fastq_files(row, run), "library_layout": layout}
        if run in records and record != records[run]:
            raise ValueError(f"{experiment}: 同一 Run 元数据相互冲突: {run}")
        records[run] = record
    if not records:
        raise ValueError(f"{experiment}: 没有公开的 Run。")
    verify_run_set(experiment, records)
    return records


def resolve_geo_series(accession: str) -> dict:
    """Metadata only: Series' own GSMs -> explicit SRX relations -> all Runs.

    A Series' parent, SubSeries and BioProject relations never expand the scope.
    No FASTQ is downloaded by this public helper.
    """
    accession = normalize_accession(accession)
    if not re.fullmatch(r"GSE\d+", accession):
        raise ValueError("GEO 项目解析需要 GSE 编号。")
    series = geo_soft(accession)
    samples = list(dict.fromkeys(re.findall(r"^!Series_sample_id\s*=\s*(GSM\d+)\s*$", series, re.MULTILINE)))
    if not samples:
        raise ValueError(f"{accession}: Series 没有公开的 GSM 样本；请提供含样本的 GEO Series 编号。")
    records, mapping, cache = {}, [], {}
    for index, gsm in enumerate(samples, 1):
        progress("metadata", step=f"解析样本 {index}/{len(samples)}", current_sample=gsm,
                 resolved_samples=index - 1, total_samples=len(samples))
        time.sleep(0.35)
        sample = geo_soft(gsm)
        strategy = re.findall(r"^!Sample_library_strategy\s*=\s*(.+)$", sample, re.MULTILINE)
        if strategy and any(value.strip().upper() not in {"RNA-SEQ", "RNA_SEQ"} for value in strategy):
            raise ValueError(f"{accession}/{gsm}: 当前 bulk 模板需要 RNA-Seq，实际为 {', '.join(strategy).strip()}。")
        relations = re.findall(r"^!Sample_relation\s*=\s*SRA:\s*(.+)$", sample, re.MULTILINE)
        experiments = list(dict.fromkeys(re.findall(r"\b[SED]RX\d+\b", "\n".join(relations))))
        if not experiments:
            raise ValueError(f"{accession}/{gsm}: 没有明确的公开 SRA 实验关联，无法自动获取 FASTQ。")
        for experiment in experiments:
            if experiment not in cache:
                cache[experiment] = experiment_metadata(experiment)
            for run, record in cache[experiment].items():
                if run not in records:
                    records[run] = {**record, "geo_samples": [], "experiments": []}
                elif records[run]["row"] != record["row"]:
                    raise ValueError(f"{accession}: 不同 GSM 返回相互冲突的 {run} 元数据。")
                if gsm not in records[run]["geo_samples"]:
                    records[run]["geo_samples"].append(gsm)
                if experiment not in records[run]["experiments"]:
                    records[run]["experiments"].append(experiment)
                mapping.append({"gse": accession, "gsm": gsm, "experiment": experiment, "run": run})
    return {"accession": accession, "gsm_accessions": samples, "mapping": mapping,
            "fields": ENA_COLUMNS, "records": records}


def write_geo_mapping(path: Path, mapping: list[dict]):
    temporary = path.with_name(path.name + ".tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["gse", "gsm", "experiment", "run"],
                                    delimiter="\t", lineterminator="\n")
            writer.writeheader()
            writer.writerows(mapping)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def verify_run_set(accession: str, records: dict) -> dict:
    """A valid TSV can still be truncated between rows; compare independently."""
    actual = set(records)
    if RUN_PATTERN.fullmatch(accession):
        if actual != {accession}:
            raise RunSetMismatch(f"Run 查询返回了其他编号，拒绝继续下载: {accession}")
        return {"version": 1, "source": "input_run", "count": 1}
    source, expected, ena_error = "ena", None, None
    try:
        if re.fullmatch(r"[SED]RA\d+", accession):
            params = urllib.parse.urlencode({"result": "read_run", "query": f'submission_accession="{accession}"',
                                            "fields": "run_accession", "format": "tsv", "limit": 0})
            url = "https://www.ebi.ac.uk/ena/portal/api/search?" + params
        else:
            params = urllib.parse.urlencode({"accession": accession, "result": "read_run",
                                            "fields": "run_accession", "format": "tsv", "limit": 0})
            url = "https://www.ebi.ac.uk/ena/portal/api/filereport?" + params
        reader = csv.DictReader(io.StringIO(_request_text(url)), delimiter="\t")
        if not reader.fieldnames or "run_accession" not in reader.fieldnames:
            raise ValueError("ENA 没有返回 Run 清单列。")
        rows = list(reader)
        if not rows or any(None in row or not RUN_PATTERN.fullmatch(row.get("run_accession", "")) for row in rows):
            raise ValueError("ENA Run 清单为空或不完整。")
        expected = {row["run_accession"] for row in rows}
    except Exception as exc:
        ena_error = str(exc)
    if expected is None:
        source = "ncbi"
        try:
            _, rows = fetch_ncbi_runinfo(accession)
            expected = {row["Run"] for row in rows}
            if not expected or any(not RUN_PATTERN.fullmatch(run) for run in expected):
                raise ValueError("NCBI RunInfo 包含无效 Run 编号。")
        except Exception as exc:
            raise ValueError(f"{accession}: 无法独立确认全部 Run，拒绝用未核实的清单启动分析。"
                             f"ENA: {ena_error}；NCBI: {exc}") from exc
    if actual != expected:
        missing, unexpected = sorted(expected - actual), sorted(actual - expected)
        raise RunSetMismatch(f"{accession}: 元数据 Run 清单不完整或范围错误。"
                         f"独立查询 {len(expected)} 个，iSeq 元数据 {len(actual)} 个；"
                         f"缺失 {len(missing)} 个（示例 {missing[:5]}）；"
                         f"额外 {len(unexpected)} 个（示例 {unexpected[:5]}）。请重试获取元数据。")
    return {"version": 1, "source": source, "count": len(expected)}


def resolve_submission(accession: str, path: Path, handle=None):
    """SRA/ERA/DRA submissions are resolved before iSeq sees individual Runs."""
    params = urllib.parse.urlencode({"result": "read_run", "query": f'submission_accession="{accession}"',
                                    "fields": ENA_FIELDS, "format": "tsv", "limit": 0})
    ena_error = None
    try:
        payload = _request_text("https://www.ebi.ac.uk/ena/portal/api/search?" + params)
        rows = list(csv.DictReader(io.StringIO(payload), delimiter="\t"))
        if rows and "run_accession" in rows[0]:
            write_metadata(path, list(rows[0]), rows)
            return
    except Exception as exc:
        ena_error = str(exc)
    log_message(f"{accession}: ENA 未返回 Run，尝试 NCBI。", handle)
    try:
        fieldnames, all_rows = fetch_ncbi_runinfo(accession)
        write_metadata(path, fieldnames, all_rows)
    except Exception as exc:
        raise ValueError(f"{accession}: 无法解析公开 Run 清单。ENA: {ena_error or '无记录'}；NCBI: {exc}") from exc


def logged_runs(path: Path) -> set[str]:
    if not path.is_file():
        return set()
    runs = set()
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        words = line.split()
        if words and RUN_PATTERN.fullmatch(words[-1]):
            runs.add(words[-1])
    return runs


def remove_success_marker(path: Path, run: str):
    if not path.is_file():
        return
    # iSeq uses a substring grep. Strip even longer accessions containing this
    # Run while redownloading, so a missing pair cannot be falsely skipped.
    lines = [line for line in path.read_text(encoding="utf-8", errors="replace").splitlines()
             if run not in line]
    temporary = path.with_name(path.name + ".teflow.tmp")
    temporary.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    os.replace(temporary, path)


def validate_fastq(path: Path, expected: dict):
    if not path.is_file() or path.stat().st_size <= 0:
        raise ValueError(f"FASTQ 文件缺失或为空: {path}")
    if expected["bytes"] is not None and path.stat().st_size != expected["bytes"]:
        raise KnownFailure("fastq_integrity", "下载文件未通过完整性校验，自动重试后仍未成功。",
                           detail=f"FASTQ 下载尚未完成，文件大小不符: {path}")
    try:
        with gzip.open(path, "rb") as stream:
            header, sequence, separator, quality = [stream.readline() for _ in range(4)]
            if not header.startswith(b"@") or not separator.startswith(b"+") or not sequence.strip():
                raise ValueError("首条 FASTQ 记录格式错误")
            if len(sequence.rstrip(b"\r\n")) != len(quality.rstrip(b"\r\n")):
                raise ValueError("首条 FASTQ 记录的序列和质量长度不一致")
            # Converted SRA has no expected compressed length / ENA MD5. Read to
            # gzip EOF once to verify complete conversion, without extra hashes.
            if expected["source"] == "sra-converted":
                while stream.read(1024 * 1024):
                    pass
    except (gzip.BadGzipFile, EOFError, zlib.error, ValueError) as exc:
        raise KnownFailure("fastq_integrity", "下载文件未通过完整性校验，自动重试后仍未成功。",
                           detail=f"FASTQ 内容无效: {path}: {exc}") from exc


def pair_is_complete(samples_dir: Path, files: list[dict]) -> bool:
    try:
        for item in files:
            validate_fastq(samples_dir / item["name"], item)
    except ValueError:
        return False
    return True


def check_space(samples_dir: Path, output_dir: Path, records: dict, multiplier: float):
    if multiplier < 1:
        raise ValueError("OUTPUT_SPACE_MULTIPLIER 必须至少为 1。")
    known_bytes = sum(item["bytes"] or 0 for record in records.values() for item in record["files"])
    missing_bytes = sum(max(0, (item["bytes"] or 0) -
                            ((Path(record.get("raw_dir", samples_dir)) / item["name"]).stat().st_size
                             if (Path(record.get("raw_dir", samples_dir)) / item["name"]).is_file() else 0))
                        for record in records.values() for item in record["files"])
    reserve = 1024 ** 3
    raw_free, out_free = shutil.disk_usage(samples_dir).free, shutil.disk_usage(output_dir).free
    output_estimate = int(known_bytes * multiplier)
    same_device = samples_dir.stat().st_dev == output_dir.stat().st_dev
    raw_required = missing_bytes + reserve + (output_estimate if same_device else 0)
    if raw_free < raw_required:
        raise ValueError(f"原始数据盘空间不足: 需要约 {raw_required / 1024**3:.2f} GiB，"
                         f"可用 {raw_free / 1024**3:.2f} GiB。")
    if not same_device and out_free < output_estimate + reserve:
        raise ValueError(f"结果盘空间不足: FASTQ 和 BAM 中间文件估算需要约 "
                         f"{(output_estimate + reserve) / 1024**3:.2f} GiB，"
                         f"可用 {out_free / 1024**3:.2f} GiB。")
    return known_bytes


def run_iseq(command: list[str], samples_dir: Path, handle, timeout=None):
    handle.write("\n$ " + " ".join(command) + "\n")
    handle.flush()
    invocation_start = Path(handle.name).stat().st_size
    process = subprocess.Popen(command, cwd=samples_dir, stdout=handle, stderr=subprocess.STDOUT,
                               start_new_session=True)

    def interrupted(signum, frame):
        raise InterruptedError(f"iSeq 被信号 {signum} 中断。")

    previous_term = signal.signal(signal.SIGTERM, interrupted)
    try:
        try:
            returncode = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(f"iSeq 元数据请求超时（{timeout}s）；详情见 {handle.name}。") from exc
        if returncode:
            # Only an explicit diagnostic in this invocation proves integrity
            # failure. Old attempts and generic nonzero exits cannot classify it.
            with Path(handle.name).open("rb") as source:
                source.seek(max(invocation_start, source.seek(0, os.SEEK_END) - 65536))
                recent = source.read().decode("utf-8", errors="replace")
            if re.search(r"(?im)^[SED]RR\d+ md5 check failed after trying \d+ times[^\r\n]*\r?$", recent):
                run = command[command.index("-i") + 1] if "-i" in command else ""
                scope = {"run": run} if RUN_PATTERN.fullmatch(run) else {}
                raise KnownFailure("fastq_integrity", "下载文件未通过完整性校验，自动重试后仍未成功。",
                                   detail=f"iSeq 退出码 {returncode}；本次下载明确报告 MD5 校验失败；详情见 {handle.name}。", **scope)
            raise RuntimeError(f"iSeq 退出码 {returncode}；详情见 {handle.name}。")
    finally:
        # iSeq is Bash and launches wget/fasterq-dump/pigz children. Killing Bash
        # alone leaves writers alive, so terminate its complete process group.
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=5)
        signal.signal(signal.SIGTERM, previous_term)


def metadata_completion_marker(samples_dir: Path, accession: str) -> Path:
    return samples_dir / f".teflow-{accession}.metadata.complete.json"


def obtain_metadata(accession: str, samples_dir: Path, iseq_bin: str,
                    metadata_timeout: int, log) -> tuple[Path, list[str], dict]:
    metadata = samples_dir / f"{accession}.metadata.tsv"
    marker = metadata_completion_marker(samples_dir, accession)
    valid_cache = False
    if metadata.is_file() and marker.is_file():
        try:
            completed = json.loads(marker.read_text(encoding="utf-8"))
            fields, records = parse_metadata(metadata)
            valid_cache = (completed.get("accession") == accession and
                           completed.get("bytes") == metadata.stat().st_size and
                           completed.get("runs") == list(records))
        except (ValueError, OSError):
            valid_cache = False
    if valid_cache:
        if completed.get("run_set_verification", {}).get("version") != 1:
            try:
                completed["run_set_verification"] = verify_run_set(accession, records)
            except RunSetMismatch as exc:
                valid_cache = False
                log_message(f"旧元数据缓存范围不完整，重新获取: {exc}", log)
            else:
                atomic_json(marker, completed)
        if valid_cache:
            return metadata, fields, records
    log_message(f"获取 {accession} 的全部 Run 元数据。", log)
    if metadata.is_file():
        os.replace(metadata, metadata.with_name(metadata.name + ".incomplete"))
    marker.unlink(missing_ok=True)
    staging = samples_dir / ".teflow-metadata" / accession
    staging.mkdir(parents=True, exist_ok=True)
    staged_metadata = staging / metadata.name
    staged_metadata.unlink(missing_ok=True)
    try:
        if re.fullmatch(r"[SED]RA\d+", accession):
            resolve_submission(accession, staged_metadata, log)
        else:
            run_iseq([iseq_bin, "-i", accession, "-m", "-r", "https", "-o", str(staging)],
                     staging, log, timeout=metadata_timeout)
        fields, records = parse_metadata(staged_metadata)
        verification = verify_run_set(accession, records)
        fields = canonical_metadata_fields(fields)
        write_metadata(metadata, fields, (record["row"] for record in records.values()))
        atomic_json(marker, {"accession": accession, "bytes": metadata.stat().st_size,
                             "runs": list(records), "run_set_verification": verification,
                             "completed_at": datetime.now(timezone.utc).isoformat()})
        return metadata, fields, records
    finally:
        # Only metadata under our scratch namespace is removed. Nothing is
        # published to the reusable cache until the request has fully succeeded.
        staged_metadata.unlink(missing_ok=True)


def obtain_geo_metadata(accession: str, samples_dir: Path, log):
    metadata = samples_dir / f"{accession}.metadata.tsv"
    cache_path = samples_dir / f".teflow-{accession}.geo.complete.json"
    if metadata.is_file() and cache_path.is_file():
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            _, parsed = parse_metadata(metadata, allow_duplicate_names=True)
            if (cached.get("version") == 1 and cached.get("accession") == accession and
                    cached.get("metadata_bytes") == metadata.stat().st_size and
                    list(parsed) == list(cached["records"]) and cached.get("mapping") and
                    all(parsed[run]["row"] == cached["records"][run]["row"] for run in parsed)):
                # Older caches can omit derived layout/file fields. Recompute
                # these from the verified TSV while retaining GEO associations.
                records = {run: {**cached["records"][run], **record} for run, record in parsed.items()}
                return metadata, cached["fields"], records, cached["mapping"]
        except (OSError, ValueError, KeyError, TypeError):
            pass
    log_message(f"解析 {accession} 自身的 GSM 样本及对应全部 Run。", log)
    resolved = resolve_geo_series(accession)
    write_metadata(metadata, resolved["fields"], (record["row"] for record in resolved["records"].values()))
    # Canonical TSV pads fields absent in an ENA response. Cache that same schema.
    _, parsed = parse_metadata(metadata, allow_duplicate_names=True)
    for run, record in resolved["records"].items():
        record["row"] = parsed[run]["row"]
    atomic_json(cache_path, {"version": 1, **resolved, "metadata_bytes": metadata.stat().st_size})
    return metadata, resolved["fields"], resolved["records"], resolved["mapping"]


@contextmanager
def download_lock(samples_dir: Path):
    # On NFS this is a file lock supported by the mounted Linux client. Keep the
    # inode in place after release so future holders cannot acquire another inode.
    import fcntl
    with (samples_dir / ".teflow-download.lock").open("a") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("该编号已有下载任务运行，请等待当前任务完成。") from exc
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


ENA_PROBE = "https://www.ebi.ac.uk/ena/portal/api/"
DOWNLOAD_SOURCES = {"ena", "aws"}


METADATA_TRIES = 3
METADATA_RETRY_SECONDS = 30


def with_network(action, network_wait: float, log, probe=ENA_PROBE, rounds=20, sleep=time.sleep):
    """Run a metadata action robustly when network waiting is enabled (network_wait > 0).

    - The network is down: wait for it (up to network_wait), then try again.
    - The network works but the answer is incomplete or names other Runs (ENA timed out
      and iSeq fell back to a broader NCBI search): try again after a pause, at most
      METADATA_TRIES times; RunSetMismatch is still raised if it persists.
    With network_wait=0 (the default) errors propagate unchanged, as before 2.7.
    """
    failures = 0
    for _ in range(rounds):
        try:
            return action()
        except Exception as exc:
            if network_wait <= 0:
                raise
            if aws_download.network_ok(probe):
                failures += 1
                if failures >= METADATA_TRIES or not isinstance(exc, (MetadataFormatError, FileNotFoundError)):
                    raise
                log_message(f"元数据不完整或范围不符（{exc}），{METADATA_RETRY_SECONDS * failures} 秒后重试"
                            f"（{failures}/{METADATA_TRIES - 1}）。", log)
                sleep(METADATA_RETRY_SECONDS * failures)
                continue
            log_message(f"网络中断（{type(exc).__name__}: {exc}），等待恢复后重试。", log)
            progress("metadata", step="等待网络恢复")
            aws_download.wait_for_network(network_wait, lambda text: log_message(text, log), probe=probe)
    return action()


def converted_files(run: str, layout: str) -> list[dict]:
    if layout == "SINGLE":
        return [{"name": f"{run}.fastq.gz", "bytes": None, "mate": 1, "source": "sra-converted"}]
    return [{"name": f"{run}_{mate}.fastq.gz", "bytes": None, "mate": mate, "source": "sra-converted"}
            for mate in (1, 2)]


def ena_links(row: dict) -> dict[str, tuple[str, str]]:
    """FASTQ file name -> (HTTPS URL, MD5) from the ENA metadata row."""
    links = [link.strip() for link in str(row.get("fastq_ftp", "")).split(";") if link.strip()]
    md5s = [value.strip() for value in str(row.get("fastq_md5", "")).split(";")]
    result = {}
    for i, link in enumerate(links):
        url = link if "://" in link else "https://" + link
        result[_safe_fastq_name(url)] = (url, md5s[i] if i < len(md5s) and len(links) == len(md5s) else "")
    return result


def file_md5(path: Path) -> str:
    digest = hashlib.md5()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def download_run_ena_parallel(run: str, record: dict, run_dir: Path, files: list[dict], connections: int,
                              retries: int, network_wait: float, log, report=None) -> bool:
    """ENA .fastq.gz over HTTPS with several connections per file; size + MD5 + gzip checked.

    Returns False when ENA has no FASTQ links for this Run (the caller then uses iSeq).
    """
    links = ena_links(record["row"])
    if not files or any(item["source"] != "ena-fastq" or item["name"] not in links for item in files):
        return False
    attempts, outages = 0, 0

    def say(text):
        log_message(text, log)

    while True:
        try:
            for item in files:
                target = run_dir / item["name"]
                url, md5 = links[item["name"]]
                if network_wait > 0:
                    aws_download.wait_for_network(network_wait, say, probe=ENA_PROBE)
                say(f"{run}: ENA 下载 {item['name']} {item['bytes'] / 1024**3:.2f} GiB，{connections} 个连接。")
                aws_download.parallel_download(url, target, item["bytes"], connections, report=report)
                if md5 and file_md5(target) != md5.lower():
                    target.unlink(missing_ok=True)
                    raise ValueError(f"{item['name']}: MD5 与 ENA 元数据不符。")
                validate_fastq(target, item)
            with (run_dir / "success.log").open("a", encoding="utf-8") as handle:
                handle.write(f"ena-parallel {run}\n")
            return True
        except aws_download.NetworkDown as exc:
            outages += 1
            if network_wait <= 0 or outages > 50:
                raise RuntimeError(f"{run}: 下载中断：{exc}") from exc
            say(f"{run}: {exc}；等待网络后续传。")
            aws_download.wait_for_network(network_wait, say, probe=ENA_PROBE)
        except (OSError, ValueError) as exc:
            if network_wait > 0 and not aws_download.network_ok(ENA_PROBE):
                outages += 1
                if outages > 50:
                    raise RuntimeError(f"{run}: 网络反复中断：{exc}") from exc
                continue
            attempts += 1
            say(f"{run}: ENA 多连接下载失败（{attempts}/{retries}）：{exc}")
            if attempts >= retries:
                raise RuntimeError(f"{run}: {retries} 次 ENA 下载后仍未完成；已下载部分保留，可重试。{exc}") from exc


def download_run_aws(run: str, record: dict, run_dir: Path, files: list[dict], threads: int,
                     connections: int, retries: int, network_wait: float, log, report=None) -> None:
    """AWS SRA object -> multi-connection download -> fasterq-dump -> validated FASTQ.gz.

    Raises aws_download.NotOnAws when the bucket has no such run (the caller falls back
    to ENA). Network outages wait (up to network_wait each) without using attempts.
    """
    url = aws_download.aws_url(run)
    sra = run_dir / f"{run}.sra"
    read_count = str(record["row"].get("read_count", "")).strip()
    expected_spots = int(read_count) if read_count.isdigit() and int(read_count) > 0 else None
    attempts, outages = 0, 0

    def say(text):
        log_message(text, log)

    while True:
        try:
            if network_wait > 0:
                aws_download.wait_for_network(network_wait, say)
            size = aws_download.remote_size(url)
            say(f"{run}: AWS 下载 {size / 1024**3:.2f} GiB，{connections} 个连接。")
            aws_download.parallel_download(url, sra, size, connections, report=report)
            progress("download", current_sample=run, step="转换 FASTQ")
            aws_download.convert_sra(sra, run_dir, run, record["library_layout"], threads, log,
                                     expected_spots=expected_spots)
            progress("download", current_sample=run, step="校验 FASTQ")
            for item in files:
                validate_fastq(run_dir / item["name"], item)
            sra.unlink(missing_ok=True)
            with (run_dir / "success.log").open("a", encoding="utf-8") as handle:
                handle.write(f"aws-sra {run}\n")
            return
        except aws_download.NotOnAws:
            raise
        except aws_download.NetworkDown as exc:
            outages += 1
            if network_wait <= 0 or outages > 50:
                raise RuntimeError(f"{run}: 下载中断：{exc}") from exc
            say(f"{run}: {exc}；等待网络后续传。")
            aws_download.wait_for_network(network_wait, say)
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            if network_wait > 0 and not aws_download.network_ok():
                outages += 1
                if outages > 50:
                    raise RuntimeError(f"{run}: 网络反复中断：{exc}") from exc
                continue
            attempts += 1
            say(f"{run}: AWS 下载或转换失败（{attempts}/{retries}）：{exc}")
            if attempts >= retries:
                raise RuntimeError(f"{run}: {retries} 次 AWS 下载后仍未完成；已下载部分保留，可重试。{exc}") from exc
            if isinstance(exc, ValueError):
                # A complete but unusable archive is fetched again from scratch.
                sra.unlink(missing_ok=True)
                sra.with_name(sra.name + ".chunks.json").unlink(missing_ok=True)


def prepare(accession: str, samples_dir: Path, output_dir: Path, manifest_path: Path,
            log_path: Path, iseq_bin="iseq", threads=8, retries=3, metadata_timeout=180,
            output_space_multiplier=4, gsm="", runs=None, download_source="ena",
            connections=1, network_wait=0) -> dict:
    accession = normalize_accession(accession)
    runs = [normalize_accession(run) for run in (runs or [])]
    pooled = bool(runs)
    if pooled and not re.fullmatch(r"GSM\d+", accession):
        raise ValueError("合并模式的任务编号必须是 GSM。")
    if pooled and not all(RUN_PATTERN.fullmatch(run) for run in runs):
        raise ValueError(f"{accession}: Run 列表无效。")
    if threads < 1 or retries < 1 or metadata_timeout < 1:
        raise ValueError("下载线程数、重试次数和元数据超时必须大于零。")
    if download_source not in DOWNLOAD_SOURCES:
        raise ValueError(f"下载来源必须是 {'、'.join(sorted(DOWNLOAD_SOURCES))} 之一。")
    connections, network_wait = int(connections), float(network_wait)
    if connections < 1 or network_wait < 0:
        raise ValueError("下载连接数必须大于零，网络等待时间不能为负。")
    if (download_source == "aws" or network_wait > 0) and aws_download is None:
        raise RuntimeError("找不到 aws_download 模块。")
    if not shutil.which(iseq_bin):
        raise RuntimeError(f"当前环境找不到 iSeq: {iseq_bin}")
    samples_dir, output_dir = Path(samples_dir).resolve(), Path(output_dir).resolve()
    samples_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = Path(log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path = Path(manifest_path)
    with download_lock(samples_dir), log_path.open("a", encoding="utf-8", buffering=1) as log:
        progress("metadata", step="获取项目元数据", total_runs=0, downloaded_runs=0)
        geo = accession.startswith("GSE")
        mapping = []
        if pooled:
            # One GSM, several Runs: each Run keeps its own raw directory and metadata
            # cache; the workflow concatenates their FASTQs into a single sample.
            records, fields = {}, None
            for run in runs:
                run_raw = samples_dir / run
                run_raw.mkdir(parents=True, exist_ok=True)
                metadata, run_fields, run_records = with_network(
                    lambda: obtain_metadata(run, run_raw, iseq_bin, metadata_timeout, log), network_wait, log)
                if list(run_records) != [run]:
                    raise ValueError(f"{accession}/{run}: 元数据返回的 Run 与清单不一致：{list(run_records)[:5]}")
                record = run_records[run]
                record.update(geo_samples=[accession], fields=run_fields, metadata_path=str(metadata))
                records[run] = record
                fields = fields or run_fields
                mapping.append({"gse": "", "gsm": accession, "run": run,
                                "experiment": record["row"].get("experiment_accession", "")})
            layouts = {record["library_layout"] for record in records.values()}
            if len(layouts) != 1:
                detail = "，".join(f"{run}={record['library_layout']}" for run, record in records.items())
                raise ValueError(f"{accession}: Run 的单双端类型不一致（{detail}），无法合并为一个样本。")
            write_geo_mapping(output_dir / "samples.tsv", mapping)
        elif geo:
            metadata, fields, records, mapping = with_network(
                lambda: obtain_geo_metadata(accession, samples_dir, log), network_wait, log)
            write_geo_mapping(output_dir / "samples.tsv", mapping)
        else:
            metadata, fields, records = with_network(
                lambda: obtain_metadata(accession, samples_dir, iseq_bin, metadata_timeout, log), network_wait, log)
        if not geo and not pooled:
            for run, record in records.items():
                row = record["row"]
                aliases = [v for v in (gsm, accession, row.get("sample_alias"), row.get("experiment_alias"))
                           if v and re.fullmatch(r"GSM\d+", v)]
                record["geo_samples"] = list(dict.fromkeys(aliases))
                for sample_id in record["geo_samples"]:
                    mapping.append({"gse": "", "gsm": sample_id, "run": run,
                                    "experiment": row.get("experiment_accession", "")})
            if mapping:
                write_geo_mapping(output_dir / "samples.tsv", mapping)
        for run, record in records.items():
            raw_dir = samples_dir / run if geo or pooled else samples_dir
            raw_dir.mkdir(parents=True, exist_ok=True)
            record["raw_dir"] = str(raw_dir)
            if download_source == "aws":
                # AWS objects are SRA archives; FASTQ names follow the conversion.
                record["ena_files"] = record["files"]
                record["files"] = converted_files(run, record["library_layout"])
        raw_bytes = check_space(samples_dir, output_dir, records, float(output_space_multiplier))
        single_runs = sum(record["library_layout"] == "SINGLE" for record in records.values())
        paired_runs = len(records) - single_runs
        log_message(f"共 {len(records)} 个 Run（单端 {single_runs}、双端 {paired_runs}）；全部纳入，"
                    f"FASTQ 大小 {raw_bytes / 1024**3:.2f} GiB。", log)
        samples = {}
        progress("download", total_runs=len(records), downloaded_runs=0, step="准备下载")
        for index, (run, record) in enumerate(records.items(), 1):
            files = record["files"]
            run_dir = Path(record["raw_dir"])
            success_log = run_dir / "success.log"
            progress("download", total_runs=len(records), downloaded_runs=index - 1,
                     current_sample=run, step="下载 FASTQ")
            # Use the already obtained per-Run row. Its complete column order is
            # retained because iSeq also has positional fields in its output code.
            run_metadata = run_dir / f"{run}.metadata.tsv"
            if run_metadata != Path(record.get("metadata_path", metadata)):
                write_metadata(run_metadata, record.get("fields", fields), [record["row"]])
            status = "reused"
            already_complete = run in logged_runs(success_log) and pair_is_complete(run_dir, files)
            if (not already_complete and record.get("ena_files") and run in logged_runs(success_log)
                    and pair_is_complete(run_dir, record["ena_files"])):
                files = record["files"] = record["ena_files"]  # finished earlier from ENA
                already_complete = True
            if not already_complete and download_source == "aws":
                remove_success_marker(success_log, run)

                def report(done, total, speed, run=run, index=index):
                    progress("download", total_runs=len(records), downloaded_runs=index - 1, current_sample=run,
                             step=f"AWS 下载 {done / total:.0%}（{speed / 1024**2:.2f} MB/s）")
                try:
                    download_run_aws(run, record, run_dir, files, threads, connections, retries,
                                     network_wait, log, report)
                    already_complete, status = True, "downloaded"
                except aws_download.NotOnAws as exc:
                    log_message(f"{exc}；改用 ENA。", log)
                    files = record["files"] = record["ena_files"]
                    if connections > 1 and download_run_ena_parallel(run, record, run_dir, files, connections,
                                                                     retries, network_wait, log, report):
                        already_complete, status = True, "downloaded"
            if already_complete:
                log_message(f"Run {index}/{len(records)} {run}: "
                            f"{'复用已完成 FASTQ' if status == 'reused' else '下载完成'}。", log)
            else:
                status, last_error = "downloaded", None
                for attempt in range(1, retries + 1):
                    remove_success_marker(success_log, run)
                    if files[0]["source"] == "sra-converted" and not pair_is_complete(run_dir, files):
                        # A partial SRA conversion prevents iSeq from converting
                        # again. Remove only the explicitly expected derived FASTQs.
                        for item in files:
                            (run_dir / item["name"]).unlink(missing_ok=True)
                    log_message(f"Run {index}/{len(records)} {run}: iSeq 下载（{attempt}/{retries}）。", log)
                    try:
                        command = [iseq_bin, "-i", run, "-g", "-r", "https", "-t", str(threads),
                                   "-o", str(run_dir)]
                        if files[0]["source"] == "sra-converted":
                            command.extend(["-d", "sra"])
                        run_iseq(command, run_dir, log)
                        progress("download", total_runs=len(records), downloaded_runs=index - 1,
                                 current_sample=run, step="校验 FASTQ")
                        if run not in logged_runs(success_log):
                            raise RuntimeError(f"{run}: iSeq 未记录成功下载。")
                        for item in files:
                            validate_fastq(run_dir / item["name"], item)
                        last_error = None
                        break
                    except (ValueError, RuntimeError) as exc:
                        last_error = exc
                        log_message(str(exc), log)
                if last_error is not None:
                    explicit = known_failure(last_error)
                    if explicit:
                        explicit.failure.setdefault("run", run)
                    raise RuntimeError(f"{run}: {retries} 次下载后仍未完成；已下载数据保留，可重试。{last_error}") from last_error
            count_root = output_dir / run if geo else output_dir
            count_name = accession if pooled else run
            samples[run] = {"r1": str(run_dir / files[0]["name"]),
                            "r2": str(run_dir / files[1]["name"]) if record["library_layout"] == "PAIRED" else None,
                            "raw_dir": str(run_dir),
                            "count_path": str(count_root / "abundance" / f"{count_name}.cntTable"),
                            "geo_samples": record.get("geo_samples", []),
                            "experiments": record.get("experiments", []),
                            "files": [{"name": item["name"], "path": str(run_dir / item["name"]),
                                       "bytes": (run_dir / item["name"]).stat().st_size}
                                      for item in files],
                            "library_layout": record["library_layout"], "download_status": status}
            progress("download", total_runs=len(records), downloaded_runs=index,
                     current_sample=run, step="FASTQ 已就绪")
        document = {"accession": accession, "samples": samples, "total_runs": len(samples),
                    "raw_bytes": sum(item["bytes"] for sample in samples.values() for item in sample["files"]),
                    "prepared_at": datetime.now(timezone.utc).isoformat(),
                    "metadata": str(metadata), "layout": "geo_nested" if geo else "pooled" if pooled else "flat",
                    "mapping": mapping, "mapping_path": str(output_dir / "samples.tsv") if mapping else None}
        if pooled:
            document["pooled"] = {"sample": accession, "runs": list(samples),
                                  "library_layout": next(iter(samples.values()))["library_layout"]}
        atomic_json(manifest_path, document)
        log_message(f"下载准备完成: {len(samples)} 个 Run。", log)
        return document


def run_preparation(*args, **kwargs):
    """Publish explanations only when a known error actually escapes all retries."""
    try:
        return prepare(*args, **kwargs)
    except Exception as exc:
        explicit = known_failure(exc)
        if explicit:
            print("[TEFLOW_FAILURE] " + json.dumps({**explicit.failure, "detail": explicit.detail}, ensure_ascii=False), flush=True)
        raise


if "snakemake" in globals():
    if aws_download is None:
        sys.path.insert(0, snakemake.params.script_dir)
        import aws_download
    run_preparation(snakemake.params.accession, Path(snakemake.params.samples_dir),
                    Path(snakemake.params.output_dir), Path(snakemake.output[0]), Path(snakemake.log[0]),
                    iseq_bin=snakemake.params.iseq_bin, threads=int(snakemake.threads),
                    retries=int(snakemake.params.retries), metadata_timeout=int(snakemake.params.metadata_timeout),
                    output_space_multiplier=float(snakemake.params.output_space_multiplier), gsm=snakemake.params.gsm,
                    runs=list(snakemake.params.runs), download_source=snakemake.params.download_source,
                    connections=int(snakemake.params.connections), network_wait=float(snakemake.params.network_wait))
