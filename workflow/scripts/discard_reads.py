"""Remove this Run's (or pooled GSM's) reads only after its count table can be parsed."""
import json
import re
import sys
from pathlib import Path


def discard_pooled(gsm, accession, raw_root, output_root, manifest, table, receipt):
    """A pooled GSM: every member Run's raw FASTQs, the pooled FASTQs, cleaned reads and the BAM."""
    from merge_counts import read_count_table

    pooled = manifest.get("pooled") or {}
    if gsm != accession or manifest["accession"] != accession or pooled.get("sample") != gsm:
        raise ValueError("GSM 与样本清单不匹配。")
    if table != output_root / "abundance" / f"{gsm}.cntTable":
        raise ValueError(f"{gsm} 计数表路径与工作目录不一致。")
    read_count_table(table)
    paths = []
    for run in pooled["runs"]:
        sample = manifest["samples"][run]
        for key in ("r1", "r2"):
            if sample.get(key):
                path = Path(sample[key])
                if path.parent != raw_root / run or not path.name.lower().endswith((".fastq.gz", ".fq.gz")):
                    raise ValueError(f"原始 FASTQ 路径不属于 {run}: {path}")
                paths.append((path, raw_root))
    for name in (f"{gsm}.fastq.gz", f"{gsm}_R1.fastq.gz", f"{gsm}_R2.fastq.gz"):
        paths.append((output_root / "pooled" / name, output_root))
    for mate in ("1", "2"):
        paths.append((output_root / "qc" / f"{gsm}_clean_R{mate}.fq.gz", output_root))
    paths.append((output_root / "align" / f"{gsm}Aligned.sortedByCoord.out.bam", output_root))
    for path, root in paths:
        if not path.parent.resolve().is_relative_to(root.resolve()):
            raise ValueError(f"文件目录超出任务路径: {path}")
    pending = receipt.with_suffix(".pending")
    pending.write_text(gsm + "\n", encoding="utf-8")
    for path, _ in paths:
        path.unlink(missing_ok=True)
    pending.replace(receipt)


def discard(run, accession, raw_root, output_root, manifest, table, receipt):
    from merge_counts import read_count_table

    if manifest.get("pooled"):
        return discard_pooled(run, accession, raw_root, output_root, manifest, table, receipt)
    if not re.fullmatch(r"[SED]RR\d+", run) or manifest["accession"] != accession:
        raise ValueError("Run 与样本清单不匹配。")
    nested = accession.startswith("GSE")
    raw = raw_root / run if nested else raw_root
    base = output_root / run if nested else output_root
    sample = manifest["samples"][run]
    if table != base / "abundance" / f"{run}.cntTable":
        raise ValueError(f"{run} 计数表路径与工作目录不一致。")
    read_count_table(table)
    paths = []
    for key in ("r1", "r2"):
        if sample.get(key):
            path = Path(sample[key])
            if path.parent != raw or not path.name.lower().endswith((".fastq.gz", ".fq.gz")):
                raise ValueError(f"原始 FASTQ 路径不属于 {run}: {path}")
            paths.append((path, raw_root))
            paths.append((base / "qc" / f"{run}_clean_R{key[1]}.fq.gz", output_root))
    paths.append((base / "align" / f"{run}Aligned.sortedByCoord.out.bam", output_root))
    for path, root in paths:
        if not path.parent.resolve().is_relative_to(root.resolve()):
            raise ValueError(f"文件目录超出任务路径: {path}")
    pending = receipt.with_suffix(".pending")
    pending.write_text(run + "\n", encoding="utf-8")
    for path, _ in paths:
        path.unlink(missing_ok=True)
    pending.replace(receipt)


if "snakemake" in globals():
    if snakemake.params.enabled is not True:
        raise ValueError("任务未启用 FASTQ / BAM 清理。")
    sys.path.insert(0, snakemake.params.script_dir)
    discard(snakemake.wildcards.sample, snakemake.params.accession,
            Path(snakemake.params.raw_root), Path(snakemake.params.output_root),
            json.loads(Path(snakemake.input.manifest).read_text(encoding="utf-8")),
            Path(snakemake.input.table), Path(snakemake.output.receipt))
