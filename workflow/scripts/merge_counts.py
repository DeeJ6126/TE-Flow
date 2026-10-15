"""Merge TEcount tables using the original template's inner-join semantics."""
import csv
import json
import os
from decimal import Decimal, InvalidOperation
from pathlib import Path


def read_count_table(path: Path) -> dict[str, str]:
    """Read one two-column TEcount .cntTable; reject silent data loss."""
    counts: dict[str, str] = {}
    with Path(path).open(encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle, delimiter="\t")
        header = next(reader, None)
        if header is None or len(header) != 2:
            raise ValueError(f"计数表表头无效: {path}")
        for number, row in enumerate(reader, 2):
            if len(row) != 2 or not row[0].strip():
                raise ValueError(f"计数表第 {number} 行格式无效: {path}")
            gene, count = row
            if gene in counts:
                raise ValueError(f"计数表有重复 gene/TE 标识 {gene}: {path}")
            try:
                numeric = Decimal(count)
            except InvalidOperation as exc:
                raise ValueError(f"计数表包含非数值 {count}: {path}") from exc
            if not numeric.is_finite() or numeric < 0:
                raise ValueError(f"计数表包含无效计数 {count}: {path}")
            counts[gene] = count
    if not counts:
        raise ValueError(f"计数表没有数据行: {path}")
    return counts


def merge_tables(paths, runs, output: Path) -> dict:
    paths, runs = list(paths), list(runs)
    if not runs or len(runs) != len(paths) or len(set(runs)) != len(runs):
        raise ValueError("合并样本清单为空、重复，或与计数表数量不一致。")
    for run, path in zip(runs, paths):
        if Path(path).name != f"{run}.cntTable":
            raise ValueError(f"计数表与 Run 对应错误: {run}: {path}")
    tables = [read_count_table(Path(path)) for path in paths]
    common = set(tables[0])
    for table in tables[1:]:
        common.intersection_update(table)
    if not common:
        raise ValueError("计数表之间没有共同 gene/TE 标识，无法生成合并表。")
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(output.name + ".tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
            writer.writerow(["gene", *runs])
            for gene in tables[0]:
                if gene in common:
                    writer.writerow([gene, *(table[gene] for table in tables)])
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)
    return {"samples": len(runs), "rows": len(common), "columns": ["gene", *runs],
            "join": "inner", "input_rows": {run: len(table) for run, table in zip(runs, tables)},
            "output": str(output)}


if "snakemake" in globals():
    manifest = json.loads(Path(snakemake.input.manifest).read_text(encoding="utf-8"))
    summary = merge_tables(snakemake.input.counts, manifest["samples"], Path(snakemake.output[0]))
    log = Path(snakemake.log[0])
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"[TEFLOW] 合并完成: {summary['samples']} 个 Run，{summary['rows']} 个 gene/TE，{summary['output']}", flush=True)
