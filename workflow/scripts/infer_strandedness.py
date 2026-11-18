"""Infer library strandedness from a coordinate-sorted BAM and the gene GTF.

Uniquely mapped primary reads that fall inside exactly one gene strand are compared with
that gene's strand (read 2 of a pair is flipped so every fragment speaks for read 1).
The result maps to TEcount's --stranded option:
  forward  - read 1 on the transcript strand ("second-strand" libraries)
  reverse  - read 1 opposite the transcript strand ("first-strand", e.g. dUTP / TruSeq Stranded)
  no       - unstranded
"""
import gzip
import json
import sys
from collections import defaultdict
from pathlib import Path

BIN = 100_000
THRESHOLD = 0.8      # share of informative reads needed to call a stranded library
UNSTRANDED = 0.6     # both directions below this share -> unstranded
MIN_INFORMATIVE = 2_000
MAX_READS = 400_000


def load_genes(gtf: Path):
    """Exon intervals per chromosome bin: {(chrom, bin): [(start, end, strand)]}."""
    index = defaultdict(list)
    opener = gzip.open if str(gtf).endswith(".gz") else open
    with opener(gtf, "rt", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if line.startswith("#"):
                continue
            fields = line.rstrip("\n").split("\t")
            if len(fields) < 8 or fields[2] != "exon" or fields[6] not in "+-":
                continue
            start, end = int(fields[3]) - 1, int(fields[4])
            for b in range(start // BIN, end // BIN + 1):
                index[fields[0], b].append((start, end, fields[6]))
    if not index:
        raise ValueError(f"基因注释中没有 exon 记录：{gtf}")
    return index


def strands_at(index, chrom, position):
    return {strand for start, end, strand in index.get((chrom, position // BIN), ()) if start <= position < end}


def classify(same: int, opposite: int) -> tuple[str, str]:
    informative = same + opposite
    if informative < MIN_INFORMATIVE:
        return "no", f"可判定的 reads 仅 {informative} 条，不足以判断，按不分链处理"
    share = same / informative
    if share >= THRESHOLD:
        return "forward", f"read1 与基因同向占 {share:.1%}"
    if share <= 1 - THRESHOLD:
        return "reverse", f"read1 与基因反向占 {1 - share:.1%}"
    if 1 - UNSTRANDED <= share <= UNSTRANDED:
        return "no", f"同向 {share:.1%}、反向 {1 - share:.1%}，为不分链建库"
    return "no", f"同向 {share:.1%}，介于分链与不分链之间，按不分链处理（需人工核对）"


def infer(bam: Path, gtf: Path, max_reads: int = MAX_READS) -> dict:
    import pysam

    index = load_genes(gtf)
    same = opposite = ambiguous = 0
    seen = 0
    with pysam.AlignmentFile(str(bam), "rb") as alignments:
        for read in alignments.fetch(until_eof=True):
            if read.is_unmapped or read.is_secondary or read.is_supplementary or read.is_qcfail:
                continue
            if read.has_tag("NH") and read.get_tag("NH") != 1:
                continue
            if read.is_paired and not read.is_proper_pair:
                continue
            seen += 1
            if seen > max_reads:
                break
            middle = (read.reference_start + read.reference_end) // 2
            gene_strands = strands_at(index, read.reference_name, middle)
            if len(gene_strands) != 1:
                ambiguous += bool(gene_strands)
                continue
            read_strand = "-" if read.is_reverse else "+"
            if read.is_paired and read.is_read2:
                read_strand = "+" if read_strand == "-" else "-"
            if read_strand == next(iter(gene_strands)):
                same += 1
            else:
                opposite += 1
    mode, reason = classify(same, opposite)
    return {"tecount": mode, "reason": reason, "same_strand": same, "opposite_strand": opposite,
            "ambiguous": ambiguous, "reads_examined": min(seen, max_reads)}


def main(bam, gtf, output, override="auto"):
    if override in {"no", "forward", "reverse"}:
        result = {"tecount": override, "reason": "配置中指定", "override": True}
    else:
        result = infer(Path(bam), Path(gtf))
    Path(output).write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"[TEFLOW] strandedness {Path(bam).name}: {result['tecount']} ({result['reason']})", flush=True)


if "snakemake" in globals():
    main(snakemake.input.bam, snakemake.input.gene, snakemake.output[0], snakemake.params.override)
elif __name__ == "__main__":
    main(*sys.argv[1:4], *(sys.argv[4:5] or ["auto"]))
