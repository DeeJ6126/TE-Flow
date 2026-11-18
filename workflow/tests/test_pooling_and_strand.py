"""Pooled GSM jobs, read cleanup and strandedness inference (no biology tools run)."""
import gzip
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import discard_reads  # noqa: E402
import infer_strandedness  # noqa: E402


class StrandednessTests(unittest.TestCase):
    def test_classify(self):
        self.assertEqual(infer_strandedness.classify(9500, 500)[0], "forward")
        self.assertEqual(infer_strandedness.classify(400, 9600)[0], "reverse")
        self.assertEqual(infer_strandedness.classify(5100, 4900)[0], "no")
        self.assertEqual(infer_strandedness.classify(7000, 3000)[0], "no")   # in between: unstranded, flagged
        self.assertIn("需人工核对", infer_strandedness.classify(7000, 3000)[1])
        self.assertEqual(infer_strandedness.classify(50, 0)[0], "no")        # too few informative reads

    def test_gene_index_and_lookup(self):
        with tempfile.TemporaryDirectory() as tmp:
            gtf = Path(tmp) / "genes.gtf.gz"
            with gzip.open(gtf, "wt") as handle:
                handle.write("#header\n")
                handle.write('chr1\tsrc\texon\t101\t200\t.\t+\t.\tgene_id "A";\n')
                handle.write('chr1\tsrc\texon\t150\t300\t.\t-\t.\tgene_id "B";\n')
                handle.write('chr1\tsrc\tgene\t1\t99999\t.\t+\t.\tgene_id "C";\n')
            index = infer_strandedness.load_genes(gtf)
            self.assertEqual(infer_strandedness.strands_at(index, "chr1", 120), {"+"})
            self.assertEqual(infer_strandedness.strands_at(index, "chr1", 170), {"+", "-"})
            self.assertEqual(infer_strandedness.strands_at(index, "chr1", 250), {"-"})
            self.assertEqual(infer_strandedness.strands_at(index, "chr2", 120), set())

    def test_override_skips_inference(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "s.json"
            infer_strandedness.main("missing.bam", "missing.gtf", out, "reverse")
            self.assertEqual(json.loads(out.read_text(encoding="utf-8"))["tecount"], "reverse")


class PooledDiscardTests(unittest.TestCase):
    def test_removes_member_runs_pooled_and_intermediate_reads(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw, out = Path(tmp) / "raw" / "GSM1", Path(tmp) / "out" / "GSM1"
            samples, files = {}, []
            for run in ("SRR1", "SRR2"):
                (raw / run).mkdir(parents=True)
                paths = [raw / run / f"{run}_{mate}.fastq.gz" for mate in (1, 2)]
                for path in paths:
                    path.write_bytes(b"reads")
                files += paths
                samples[run] = {"r1": str(paths[0]), "r2": str(paths[1]), "library_layout": "PAIRED"}
            for sub, name in (("pooled", "GSM1_R1.fastq.gz"), ("pooled", "GSM1_R2.fastq.gz"),
                              ("qc", "GSM1_clean_R1.fq.gz"), ("qc", "GSM1_clean_R2.fq.gz"),
                              ("align", "GSM1Aligned.sortedByCoord.out.bam")):
                (out / sub).mkdir(parents=True, exist_ok=True)
                (out / sub / name).write_bytes(b"x")
                files.append(out / sub / name)
            table = out / "abundance" / "GSM1.cntTable"
            table.parent.mkdir()
            table.write_text("gene\tcount\nA\t1\n")
            manifest = {"accession": "GSM1", "samples": samples,
                        "pooled": {"sample": "GSM1", "runs": ["SRR1", "SRR2"], "library_layout": "PAIRED"}}
            receipt = table.parent / ".GSM1.cleaned"
            discard_reads.discard("GSM1", "GSM1", raw, out, manifest, table, receipt)
            self.assertTrue(receipt.is_file())
            self.assertFalse(any(path.exists() for path in files))
            self.assertTrue(table.is_file())

    def test_refuses_unparseable_table(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "GSM1"
            table = out / "abundance" / "GSM1.cntTable"
            table.parent.mkdir(parents=True)
            table.write_text("broken\n")
            manifest = {"accession": "GSM1", "samples": {},
                        "pooled": {"sample": "GSM1", "runs": [], "library_layout": "SINGLE"}}
            with self.assertRaises(ValueError):
                discard_reads.discard("GSM1", "GSM1", Path(tmp) / "raw", out, manifest, table,
                                      table.parent / ".GSM1.cleaned")


if __name__ == "__main__":
    unittest.main()
