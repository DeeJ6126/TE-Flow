"""DAG checks use miniature local files and dry-run only; no biology tools run."""
import gzip
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


@unittest.skipUnless(importlib.util.find_spec("snakemake"), "需要已安装的 Snakemake")
class SnakefileDagTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.raw = self.directory / "raw data"
        self.raw.mkdir()
        self.output = self.directory / "results"
        self.output.mkdir()
        self.index = self.directory / "star index"
        self.index.mkdir()
        for name in ("Genome", "SA", "SAindex", "genomeParameters.txt"):
            (self.index / name).write_text("fixture\n")
        self.gene = self.directory / "gene annotation.gtf"
        self.te = self.directory / "te annotation.gtf"
        self.gene.write_text("fixture\n")
        self.te.write_text("fixture\n")
        self.snakefile = Path(__file__).resolve().parents[1] / "Snakefile"
        self.config = self.directory / "config.yaml"
        self.config.write_text(json.dumps({
            "ACCESSION": "SRR123456", "SAMPLES_DIR": str(self.raw), "OUTPUT_DIR": str(self.output),
            "INDEX_PREFIX": str(self.index), "GENOME_ANNOTATION": str(self.gene),
            "TE_ANNOTATION": str(self.te), "THREADS": 10
        }))

    def tearDown(self):
        self.temporary.cleanup()

    def dry_run(self):
        result = subprocess.run([
            sys.executable, "-m", "snakemake", "--dry-run", "--printshellcmds", "--snakefile", str(self.snakefile),
            "--directory", str(self.output), "--configfile", str(self.config), "--cores", "10",
            "--resources", "star_slots=1", "download_slots=1", "mem_mb=256000"
        ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stdout)
        return result.stdout

    def test_new_accession_schedules_prepare_checkpoint(self):
        output = self.dry_run()
        self.assertIn("prepare_samples", output)
        self.assertIn("te_count", output)
        self.assertNotIn("rule merge_counts:", output)

    def test_completed_manifest_expands_all_analysis_rules_with_quoted_paths(self):
        files = []
        for mate in (1, 2):
            path = self.raw / f"SRR123456_{mate}.fastq.gz"
            with gzip.open(path, "wt") as handle:
                handle.write("@read\nACGT\n+\nIIII\n")
            files.append(str(path))
        (self.output / "samples.json").write_text(json.dumps({
            "accession": "SRR123456", "samples": {"SRR123456": {"r1": files[0], "r2": files[1]}}
        }))
        output = self.dry_run()
        for rule in ("quality_control", "star_align", "te_count"):
            self.assertIn(rule, output)
        self.assertNotIn("rule merge_counts:", output)
        self.assertIn("--outFilterMultimapNmax 100", output)
        self.assertIn("--limitBAMsortRAM 168824282496", output)
        self.assertIn("--GTF '" + str(self.gene) + "'", output)
        self.assertIn("-i '" + files[0] + "'", output)

    def test_geo_manifest_expands_nested_runs_and_project_merge(self):
        config = json.loads(self.config.read_text())
        config["ACCESSION"] = "GSE123456"
        self.config.write_text(json.dumps(config))
        samples = {}
        for run in ("SRR123456", "SRR654321"):
            directory = self.raw / run
            directory.mkdir()
            for mate in (1, 2):
                with gzip.open(directory / f"{run}_{mate}.fastq.gz", "wt") as handle:
                    handle.write("@read\nACGT\n+\nIIII\n")
            samples[run] = {f"r{mate}": str(directory / f"{run}_{mate}.fastq.gz") for mate in (1, 2)}
        (self.output / "samples.json").write_text(json.dumps({"accession": "GSE123456", "samples": samples}))
        output = self.dry_run()
        self.assertIn("rule merge_counts:", output)
        for run in samples:
            self.assertIn(str(self.output / run / "abundance" / f"{run}.cntTable"), output)
            self.assertIn(str(self.output / run / "align" / f"{run}Aligned.sortedByCoord.out.bam"), output)
            self.assertIn(str(self.output / run / "qc" / f"{run}_clean_R1.fq.gz"), output)
            self.assertNotIn(str(self.output / run / "merged.count"), output)

    @unittest.skipUnless(os.name == "posix", "执行回归需要 Linux")
    def test_prepare_and_merge_scripts_execute_under_real_snakemake_injection(self):
        mock_iseq = self.directory / "mock-iseq"
        mock_iseq.write_text(
            "#!/usr/bin/env python3\n"
            "import csv, gzip, sys\n"
            "from pathlib import Path\n"
            "args = sys.argv[1:]\n"
            "out = Path(args[args.index('-o') + 1]); out.mkdir(parents=True, exist_ok=True)\n"
            "run = args[args.index('-i') + 1]\n"
            "payload = gzip.compress(b'@read\\nACGT\\n+\\nIIII\\n', mtime=0)\n"
            "if '-m' in args:\n"
            "    with (out / (run + '.metadata.tsv')).open('w', newline='') as handle:\n"
            "        writer = csv.DictWriter(handle, fieldnames=['run_accession', 'library_layout', 'fastq_ftp', 'fastq_bytes'], delimiter='\\t')\n"
            "        writer.writeheader()\n"
            "        writer.writerow({'run_accession': run, 'library_layout': 'PAIRED', 'fastq_ftp': ';'.join('ftp.sra.ebi.ac.uk/' + run + '_' + str(mate) + '.fastq.gz' for mate in (1, 2)), 'fastq_bytes': str(len(payload)) + ';' + str(len(payload))})\n"
            "else:\n"
            "    for mate in (1, 2): (out / (run + '_' + str(mate) + '.fastq.gz')).write_bytes(payload)\n"
            "    with (out / 'success.log').open('a') as handle: handle.write('Oct 13\\t' + run + '\\n')\n"
        )
        mock_iseq.chmod(0o755)
        config = json.loads(self.config.read_text())
        config["ISEQ_BIN"] = str(mock_iseq)
        self.config.write_text(json.dumps(config))
        result = subprocess.run([
            sys.executable, "-m", "snakemake", "--snakefile", str(self.snakefile),
            "--directory", str(self.output), "--configfile", str(self.config), "--cores", "10",
            "--resources", "star_slots=1", "download_slots=1", "mem_mb=256000",
            "--until", "prepare_samples", "--rerun-incomplete"
        ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stdout)
        document = json.loads((self.output / "samples.json").read_text())
        self.assertEqual(document["total_runs"], 1)
        self.assertTrue(Path(document["samples"]["SRR123456"]["r1"]).is_file())
        # Supply fixture outputs for scientific tools; this additional call
        # executes merge_counts.py through Snakemake's script preamble too.
        qc, align, abundance = [self.output / name for name in ("qc", "align", "abundance")]
        for directory in (qc, align, abundance):
            directory.mkdir()
        for mate in (1, 2):
            (qc / f"SRR123456_clean_R{mate}.fq.gz").write_bytes(Path(document["samples"]["SRR123456"][f"r{mate}"]).read_bytes())
        (qc / "SRR123456.html").write_text("fixture\n")
        (qc / "SRR123456.json").write_text("{}\n")
        (align / "SRR123456Aligned.sortedByCoord.out.bam").write_text("fixture\n")
        (abundance / "SRR123456.cntTable").write_text("gene\tcount\ngeneA\t2\nTE1\t4\n")
        result = subprocess.run([
            sys.executable, "-m", "snakemake", "--snakefile", str(self.snakefile),
            "--directory", str(self.output), "--configfile", str(self.config), "--cores", "10",
            "--resources", "star_slots=1", "download_slots=1", "mem_mb=256000",
            "--until", "merge_counts", "--rerun-incomplete", str(self.output / "merged.count")
        ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual((self.output / "merged.count").read_text(), "gene\tSRR123456\ngeneA\t2\nTE1\t4\n")


if __name__ == "__main__":
    unittest.main(verbosity=2)
