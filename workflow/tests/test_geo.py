"""GEO scope, completeness and storage tests without real sequencing downloads."""
import csv
import gzip
import io
import json
import sys
import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import prepare_samples as pipeline


def tsv(rows):
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=list(rows[0]), delimiter="\t")
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue()


def row(run, experiment="SRX1", layout="PAIRED"):
    return {"run_accession": run, "experiment_accession": experiment,
            "library_layout": layout, "library_strategy": "RNA-Seq", "tax_id": "9606",
            "fastq_ftp": ";".join(f"ftp.sra.ebi.ac.uk/{run}_{mate}.fastq.gz" for mate in (1, 2)),
            "fastq_bytes": "60;60"}


class GeoTests(unittest.TestCase):
    def request(self, url):
        query = parse_qs(urlsplit(url).query)
        accession = query.get("acc", query.get("accession", [""]))[0]
        if accession == "GSE1":
            return ("^SERIES = GSE1\n!Series_sample_id = GSM1\n!Series_sample_id = GSM2\n"
                    "!Series_sample_id = GSM1\n!Series_relation = BioProject: PRJNA9999\n"
                    "!Series_relation = SubSeries of: GSE9999\n")
        if accession == "GSM1":
            return ("^SAMPLE = GSM1\n!Sample_library_strategy = RNA-Seq\n"
                    "!Sample_relation = SRA: https://www.ncbi.nlm.nih.gov/sra?term=SRX1\n")
        if accession == "GSM2":
            return ("^SAMPLE = GSM2\n!Sample_library_strategy = RNA-Seq\n"
                    "!Sample_relation = SRA: https://www.ncbi.nlm.nih.gov/sra?term=SRX1\n"
                    "!Sample_relation = SRA: https://www.ncbi.nlm.nih.gov/sra?term=SRX2\n")
        if accession in {"SRX1", "SRX2"}:
            rows = [row("SRR1"), row("SRR2")] if accession == "SRX1" else [row("SRR3", "SRX2")]
            if query.get("fields") == ["run_accession"]:
                return "run_accession\n" + "\n".join(item["run_accession"] for item in rows) + "\n"
            return tsv(rows)
        self.fail(f"Unexpected out-of-scope metadata request: {url}")

    def test_exact_series_scope_multi_run_gsm_and_duplicates(self):
        with patch.object(pipeline, "_request_text", side_effect=self.request) as request, \
             patch.object(pipeline.time, "sleep"), patch.object(pipeline, "run_iseq") as download:
            result = pipeline.resolve_geo_series("gse1")
        self.assertEqual(result["gsm_accessions"], ["GSM1", "GSM2"])
        self.assertEqual(list(result["records"]), ["SRR1", "SRR2", "SRR3"])
        self.assertEqual(result["records"]["SRR1"]["geo_samples"], ["GSM1", "GSM2"])
        self.assertEqual(len(result["mapping"]), 5)
        self.assertEqual(result["mapping"][0], {"gse": "GSE1", "gsm": "GSM1", "experiment": "SRX1", "run": "SRR1"})
        # Each experiment is fetched only once, plus its independent Run-ID query.
        self.assertEqual(request.call_count, 7)
        download.assert_not_called()

    def test_private_missing_and_non_sequencing_samples_fail_explicitly(self):
        series = "^SERIES = GSE1\n!Series_sample_id = GSM1\n"
        samples = ("No public sample", "^SAMPLE = GSM1\n!Sample_type = RNA\n",
                   "^SAMPLE = GSM1\n!Sample_library_strategy = ATAC-seq\n")
        for sample in samples:
            with self.subTest(sample=sample), \
                 patch.object(pipeline, "_request_text", side_effect=[series, sample]), \
                 patch.object(pipeline.time, "sleep"):
                with self.assertRaises(ValueError):
                    pipeline.resolve_geo_series("GSE1")

    def test_series_without_gsm_does_not_expand_bioproject(self):
        with patch.object(pipeline, "_request_text", return_value="^SERIES = GSE1\n!Series_relation = BioProject: PRJNA99\n") as request:
            with self.assertRaisesRegex(ValueError, "没有公开的 GSM"):
                pipeline.resolve_geo_series("GSE1")
        self.assertEqual(request.call_count, 1)

    def test_wrong_experiment_and_single_end_fail_before_download(self):
        for record in (row("SRR1", "SRX99"), row("SRR1", layout="SINGLE")):
            with patch.object(pipeline, "_request_text", return_value=tsv([record])):
                with self.assertRaises(ValueError):
                    pipeline.experiment_metadata("SRX1")

    def test_experiment_truncated_at_complete_row_fails_independent_check(self):
        with patch.object(pipeline, "_request_text", side_effect=[tsv([row("SRR1")]), "run_accession\nSRR1\nSRR2\n"]):
            with self.assertRaisesRegex(ValueError, "缺失 1 个"):
                pipeline.experiment_metadata("SRX1")

    def test_ena_unavailable_ncbi_fallback_keeps_scoped_sra_conversion(self):
        with patch.object(pipeline, "_metadata_request", side_effect=OSError("ENA unavailable")), \
             patch.object(pipeline, "fetch_ncbi_runinfo", return_value=([], [{
                 "Run": "SRR1", "Experiment": "SRX1", "LibraryLayout": "PAIRED", "LibraryStrategy": "RNA-Seq"}])), \
             patch.object(pipeline, "verify_run_set", return_value={"source": "fixture"}):
            result = pipeline.experiment_metadata("SRX1")
        self.assertEqual(list(result), ["SRR1"])
        self.assertEqual(result["SRR1"]["files"][0]["source"], "sra-converted")

    def test_geo_nested_download_manifest_mapping_and_resume(self):
        payload = gzip.compress(b"@read\nACGT\n+\nIIII\n", mtime=0)
        records = {}
        mapping = []
        for number in (1, 2):
            run, gsm, experiment = f"SRR{number}", f"GSM{number}", f"SRX{number}"
            metadata_row = row(run, experiment)
            metadata_row["fastq_bytes"] = f"{len(payload)};{len(payload)}"
            records[run] = {"row": metadata_row, "files": pipeline.paired_files(metadata_row, run),
                            "library_layout": "PAIRED", "geo_samples": [gsm], "experiments": [experiment]}
            mapping.append({"gse": "GSE1", "gsm": gsm, "experiment": experiment, "run": run})
        resolved = {"accession": "GSE1", "gsm_accessions": ["GSM1", "GSM2"], "mapping": mapping,
                    "fields": pipeline.ENA_COLUMNS, "records": records}

        def download(command, directory, handle, timeout=None):
            run = command[command.index("-i") + 1]
            self.assertEqual(directory.name, run)
            self.assertEqual(command[command.index("-o") + 1], str(directory))
            for mate in (1, 2):
                (directory / f"{run}_{mate}.fastq.gz").write_bytes(payload)
            (directory / "success.log").write_text(f"fixture\t{run}\n")

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            raw, output = root / "raw" / "GSE1", root / "results" / "GSE1"
            with patch.object(pipeline, "download_lock", side_effect=lambda _: nullcontext()), \
                 patch.object(pipeline.shutil, "which", return_value="/bin/iseq"), \
                 patch.object(pipeline, "check_space", return_value=4 * len(payload)), \
                 patch.object(pipeline, "resolve_geo_series", return_value=resolved) as resolver, \
                 patch.object(pipeline, "run_iseq", side_effect=download) as iseq:
                manifest = pipeline.prepare("GSE1", raw, output, output / "samples.json", output / "prepare.log")
                resumed = pipeline.prepare("GSE1", raw, output, output / "samples.json", output / "prepare.log")
            self.assertEqual(iseq.call_count, 2)
            self.assertEqual(resolver.call_count, 1)
            self.assertTrue(all(value["download_status"] == "reused" for value in resumed["samples"].values()))
            self.assertEqual(manifest["layout"], "geo_nested")
            for run, sample in manifest["samples"].items():
                self.assertEqual(Path(sample["r1"]), raw / run / f"{run}_1.fastq.gz")
                self.assertEqual(Path(sample["count_path"]), output / run / "abundance" / f"{run}.cntTable")
                self.assertTrue(Path(sample["files"][0]["path"]).is_file())
                self.assertFalse((raw / f"{run}_1.fastq.gz").exists())
            self.assertEqual((output / "samples.tsv").read_text().splitlines(),
                             ["gse\tgsm\texperiment\trun", "GSE1\tGSM1\tSRX1\tSRR1", "GSE1\tGSM2\tSRX2\tSRR2"])
            self.assertEqual(json.loads((output / "samples.json").read_text())["total_runs"], 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
