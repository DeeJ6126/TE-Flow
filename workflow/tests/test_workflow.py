"""Focused unit and download-orchestration tests; no real network/downloads."""
import csv
import gzip
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from prepare_samples import (normalize_accession, paired_files, parse_metadata, validate_fastq,
                             logged_runs, remove_success_marker, prepare, resolve_submission,
                             write_metadata, metadata_completion_marker, atomic_json, run_iseq,
                             verify_run_set, obtain_metadata)
from merge_counts import merge_tables, read_count_table


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.workspace = tempfile.TemporaryDirectory()
        self.directory = Path(self.workspace.name)

    def tearDown(self):
        self.workspace.cleanup()

    def fastq(self, name, record=b"@read/1\nACGT\n+\nIIII\n"):
        path = self.directory / name
        with gzip.open(path, "wb") as handle:
            handle.write(record)
        return path

    def row(self, run="SRR123456", files=None, layout="PAIRED"):
        files = files or [f"{run}_1.fastq.gz", f"{run}_2.fastq.gz"]
        return {"run_accession": run, "library_layout": layout,
                "fastq_ftp": ";".join(f"ftp.sra.ebi.ac.uk/vol1/fastq/{name}" for name in files),
                "fastq_bytes": ";".join("60" for name in files)}

    def metadata(self, rows, name="SRP123456.metadata.tsv"):
        path = self.directory / name
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]), delimiter="\t")
            writer.writeheader()
            writer.writerows(rows)
        accession = name.split(".metadata")[0]
        atomic_json(metadata_completion_marker(self.directory, accession),
                    {"accession": accession, "bytes": path.stat().st_size,
                     "runs": [row.get("run_accession", row.get("Run")) for row in rows],
                     "run_set_verification": {"version": 1, "source": "fixture", "count": len(rows)}})
        return path

    def table(self, run, rows):
        path = self.directory / f"{run}.cntTable"
        path.write_text("gene\tcount\n" + "".join(f"{gene}\t{count}\n" for gene, count in rows), encoding="utf-8")
        return path

    def test_accession_validation(self):
        for value in ("SRR123456", "SRP123456", "PRJNA1234", "SRA123456", "ERA123456", "DRR123456"):
            self.assertEqual(normalize_accession(" " + value.lower() + " "), value)
        for value in ("", "SRA000/../../", "SRR1234;rm -rf", "ABC000", 123):
            with self.assertRaises(ValueError):
                normalize_accession(value)

    def test_pair_mapping_alias_and_order(self):
        pair = paired_files(self.row(files=["alias_R2_001.fq.gz", "alias_R1_001.fq.gz"]), "SRR123456")
        self.assertEqual([item["name"] for item in pair], ["alias_R1_001.fq.gz", "alias_R2_001.fq.gz"])

    def test_orphan_file_does_not_enter_paired_analysis(self):
        pair = paired_files(self.row(files=["SRR123456.fastq.gz", "SRR123456_2.fastq.gz", "SRR123456_1.fastq.gz"]), "SRR123456")
        self.assertEqual(len(pair), 2)
        self.assertTrue(pair[0]["name"].endswith("_1.fastq.gz"))

    def test_missing_ambiguous_or_single_pair_fails(self):
        for row in (self.row(layout="SINGLE"), self.row(files=["SRR123456_1.fastq.gz"]),
                    self.row(files=["alpha.fastq.gz", "beta.fastq.gz"]),
                    self.row(files=["laneA_R1.fastq.gz", "laneB_R1.fastq.gz", "laneA_R2.fastq.gz"])):
            with self.assertRaises(ValueError):
                paired_files(row, "SRR123456")

    def test_metadata_is_authoritative_not_directory_scan(self):
        path = self.metadata([self.row()])
        (self.directory / "success.log").write_text("not a sample\n")
        (self.directory / "another.metadata.tsv").write_text("not a sample\n")
        (self.directory / "unused_1.fastq.gz").write_text("not in metadata\n")
        _, records = parse_metadata(path)
        self.assertEqual(list(records), ["SRR123456"])

    def test_empty_and_conflicting_metadata_fail(self):
        empty = self.directory / "empty.tsv"
        empty.write_text("run_accession\tlibrary_layout\tfastq_ftp\tfastq_bytes\n")
        with self.assertRaises(ValueError):
            parse_metadata(empty)
        row = self.row()
        different = self.row()
        different["fastq_bytes"] = "80;80"
        with self.assertRaises(ValueError):
            parse_metadata(self.metadata([row, different]))

    def test_fastq_missing_partial_and_bad_record(self):
        path = self.fastq("sample_1.fastq.gz")
        expected = {"bytes": path.stat().st_size, "source": "ena-fastq"}
        validate_fastq(path, expected)
        with self.assertRaises(ValueError):
            validate_fastq(path, {**expected, "bytes": path.stat().st_size + 1})
        bad = self.fastq("bad.fastq.gz", b"@read\nACGT\n+\nI\n")
        with self.assertRaises(ValueError):
            validate_fastq(bad, {"bytes": None, "source": "sra-converted"})

    def test_success_log_matches_exact_runs(self):
        path = self.directory / "success.log"
        path.write_text("Tue Oct 13\tSRR123456\nTue Oct 13\tSRR1234567\n")
        self.assertEqual(logged_runs(path), {"SRR123456", "SRR1234567"})
        remove_success_marker(path, "SRR123456")
        self.assertNotIn("SRR123456", logged_runs(path))
        self.assertNotIn("SRR1234567", logged_runs(path))

    def test_merge_preserves_inner_join_and_run_columns(self):
        paths = [self.table("SRR1", [("geneA", "2"), ("TE1", "4"), ("onlyA", "7")]),
                 self.table("SRR2", [("TE1", "8"), ("geneA", "3"), ("onlyB", "9")])]
        output = self.directory / "merged.count"
        summary = merge_tables(paths, ["SRR1", "SRR2"], output)
        self.assertEqual(output.read_text(), "gene\tSRR1\tSRR2\ngeneA\t2\t3\nTE1\t4\t8\n")
        self.assertEqual(summary["rows"], 2)
        self.assertEqual(summary["samples"], 2)

    def test_merge_rejects_empty_duplicate_negative_or_nan(self):
        for rows in ([], [("gene", "1"), ("gene", "2")], [("gene", "-1")], [("gene", "NaN")], [("gene", "abc")]):
            with self.assertRaises(ValueError):
                read_count_table(self.table("SRR1", rows))
        with self.assertRaises(ValueError):
            merge_tables([], [], self.directory / "merged.count")
        with self.assertRaises(ValueError):
            merge_tables([self.table("SRR1", [("a", "1")]), self.table("SRR2", [("b", "2")])],
                         ["SRR1", "SRR2"], self.directory / "merged.count")

    def test_sra_submission_resolves_and_keeps_complete_metadata(self):
        # API search can return requested columns in a different order.
        row = {"study_accession": "PRJNA1234", **self.row()}
        content = io_tsv([row])
        with patch("prepare_samples._request_text", return_value=content) as request:
            path = self.directory / "SRA123456.metadata.tsv"
            resolve_submission("SRA123456", path)
            self.assertIn("submission_accession", request.call_args.args[0])
            _, records = parse_metadata(path)
            self.assertEqual(list(records), ["SRR123456"])
            with path.open() as handle:
                header = handle.readline().rstrip().split("\t")
            self.assertEqual(header[0], "run_accession")
            self.assertEqual(header[27], "fastq_bytes")
            self.assertEqual(header[29], "fastq_ftp")

    def test_ncbi_metadata_reorders_run_column_only(self):
        path = self.directory / "SRR123456.metadata.tsv"
        row = {"LibraryLayout": "PAIRED", "Run": "SRR123456", "ScientificName": "Homo sapiens"}
        write_metadata(path, list(row), [row])
        self.assertEqual(path.read_text().splitlines()[0], "Run\tLibraryLayout\tScientificName")

    @unittest.skipIf(os.name != "posix", "下载编排的 NFS 文件锁需要 Linux")
    def test_incomplete_metadata_cache_is_retried(self):
        self.metadata([self.row()]).write_text("run_accession\tlibrary_layout\n")

        def fake_metadata(command, samples_dir, handle, timeout=None):
            self.assertIn("-m", command)
            (samples_dir / "SRP123456.metadata.tsv").write_text(io_tsv([self.row()]))

        with patch("prepare_samples.shutil.which", return_value="/bin/iseq"), \
             patch("prepare_samples.run_iseq", side_effect=fake_metadata) as fetch, \
             patch("prepare_samples.verify_run_set", return_value={"version": 1}), \
             patch("prepare_samples.check_space", side_effect=ValueError("stop after metadata")):
            with self.assertRaisesRegex(ValueError, "stop after metadata"):
                prepare("SRP123456", self.directory, self.directory, self.directory / "samples.json",
                        self.directory / "prepare.log")
            self.assertEqual(fetch.call_count, 1)
            self.assertTrue((self.directory / "SRP123456.metadata.tsv.incomplete").exists())

    def test_project_complete_rows_but_missing_run_is_rejected(self):
        # This is syntactically valid metadata, as produced when wget terminates
        # exactly after a complete row. Independent IDs must reject the subset.
        _, records = parse_metadata(self.metadata([self.row()]))
        with patch("prepare_samples._request_text", return_value="run_accession\nSRR123456\nSRR654321\n") as request:
            with self.assertRaisesRegex(ValueError, "缺失 1 个"):
                verify_run_set("SRP123456", records)
            query = urllib_query(request.call_args.args[0])
            self.assertEqual(query["fields"], ["run_accession"])
            self.assertEqual(query["limit"], ["0"])

    def test_project_full_set_accepts_any_query_order(self):
        _, records = parse_metadata(self.metadata([self.row(), self.row("SRR654321")]))
        with patch("prepare_samples._request_text", return_value="run_accession\nSRR654321\nSRR123456\n"):
            result = verify_run_set("PRJNA1234", records)
            self.assertEqual(result, {"version": 1, "source": "ena", "count": 2})

    def test_independent_ena_unavailable_falls_back_to_ncbi(self):
        _, records = parse_metadata(self.metadata([self.row()]))
        payloads = [OSError("ENA unavailable"),
                    "<eSearchResult><Count>1</Count><WebEnv>env</WebEnv><QueryKey>1</QueryKey></eSearchResult>",
                    "Run,LibraryLayout\nSRR123456,PAIRED\n"]
        with patch("prepare_samples._request_text", side_effect=payloads) as request, \
             patch("prepare_samples.time.sleep"):
            result = verify_run_set("SRP123456", records)
            self.assertEqual(result["source"], "ncbi")
            self.assertEqual(request.call_count, 3)

    def test_independent_query_unavailable_fails_closed(self):
        _, records = parse_metadata(self.metadata([self.row()]))
        with patch("prepare_samples._request_text", side_effect=OSError("network unavailable")):
            with self.assertRaisesRegex(ValueError, "无法独立确认全部 Run"):
                verify_run_set("SRP123456", records)

    def test_single_run_requires_exact_input_without_network(self):
        with patch("prepare_samples._request_text") as request:
            self.assertEqual(verify_run_set("SRR123456", {"SRR123456": {}})["source"], "input_run")
            with self.assertRaises(ValueError):
                verify_run_set("SRR123456", {"SRR654321": {}})
            request.assert_not_called()

    def test_truncated_metadata_is_not_published_or_marked_complete(self):
        def partial_iseq(command, samples_dir, handle, timeout=None):
            (samples_dir / "SRP123456.metadata.tsv").write_text(io_tsv([self.row()]))
        with (self.directory / "metadata.log").open("w") as log, \
             patch("prepare_samples.run_iseq", side_effect=partial_iseq), \
             patch("prepare_samples._request_text", return_value="run_accession\nSRR123456\nSRR654321\n"):
            with self.assertRaisesRegex(ValueError, "缺失 1 个"):
                obtain_metadata("SRP123456", self.directory, "iseq", 180, log)
        self.assertFalse((self.directory / "SRP123456.metadata.tsv").exists())
        self.assertFalse(metadata_completion_marker(self.directory, "SRP123456").exists())

    def test_old_cache_marker_is_independently_verified_without_refetch(self):
        metadata = self.metadata([self.row()])
        marker = metadata_completion_marker(self.directory, "SRP123456")
        old = json.loads(marker.read_text())
        del old["run_set_verification"]
        atomic_json(marker, old)
        with (self.directory / "metadata.log").open("w") as log, \
             patch("prepare_samples.run_iseq") as iseq, \
             patch("prepare_samples._request_text", return_value="run_accession\nSRR123456\n"):
            path, _, _ = obtain_metadata("SRP123456", self.directory, "iseq", 180, log)
            self.assertEqual(path, metadata)
            iseq.assert_not_called()
        self.assertEqual(json.loads(marker.read_text())["run_set_verification"]["source"], "ena")

    @unittest.skipIf(os.name != "posix", "进程组清理需要 Linux")
    def test_metadata_timeout_kills_writing_child(self):
        import time
        script = self.directory / "spawn_writer.py"
        late_file = self.directory / "late.txt"
        script.write_text("import subprocess, sys, time\n"
                          "subprocess.Popen([sys.executable, '-c', "
                          "\"import time; from pathlib import Path; time.sleep(1); "
                          f"Path({str(late_file)!r}).write_text('orphan')\"])\n"
                          "time.sleep(20)\n")
        with (self.directory / "process.log").open("w") as log:
            with self.assertRaisesRegex(RuntimeError, "超时"):
                run_iseq([sys.executable, str(script)], self.directory, log, timeout=0.1)
        time.sleep(1.2)
        self.assertFalse(late_file.exists())

    @unittest.skipIf(os.name != "posix", "下载编排的 NFS 文件锁需要 Linux")
    def test_sra_fallback_explicitly_uses_sra_database(self):
        row = {"Run": "SRR123456", "LibraryLayout": "PAIRED"}
        self.metadata([row])

        def fake_download(command, samples_dir, handle, timeout=None):
            self.assertEqual(command[command.index("-d") + 1], "sra")
            for mate in (1, 2):
                self.fastq(f"SRR123456_{mate}.fastq.gz")
            (samples_dir / "success.log").write_text("Oct 13\tSRR123456\n")

        with patch("prepare_samples.shutil.which", return_value="/bin/iseq"), \
             patch("prepare_samples.check_space", return_value=0), \
             patch("prepare_samples.run_iseq", side_effect=fake_download) as download:
            document = prepare("SRP123456", self.directory, self.directory, self.directory / "samples.json",
                               self.directory / "prepare.log")
            self.assertEqual(download.call_count, 1)
            self.assertEqual(document["total_runs"], 1)

    @unittest.skipIf(os.name != "posix", "下载编排的 NFS 文件锁需要 Linux")
    def test_prepare_downloads_all_runs_and_resume_reuses(self):
        runs = ["SRR123456", "SRR654321"]
        rows = [self.row(run) for run in runs]
        for row in rows:
            names = [Path(url).name for url in row["fastq_ftp"].split(";")]
            # Determine fixture compressed sizes before mocking iSeq download.
            sizes = []
            for name in names:
                path = self.fastq(name)
                sizes.append(str(path.stat().st_size))
                path.unlink()
            row["fastq_bytes"] = ";".join(sizes)
        self.metadata(rows)
        manifest = self.directory / "samples.json"
        log = self.directory / "logs" / "prepare.log"

        def fake_iseq(command, samples_dir, handle, timeout=None):
            self.assertIn("-g", command)
            self.assertEqual(command[command.index("-r") + 1], "https")
            run = command[command.index("-i") + 1]
            for mate in (1, 2):
                self.fastq(f"{run}_{mate}.fastq.gz")
            with (self.directory / "success.log").open("a") as success:
                success.write("Oct 13\t" + run + "\n")

        with patch("prepare_samples.shutil.which", return_value="/bin/iseq"), \
             patch("prepare_samples.check_space", return_value=240), \
             patch("prepare_samples.run_iseq", side_effect=fake_iseq) as download:
            first = prepare("SRP123456", self.directory, self.directory, manifest, log)
            self.assertEqual(list(first["samples"]), runs)
            self.assertEqual(download.call_count, 2)
            second = prepare("SRP123456", self.directory, self.directory, manifest, log)
            self.assertEqual(download.call_count, 2)
            self.assertTrue(all(sample["download_status"] == "reused" for sample in second["samples"].values()))
            self.assertEqual(json.loads(manifest.read_text())["total_runs"], 2)

    @unittest.skipIf(os.name != "posix", "下载编排的 NFS 文件锁需要 Linux")
    def test_failed_download_does_not_publish_manifest(self):
        self.metadata([self.row()])
        manifest = self.directory / "samples.json"
        with patch("prepare_samples.shutil.which", return_value="/bin/iseq"), \
             patch("prepare_samples.check_space", return_value=120), \
             patch("prepare_samples.run_iseq", side_effect=RuntimeError("网络中断")) as download:
            with self.assertRaises(RuntimeError):
                prepare("SRP123456", self.directory, self.directory, manifest,
                        self.directory / "prepare.log", retries=2)
            self.assertEqual(download.call_count, 2)
            self.assertFalse(manifest.exists())


def io_tsv(rows):
    import io
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(rows[0]), delimiter="\t")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def urllib_query(url):
    from urllib.parse import parse_qs, urlsplit
    return parse_qs(urlsplit(url).query)


if __name__ == "__main__":
    unittest.main(verbosity=2)
