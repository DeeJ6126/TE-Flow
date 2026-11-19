"""Multi-connection AWS download, resume, network waiting and the prepare_samples AWS path."""
import gzip
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import aws_download  # noqa: E402
import prepare_samples  # noqa: E402


class FakeResponse(io.BytesIO):
    status = 206


def range_opener(payload, failures=None):
    """Serve byte ranges of payload; `failures` maps chunk start -> remaining failures."""
    calls = []

    def opener(url, start, end):
        calls.append((start, end))
        if failures and failures.get(start, 0) > 0:
            failures[start] -= 1
            raise OSError("connection reset")
        return FakeResponse(payload[start:end + 1])
    opener.calls = calls
    return opener


class ParallelDownloadTests(unittest.TestCase):
    def setUp(self):
        self.workspace = tempfile.TemporaryDirectory()
        self.directory = Path(self.workspace.name)
        self.payload = bytes(range(256)) * 400  # 102400 bytes
        patcher = patch.object(aws_download.time, "sleep", lambda seconds: None)
        patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self):
        self.workspace.cleanup()

    def test_ranges_are_assembled_exactly(self):
        dest = self.directory / "SRR1.sra"
        opener = range_opener(self.payload)
        aws_download.parallel_download("u", dest, len(self.payload), 4, chunk=10000, opener=opener)
        self.assertEqual(dest.read_bytes(), self.payload)
        self.assertEqual(len(opener.calls), 11)
        self.assertFalse(dest.with_name("SRR1.sra.chunks.json").exists())

    def test_transient_chunk_failure_is_retried(self):
        dest = self.directory / "SRR1.sra"
        opener = range_opener(self.payload, failures={20000: 2})
        aws_download.parallel_download("u", dest, len(self.payload), 3, chunk=10000, opener=opener)
        self.assertEqual(dest.read_bytes(), self.payload)

    def test_outage_keeps_finished_chunks_for_resume(self):
        dest = self.directory / "SRR1.sra"
        size = len(self.payload)
        failing = {start: 10 ** 6 for start in range(50000, size, 10000)}
        with self.assertRaises(aws_download.NetworkDown):
            aws_download.parallel_download("u", dest, size, 1, chunk=10000, chunk_tries=3,
                                           opener=range_opener(self.payload, failing))
        state = json.loads(dest.with_name("SRR1.sra.chunks.json").read_text())
        self.assertEqual(state["done"], [0, 1, 2, 3, 4])
        opener = range_opener(self.payload)
        aws_download.parallel_download("u", dest, size, 2, chunk=10000, opener=opener)
        self.assertEqual(dest.read_bytes(), self.payload)
        self.assertEqual(sorted(start for start, _ in opener.calls), list(range(50000, size, 10000)))

    def test_changed_remote_size_restarts(self):
        dest = self.directory / "SRR1.sra"
        dest.write_bytes(b"x" * 10)
        dest.with_name("SRR1.sra.chunks.json").write_text(json.dumps({"size": 10, "chunk": 10000, "done": [0]}))
        aws_download.parallel_download("u", dest, len(self.payload), 2, chunk=10000,
                                       opener=range_opener(self.payload))
        self.assertEqual(dest.read_bytes(), self.payload)

    def test_overlong_range_response_is_rejected(self):
        dest = self.directory / "SRR1.sra"
        with self.assertRaises(aws_download.NetworkDown):
            aws_download.parallel_download("u", dest, 100, 1, chunk=50, chunk_tries=2,
                                           opener=lambda url, start, end: FakeResponse(b"z" * 80))


class NetworkWaitTests(unittest.TestCase):
    def test_waits_until_probe_answers(self):
        answers = iter([False, False, False, True])
        clock = iter(range(0, 10 ** 6, 60))
        messages = []
        with patch.object(aws_download, "network_ok", lambda probe=None: next(answers)):
            aws_download.wait_for_network(3600, messages.append, sleep=lambda s: None, clock=lambda: next(clock))
        self.assertIn("网络已恢复", messages[-1])

    def test_gives_up_after_max_wait(self):
        clock = iter(range(0, 10 ** 6, 600))
        with patch.object(aws_download, "network_ok", lambda probe=None: False):
            with self.assertRaises(aws_download.NetworkDown):
                aws_download.wait_for_network(1200, lambda m: None, sleep=lambda s: None, clock=lambda: next(clock))

    def test_spot_count_parsing(self):
        self.assertEqual(aws_download.parse_spots("spots read      : 1,234,567\nreads read : 2"), 1234567)
        self.assertIsNone(aws_download.parse_spots("nothing"))


class PrepareAwsTests(unittest.TestCase):
    def setUp(self):
        self.workspace = tempfile.TemporaryDirectory()
        self.run_dir = Path(self.workspace.name)

    def tearDown(self):
        self.workspace.cleanup()

    def fake_convert(self, names):
        def convert(sra, run_dir, run, layout, threads, log, expected_spots=None):
            self.assertTrue(Path(sra).is_file())
            self.assertEqual(expected_spots, 42)
            for name in names:
                with gzip.open(Path(run_dir) / name, "wb") as handle:
                    handle.write(b"@r\nACGT\n+\nIIII\n")
            return names
        return convert

    def test_converted_file_names_follow_layout(self):
        self.assertEqual([f["name"] for f in prepare_samples.converted_files("SRR1", "PAIRED")],
                         ["SRR1_1.fastq.gz", "SRR1_2.fastq.gz"])
        self.assertEqual([f["name"] for f in prepare_samples.converted_files("SRR1", "SINGLE")], ["SRR1.fastq.gz"])

    def test_download_convert_validate_and_mark_success(self):
        record = {"row": {"read_count": "42"}, "library_layout": "PAIRED"}
        files = prepare_samples.converted_files("SRR1", "PAIRED")
        log = io.StringIO()
        with patch.object(aws_download, "remote_size", lambda url: 5), \
                patch.object(aws_download, "parallel_download",
                             lambda url, dest, size, connections, report=None: Path(dest).write_bytes(b"12345")), \
                patch.object(aws_download, "convert_sra", self.fake_convert(["SRR1_1.fastq.gz", "SRR1_2.fastq.gz"])):
            prepare_samples.download_run_aws("SRR1", record, self.run_dir, files, 4, 16, 3, 0, log)
        self.assertFalse((self.run_dir / "SRR1.sra").exists())
        self.assertIn("SRR1", prepare_samples.logged_runs(self.run_dir / "success.log"))
        self.assertTrue(prepare_samples.pair_is_complete(self.run_dir, files))

    def test_not_on_aws_propagates_for_ena_fallback(self):
        def missing(url):
            raise aws_download.NotOnAws("missing")
        with patch.object(aws_download, "remote_size", missing):
            with self.assertRaises(aws_download.NotOnAws):
                prepare_samples.download_run_aws("SRR1", {"row": {}, "library_layout": "SINGLE"}, self.run_dir,
                                                 prepare_samples.converted_files("SRR1", "SINGLE"), 4, 16, 3, 0,
                                                 io.StringIO())

    def test_bad_conversion_is_retried_then_fails(self):
        calls = []

        def broken(*args, **kwargs):
            calls.append(1)
            raise ValueError("spot mismatch")
        with patch.object(aws_download, "remote_size", lambda url: 5), \
                patch.object(aws_download, "parallel_download",
                             lambda url, dest, size, connections, report=None: Path(dest).write_bytes(b"12345")), \
                patch.object(aws_download, "convert_sra", broken):
            with self.assertRaisesRegex(RuntimeError, "3 次 AWS 下载"):
                prepare_samples.download_run_aws("SRR1", {"row": {}, "library_layout": "SINGLE"}, self.run_dir,
                                                 prepare_samples.converted_files("SRR1", "SINGLE"), 4, 16, 3, 0,
                                                 io.StringIO())
        self.assertEqual(len(calls), 3)

    def test_network_outage_waits_without_using_attempts(self):
        outcomes = [aws_download.NetworkDown("down"), aws_download.NetworkDown("down"), None]

        def flaky(url, dest, size, connections, report=None):
            outcome = outcomes.pop(0)
            if outcome:
                raise outcome
            Path(dest).write_bytes(b"12345")
        waits = []
        with patch.object(aws_download, "remote_size", lambda url: 5), \
                patch.object(aws_download, "parallel_download", flaky), \
                patch.object(aws_download, "wait_for_network", lambda *a, **k: waits.append(1)), \
                patch.object(aws_download, "convert_sra", self.fake_convert(["SRR1.fastq.gz"])):
            prepare_samples.download_run_aws("SRR1", {"row": {"read_count": "42"}, "library_layout": "SINGLE"},
                                             self.run_dir, prepare_samples.converted_files("SRR1", "SINGLE"),
                                             4, 16, 1, 600, io.StringIO())
        self.assertGreaterEqual(len(waits), 2)

    def test_metadata_step_waits_for_network_then_retries(self):
        attempts = []

        def action():
            attempts.append(1)
            if len(attempts) == 1:
                raise FileNotFoundError("metadata.tsv")
            return "ok"
        log = io.StringIO()
        with patch.object(aws_download, "network_ok", lambda probe=None: False), \
                patch.object(aws_download, "wait_for_network", lambda *a, **k: None):
            self.assertEqual(prepare_samples.with_network(action, 600, log), "ok")
        self.assertEqual(len(attempts), 2)
        with self.assertRaises(FileNotFoundError):
            prepare_samples.with_network(lambda: (_ for _ in ()).throw(FileNotFoundError("x")), 0, log)

    def test_transient_run_set_mismatch_is_retried_then_raised(self):
        attempts = []

        def flaky():
            attempts.append(1)
            if len(attempts) < 3:
                raise prepare_samples.RunSetMismatch("Run 查询返回了其他编号")
            return "ok"
        sleeps = []
        with patch.object(aws_download, "network_ok", lambda probe=None: True):
            self.assertEqual(prepare_samples.with_network(flaky, 600, io.StringIO(), sleep=sleeps.append), "ok")
            self.assertEqual(sleeps, [30, 60])
            with self.assertRaises(prepare_samples.RunSetMismatch):
                prepare_samples.with_network(lambda: (_ for _ in ()).throw(prepare_samples.RunSetMismatch("x")),
                                             600, io.StringIO(), sleep=lambda s: None)
            with self.assertRaises(KeyError):  # unrelated errors are not retried
                prepare_samples.with_network(lambda: {}["x"], 600, io.StringIO(), sleep=lambda s: None)

    def ena_record(self, payload):
        import hashlib
        return {"row": {"fastq_ftp": "ftp.sra.ebi.ac.uk/vol1/fastq/SRR1/SRR1.fastq.gz",
                        "fastq_md5": hashlib.md5(payload).hexdigest(), "fastq_bytes": str(len(payload))},
                "library_layout": "SINGLE"}

    def gz(self):
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode="wb") as handle:
            handle.write(b"@r\nACGT\n+\nIIII\n")
        return buffer.getvalue()

    def test_ena_fallback_downloads_with_connections_and_checks_md5(self):
        payload = self.gz()
        record = self.ena_record(payload)
        files = [{"name": "SRR1.fastq.gz", "bytes": len(payload), "mate": 1, "source": "ena-fastq"}]
        urls = []

        def fake(url, dest, size, connections, report=None):
            urls.append((url, connections))
            Path(dest).write_bytes(payload)
        with patch.object(aws_download, "parallel_download", fake):
            self.assertTrue(prepare_samples.download_run_ena_parallel("SRR1", record, self.run_dir, files, 16, 3, 0,
                                                                      io.StringIO()))
        self.assertEqual(urls, [("https://ftp.sra.ebi.ac.uk/vol1/fastq/SRR1/SRR1.fastq.gz", 16)])
        self.assertIn("SRR1", prepare_samples.logged_runs(self.run_dir / "success.log"))

    def test_ena_fallback_rejects_md5_mismatch_and_skips_without_links(self):
        payload = self.gz()
        record = self.ena_record(payload)
        record["row"]["fastq_md5"] = "0" * 32
        files = [{"name": "SRR1.fastq.gz", "bytes": len(payload), "mate": 1, "source": "ena-fastq"}]
        with patch.object(aws_download, "parallel_download",
                          lambda url, dest, size, connections, report=None: Path(dest).write_bytes(payload)):
            with self.assertRaisesRegex(RuntimeError, "ENA 下载"):
                prepare_samples.download_run_ena_parallel("SRR1", record, self.run_dir, files, 4, 2, 0, io.StringIO())
        converted = prepare_samples.converted_files("SRR1", "SINGLE")
        self.assertFalse(prepare_samples.download_run_ena_parallel("SRR1", {"row": {}}, self.run_dir, converted,
                                                                   4, 2, 0, io.StringIO()))


if __name__ == "__main__":
    unittest.main()
