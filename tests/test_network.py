import csv
import gzip
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import zlib

import pytest

from teflow import network


def test_one_batch_checks_geo_once_with_optional_metadata_fallback(monkeypatch):
    calls = []
    def probes(urls, environment, timeout, groups):
        calls.append((urls, timeout))
        return {network.GEO: {"reachable": True}, network.ENA: {"reachable": False},
                network.NCBI: {"reachable": True}}
    monkeypatch.setattr(network, "_run_probes", probes)
    network.check_network(["GSE1", "GSE2", "SRR1"], {})
    assert len(calls) == 1
    assert calls[0][0] == [network.GEO, network.ENA, network.NCBI]
    assert calls[0][1] == 5


def test_mandatory_geo_failure_names_website_without_proxy(monkeypatch):
    monkeypatch.setattr(network, "_run_probes", lambda *a: {
        network.GEO: {"reachable": False, "detail": "ConnectionRefusedError 127.0.0.1:17890"},
        network.ENA: {"reachable": True}})
    with pytest.raises(network.NetworkUnavailable) as captured:
        network.check_network(["GSE1"], {})
    assert "无法连接 NCBI GEO（www.ncbi.nlm.nih.gov）" in str(captured.value)
    assert "127.0.0.1" not in str(captured.value)
    assert "127.0.0.1" in captured.value.detail


def test_all_metadata_alternatives_must_fail_to_block(monkeypatch):
    monkeypatch.setattr(network, "_run_probes", lambda *a: {})
    with pytest.raises(network.NetworkUnavailable, match="ENA.*NCBI SRA"):
        network.check_network(["SRR1"], {})


def test_stuck_dns_is_killed_at_total_deadline_and_partial_success_retained(monkeypatch):
    original = subprocess.Popen
    launched = []
    def delayed(*args, **kwargs):
        script = "import time; print('{\"url\": \"ok\", \"reachable\": true}', flush=True); time.sleep(60)"
        process = original([sys.executable, "-c", script], **kwargs)
        launched.append(process)
        return process
    monkeypatch.setattr(network.subprocess, "Popen", delayed)
    started = time.monotonic()
    result = network._run_probes(["ok", "stuck"], dict(os.environ), 0.3, [("ok",), ("stuck",)])
    assert time.monotonic() - started < 1.5
    assert result["ok"]["reachable"]
    assert launched[0].poll() is not None


def test_cached_geo_checks_download_host_instead_of_geo(tmp_path):
    row = {"run_accession": "SRR1", "library_layout": "PAIRED",
           "fastq_ftp": "ftp.sra.ebi.ac.uk/a_1.fastq.gz;ftp.sra.ebi.ac.uk/a_2.fastq.gz"}
    metadata = tmp_path / "GSE1.metadata.tsv"
    with metadata.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row), delimiter="\t")
        writer.writeheader()
        writer.writerow(row)
    marker = tmp_path / ".teflow-GSE1.geo.complete.json"
    marker.write_text(json.dumps({"accession": "GSE1", "version": 1,
                                 "metadata_bytes": metadata.stat().st_size,
                                 "records": {"SRR1": {"row": row}}, "mapping": [{"run": "SRR1"}]}))
    assert network.requirements_for_job("GSE1", tmp_path) == [("https://ftp.sra.ebi.ac.uk/a_1.fastq.gz",)]
    metadata.write_text("invalid cache")
    assert network.requirements_for_job("GSE1", tmp_path) == network.required_groups(["GSE1"])


def test_no_groups_does_not_launch_probe(monkeypatch):
    monkeypatch.setattr(network, "_run_probes", lambda *a: pytest.fail("local work launched probe"))
    network.check_network([], {}, groups=[])


def test_cached_batch_checks_once_per_endpoint_and_keeps_distinct_sites(monkeypatch):
    first = "https://ftp.sra.ebi.ac.uk/vol1/first.fastq.gz"
    second = "https://ftp.sra.ebi.ac.uk:443/vol1/second.fastq.gz"
    third = "https://ftp.sra.ebi.ac.uk/vol1/third.fastq.gz"
    other_port = "https://ftp.sra.ebi.ac.uk:8443/health"
    calls = []
    def probes(urls, environment, timeout, groups):
        calls.append((urls, groups))
        return {url: {"reachable": True} for url in urls}
    monkeypatch.setattr(network, "_run_probes", probes)
    network.check_network(["GSE1", "GSE2", "GSE3"], {},
                          groups=[(first,), (second,), (third,), (network.NCBI, network.ENA), (other_port,)])
    assert calls == [([first, network.NCBI, network.ENA, other_port],
                      [(first,), (network.NCBI, network.ENA), (other_port,)])]


def test_endpoint_alias_keeps_original_failure_website_label(monkeypatch):
    first = "https://www.ncbi.nlm.nih.gov/Traces/"
    monkeypatch.setattr(network, "_run_probes", lambda *a: {network.ENA: {"reachable": True}})
    with pytest.raises(network.NetworkUnavailable, match="NCBI GEO"):
        network.check_network(["GSE1"], {}, groups=[(first, network.ENA), (network.GEO,)])


def test_complete_ena_pair_metadata_and_success_cache_needs_no_network_without_manifest(tmp_path):
    paths = [tmp_path / f"SRR1_{mate}.fastq.gz" for mate in (1, 2)]
    for path in paths:
        with gzip.open(path, "wb") as handle:
            handle.write(b"@read\nACGT\n+\nIIII\n")
    row = {"run_accession": "SRR1", "library_layout": "PAIRED",
           "fastq_ftp": ";".join("ftp.sra.ebi.ac.uk/" + path.name for path in paths),
           "fastq_bytes": ";".join(str(path.stat().st_size) for path in paths)}
    metadata = tmp_path / "SRR1.metadata.tsv"
    with metadata.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row), delimiter="\t")
        writer.writeheader()
        writer.writerow(row)
    marker = tmp_path / ".teflow-SRR1.metadata.complete.json"
    marker.write_text(json.dumps({"accession": "SRR1", "bytes": metadata.stat().st_size,
                                 "runs": ["SRR1"], "run_set_verification": {"version": 1}}))
    (tmp_path / "success.log").write_text("2026 SRR1\n")
    assert network.requirements_for_job("SRR1", tmp_path) == []
    paths[0].write_bytes(b"partial")
    assert network.requirements_for_job("SRR1", tmp_path) == [("https://ftp.sra.ebi.ac.uk/SRR1_1.fastq.gz",)]


def test_corrupt_deflate_cache_is_left_for_downloader_validation(tmp_path, monkeypatch):
    first, second = [tmp_path / f"SRR1_{mate}.fastq.gz" for mate in (1, 2)]
    for path in (first, second):
        path.write_bytes(b"compressed fixture")
    (tmp_path / "success.log").write_text("2026 SRR1\n")
    row = {"fastq_ftp": ";".join("ftp.sra.ebi.ac.uk/" + path.name for path in (first, second)),
           "fastq_bytes": ";".join(str(path.stat().st_size) for path in (first, second))}
    class CorruptGzip:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return False
        def readline(self, *args):
            raise zlib.error("Error -3 while decompressing data: invalid distance too far back")
    monkeypatch.setattr(network.gzip, "open", lambda *args, **kwargs: CorruptGzip())
    assert network._cached_pair_ready("SRR1", row, tmp_path) is False


def test_complete_single_metadata_cache_skips_network_and_missing_read_requires_host(tmp_path):
    path = tmp_path / "SRR1.fastq.gz"
    with gzip.open(path, "wb") as handle:
        handle.write(b"@read\nACGT\n+\nIIII\n")
    row = {"run_accession": "SRR1", "library_layout": "SINGLE",
           "fastq_ftp": "ftp.sra.ebi.ac.uk/" + path.name, "fastq_bytes": str(path.stat().st_size)}
    metadata = tmp_path / "SRR1.metadata.tsv"
    with metadata.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row), delimiter="\t")
        writer.writeheader()
        writer.writerow(row)
    (tmp_path / ".teflow-SRR1.metadata.complete.json").write_text(json.dumps({
        "accession": "SRR1", "bytes": metadata.stat().st_size, "runs": ["SRR1"],
        "run_set_verification": {"version": 1}}))
    (tmp_path / "success.log").write_text("2026 SRR1\n")
    assert network.requirements_for_job("SRR1", tmp_path) == []
    path.unlink()
    assert network.requirements_for_job("SRR1", tmp_path) == [("https://ftp.sra.ebi.ac.uk/SRR1.fastq.gz",)]
