"""Bounded lightweight website preflight; ongoing requests remain authoritative.

DNS resolution can ignore Python socket timeouts. Probes therefore run in one
short-lived child process, with the entire batch bounded by the parent deadline.
No background executor is left waiting for a DNS or proxy connection.
"""
from __future__ import annotations

import json
import csv
import gzip
from pathlib import Path
import subprocess
import sys
import time
import zlib
from urllib.parse import urlsplit, unquote

from .failures import KnownFailure, website_name


GEO = "https://www.ncbi.nlm.nih.gov/geo/"
ENA = "https://www.ebi.ac.uk/ena/portal/api/"
NCBI = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/einfo.fcgi?db=sra"


class NetworkUnavailable(KnownFailure):
    pass


def required_groups(accessions: list[str]) -> list[tuple[str, ...]]:
    # GEO itself is required for exact Series/GSM mapping. Run metadata has two
    # interchangeable sources: failure of either alone must not block work.
    groups = [(ENA, NCBI)]
    if any(accession.startswith(("GSE", "GSM")) for accession in accessions):
        groups.insert(0, (GEO,))
    return groups


def requirements_for_job(accession: str, raw_dir: Path) -> list[tuple[str, ...]]:
    """A complete metadata cache can proceed straight to its actual file hosts."""
    metadata = raw_dir / f"{accession}.metadata.tsv"
    geo = accession.startswith("GSE")
    marker = raw_dir / (f".teflow-{accession}.geo.complete.json" if geo
                        else f".teflow-{accession}.metadata.complete.json")
    try:
        if metadata.stat().st_size > 8 * 1024 * 1024 or marker.stat().st_size > 8 * 1024 * 1024:
            return required_groups([accession])
        cached = json.loads(marker.read_text(encoding="utf-8"))
        with metadata.open(encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle, delimiter="\t"))
        runs = [row.get("run_accession", row.get("Run")) for row in rows]
        if cached.get("accession") != accession or not rows:
            return required_groups([accession])
        if any(str(row.get("library_layout", row.get("LibraryLayout", ""))).upper() != "PAIRED" for row in rows):
            return required_groups([accession])
        if geo:
            records = cached.get("records", {})
            valid = (cached.get("version") == 1 and cached.get("metadata_bytes") == metadata.stat().st_size
                     and list(records) == runs and cached.get("mapping")
                     and all(records[run]["row"] == row for run, row in zip(runs, rows)))
        else:
            valid = (cached.get("bytes") == metadata.stat().st_size and cached.get("runs") == runs
                     and cached.get("run_set_verification", {}).get("version") == 1)
        if not valid:
            return required_groups([accession])
        by_host = {}
        for row in rows:
            run = row.get("run_accession", row.get("Run", ""))
            run_dir = raw_dir / run if geo else raw_dir
            if _cached_pair_ready(run, row, run_dir):
                continue
            for url in str(row.get("fastq_ftp", "")).split(";"):
                if not url.strip():
                    continue
                url = url.strip() if "://" in url else "https://" + url.strip()
                by_host.setdefault(urlsplit(url).hostname, (url,))
            if not str(row.get("fastq_ftp", "")).strip():
                by_host.setdefault("eutils.ncbi.nlm.nih.gov", (NCBI,))
        # SRA conversion without ENA FASTQ URLs still resolves its archive from
        # NCBI. Do not require GEO again when its own mapping is already cached.
        return list(by_host.values())
    except (OSError, ValueError, KeyError, TypeError):
        return required_groups([accession])


def _cached_pair_ready(run: str, row: dict, raw_dir: Path) -> bool:
    """Match the existing downloader cache marker without extra checksum scans."""
    import re
    try:
        success = raw_dir / "success.log"
        if success.stat().st_size > 1024 * 1024:
            return False
        if not any(line.split() and line.split()[-1] == run for line in
                   success.read_text(encoding="utf-8", errors="replace").splitlines()):
            return False
        links = [url.strip() for url in str(row.get("fastq_ftp", "")).split(";") if url.strip()]
        sizes = str(row.get("fastq_bytes", "")).split(";")
        # Converted SRA needs a full gzip EOF check in the real downloader, so
        # only ENA pairs with independently known compressed lengths skip here.
        if not links or len(links) != len(sizes):
            return False
        mates = {}
        for url, size in zip(links, sizes):
            name = Path(unquote(urlsplit(url if "://" in url else "https://" + url).path)).name
            match = re.search(r"(?:^|[_.-])(?:R)?([12])(?:_\d+)?\.(?:fastq|fq)\.gz$", name, re.IGNORECASE)
            if match:
                if match[1] in mates or not size.isdigit():
                    return False
                path = raw_dir / name
                if not path.is_file() or path.stat().st_size != int(size) or not int(size):
                    return False
                with gzip.open(path, "rb") as stream:
                    header, sequence, separator, quality = [stream.readline(8192) for _ in range(4)]
                if (not header.startswith(b"@") or not separator.startswith(b"+") or not sequence.strip()
                        or len(sequence.rstrip(b"\r\n")) != len(quality.rstrip(b"\r\n"))):
                    return False
                mates[match[1]] = path
        return set(mates) == {"1", "2"}
    except (OSError, EOFError, ValueError, zlib.error):
        return False


def _results(stdout: str | bytes | None) -> dict[str, dict]:
    values = {}
    if isinstance(stdout, bytes):
        stdout = stdout.decode("utf-8", errors="replace")
    for line in (stdout or "").splitlines():
        try:
            record = json.loads(line)
            if isinstance(record, dict) and isinstance(record.get("url"), str):
                values[record["url"]] = record
        except (ValueError, TypeError):
            pass
    return values


def _run_probes(urls: list[str], environment: dict, timeout: float,
                groups: list[tuple[str, ...]]) -> dict[str, dict]:
    started = time.monotonic()
    process = subprocess.Popen(
        [sys.executable, "-m", "teflow.network", "--probe", json.dumps({"urls": urls, "groups": groups}), str(timeout)],
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env=environment, cwd=Path(__file__).resolve().parent.parent,
        text=True, encoding="utf-8", errors="replace")
    try:
        stdout, _ = process.communicate(timeout=max(0.001, timeout - (time.monotonic() - started)))
    except subprocess.TimeoutExpired as exc:
        # communicate() after kill only drains closed pipes; it performs no DNS.
        process.kill()
        stdout, _ = process.communicate()
        if not stdout:
            stdout = exc.output
    return _results(stdout)


def check_network(accessions: list[str], environment: dict, timeout: float = 5.0,
                  *, groups: list[tuple[str, ...]] | None = None) -> None:
    groups = required_groups(accessions) if groups is None else groups
    if not groups:
        return
    # Cached jobs carry per-file URLs. One lightweight request per endpoint is
    # sufficient even when a batch contains thousands of FASTQs on that host.
    endpoints, aliases = {}, {}
    for group in groups:
        for url in group:
            parsed = urlsplit(url)
            scheme = parsed.scheme.lower()
            endpoint = (scheme, parsed.hostname, parsed.port or {"http": 80, "https": 443}.get(scheme))
            aliases[url] = endpoints.setdefault(endpoint, url)
    probe_groups = list(dict.fromkeys(tuple(dict.fromkeys(aliases[url] for url in group)) for group in groups))
    results = _run_probes(list(endpoints.values()), environment, timeout, probe_groups)
    failed = [group for group in groups if not any(results.get(aliases[url], {}).get("reachable") for url in group)]
    if not failed:
        return
    group = failed[0]
    labels = [f"{name}（{host}）" for name, host in (website_name(url) for url in group)]
    detail = "\n".join(f"{url}: {results.get(aliases[url], {}).get('detail', f'{timeout:g}s 内未收到响应')}" for url in group)
    raise NetworkUnavailable("website_connection", "无法连接 " + "、".join(labels) + "，请检查网络后重试。",
                             detail=detail, website="、".join(website_name(url)[0] for url in group),
                             host=",".join(website_name(url)[1] for url in group))


def _probe_main(plan: dict, timeout: float) -> None:
    import queue
    import threading
    import urllib.error
    import urllib.request

    events = queue.Queue()

    def probe(url):
        try:
            request = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "TEflow network check"})
            with urllib.request.urlopen(request, timeout=timeout):
                pass
            events.put({"url": url, "reachable": True})
        except urllib.error.HTTPError as exc:
            # Even 404/405 proves that the website is reachable. Content, access
            # and service errors are handled by the real metadata request.
            events.put({"url": url, "reachable": True, "detail": f"HTTP {exc.code}"})
        except Exception as exc:
            events.put({"url": url, "reachable": False, "detail": f"{type(exc).__name__}: {exc}"})

    urls, groups = plan["urls"], plan["groups"]
    for url in urls:
        threading.Thread(target=probe, args=(url,), daemon=True).start()
    deadline = time.monotonic() + timeout
    reached = set()
    for _ in urls:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            result = events.get(timeout=remaining)
            print(json.dumps(result, ensure_ascii=False), flush=True)
            if result["reachable"]:
                reached.add(result["url"])
            if all(any(url in reached for url in group) for group in groups):
                return
        except queue.Empty:
            break


if __name__ == "__main__" and len(sys.argv) == 4 and sys.argv[1] == "--probe":
    _probe_main(json.loads(sys.argv[2]), float(sys.argv[3]))
