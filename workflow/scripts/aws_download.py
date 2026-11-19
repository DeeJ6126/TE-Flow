"""Multi-connection, resumable download of NCBI SRA runs from the public AWS bucket.

The campus link limits each TCP connection (tens of KB/s) but not their sum, so one
file is fetched as fixed-size byte ranges by several connections at once. Only the
Python standard library is used; urllib honours the http(s)_proxy environment, so the
same proxy tunnel as the rest of the workflow applies.

Resume: completed chunk indices are recorded next to the target file. A restarted
download keeps finished chunks and refetches the rest.
"""
from __future__ import annotations

import json
import os
import queue
import re
import shutil
import subprocess
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

AWS_TEMPLATE = "https://sra-pub-run-odp.s3.amazonaws.com/sra/{run}/{run}"
NETWORK_PROBE = "https://sra-pub-run-odp.s3.amazonaws.com/"
USER_AGENT = "TEflow/2.7"
CHUNK_BYTES = 16 * 1024 ** 2
BLOCK_BYTES = 256 * 1024


class NotOnAws(Exception):
    """The run has no public object in the AWS bucket; another source must be used."""


class NetworkDown(Exception):
    """Every connection keeps failing; the caller should wait for the network."""


def aws_url(run: str) -> str:
    return AWS_TEMPLATE.format(run=run)


def _open(url: str, method="GET", headers=None, timeout=60):
    request = urllib.request.Request(url, method=method, headers={"User-Agent": USER_AGENT, **(headers or {})})
    return urllib.request.urlopen(request, timeout=timeout)


def network_ok(probe: str = NETWORK_PROBE, timeout=20) -> bool:
    try:
        with _open(probe, method="HEAD", timeout=timeout):
            return True
    except urllib.error.HTTPError:
        return True  # The server answered: the network path works.
    except Exception:
        return False


def wait_for_network(max_wait: float, log=print, probe: str = NETWORK_PROBE, interval=60,
                     sleep=time.sleep, clock=time.monotonic) -> None:
    """Block until the probe answers; raise NetworkDown after max_wait seconds."""
    if network_ok(probe):
        return
    start = clock()
    log(f"网络不可用（{probe}），每 {interval} 秒检查一次，最多等待 {max_wait / 3600:.1f} 小时。")
    while clock() - start < max_wait:
        sleep(interval)
        if network_ok(probe):
            log(f"网络已恢复，等待了 {(clock() - start) / 60:.0f} 分钟。")
            return
    raise NetworkDown(f"等待 {max_wait / 3600:.1f} 小时后网络仍不可用：{probe}")


def remote_size(url: str, timeout=60) -> int:
    try:
        with _open(url, method="HEAD", timeout=timeout) as response:
            length = response.headers.get("Content-Length")
    except urllib.error.HTTPError as exc:
        if exc.code in (403, 404):
            raise NotOnAws(f"AWS 上没有该 Run（HTTP {exc.code}）：{url}") from exc
        raise
    if not length or not length.isdigit() or int(length) <= 0:
        raise NotOnAws(f"AWS 未返回有效文件大小：{url}")
    return int(length)


def _load_state(path: Path, size: int, chunk: int) -> set[int]:
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
        if state.get("size") == size and state.get("chunk") == chunk:
            return {int(i) for i in state.get("done", [])}
    except (OSError, ValueError, TypeError):
        pass
    return set()


def _save_state(path: Path, size: int, chunk: int, done: set[int]) -> None:
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps({"size": size, "chunk": chunk, "done": sorted(done)}), encoding="utf-8")
    os.replace(temporary, path)


def parallel_download(url: str, dest: Path, size: int, connections: int, *, chunk=CHUNK_BYTES,
                      chunk_tries=8, report=None, report_every=60.0, opener=None) -> None:
    """Fetch url into dest with `connections` concurrent range requests.

    Raises NetworkDown when chunks keep failing (finished chunks are kept for resume).
    `opener(url, start, end)` may be injected by tests; it returns a readable response.
    """
    if connections < 1:
        raise ValueError("下载连接数必须大于零。")
    dest = Path(dest)
    state_path = dest.with_name(dest.name + ".chunks.json")
    total = (size + chunk - 1) // chunk
    done = _load_state(state_path, size, chunk)
    if not done or not dest.is_file() or dest.stat().st_size != size:
        done = set()
        with dest.open("wb") as handle:
            handle.truncate(size)
        _save_state(state_path, size, chunk, done)
    pending: queue.Queue[int] = queue.Queue()
    for index in range(total):
        if index not in done:
            pending.put(index)
    lock = threading.Lock()
    received = [sum(min(chunk, size - i * chunk) for i in done)]
    failures = {"consecutive": 0, "fatal": None}
    stop = threading.Event()

    def fetch(start, end):
        if opener:
            return opener(url, start, end)
        return _open(url, headers={"Range": f"bytes={start}-{end}"}, timeout=60)

    def worker():
        with dest.open("r+b") as handle:
            while not stop.is_set():
                try:
                    index = pending.get_nowait()
                except queue.Empty:
                    return
                start = index * chunk
                end = min(size, start + chunk) - 1
                for attempt in range(1, chunk_tries + 1):
                    if stop.is_set():
                        return
                    got = 0
                    try:
                        with fetch(start, end) as response:
                            status = getattr(response, "status", 206)
                            if status not in (206, 200) or (status == 200 and (start, end) != (0, size - 1)):
                                raise OSError(f"服务器不支持分段下载（HTTP {status}）")
                            handle.seek(start)
                            while True:
                                block = response.read(BLOCK_BYTES)
                                if not block:
                                    break
                                if got + len(block) > end - start + 1:
                                    raise OSError("分段返回的数据超出请求范围")
                                handle.write(block)
                                got += len(block)
                                with lock:
                                    received[0] += len(block)
                        if got != end - start + 1:
                            raise OSError(f"分段不完整：{got}/{end - start + 1} 字节")
                        handle.flush()
                        with lock:
                            done.add(index)
                            failures["consecutive"] = 0
                            _save_state(state_path, size, chunk, done)
                        break
                    except Exception as exc:  # network errors of any kind: retry this chunk
                        with lock:
                            received[0] -= got
                            failures["consecutive"] += 1
                            # Every connection failing several times in a row: the link is down.
                            if failures["consecutive"] >= max(3 * connections, 12):
                                failures["fatal"] = exc
                                stop.set()
                        if attempt == chunk_tries:
                            with lock:
                                failures["fatal"] = failures["fatal"] or exc
                            stop.set()
                            return
                        time.sleep(min(60, 2 ** attempt))

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(min(connections, total) or 1)]
    began, last = time.monotonic(), time.monotonic()
    start_bytes = received[0]
    for thread in threads:
        thread.start()
    while any(thread.is_alive() for thread in threads):
        for thread in threads:
            thread.join(timeout=1.0)
        now = time.monotonic()
        if report and now - last >= report_every:
            last = now
            with lock:
                current = received[0]
            report(current, size, (current - start_bytes) / max(now - began, 1e-9))
    if len(done) != total:
        raise NetworkDown(f"下载中断，已完成 {len(done)}/{total} 段，可续传：{failures['fatal']}")
    if dest.stat().st_size != size:
        raise OSError(f"下载文件大小不符：{dest.stat().st_size} != {size}")
    state_path.unlink(missing_ok=True)


def parse_spots(text: str) -> int | None:
    match = re.search(r"spots read\s*:\s*([\d,]+)", text)
    return int(match.group(1).replace(",", "")) if match else None


def convert_sra(sra: Path, run_dir: Path, run: str, layout: str, threads: int, log_handle=None,
                expected_spots: int | None = None) -> list[str]:
    """fasterq-dump --split-3 (technical reads skipped, as in ENA FASTQ) → pigz.

    Returns the gzip file names in mate order; removes orphan reads of paired runs.
    When the metadata read count is known, fasterq-dump's spot count must match it.
    """
    run_dir = Path(run_dir)
    scratch = run_dir / ".fasterq-tmp"
    scratch.mkdir(exist_ok=True)
    expected = [f"{run}_1.fastq", f"{run}_2.fastq"] if layout == "PAIRED" else [f"{run}.fastq"]
    for name in [f"{run}.fastq", f"{run}_1.fastq", f"{run}_2.fastq", f"{run}_3.fastq"]:
        (run_dir / name).unlink(missing_ok=True)
        (run_dir / (name + ".gz")).unlink(missing_ok=True)
    result = subprocess.run(["fasterq-dump", "--split-3", "-e", str(threads), "-t", str(scratch),
                             "-O", str(run_dir), str(sra)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, errors="replace")
    shutil.rmtree(scratch, ignore_errors=True)
    if log_handle is not None:
        log_handle.write(result.stdout)
        log_handle.flush()
    if result.returncode:
        raise ValueError(f"{run}: fasterq-dump 退出码 {result.returncode}。")
    spots = parse_spots(result.stdout)
    if expected_spots and spots is not None and spots != expected_spots:
        for name in run_dir.glob(f"{run}*.fastq"):
            name.unlink(missing_ok=True)
        raise ValueError(f"{run}: SRA 转换得到 {spots} 条 spot，元数据为 {expected_spots}，文件可能不完整。")
    produced = sorted(path.name for path in run_dir.glob(f"{run}*.fastq"))
    if layout == "PAIRED":
        (run_dir / f"{run}.fastq").unlink(missing_ok=True)  # unpaired orphans
        produced = [name for name in produced if name != f"{run}.fastq"]
    elif produced == [f"{run}_1.fastq"]:
        (run_dir / produced[0]).rename(run_dir / f"{run}.fastq")
        produced = [f"{run}.fastq"]
    if produced != expected:
        for name in produced:
            (run_dir / name).unlink(missing_ok=True)
        raise ValueError(f"{run}: SRA 转换结果与 {layout} 不符，得到 {produced or '无文件'}。")
    output = log_handle if log_handle is not None else subprocess.DEVNULL
    subprocess.run(["pigz", "-f", "-p", str(threads), *[str(run_dir / name) for name in expected]],
                   check=True, stdout=output, stderr=subprocess.STDOUT)
    return [name + ".gz" for name in expected]
