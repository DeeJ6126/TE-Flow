from __future__ import annotations

import contextlib
import enum
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml

from .data_types import validate_data_type
from .inputs import normalize_accession, parse_inputs
from .resources import available_capacity, detect_budget

try:
    import fcntl
except ImportError:
    fcntl = None


class Stage(str, enum.Enum):
    PREPARING = "metadata"
    METADATA = "metadata"
    DOWNLOAD = "download"
    ANALYZE = "analyze"
    QC = "qc"
    ALIGN = "align"
    COUNT = "count"
    MERGE = "merge"
    DONE = "done"


ACTIVE = {"queued", "running"}
TERMINAL = {"succeeded", "failed", "cancelled", "interrupted"}
PROXY_KEYS = {"http_proxy", "https_proxy", "all_proxy", "no_proxy"}


def timestamp() -> str:
    return datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds")


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o664)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def pid_alive(pid: int | None) -> bool:
    if not pid:
        return False
    if os.name == "nt":
        import psutil
        return psutil.pid_exists(int(pid))
    try:
        stat = Path(f"/proc/{int(pid)}/stat")
        if stat.exists() and stat.read_text().rsplit(")", 1)[1].split()[0] == "Z":
            return False
        os.kill(int(pid), 0)
        return True
    except PermissionError:
        return True
    except (ProcessLookupError, OSError, ValueError):
        return False


def proc_start(pid: int | None) -> str | None:
    try:
        return Path(f"/proc/{int(pid)}/stat").read_text().rsplit(")", 1)[1].split()[19]
    except (OSError, ValueError, TypeError, IndexError):
        return None


def owned_pid_alive(pid, expected_start=None) -> bool:
    if not pid_alive(pid):
        return False
    actual = proc_start(pid)
    return not (expected_start and actual and actual != expected_start)


def worker_environment(settings: dict) -> dict:
    environment = dict(os.environ)
    prefix = str(settings["environment"])
    environment["PATH"] = str(Path(prefix) / "bin") + os.pathsep + environment.get("PATH", os.defpath)
    environment["CONDA_PREFIX"] = prefix
    environment["CONDA_DEFAULT_ENV"] = "TE"
    environment.setdefault("TERM", "xterm-256color")
    environment["PYTHONUNBUFFERED"] = "1"
    environment["TZ"] = "Asia/Shanghai"
    proxy_file = Path(settings.get("proxy_env", "/home/codex/server-213-setup/proxy.env"))
    if proxy_file.is_file():
        for line in proxy_file.read_text().splitlines():
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            for word in shlex.split(line, comments=True):
                if "=" in word:
                    key, value = word.split("=", 1)
                    if key.lower() in PROXY_KEYS:
                        environment[key] = value
    return environment


def is_run(accession: str) -> bool:
    return bool(re.fullmatch(r"[SED]RR\d+", accession))


def count_path(job: dict, run: str, sample: dict | None = None) -> Path:
    if sample and sample.get("count_path"):
        return Path(sample["count_path"])
    base = Path(job["job_dir"])
    if job["accession"].startswith("GSE"):
        base /= run
    return base / "abundance" / f"{run}.cntTable"


def raw_manifest_valid(job: dict) -> bool:
    try:
        document = json.loads((Path(job["job_dir"]) / "samples.json").read_text())
        if document.get("accession") != job["accession"] or not document.get("samples"):
            return False
        for sample in document["samples"].values():
            expected = {item["name"]: item.get("bytes") for item in sample.get("files", [])}
            for mate in ("r1", "r2"):
                path = Path(sample[mate])
                if not path.is_file() or not path.stat().st_size:
                    return False
                size = expected.get(path.name)
                if size is not None and path.stat().st_size != size:
                    return False
        return True
    except (OSError, ValueError, KeyError, TypeError):
        return False


class JobManager:
    def __init__(self, settings_path: str | Path | None = None):
        self.app_dir = Path(__file__).resolve().parent.parent
        self.settings_path = Path(settings_path or os.environ.get("TEFLOW_SETTINGS", self.app_dir / "settings.yaml")).resolve()
        self.settings = yaml.safe_load(self.settings_path.read_text(encoding="utf-8"))
        if not isinstance(self.settings, dict):
            raise ValueError("settings.yaml 格式错误。")
        for key in ("raw_root", "output_root", "template_dir", "environment"):
            self.settings[key] = str(Path(self.settings[key]).expanduser().resolve())
        self.settings["threads"] = int(self.settings.get("threads", 10))
        self.settings["mem_mb"] = int(self.settings.get("mem_mb", 256000))
        if self.settings["threads"] < 1 or self.settings["mem_mb"] < 200000:
            raise ValueError("threads 必须大于零，单工作流内存预算至少 200000 MB，以容纳模板的 STAR 配置。")
        self.state_dir = Path(self.settings.get("state_dir", self.app_dir / "state")).resolve()
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.registry_path = self.state_dir / "jobs.json"

    @contextlib.contextmanager
    def _lock(self):
        with (self.state_dir / "jobs.lock").open("a+") as handle:
            if os.name != "nt" and os.fstat(handle.fileno()).st_uid == os.getuid():
                os.fchmod(handle.fileno(), 0o664)
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            else:
                import msvcrt
                handle.seek(0)
                if not handle.read(1):
                    handle.write("0")
                    handle.flush()
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                if fcntl is not None:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                else:
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)

    def _load(self) -> dict:
        if not self.registry_path.exists():
            return {"jobs": {}}
        registry = json.loads(self.registry_path.read_text())
        if not isinstance(registry.get("jobs"), dict):
            raise RuntimeError("任务记录格式错误，请保留并检查 state/jobs.json。")
        for job in registry["jobs"].values():
            job.setdefault("data_type", "bulk")
            if job["status"] in ACTIVE:
                job.setdefault("cores", self.settings["threads"])
                job.setdefault("mem_mb", self.settings["mem_mb"])
                if job["status"] == "queued" and owned_pid_alive(job.get("pid"), job.get("pid_start")):
                    job["status"] = "running"
        return registry

    def _save(self, registry: dict) -> None:
        atomic_json(self.registry_path, registry)

    def _job_save(self, job: dict) -> None:
        atomic_json(Path(job["job_dir"]) / "job.json", job)

    def _refresh(self, registry: dict) -> bool:
        changed = False
        for job in registry["jobs"].values():
            if job["status"] == "running":
                worker = owned_pid_alive(job.get("pid"), job.get("pid_start"))
                snake = owned_pid_alive(job.get("snakemake_pid"), job.get("snakemake_pid_start"))
                if not worker and not snake:
                    job.update(status="interrupted", error="后台进程已结束，结果未确认完成；可重新提交该编号恢复。",
                               updated_at=timestamp())
                    self._job_save(job)
                    changed = True
            if job["status"] in ACTIVE:
                try:
                    manifest = json.loads((Path(job["job_dir"]) / "samples.json").read_text())
                    samples = manifest.get("samples", {})
                    if manifest.get("accession") == job["accession"] and samples:
                        analyzed = sum(count_path(job, run, sample).is_file() and
                                       count_path(job, run, sample).stat().st_size > 0
                                       for run, sample in samples.items())
                        progress = {"total_runs": len(samples), "downloaded_runs": len(samples),
                                    "analyzed_runs": analyzed}
                        if any(job.get(k) != v for k, v in progress.items()):
                            job.update(progress)
                            self._job_save(job)
                            changed = True
                except (OSError, ValueError, KeyError, TypeError):
                    pass
        return changed

    def _ensure_scheduler(self, registry: dict) -> None:
        if not any(job["status"] == "queued" for job in registry["jobs"].values()):
            return
        daemon = registry.get("scheduler", {})
        if owned_pid_alive(daemon.get("pid"), daemon.get("pid_start")):
            return
        try:
            with (self.state_dir / "scheduler.log").open("a", encoding="utf-8") as output:
                process = subprocess.Popen(
                    [str(Path(self.settings["environment"]) / "bin" / "python"), "-m", "teflow.scheduler",
                     "--settings", str(self.settings_path)],
                    stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT,
                    env=worker_environment(self.settings), cwd=self.app_dir, start_new_session=True)
        except OSError as exc:
            for job in registry["jobs"].values():
                if job["status"] == "queued":
                    job.update(status="failed", error=f"调度器启动失败：{exc}", exit_code=1,
                               updated_at=timestamp(), finished_at=timestamp())
                    self._job_save(job)
            registry["scheduler"] = {"error": str(exc)}
            self._save(registry)
            return
        registry["scheduler"] = {"pid": process.pid, "pid_start": proc_start(process.pid),
                                 "started_at": timestamp()}
        self._save(registry)

    def list_jobs(self) -> list[dict]:
        with self._lock():
            registry = self._load()
            if self._refresh(registry):
                self._save(registry)
            self._ensure_scheduler(registry)
            return sorted((dict(job) for job in registry["jobs"].values()),
                          key=lambda job: job["created_at"], reverse=True)

    def get_job(self, accession: str) -> dict | None:
        return next((job for job in self.list_jobs() if job["accession"] == accession.upper()), None)

    def update(self, accession: str, **values) -> dict:
        with self._lock():
            registry = self._load()
            job = registry["jobs"][accession]
            job.update(values, updated_at=timestamp())
            self._save(registry)
            self._job_save(job)
            return dict(job)

    def submit(self, accession: str, *, data_type: str | None = None) -> dict:
        return self.submit_many(accession, input_mode="accessions", data_type=data_type)[0]

    def submit_many(self, source: str, input_mode: str = "auto", cores: int | None = None,
                    *, data_type: str | None = None) -> list[dict]:
        data_type = validate_data_type(data_type)
        accessions = parse_inputs(source, input_mode)
        if cores is not None and (isinstance(cores, bool) or int(cores) < 1):
            raise ValueError("cores 必须大于零。")
        for accession in accessions:
            if (Path(self.settings["output_root"]) / accession).resolve() == Path(self.settings["template_dir"]).resolve():
                raise ValueError("该编号与模板目录同名，请使用真实测序编号。")
        with self._lock():
            registry = self._load()
            self._refresh(registry)
            reusable = {}
            for accession in accessions:
                old = registry["jobs"].get(accession)
                if old and old["data_type"] != data_type:
                    raise ValueError(f"{accession} 已记录为 {old['data_type']} 类型，无法按 {data_type} 提交。")
                if old and old["status"] in ACTIVE:
                    reusable[accession] = dict(old)
                elif old and old["status"] == "succeeded":
                    result = Path(old["result_path"])
                    if result.is_file() and result.stat().st_size and raw_manifest_valid(old):
                        reusable[accession] = dict(old)
            if len(reusable) == len(accessions):
                self._save(registry)
                self._ensure_scheduler(registry)
                return [reusable[accession] for accession in accessions]
            active = any(j["status"] in ACTIVE for j in registry["jobs"].values())
            if not active or "budget" not in registry:
                registry["budget"] = detect_budget(self.settings)
                registry["budget"]["detected_at"] = timestamp()
            budget = registry["budget"]
            allocated_cores = int(cores) if cores is not None else min(self.settings["threads"], budget["cores"])
            if allocated_cores > budget["cores"]:
                raise ValueError(f"每项目请求 {allocated_cores} CPU，超过总预算 {budget['cores']}。")
            if self.settings["mem_mb"] > budget["mem_mb"]:
                raise ValueError(f"当前总内存预算 {budget['mem_mb']} MB，无法容纳模板所需的 {self.settings['mem_mb']} MB。")
            batch_id = uuid.uuid4().hex[:12]
            jobs = []
            for accession in accessions:
                existing = registry["jobs"].get(accession)
                if existing and existing["status"] in ACTIVE:
                    jobs.append(dict(existing))
                    continue
                if existing and existing["status"] == "succeeded":
                    result = Path(existing["result_path"])
                    if result.is_file() and result.stat().st_size and raw_manifest_valid(existing):
                        jobs.append(dict(existing))
                        continue
                job_dir = Path(self.settings["output_root"]) / accession
                raw_dir = Path(self.settings["raw_root"]) / accession
                job_dir.mkdir(parents=True, exist_ok=True)
                raw_dir.mkdir(parents=True, exist_ok=True)
                result = job_dir / "abundance" / f"{accession}.cntTable" if is_run(accession) else job_dir / "merged.count"
                job = {"accession": accession, "data_type": data_type,
                       "status": "queued", "stage": "metadata", "step": "等待资源",
                       "pid": None, "snakemake_pid": None, "job_dir": str(job_dir), "raw_dir": str(raw_dir),
                       "result_path": str(result), "total_runs": 0, "downloaded_runs": 0, "analyzed_runs": 0,
                       "created_at": existing["created_at"] if existing else timestamp(),
                       "queued_at": timestamp(), "updated_at": timestamp(), "error": "",
                       "attempt": existing.get("attempt", 0) + 1 if existing else 1,
                       "batch_id": batch_id, "cores": allocated_cores, "mem_mb": self.settings["mem_mb"],
                       "queue_wait_reason": "调度器将按共享 CPU / 内存预算自动启动。"}
                registry["jobs"][accession] = job
                self._job_save(job)
                jobs.append(dict(job))
            self._save(registry)
            self._ensure_scheduler(registry)
            return jobs

    def log_tail(self, accession: str, lines: int = 100) -> str:
        job = self.get_job(accession)
        if not job:
            return ""
        names = ["worker.log", "logs/prepare_samples.log", "run.log"]
        if job.get("stage") == "download":
            names = ["worker.log", "run.log", "logs/prepare_samples.log"]
        sample = job.get("current_sample")
        suffix = {"qc": "fastp", "align": "STAR", "count": "TEcount",
                  "质控": "fastp", "比对": "STAR", "计数": "TEcount"}.get(job.get("step")) or {
                      "qc": "fastp", "align": "STAR", "count": "TEcount"}.get(job.get("stage"))
        if sample and suffix and re.fullmatch(r"[SED]RR\d+", sample):
            base = f"{sample}/" if job["accession"].startswith("GSE") else ""
            names.append(f"{base}logs/{sample}.{suffix}.log")
            if base:
                names.append(f"logs/{sample}.{suffix}.log")
        text = []
        for name in names:
            path = Path(job["job_dir"]) / name
            if path.is_file():
                with path.open("rb") as handle:
                    handle.seek(max(0, path.stat().st_size - 32768))
                    tail = handle.read().decode("utf-8", errors="replace")
                text.append(f"--- {name} ---\n" + "\n".join(tail.splitlines()[-lines:]))
        return "\n".join(text)


def scheduler_tick(manager: JobManager) -> bool:
    """Atomically reserve resources and launch ready projects; return work remains."""
    with manager._lock():
        registry = manager._load()
        manager._refresh(registry)
        jobs = registry["jobs"]
        if not any(job["status"] in ACTIVE for job in jobs.values()):
            registry["scheduler"] = {}
            manager._save(registry)
            return False
        if "budget" not in registry:
            registry["budget"] = detect_budget(manager.settings)
        free_cores, free_mem = available_capacity(registry["budget"], list(jobs.values()))
        queued = sorted((job for job in jobs.values() if job["status"] == "queued"),
                        key=lambda job: job.get("queued_at", job["created_at"]))
        for job in queued:
            if job["cores"] > free_cores or job["mem_mb"] > free_mem:
                job["queue_wait_reason"] = f"等待资源：需 {job['cores']} CPU / {job['mem_mb']} MB；当前余量 {free_cores} CPU / {max(0, free_mem)} MB。"
                continue
            try:
                validate_data_type(job.get("data_type", "bulk"))
                with (Path(job["job_dir"]) / "worker.log").open("w", encoding="utf-8") as output:
                    process = subprocess.Popen(
                        [str(Path(manager.settings["environment"]) / "bin" / "python"), "-m", "teflow.worker",
                         "--settings", str(manager.settings_path), "--accession", job["accession"]],
                        stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT,
                        env=worker_environment(manager.settings), cwd=manager.app_dir, start_new_session=True)
                job.update(status="running", pid=process.pid, pid_start=proc_start(process.pid),
                           attempt_started_at=timestamp(), step="准备解析项目", queue_wait_reason="")
                free_cores -= job["cores"]
                free_mem -= job["mem_mb"]
            except Exception as exc:
                job.update(status="failed", error=f"后台启动失败：{exc}", exit_code=1, finished_at=timestamp())
            job["updated_at"] = timestamp()
            manager._job_save(job)
        manager._save(registry)
        return True


def run_scheduler(settings_path: str) -> int:
    os.umask(0o002)
    manager = JobManager(settings_path)
    try:
        while scheduler_tick(manager):
            time.sleep(float(manager.settings.get("scheduler_interval", 1)))
        return 0
    except Exception as exc:
        print(f"{timestamp()} 调度器失败：{exc}", file=sys.stderr, flush=True)
        # Jobs stay queued and are restarted by the next viewer/submission.
        with contextlib.suppress(Exception):
            with manager._lock():
                registry = manager._load()
                registry["scheduler"] = {}
                manager._save(registry)
        return 1


def effective_config(manager: JobManager, job: dict) -> dict:
    validate_data_type(job.get("data_type", "bulk"))
    config = yaml.safe_load((Path(manager.settings["template_dir"]) / "config.yaml").read_text())
    for key in ("INDEX_PREFIX", "GENOME_ANNOTATION", "TE_ANNOTATION"):
        value = str(config[key])
        if value.startswith("~/"):
            value = str(Path(manager.settings.get("template_home", "/root")) / value[2:])
        path = Path(value).resolve()
        if not path.exists() or not os.access(path, os.R_OK):
            raise RuntimeError(f"{key} 文件不存在或不可读：{path}")
        config[key] = str(path)
    config.update({"ACCESSION": job["accession"], "SAMPLES_DIR": job["raw_dir"], "OUTPUT_DIR": job["job_dir"],
                   "THREADS": job.get("cores", manager.settings["threads"]),
                   "MEM_GB": job.get("mem_mb", manager.settings["mem_mb"]) / 1024,
                   "ISEQ_BIN": str(Path(manager.settings["environment"]) / "bin" / "iseq"),
                   "DOWNLOAD_RETRIES": int(manager.settings.get("download_retries", 3)),
                   "METADATA_TIMEOUT": int(manager.settings.get("metadata_timeout", 180)),
                   "OUTPUT_SPACE_MULTIPLIER": 4})
    return config


def terminate_owned_process(process: subprocess.Popen) -> None:
    import psutil
    try:
        parent = psutil.Process(process.pid)
        descendants = parent.children(recursive=True)
    except psutil.NoSuchProcess:
        parent, descendants = None, []
    targets = descendants + ([parent] if parent else [])
    for target in reversed(targets):
        with contextlib.suppress(psutil.NoSuchProcess, psutil.AccessDenied):
            target.terminate()
    _, alive = psutil.wait_procs(targets, timeout=10)
    for target in alive:
        with contextlib.suppress(psutil.NoSuchProcess, psutil.AccessDenied):
            target.kill()
    psutil.wait_procs(alive, timeout=5)
    with contextlib.suppress(subprocess.TimeoutExpired):
        process.wait(timeout=5)


def _failure_message(output: Path, exit_code: int, current: dict) -> str:
    paths = [output / "logs/prepare_samples.log", output / "run.log"]
    if current.get("stage") not in {"metadata", "download"}:
        paths.reverse()
    run = current.get("current_sample")
    suffix = {"质控": "fastp", "比对": "STAR", "计数": "TEcount"}.get(current.get("step"))
    if run and suffix:
        base = output / run if current["accession"].startswith("GSE") else output
        paths.insert(0, base / "logs" / f"{run}.{suffix}.log")
        if base != output:
            paths.insert(1, output / "logs" / f"{run}.{suffix}.log")
    for path in paths:
        if not path.is_file():
            continue
        with path.open("rb") as handle:
            handle.seek(max(0, path.stat().st_size - 16000))
            lines = handle.read().decode("utf-8", errors="replace").splitlines()
        useful = [line.strip() for line in lines if re.search(
            r"(?:\b(?:ValueError|RuntimeError|MetadataFormatError|RunSetMismatch|PermissionError|FileNotFoundError|OSError):|FATAL|EXITING because)", line)]
        if not useful:
            useful = [line.strip() for line in lines if "Error:" in line and not re.match(r"^(?:WorkflowError|CalledProcessError):?\s*$", line.strip())]
        if useful:
            return f"{useful[-1]}\n日志：{path}"
    return f"Snakemake 退出码 {exit_code}。请查看 {output / 'run.log'}"


def run_worker(settings_path: str, accession: str) -> int:
    from .activity import ActivityRecorder

    os.umask(0o002)
    manager = JobManager(settings_path)
    job = manager.get_job(accession)
    if not job:
        raise RuntimeError("找不到待执行任务。")
    output = Path(job["job_dir"])
    environment = worker_environment(manager.settings)
    process = None
    activity = ActivityRecorder(job, output / ".teflow-activity.jsonl")
    activity.begin()
    previous_term = signal.getsignal(signal.SIGTERM)
    def interrupted(signum, frame):
        raise InterruptedError(f"任务被信号 {signum} 中断，可重新提交恢复。")
    signal.signal(signal.SIGTERM, interrupted)
    try:
        validate_data_type(job.get("data_type", "bulk"))
        manager.update(accession, status="running", stage="metadata", step="解析项目", error="")
        for program in ("iseq", "snakemake", "fastp", "STAR", "TEcount", "zcat"):
            if not shutil.which(program, path=environment["PATH"]):
                raise RuntimeError(f"TE 环境缺少 {program}。")
        config = effective_config(manager, job)
        (output / "config.yaml").write_text(yaml.safe_dump(config, sort_keys=False, allow_unicode=True))
        manifest = output / "samples.json"
        if manifest.exists() and not raw_manifest_valid(job):
            manifest.rename(manifest.with_name(f"samples.json.incomplete.{uuid.uuid4().hex[:8]}"))
        command = [str(Path(manager.settings["environment"]) / "bin" / "snakemake"),
                   "--snakefile", str(manager.app_dir / "workflow" / "Snakefile"), "--directory", str(output),
                   "--configfile", str(output / "config.yaml"), "--cores", str(job.get("cores", manager.settings["threads"])),
                   "--resources", "star_slots=1", "download_slots=1",
                   f"mem_mb={job.get('mem_mb', manager.settings['mem_mb'])}", "--rerun-incomplete", "--printshellcmds"]
        rule_steps = {"quality_control": "质控", "star_align": "比对", "te_count": "计数"}
        process = subprocess.Popen(command, cwd=manager.app_dir, env=environment, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace", bufsize=1,
                                   start_new_session=True)
        manager.update(accession, snakemake_pid=process.pid, snakemake_pid_start=proc_start(process.pid))
        with (output / "run.log").open("a", encoding="utf-8") as log:
            log.write(f"\n=== {timestamp()} attempt {job['attempt']} ===\n")
            for line in process.stdout:
                log.write(line)
                log.flush()
                activity.feed(line)
                event = re.search(r"\[TEFLOW_PROGRESS\]\s*(\{.*\})", line)
                if event:
                    try:
                        values = json.loads(event.group(1))
                        allowed = {"stage", "step", "total_runs", "downloaded_runs", "analyzed_runs", "current_sample"}
                        manager.update(accession, **{k: v for k, v in values.items() if k in allowed})
                    except (ValueError, TypeError):
                        pass
                match = re.search(r"(?:localrule|localcheckpoint|rule|checkpoint)\s+(\w+):", line)
                if match:
                    rule = match.group(1)
                    if rule in rule_steps:
                        manager.update(accession, stage="analyze", step=rule_steps[rule])
                    elif rule == "merge_counts":
                        manager.update(accession, stage="merge", step="汇总结果")
                sample = re.search(r"wildcards:\s+sample=([SED]RR\d+)", line)
                if sample:
                    manager.update(accession, current_sample=sample.group(1))
        exit_code = process.wait()
        if exit_code:
            raise RuntimeError(_failure_message(output, exit_code, manager.get_job(accession)))
        result = Path(job["result_path"])
        if not result.is_file() or not result.stat().st_size:
            raise RuntimeError(f"流程退出成功，但未生成预期结果：{result}")
        current = manager.get_job(accession)
        manager.update(accession, status="succeeded", stage="done", step="完成", exit_code=0,
                       analyzed_runs=current.get("total_runs", 0), finished_at=timestamp(),
                       result_bytes=result.stat().st_size, error="")
        activity.finish("succeeded")
        return 0
    except BaseException as error:
        if process and process.poll() is None:
            with contextlib.suppress(Exception):
                terminate_owned_process(process)
        manager.update(accession, status="failed", exit_code=1, error=str(error), finished_at=timestamp())
        activity.finish("failed", str(error))
        print(str(error), file=sys.stderr, flush=True)
        return 1
    finally:
        signal.signal(signal.SIGTERM, previous_term)
