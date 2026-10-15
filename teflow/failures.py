"""Explanations only for errors established by an explicit check or exception.

Tool log tails are deliberately not classified here. Unknown failures retain the
existing original-error and complete-log presentation.
"""
from __future__ import annotations

import re
from urllib.parse import urlsplit


KNOWN_CODES = {"single_end", "website_connection", "fastq_integrity"}


class KnownFailure(ValueError):
    def __init__(self, code: str, message: str, *, detail: str = "", **scope):
        if code not in KNOWN_CODES:
            raise ValueError("Unsupported failure explanation")
        super().__init__(message)
        self.failure = {"code": code, "message": message, **scope}
        self.detail = detail or message


def website_name(url: str) -> tuple[str, str]:
    host = (urlsplit(url).hostname or "").lower()
    if host == "www.ncbi.nlm.nih.gov" and "/geo" in urlsplit(url).path:
        return "NCBI GEO", host
    if host in {"www.ebi.ac.uk", "ftp.sra.ebi.ac.uk"}:
        return "ENA", host
    if host.endswith("ncbi.nlm.nih.gov"):
        return "NCBI SRA", host
    return host or "目标网站", host


def website_failure(url: str, error: BaseException | str) -> KnownFailure:
    website, host = website_name(url)
    return KnownFailure("website_connection", f"无法连接 {website}（{host}），请检查网络后重试。",
                        detail=f"{type(error).__name__}: {error}", website=website, host=host)


def known_failure(error: BaseException) -> KnownFailure | None:
    """Follow explicit causes, never implicit context from a recovered fallback."""
    seen = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        if isinstance(error, KnownFailure):
            return error
        error = error.__cause__
    return None


def valid_failure(value) -> dict | None:
    if not isinstance(value, dict) or value.get("code") not in KNOWN_CODES:
        return None
    message = value.get("message")
    if not isinstance(message, str) or not message.strip() or len(message) > 2000:
        return None
    if value.get("run") is not None and not re.fullmatch(r"[SED]RR\d+", str(value["run"])):
        return None
    return {key: value[key] for key in ("code", "message", "run", "website", "host") if key in value}


def friendly_failure(job: dict) -> str | None:
    if job.get("status") != "failed":
        return None
    failure = valid_failure(job.get("failure"))
    return failure["message"] if failure else None
