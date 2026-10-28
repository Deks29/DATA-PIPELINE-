"""openFDA REST API client used by the extraction stage.

The endpoint is configured through the ``API`` environment variable (see
``.env``). openFDA returns a JSON envelope of the form::

    {"meta": {"last_updated": "...", "results": {"total": N}}, "results": [...]}

This module fetches every page, normalises the records so the Polars schema is
stable, and writes them to a local NDJSON file that the existing file-based
pipeline can ingest without any special casing.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import date
from pathlib import Path
from typing import Any

MAX_PAGE_SIZE = 1000
DEFAULT_TIMEOUT = 30


def build_url(
    base_url: str,
    search: str | None = None,
    limit: int = 500,
    skip: int = 0,
    api_key: str | None = None,
) -> str:
    """Merge paging/search parameters into the configured API URL."""

    parts = urllib.parse.urlsplit(base_url)
    params = dict(urllib.parse.parse_qsl(parts.query, keep_blank_values=True))
    if search:
        params["search"] = search
    params["limit"] = str(max(1, min(limit, MAX_PAGE_SIZE)))
    if skip:
        params["skip"] = str(skip)
    else:
        params.pop("skip", None)
    if api_key:
        params["api_key"] = api_key
    return urllib.parse.urlunsplit(
        (parts.scheme, parts.netloc, parts.path, urllib.parse.urlencode(params), parts.fragment)
    )


def _get_json(url: str, timeout: int = DEFAULT_TIMEOUT) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "sightsync-extraction/1.0",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            error = json.loads(body).get("error", {})
        except json.JSONDecodeError:
            error = {}
        # openFDA answers 404/NOT_FOUND when a search matches zero records.
        if exc.code == 404 or error.get("code") == "NOT_FOUND":
            return {"meta": {"results": {"total": 0}}, "results": []}
        detail = error.get("message") or body or exc.reason
        raise RuntimeError(f"openFDA API request failed ({exc.code}): {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"openFDA API is unreachable: {exc.reason}") from exc


def fetch_records(
    base_url: str,
    search: str | None = None,
    limit: int = 500,
    api_key: str | None = None,
    timeout: int = DEFAULT_TIMEOUT,
    max_records: int | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Fetch all pages for ``search`` and return ``(records, meta)``."""

    records: list[dict[str, Any]] = []
    meta: dict[str, Any] = {}
    skip = 0

    while True:
        url = build_url(base_url, search=search, limit=limit, skip=skip, api_key=api_key)
        payload = _get_json(url, timeout=timeout)
        meta = payload.get("meta") or meta
        page = payload.get("results") or []
        records.extend(page)

        total = (meta.get("results") or {}).get("total")
        skip += len(page)
        if not page or total is None or skip >= int(total):
            break
        if max_records is not None and len(records) >= max_records:
            records = records[:max_records]
            break

    return records, meta


def normalise_records(
    records: list[dict[str, Any]],
    string_fields: list[str],
    last_updated: str,
) -> list[dict[str, Any]]:
    """Keep a stable schema: every contracted string field is present as text."""

    normalised: list[dict[str, Any]] = []
    for record in records:
        row: dict[str, Any] = {}
        for field_name in string_fields:
            value = record.get(field_name)
            if value is None:
                row[field_name] = ""
            elif isinstance(value, (dict, list)):
                row[field_name] = json.dumps(value)
            else:
                row[field_name] = str(value)
        row["last_updated"] = last_updated
        normalised.append(row)
    return normalised


def write_ndjson(path: Path, records: list[dict[str, Any]]) -> Path:
    """Atomically write records as NDJSON so Polars can stream them."""

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    os.replace(tmp, path)
    return path


def fetch_to_ndjson(
    base_url: str,
    out_path: Path,
    string_fields: list[str],
    search: str | None = None,
    limit: int = 500,
    api_key: str | None = None,
    timeout: int = DEFAULT_TIMEOUT,
    max_records: int | None = None,
) -> tuple[Path, int, dict[str, Any]]:
    """Fetch the API, normalise the rows and persist them to ``out_path``."""

    if not base_url:
        raise ValueError("No API endpoint configured. Set API=... in .env.")

    records, meta = fetch_records(
        base_url,
        search=search,
        limit=limit,
        api_key=api_key,
        timeout=timeout,
        max_records=max_records,
    )
    last_updated = str((meta or {}).get("last_updated") or date.today().isoformat())
    normalised = normalise_records(records, string_fields, last_updated)
    write_ndjson(out_path, normalised)
    return out_path, len(normalised), meta
