#!/usr/bin/env python3
import argparse
import json
import os
import shlex
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


NOISE_PREFIXES = (
    "(node:",
    "Reparsing as ES module",
    "To eliminate this warning",
    "(Use `node --trace-warnings",
)
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


def format_timestamp(dt: datetime) -> str:
    return dt.strftime(TIMESTAMP_FORMAT)


def make_run_id(started_at: datetime) -> str:
    return started_at.strftime("%Y%m%d-%H%M%S-%f")


def write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            handle.write(text)
            temp_path = Path(handle.name)
        os.replace(temp_path, path)
    except Exception:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
        raise


def write_json_file(path: Path, payload: Any) -> None:
    write_text_atomic(
        path,
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
    )


def normalize_time(raw_time: Any) -> str:
    if raw_time is None:
        return "页面未显示"
    text = str(raw_time).strip()
    return text if text else "页面未显示"


def summarize_error(stdout_text: str, stderr_text: str, returncode: int) -> str:
    lines: list[str] = []
    for line in (stderr_text + "\n" + stdout_text).splitlines():
        clean = line.strip()
        if not clean:
            continue
        if "MODULE_TYPELESS_PACKAGE_JSON" in clean:
            continue
        if clean.startswith(NOISE_PREFIXES):
            continue
        lines.append(clean)

    if lines:
        preferred = [
            line
            for line in lines
            if ("error" in line.lower()) or ("unknown" in line.lower())
        ]
        if preferred:
            return preferred[-1]
        return lines[-1]
    return f"exit code {returncode}"


def parse_json_array(stdout_text: str) -> list[dict[str, Any]]:
    text = stdout_text.strip()
    if not text:
        raise ValueError("empty stdout")

    try:
        data = json.loads(text)
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
    except Exception:
        pass

    lines = text.splitlines()
    for idx, line in enumerate(lines):
        if not line.lstrip().startswith("["):
            continue
        candidate = "\n".join(lines[idx:]).strip()
        try:
            data = json.loads(candidate)
            if isinstance(data, list):
                return [row for row in data if isinstance(row, dict)]
        except Exception:
            continue

    raise ValueError("stdout does not contain a valid JSON array")


def parse_json_items(stdout_text: str) -> list[dict[str, Any]]:
    text = stdout_text.strip()
    if not text:
        raise ValueError("empty stdout")

    try:
        data = json.loads(text)
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
        if isinstance(data, dict):
            rows = data.get("data")
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
            raise ValueError("JSON object is missing list field 'data'")
    except json.JSONDecodeError:
        pass

    return parse_json_array(stdout_text)


def parse_command(raw_command: Any) -> list[str]:
    if isinstance(raw_command, list):
        parts = [str(part).strip() for part in raw_command]
        parts = [part for part in parts if part]
        if not parts:
            raise ValueError("command list is empty")
        return parts

    if isinstance(raw_command, str):
        parts = shlex.split(raw_command)
        if not parts:
            raise ValueError("command string is empty")
        return parts

    raise ValueError("command must be list[str] or string")


def compact_text(raw: Any) -> str:
    text = str(raw or "").strip()
    return " ".join(text.split())


def normalize_row(section: str, row: dict[str, Any]) -> dict[str, str] | None:
    title = str(row.get("title", "")).strip()
    url = str(row.get("url") or row.get("link") or "").strip()
    raw_time = row.get("time")
    if raw_time is None or not str(raw_time).strip():
        for fallback_key in (
            "publishedAt",
            "pubDate",
            "date",
            "published_at",
            "createdAt",
            "created_at",
        ):
            candidate = row.get(fallback_key)
            if candidate is None:
                continue
            if not str(candidate).strip():
                continue
            raw_time = candidate
            break
    if title and url:
        return {
            "section": section,
            "title": title,
            "time": normalize_time(raw_time),
            "url": url,
        }

    tweet_id = str(row.get("id", "")).strip()
    tweet_text = compact_text(row.get("text", ""))
    author = row.get("author")
    if isinstance(author, dict):
        screen_name = str(author.get("screenName", "")).strip()
        author_name = str(author.get("name", "")).strip()
    else:
        screen_name = ""
        author_name = ""

    if not tweet_id or not tweet_text:
        return None

    if not screen_name:
        return None

    quote_text = ""
    quoted = row.get("quotedTweet")
    if isinstance(quoted, dict):
        quote_text = compact_text(quoted.get("text", ""))

    return {
        "section": section,
        "title": tweet_text,
        "time": normalize_time(row.get("createdAtLocal")),
        "url": f"https://x.com/{screen_name}/status/{tweet_id}?s=20",
        "author_name": author_name,
        "author_screen_name": screen_name,
        "quoted_text_raw": quote_text,
    }


def parse_positive_int(raw: Any, default: int) -> int:
    try:
        value = int(raw)
        if value > 0:
            return value
    except Exception:
        pass
    return default


def execute_command_once(
    *,
    section: str,
    command: list[str],
    timeout_seconds: int,
    min_valid_items: int,
    treat_empty_as_failure: bool,
) -> dict[str, Any]:
    command_str = " ".join(shlex.quote(part) for part in command)

    try:
        proc = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        return {
            "ok": False,
            "command": command,
            "command_str": command_str,
            "error": f"timeout after {timeout_seconds}s",
            "failed_reason": "timeout",
            "items": [],
            "raw_count": 0,
            "valid_count": 0,
        }

    if proc.returncode != 0:
        return {
            "ok": False,
            "command": command,
            "command_str": command_str,
            "error": summarize_error(proc.stdout, proc.stderr, proc.returncode),
            "failed_reason": "non_zero_exit",
            "items": [],
            "raw_count": 0,
            "valid_count": 0,
        }

    try:
        rows = parse_json_items(proc.stdout)
    except Exception as exc:
        return {
            "ok": False,
            "command": command,
            "command_str": command_str,
            "error": f"JSON parse error: {exc}",
            "failed_reason": "json_parse_error",
            "items": [],
            "raw_count": 0,
            "valid_count": 0,
        }

    items: list[dict[str, str]] = []
    for row in rows:
        normalized = normalize_row(section, row)
        if normalized is None:
            continue
        items.append(normalized)

    valid_count = len(items)
    if treat_empty_as_failure and valid_count < min_valid_items:
        return {
            "ok": False,
            "command": command,
            "command_str": command_str,
            "error": (
                f"normalized valid items below threshold: "
                f"{valid_count} < {min_valid_items}"
            ),
            "failed_reason": "below_min_valid_items",
            "items": items,
            "raw_count": len(rows),
            "valid_count": valid_count,
        }

    return {
        "ok": True,
        "command": command,
        "command_str": command_str,
        "error": "",
        "failed_reason": "",
        "items": items,
        "raw_count": len(rows),
        "valid_count": valid_count,
    }


def build_recovered_error(
    *,
    section: str,
    primary_command: list[str],
    primary_attempts: list[dict[str, Any]],
    success_attempt: dict[str, Any],
    used_fallback: bool,
) -> dict[str, Any]:
    primary_command_str = " ".join(shlex.quote(part) for part in primary_command)
    failed_reasons = [
        attempt["failed_reason"] or "unknown"
        for attempt in primary_attempts
        if not attempt["ok"]
    ]
    failed_reason_text = "; ".join(failed_reasons) if failed_reasons else "unknown"

    if used_fallback:
        recovered_error = (
            f"已恢复：主命令失败（尝试 {len(primary_attempts)} 次，原因：{failed_reason_text}）；"
            f"fallback 成功：{success_attempt['command_str']}"
        )
    else:
        success_index = next(
            (
                index
                for index, attempt in enumerate(primary_attempts, start=1)
                if attempt["ok"]
            ),
            len(primary_attempts),
        )
        recovered_error = (
            f"已恢复：主命令失败（原因：{failed_reason_text}），"
            f"第 {success_index} 次重试成功"
        )

    return {
        "section": section,
        "command": primary_command,
        "command_str": primary_command_str,
        "error": recovered_error,
    }


def load_config(config_path: Path) -> list[dict[str, Any]]:
    if not config_path.exists():
        raise FileNotFoundError(f"config not found: {config_path}")

    raw = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("config root must be an array")

    parsed: list[dict[str, Any]] = []
    for i, item in enumerate(raw, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"config item #{i} must be object")

        section = str(item.get("section", "")).strip()
        if not section:
            raise ValueError(f"config item #{i} missing section")

        command = parse_command(item.get("command"))

        fallback_raw = item.get("fallback_command")
        fallback_command = (
            parse_command(fallback_raw) if fallback_raw is not None else None
        )

        retry_once = bool(item.get("retry_once", False))
        treat_empty_as_failure = bool(item.get("treat_empty_as_failure", False))
        min_valid_items = parse_positive_int(item.get("min_valid_items", 1), default=1)

        parsed.append(
            {
                "section": section,
                "command": command,
                "fallback_command": fallback_command,
                "retry_once": retry_once,
                "treat_empty_as_failure": treat_empty_as_failure,
                "min_valid_items": min_valid_items,
            }
        )

    return parsed


def run_pipeline(
    entries: list[dict[str, Any]],
    timeout_seconds: int,
) -> dict[str, Any]:
    section_order: list[str] = []
    section_items: dict[str, list[dict[str, str]]] = {}
    errors: list[dict[str, Any]] = []
    recovered_attempts: list[dict[str, Any]] = []

    for entry in entries:
        section = entry["section"]
        command = entry["command"]
        fallback_command = entry.get("fallback_command")
        retry_once = bool(entry.get("retry_once", False))
        treat_empty_as_failure = bool(entry.get("treat_empty_as_failure", False))
        min_valid_items = parse_positive_int(entry.get("min_valid_items", 1), default=1)

        if section not in section_order:
            section_order.append(section)
            section_items[section] = []

        primary_attempts: list[dict[str, Any]] = []
        first = execute_command_once(
            section=section,
            command=command,
            timeout_seconds=timeout_seconds,
            min_valid_items=min_valid_items,
            treat_empty_as_failure=treat_empty_as_failure,
        )
        primary_attempts.append(first)

        if (not first["ok"]) and retry_once:
            second = execute_command_once(
                section=section,
                command=command,
                timeout_seconds=timeout_seconds,
                min_valid_items=min_valid_items,
                treat_empty_as_failure=treat_empty_as_failure,
            )
            primary_attempts.append(second)

        success_attempt = next((a for a in primary_attempts if a["ok"]), None)
        used_fallback = False
        fallback_attempt: dict[str, Any] | None = None

        if success_attempt is None and fallback_command:
            fallback_attempt = execute_command_once(
                section=section,
                command=fallback_command,
                timeout_seconds=timeout_seconds,
                min_valid_items=min_valid_items,
                treat_empty_as_failure=treat_empty_as_failure,
            )
            if fallback_attempt["ok"]:
                success_attempt = fallback_attempt
                used_fallback = True

        if success_attempt is not None:
            section_items[section].extend(success_attempt["items"])
            if (len(primary_attempts) > 1) or used_fallback:
                recovered_attempts.append(
                    {
                        "section": section,
                        "primary_command": command,
                        "primary_attempts": len(primary_attempts),
                        "primary_fail_reasons": [
                            a["failed_reason"] for a in primary_attempts if not a["ok"]
                        ],
                        "fallback_command": fallback_command,
                        "used_fallback": used_fallback,
                        "used_command": success_attempt["command"],
                    }
                )
                errors.append(
                    build_recovered_error(
                        section=section,
                        primary_command=command,
                        primary_attempts=primary_attempts,
                        success_attempt=success_attempt,
                        used_fallback=used_fallback,
                    )
                )
            continue

        final_error = ""
        final_command = command
        final_command_str = " ".join(shlex.quote(part) for part in command)
        if fallback_attempt is not None:
            final_error = (
                f"primary failed ({'; '.join(a['failed_reason'] or 'unknown' for a in primary_attempts)}); "
                f"fallback failed: {fallback_attempt['error']}"
            )
            final_command = fallback_command
            final_command_str = " ".join(shlex.quote(part) for part in fallback_command)
        else:
            last_primary = primary_attempts[-1]
            final_error = last_primary["error"]
            final_command = last_primary["command"]
            final_command_str = last_primary["command_str"]

        errors.append(
            {
                "section": section,
                "command": final_command,
                "command_str": final_command_str,
                "error": final_error,
            }
        )

    seen_urls: set[str] = set()
    deduped_items: list[dict[str, str]] = []
    for section in section_order:
        for item in section_items[section]:
            url = item["url"]
            if url in seen_urls:
                continue
            seen_urls.add(url)
            deduped_items.append(item)

    grouped: dict[str, list[dict[str, str]]] = {section: [] for section in section_order}
    for item in deduped_items:
        grouped[item["section"]].append(item)

    return {
        "section_order": section_order,
        "grouped_items": grouped,
        "deduped_items": deduped_items,
        "errors": errors,
        "recovered_attempts": recovered_attempts,
        "stats": {
            "command_count": len(entries),
            "section_count": len(section_order),
            "collected_before_dedup": sum(len(items) for items in section_items.values()),
            "after_dedup": len(deduped_items),
            "error_count": len(errors),
            "recovered_count": len(recovered_attempts),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run configurable opencli news commands sequentially and dedupe by URL."
    )
    default_config = Path(__file__).resolve().parents[1] / "references" / "commands.json"

    parser.add_argument("--config", default=str(default_config), help="Path to commands.json")
    parser.add_argument("--out-json", default="", help="Optional output JSON path")
    parser.add_argument("--timeout", type=int, default=300, help="Per-command timeout seconds")
    parser.add_argument("--timezone", default="Asia/Shanghai", help="Timezone for generated_at")

    args = parser.parse_args()

    try:
        tz = ZoneInfo(args.timezone)
    except Exception as exc:
        print(f"Invalid timezone '{args.timezone}': {exc}", file=sys.stderr)
        return 2

    started_at = datetime.now(tz)
    run_id = make_run_id(started_at)

    try:
        entries = load_config(Path(args.config).expanduser().resolve())
        result = run_pipeline(
            entries,
            timeout_seconds=args.timeout,
        )
    except Exception as exc:
        print(f"Pipeline error: {exc}", file=sys.stderr)
        return 2

    finished_at = datetime.now(tz)

    payload = {
        "run_id": run_id,
        "started_at": format_timestamp(started_at),
        "finished_at": format_timestamp(finished_at),
        "generated_at": format_timestamp(finished_at),
        "timezone": args.timezone,
        **result,
    }

    if args.out_json:
        out_path = Path(args.out_json).expanduser().resolve()
        payload["current_json_path"] = str(out_path)
        write_json_file(out_path, payload)

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
