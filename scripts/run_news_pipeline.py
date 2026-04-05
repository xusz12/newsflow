#!/usr/bin/env python3
import argparse
import json
import shlex
import subprocess
import sys
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
    url = str(row.get("url", "")).strip()
    if title and url:
        return {
            "section": section,
            "title": title,
            "time": normalize_time(row.get("time")),
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
        parsed.append({"section": section, "command": command})

    return parsed


def run_pipeline(entries: list[dict[str, Any]], timeout_seconds: int) -> dict[str, Any]:
    section_order: list[str] = []
    section_items: dict[str, list[dict[str, str]]] = {}
    errors: list[dict[str, Any]] = []

    for entry in entries:
        section = entry["section"]
        command = entry["command"]

        if section not in section_order:
            section_order.append(section)
            section_items[section] = []

        command_str = " ".join(shlex.quote(part) for part in command)

        try:
            proc = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            errors.append(
                {
                    "section": section,
                    "command": command,
                    "command_str": command_str,
                    "error": f"timeout after {timeout_seconds}s",
                }
            )
            continue

        if proc.returncode != 0:
            errors.append(
                {
                    "section": section,
                    "command": command,
                    "command_str": command_str,
                    "error": summarize_error(proc.stdout, proc.stderr, proc.returncode),
                }
            )
            continue

        try:
            rows = parse_json_items(proc.stdout)
        except Exception as exc:
            errors.append(
                {
                    "section": section,
                    "command": command,
                    "command_str": command_str,
                    "error": f"JSON parse error: {exc}",
                }
            )
            continue

        for row in rows:
            normalized = normalize_row(section, row)
            if normalized is None:
                continue
            section_items[section].append(normalized)

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
        "stats": {
            "command_count": len(entries),
            "section_count": len(section_order),
            "collected_before_dedup": sum(len(items) for items in section_items.values()),
            "after_dedup": len(deduped_items),
            "error_count": len(errors),
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

    try:
        entries = load_config(Path(args.config).expanduser().resolve())
        result = run_pipeline(entries, timeout_seconds=args.timeout)
    except Exception as exc:
        print(f"Pipeline error: {exc}", file=sys.stderr)
        return 2

    payload = {
        "generated_at": datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S"),
        "timezone": args.timezone,
        **result,
    }

    if args.out_json:
        out_path = Path(args.out_json).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
