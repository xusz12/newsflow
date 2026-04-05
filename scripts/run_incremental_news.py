#!/usr/bin/env python3
import argparse
import json
import shlex
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


DEFAULT_TIMEZONE = "Asia/Shanghai"


def normalize_time(raw_time: Any) -> str:
    if raw_time is None:
        return "页面未显示"
    text = str(raw_time).strip()
    return text if text else "页面未显示"


def escape_md_title(title: str) -> str:
    return title.replace("[", "\\[").replace("]", "\\]")


def render_blockquote(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if line:
            lines.append(f"> {line}")
        else:
            lines.append(">")
    return lines if lines else [">"]


def load_json_file(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise FileNotFoundError(f"JSON file not found: {path}") from None
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {path}: {exc}") from exc


def write_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def parse_command_str(command: Any, command_str: str) -> str:
    if command_str:
        return command_str
    if isinstance(command, list):
        parts = [str(part).strip() for part in command if str(part).strip()]
        if parts:
            return " ".join(shlex.quote(part) for part in parts)
    if isinstance(command, str):
        return command.strip()
    return ""


def normalize_item(item: Any) -> dict[str, str] | None:
    if not isinstance(item, dict):
        return None

    section = str(item.get("section", "")).strip()
    url = str(item.get("url", "")).strip()
    raw_title = str(item.get("raw_title", item.get("title", ""))).strip()
    title = str(item.get("title", raw_title)).strip()
    if not section or not url or not raw_title:
        return None

    if not title:
        title = raw_title

    quoted_text_raw = str(item.get("quoted_text_raw", item.get("quoted_text", ""))).strip()
    quoted_text = str(item.get("quoted_text", quoted_text_raw)).strip()
    author_name = str(item.get("author_name", "")).strip()
    author_screen_name = str(item.get("author_screen_name", "")).strip()

    payload = {
        "section": section,
        "title": title,
        "raw_title": raw_title,
        "time": normalize_time(item.get("time")),
        "url": url,
    }
    if quoted_text_raw:
        payload["quoted_text_raw"] = quoted_text_raw
    if quoted_text:
        payload["quoted_text"] = quoted_text
    if author_name:
        payload["author_name"] = author_name
    if author_screen_name:
        payload["author_screen_name"] = author_screen_name
    return payload


def normalize_error(error: Any) -> dict[str, str] | None:
    if not isinstance(error, dict):
        return None

    section = str(error.get("section", "")).strip()
    message = str(error.get("error", "")).strip()
    generated_at = str(error.get("generated_at", "")).strip()
    command_str = parse_command_str(error.get("command"), str(error.get("command_str", "")).strip())

    if not section or not message:
        return None

    payload = {
        "section": section,
        "command_str": command_str,
        "error": message,
    }
    if generated_at:
        payload["generated_at"] = generated_at
    return payload


def parse_run_datetime(payload: dict[str, Any]) -> tuple[datetime, str]:
    timezone_name = str(payload.get("timezone", DEFAULT_TIMEZONE)).strip() or DEFAULT_TIMEZONE
    try:
        timezone = ZoneInfo(timezone_name)
    except Exception as exc:
        raise ValueError(f"Invalid timezone '{timezone_name}': {exc}") from exc

    generated_at = str(payload.get("generated_at", "")).strip()
    if generated_at:
        try:
            dt = datetime.strptime(generated_at, "%Y-%m-%d %H:%M:%S")
        except ValueError as exc:
            raise ValueError(f"Invalid generated_at '{generated_at}': {exc}") from exc
        return dt.replace(tzinfo=timezone), timezone_name

    return datetime.now(timezone), timezone_name


def normalize_section_order(raw_sections: Any) -> list[str]:
    sections: list[str] = []
    seen: set[str] = set()
    if not isinstance(raw_sections, list):
        return sections

    for section in raw_sections:
        text = str(section).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        sections.append(text)
    return sections


def merge_section_order(*orders: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for order in orders:
        for section in order:
            text = str(section).strip()
            if not text or text in seen:
                continue
            seen.add(text)
            merged.append(text)
    return merged


def section_order_from_items(items: list[dict[str, str]]) -> list[str]:
    return merge_section_order([item["section"] for item in items])


def load_state(state_path: Path, date_text: str, timezone_name: str) -> dict[str, Any]:
    if not state_path.exists():
        return {
            "date": date_text,
            "timezone": timezone_name,
            "section_order": [],
            "today_seen_urls": [],
            "today_first_seen_items": [],
            "daily_errors": [],
            "runs": [],
        }

    raw = load_json_file(state_path)
    if not isinstance(raw, dict):
        raise ValueError(f"State file must contain an object: {state_path}")

    return {
        "date": str(raw.get("date", date_text)).strip() or date_text,
        "timezone": str(raw.get("timezone", timezone_name)).strip() or timezone_name,
        "section_order": normalize_section_order(raw.get("section_order", [])),
        "today_seen_urls": [
            str(url).strip()
            for url in raw.get("today_seen_urls", [])
            if str(url).strip()
        ],
        "today_first_seen_items": [
            item
            for item in (
                normalize_item(entry) for entry in raw.get("today_first_seen_items", [])
            )
            if item is not None
        ],
        "daily_errors": [
            err
            for err in (normalize_error(entry) for entry in raw.get("daily_errors", []))
            if err is not None
        ],
        "runs": [
            entry
            for entry in raw.get("runs", [])
            if isinstance(entry, dict)
        ],
    }


def build_markdown(
    section_order: list[str],
    items: list[dict[str, str]],
    errors: list[dict[str, str]],
) -> str:
    grouped: dict[str, list[dict[str, str]]] = {section: [] for section in section_order}

    for item in items:
        grouped.setdefault(item["section"], []).append(item)

    lines: list[str] = []
    for section in section_order:
        section_items = grouped.get(section, [])
        lines.append(f"## {section}（{len(section_items)}条）")
        lines.append("")
        for item in section_items:
            lines.append(f"### [{escape_md_title(item['title'])}]({item['url']})")
            quoted_text = str(item.get("quoted_text", "")).strip()
            if quoted_text:
                lines.extend(render_blockquote(quoted_text))
            lines.append(f"- 发布时间：{item['time']}")
            lines.append("")

    lines.append("## errors")
    lines.append("")
    if errors:
        for index, error in enumerate(errors, start=1):
            lines.append(f"### {index}. {error['section']}")
            if error.get("generated_at"):
                lines.append(f"- 抓取时间：{error['generated_at']}")
            lines.append(f"- 命令：`{error.get('command_str', '')}`")
            lines.append(f"- 错误：{error['error']}")
            lines.append("")
    else:
        lines.append("- 无")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def get_translation_map(path: Path) -> dict[str, dict[str, str]]:
    raw = load_json_file(path)
    if not isinstance(raw, dict):
        raise ValueError("translated-json must contain an object like {\"url\": \"translated title\"}")

    mapping: dict[str, dict[str, str]] = {}
    for url, value in raw.items():
        url_text = str(url).strip()
        if not url_text:
            continue
        if isinstance(value, str):
            title_text = value.strip()
            if not title_text:
                continue
            mapping[url_text] = {"title": title_text}
            continue
        if isinstance(value, dict):
            title_text = str(value.get("title", "")).strip()
            quoted_text = str(value.get("quoted_text", "")).strip()
            quoted_text_zh = str(value.get("quoted_text_zh", "")).strip()
            if not title_text and not quoted_text and not quoted_text_zh:
                continue
            payload: dict[str, str] = {}
            if title_text:
                payload["title"] = title_text
            if quoted_text:
                payload["quoted_text"] = quoted_text
            if quoted_text_zh:
                payload["quoted_text_zh"] = quoted_text_zh
            mapping[url_text] = payload
            continue
    return mapping


def finalize_item(item: dict[str, str], translations: dict[str, dict[str, str]]) -> dict[str, str]:
    translated = translations.get(item["url"], {})
    title = str(translated.get("title", "")).strip() or item["raw_title"]
    quoted_text_raw = str(item.get("quoted_text_raw", item.get("quoted_text", ""))).strip()
    quoted_text_direct = str(translated.get("quoted_text", "")).strip()
    quoted_text_zh = str(translated.get("quoted_text_zh", "")).strip()

    if quoted_text_direct:
        quoted_text = quoted_text_direct
    elif quoted_text_zh:
        quoted_text = quoted_text_zh
    else:
        quoted_text = quoted_text_raw

    result = {
        "section": item["section"],
        "title": title,
        "raw_title": item["raw_title"],
        "time": item["time"],
        "url": item["url"],
    }
    if quoted_text:
        result["quoted_text"] = quoted_text
        result["quoted_text_raw"] = quoted_text_raw or quoted_text
    if item.get("author_name"):
        result["author_name"] = str(item.get("author_name", "")).strip()
    if item.get("author_screen_name"):
        result["author_screen_name"] = str(item.get("author_screen_name", "")).strip()
    return result


def state_path_for_date(state_dir: Path, date_text: str) -> Path:
    return state_dir / f"{date_text}.json"


def prepare_incremental(args: argparse.Namespace) -> int:
    current_json_path = Path(args.current_json).expanduser().resolve()
    out_json_path = Path(args.out_json).expanduser().resolve()
    state_dir = Path(args.state_dir).expanduser().resolve()

    raw_payload = load_json_file(current_json_path)
    if not isinstance(raw_payload, dict):
        raise ValueError("current-json must contain an object")

    run_dt, timezone_name = parse_run_datetime(raw_payload)
    date_text = run_dt.strftime("%Y-%m-%d")
    run_file_timestamp = run_dt.strftime("%Y-%m-%d-%H-%M")
    yesterday_text = (run_dt - timedelta(days=1)).strftime("%Y-%m-%d")

    current_section_order = merge_section_order(
        normalize_section_order(raw_payload.get("section_order", [])),
        section_order_from_items([
            item
            for item in (
                normalize_item(entry) for entry in raw_payload.get("deduped_items", [])
            )
            if item is not None
        ]),
    )
    current_items = [
        item
        for item in (
            normalize_item(entry) for entry in raw_payload.get("deduped_items", [])
        )
        if item is not None
    ]
    current_errors = [
        err
        for err in (
            normalize_error(entry) for entry in raw_payload.get("errors", [])
        )
        if err is not None
    ]
    for error in current_errors:
        error["generated_at"] = run_dt.strftime("%Y-%m-%d %H:%M:%S")

    today_state_path = state_path_for_date(state_dir, date_text)
    yesterday_state_path = state_path_for_date(state_dir, yesterday_text)
    today_state = load_state(today_state_path, date_text, timezone_name)
    yesterday_state = load_state(yesterday_state_path, yesterday_text, timezone_name)

    merged_section_order = merge_section_order(today_state["section_order"], current_section_order)
    today_seen_before = set(today_state["today_seen_urls"])
    yesterday_urls = set(yesterday_state["today_seen_urls"])

    current_run_first_seen_items_raw: list[dict[str, str]] = []
    seen_this_batch: set[str] = set()
    for item in current_items:
        url = item["url"]
        if url in today_seen_before or url in seen_this_batch:
            continue
        seen_this_batch.add(url)
        current_run_first_seen_items_raw.append(item)

    run_fresh_items_raw = [
        item for item in current_run_first_seen_items_raw if item["url"] not in yesterday_urls
    ]

    daily_fresh_urls = {
        item["url"] for item in today_state["today_first_seen_items"] if item["url"] not in yesterday_urls
    }
    daily_fresh_urls.update(item["url"] for item in run_fresh_items_raw)

    daily_errors = today_state["daily_errors"] + current_errors

    payload = {
        "date": date_text,
        "generated_at": run_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "timezone": timezone_name,
        "section_order": merged_section_order,
        "run_file_timestamp": run_file_timestamp,
        "current_run_items_raw": current_items,
        "current_run_first_seen_items_raw": current_run_first_seen_items_raw,
        "run_fresh_items_raw": run_fresh_items_raw,
        "current_run_errors": current_errors,
        "daily_errors": daily_errors,
        "items_to_translate": run_fresh_items_raw,
        "state": {
            "today_state_path": str(today_state_path),
            "yesterday_state_path": str(yesterday_state_path),
        },
        "stats": {
            "current_run_count": len(current_items),
            "current_run_first_seen_count": len(current_run_first_seen_items_raw),
            "run_fresh_count": len(run_fresh_items_raw),
            "daily_fresh_count": len(daily_fresh_urls),
            "current_error_count": len(current_errors),
            "daily_error_count": len(daily_errors),
        },
    }
    write_json_file(out_json_path, payload)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def finalize_incremental(args: argparse.Namespace) -> int:
    incremental_json_path = Path(args.incremental_json).expanduser().resolve()
    translated_json_path = Path(args.translated_json).expanduser().resolve()
    state_dir = Path(args.state_dir).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()

    raw_payload = load_json_file(incremental_json_path)
    if not isinstance(raw_payload, dict):
        raise ValueError("incremental-json must contain an object")

    translations = get_translation_map(translated_json_path)

    date_text = str(raw_payload.get("date", "")).strip()
    if not date_text:
        raise ValueError("incremental-json is missing date")

    timezone_name = str(raw_payload.get("timezone", DEFAULT_TIMEZONE)).strip() or DEFAULT_TIMEZONE
    generated_at = str(raw_payload.get("generated_at", "")).strip()
    run_file_timestamp = str(raw_payload.get("run_file_timestamp", "")).strip()
    if not run_file_timestamp:
        raise ValueError("incremental-json is missing run_file_timestamp")

    today_state_path = state_path_for_date(state_dir, date_text)
    yesterday_text = (
        datetime.strptime(date_text, "%Y-%m-%d") - timedelta(days=1)
    ).strftime("%Y-%m-%d")
    yesterday_state_path = state_path_for_date(state_dir, yesterday_text)

    today_state = load_state(today_state_path, date_text, timezone_name)
    yesterday_state = load_state(yesterday_state_path, yesterday_text, timezone_name)
    merged_section_order = merge_section_order(
        today_state["section_order"],
        normalize_section_order(raw_payload.get("section_order", [])),
        section_order_from_items(today_state["today_first_seen_items"]),
    )

    current_run_items_raw = [
        item
        for item in (
            normalize_item(entry) for entry in raw_payload.get("current_run_items_raw", [])
        )
        if item is not None
    ]
    current_run_first_seen_items_raw = [
        item
        for item in (
            normalize_item(entry)
            for entry in raw_payload.get("current_run_first_seen_items_raw", [])
        )
        if item is not None
    ]
    run_fresh_items_raw = [
        item
        for item in (
            normalize_item(entry) for entry in raw_payload.get("run_fresh_items_raw", [])
        )
        if item is not None
    ]
    current_run_errors = [
        err
        for err in (
            normalize_error(entry) for entry in raw_payload.get("current_run_errors", [])
        )
        if err is not None
    ]
    daily_errors = [
        err
        for err in (
            normalize_error(entry) for entry in raw_payload.get("daily_errors", [])
        )
        if err is not None
    ]

    today_seen_urls = list(today_state["today_seen_urls"])
    today_seen_set = set(today_seen_urls)
    for item in current_run_items_raw:
        url = item["url"]
        if url in today_seen_set:
            continue
        today_seen_set.add(url)
        today_seen_urls.append(url)

    today_first_seen_items = list(today_state["today_first_seen_items"])
    today_first_seen_index = {
        item["url"]: index for index, item in enumerate(today_first_seen_items)
    }
    for item in current_run_first_seen_items_raw:
        finalized = finalize_item(item, translations)
        url = item["url"]
        if url in today_first_seen_index:
            today_first_seen_items[today_first_seen_index[url]] = finalized
            continue
        today_first_seen_index[url] = len(today_first_seen_items)
        today_first_seen_items.append(finalized)

    yesterday_urls = set(yesterday_state["today_seen_urls"])
    daily_fresh_items_final = [
        item for item in today_first_seen_items if item["url"] not in yesterday_urls
    ]
    run_fresh_items_final = [finalize_item(item, translations) for item in run_fresh_items_raw]

    out_dir.mkdir(parents=True, exist_ok=True)
    run_fresh_path = out_dir / f"{run_file_timestamp}_freshNews.md"
    daily_fresh_path = out_dir / f"{date_text}_dailyFreshNews.md"

    run_fresh_path.write_text(
        build_markdown(merged_section_order, run_fresh_items_final, current_run_errors),
        encoding="utf-8",
    )
    daily_fresh_path.write_text(
        build_markdown(merged_section_order, daily_fresh_items_final, daily_errors),
        encoding="utf-8",
    )

    runs = [entry for entry in today_state["runs"] if str(entry.get("generated_at", "")).strip() != generated_at]
    runs.append(
        {
            "generated_at": generated_at,
            "run_fresh_path": str(run_fresh_path),
            "daily_fresh_path": str(daily_fresh_path),
            "run_fresh_count": len(run_fresh_items_final),
            "daily_fresh_count": len(daily_fresh_items_final),
            "error_count": len(current_run_errors),
        }
    )

    state_payload = {
        "date": date_text,
        "timezone": timezone_name,
        "section_order": merged_section_order,
        "today_seen_urls": today_seen_urls,
        "today_first_seen_items": today_first_seen_items,
        "daily_errors": daily_errors,
        "runs": runs,
    }
    write_json_file(today_state_path, state_payload)

    result = {
        "date": date_text,
        "generated_at": generated_at,
        "timezone": timezone_name,
        "section_order": merged_section_order,
        "run_fresh_path": str(run_fresh_path),
        "daily_fresh_path": str(daily_fresh_path),
        "state_path": str(today_state_path),
        "stats": {
            "run_fresh_count": len(run_fresh_items_final),
            "daily_fresh_count": len(daily_fresh_items_final),
            "today_seen_url_count": len(today_seen_urls),
            "daily_error_count": len(daily_errors),
        },
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare and finalize incremental daily news outputs without doing translation."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser("prepare", help="Compute daily and per-run fresh news diffs")
    prepare.add_argument("--current-json", required=True, help="Pipeline JSON from run_news_pipeline.py")
    prepare.add_argument("--state-dir", required=True, help="Directory for per-day state JSON files")
    prepare.add_argument("--out-json", required=True, help="Output incremental JSON path")
    prepare.set_defaults(handler=prepare_incremental)

    finalize = subparsers.add_parser("finalize", help="Write markdown outputs and update daily state")
    finalize.add_argument("--incremental-json", required=True, help="Incremental JSON from prepare")
    finalize.add_argument("--translated-json", required=True, help="Model-produced translation map JSON")
    finalize.add_argument("--state-dir", required=True, help="Directory for per-day state JSON files")
    finalize.add_argument("--out-dir", required=True, help="Directory for markdown outputs")
    finalize.set_defaults(handler=finalize_incremental)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return args.handler(args)
    except Exception as exc:
        print(f"Incremental news error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
