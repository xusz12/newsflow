---
name: opencli-sequential-news-zh
description: Run configurable opencli news commands sequentially, skip failed commands, normalize JSON stories, and deduplicate globally by URL before producing daily fresh-news and per-run fresh-news markdown digests. Use when user wants repeatable multi-source news aggregation with easy command add/remove via commands.json and requires model-handled translation (no third-party translation API).
---

# OpenCLI Sequential News ZH

## Execution Mode (Rules Compatibility)

- Execute each step as a direct command invocation (argv style).
- Do not wrap the whole workflow into one `bash -lc` / `zsh -lc` script.
- Do not rely on environment-variable expansion for script paths in the executed command.
- Keep pipeline and incremental scripts invoked as literal absolute paths so `prefix_rule` can match reliably.

## Workflow

1. Use current working directory as the output directory.
2. Read command configuration:
   - Default: `references/commands.json` inside this skill.
   - Optional override: user-provided config path via `--config`.
   - Resolve skill root absolute path from this skill file location:
     - `/Users/x/.codex/skills/opencli-sequential-news-zh`
3. Define shared working paths (must be reused in steps 4, 5, 7, and 8):

```bash
WORKDIR=<absolute current working directory>
STATE_DIR=<WORKDIR>/.news_state
TMP_JSON_PATH=<STATE_DIR>/tmp_current.json
INCREMENTAL_JSON_PATH=<STATE_DIR>/tmp_incremental.json
TRANSLATED_JSON_PATH=<STATE_DIR>/tmp_translated.json
```

4. Run pipeline script sequentially:

```bash
python3 /Users/x/.codex/skills/opencli-sequential-news-zh/scripts/run_news_pipeline.py --config <commands.json> --out-json <TMP_JSON_PATH>
```

5. Prepare incremental payload:

```bash
python3 /Users/x/.codex/skills/opencli-sequential-news-zh/scripts/run_incremental_news.py prepare --current-json <TMP_JSON_PATH> --state-dir <STATE_DIR> --out-json <INCREMENTAL_JSON_PATH>
```

6. Parse incremental JSON result:
   - `run_fresh_items_raw`: this run's fresh stories after removing yesterday URLs and earlier same-day URLs.
   - `items_to_translate`: stories whose titles still need model translation for display.
   - `current_run_errors`: failed commands from this run.
   - `daily_errors`: accumulated failed commands for the current day.
7. Translate titles into Chinese in-model:
   - Translate only `items_to_translate`.
   - Translation must stay in the model, not inside any script.
   - Write a JSON object into `$TRANSLATED_JSON_PATH`:
     - Legacy format (still supported): map URL to translated title string.
     - Extended format (recommended for Twitter quote support): map URL to object with `title` and optional quote fields.
   - If `items_to_translate` is empty, still write `{}` to `$TRANSLATED_JSON_PATH`.

```json
{
  "https://example.com/story": "中文标题",
  "https://x.com/ivanalog_com/status/123?s=20": {
    "title": "中文正文标题",
    "quoted_text_zh": "引用推文中文翻译"
  }
}
```

8. Finalize outputs:

```bash
python3 /Users/x/.codex/skills/opencli-sequential-news-zh/scripts/run_incremental_news.py finalize --incremental-json <INCREMENTAL_JSON_PATH> --translated-json <TRANSLATED_JSON_PATH> --state-dir <STATE_DIR> --out-dir <WORKDIR>
```

9. Finalize writes exactly two user-facing Markdown files:
   - `YYYY-MM-DD_dailyFreshNews.md`: one rolling summary file per day.
   - `YYYY-MM-DD-HH-mm_freshNews.md`: one per-run fresh-news file.
   - Timezone: `Asia/Shanghai` unless user explicitly requests another timezone.
10. Hidden state is stored separately in the state directory, one JSON file per day.

## State Schema Notes

- Daily state file path: `<STATE_DIR>/YYYY-MM-DD.json`.
- Top-level keys are daily aggregates and metadata, for example:
  - `date`, `timezone`, `section_order`
  - `today_seen_urls`, `today_first_seen_items`
  - `daily_errors`
  - `runs` (array of per-run summaries)
- Per-run counters are stored under `runs[-1]` (latest run), not at top level.
  - Read `runs[-1].run_fresh_count` for this run's fresh count.
  - Read `runs[-1].daily_fresh_count` for current day cumulative fresh count.
  - Read `runs[-1].error_count` for this run error count.
  - Read `runs[-1].run_fresh_path` / `runs[-1].daily_fresh_path` for output files.
- If `runs` is empty, treat run-level stats as unavailable rather than `0`.

## commands.json Format

Use JSON array of objects:

```json
[
  {
    "section": "middle-east",
    "command": ["opencli", "ReutersBrowser", "news", "https://www.reuters.com/world/middle-east/", "--limit", "10", "--format", "json"]
  }
]
```

Rules:
- Keep order as desired final processing order.
- Add/remove sources by adding/removing objects only.
- `command` supports string array (recommended) or shell string.

## Output Contract

For each output Markdown file, always emit a section header even when no stories remain after filtering:

```markdown
## section（N条）

### [中文标题](https://...)
- 发布时间：YYYY-MM-DD HH:MM:SS
```

Constraints:
- Missing time must be `页面未显示`.
- Preserve first-seen order: command order first, then source order.
- Global dedupe key is absolute URL exact match.
- Daily filtering removes yesterday's URLs.
- Per-run filtering removes yesterday's URLs and URLs seen earlier the same day.
- Twitter (`twitter user-posts --json`) is supported:
  - `text` -> output title.
  - `author.name` can be used as `section` via commands config.
  - URL auto-generated as `https://x.com/{screenName}/status/{id}?s=20`.
  - `createdAtLocal` -> 发布时间.
  - `quotedTweet.text` renders as blockquote.
  - If `quoted_text_zh` is provided, only Chinese quote text is rendered (no bilingual block).
  - Recommended translation policy: only translate non-Chinese text.
- Add final block:

```markdown
## errors

### 1. section
- 命令：`...`
- 错误：...
```

## Validation Checklist

1. Pipeline runs all commands sequentially.
2. Failed command does not stop later commands.
3. Duplicate URLs are removed globally, keeping first occurrence.
4. Translation is model-handled, not external translation API.
5. All sections are present, including `（0条）` sections.
6. Finalize writes `YYYY-MM-DD_dailyFreshNews.md` and `YYYY-MM-DD-HH-mm_freshNews.md`, not `*_fullNews.md`.
