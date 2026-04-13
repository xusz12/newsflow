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
- When artifact paths include spaces, quote them explicitly. Prefer relative artifact paths under the current `workdir` when possible.

## Workflow

1. Use current working directory as the output directory.
2. Read command configuration:
   - Default: `references/commands.json` inside this skill.
   - Optional override: user-provided config path via `--config`.
   - Resolve skill root absolute path from this skill file location:
     - `/Users/x/.codex/skills/opencli-sequential-news-zh`
3. Define run-scoped working paths. Do not reuse flat temp files like `.news_state/tmp_current.json`; each run must use its own artifact directory under `.news_state/runs/<run-dir>/`:

```bash
WORKDIR=<absolute current working directory>
STATE_DIR=<WORKDIR>/.news_state
RUNS_DIR=<STATE_DIR>/runs
RUN_DIR=<RUNS_DIR>/<unique-run-dir>
CURRENT_JSON_PATH=<RUN_DIR>/current.json
INCREMENTAL_JSON_PATH=<RUN_DIR>/incremental.json
TRANSLATED_JSON_PATH=<RUN_DIR>/translated.json
```

4. Run pipeline script sequentially:

```bash
python3 /Users/x/.codex/skills/opencli-sequential-news-zh/scripts/run_news_pipeline.py --config <commands.json> --out-json <CURRENT_JSON_PATH>
```

5. Wait for step 4 to exit successfully before continuing. Never run `prepare` while the pipeline command is still in flight.
6. Prepare incremental payload:

```bash
python3 /Users/x/.codex/skills/opencli-sequential-news-zh/scripts/run_incremental_news.py prepare --current-json <CURRENT_JSON_PATH> --state-dir <STATE_DIR> --out-json <INCREMENTAL_JSON_PATH>
```

Prepare recovery policy:

- If `prepare` succeeds, continue normally.
- If `prepare` fails, inspect stderr for a bracketed `PREPARE_*` error code.
- For recoverable prepare codes, abandon the current run artifact directory, create a fresh `<RUN_DIR>`, rerun the pipeline from step 4, then rerun `prepare` once.
- Do not reuse the failed run's `current.json`, `incremental.json`, or `translated.json`.
- Do not retry more than once. If the second `prepare` fails, stop and report both the original and retry failures.
- Do not retry non-recoverable prepare codes; stop and report the error.
- When an automatic prepare retry happens, mention it in the final response with the first failure reason and the new run directory.

Recoverable prepare codes:

- `PREPARE_STALE_CURRENT_JSON`: the current payload is older than the latest finalized run.
- `PREPARE_RUN_ID_ALREADY_FINALIZED`: the run id has already been finalized today.
- `PREPARE_GENERATED_AT_ALREADY_FINALIZED`: the generated timestamp has already been finalized today.
- `PREPARE_CURRENT_JSON_UNREADABLE`: the current run artifact is missing or not valid JSON.

Non-recoverable prepare codes:

- `PREPARE_BAD_ARTIFACT_PATH`: artifact paths are outside the run directory, mixed across run directories, identical, or inconsistent with stored metadata.
- `PREPARE_BAD_CURRENT_JSON`: `current.json` exists but does not match the expected pipeline payload structure.
- `PREPARE_BAD_RUN_METADATA`: run identity, timezone, or timestamp metadata is missing or invalid.
- `PREPARE_BAD_STATE`: the daily state file is invalid or cannot be parsed safely.
- `PREPARE_WRITE_FAILED`: `incremental.json` could not be written.

7. Parse incremental JSON result:
   - `run_fresh_items_raw`: this run's fresh stories after removing yesterday URLs and earlier same-day URLs.
   - `items_to_translate`: stories whose titles still need model translation for display.
   - `current_run_errors`: errors and recovered degradations from this run. When a primary command fails but a retry or fallback succeeds, the pipeline may still emit an `已恢复：...` entry here so downstream reports can surface source health issues.
   - `daily_errors`: accumulated errors and recovered degradations for the current day.
   - `run_id` / `started_at` / `finished_at`: immutable run identity fields. Downstream steps must preserve them exactly.
   - `state_snapshot`: the latest finalized daily state seen during `prepare`. `finalize` will reject stale snapshots.
8. Translate display text into Chinese in-model:
   - Translate only `items_to_translate`.
   - For every item, translate `title`.
   - For Twitter quote items, translate quote text when present.
   - For Bloomberg items with `summary`, translate `summary` too; final Markdown displays the translated summary under the Bloomberg item.
   - Before `finalize`, verify Bloomberg items whose source `summary` is non-Chinese. If `translated.json` is missing a Chinese `summary_zh` / `summary`, add one in-model and rewrite `translated.json`.
   - If a Bloomberg summary still cannot be translated after this repair attempt, continue with the original source summary. `finalize` will record a warning in the output errors section instead of failing.
   - Translation must stay in the model, not inside any script.
   - Write a JSON object into `<TRANSLATED_JSON_PATH>`:
     - Legacy format (still supported): map URL to translated title string.
     - Extended format (recommended): map URL to object with `title`, optional quote fields, and optional `summary` / `summary_zh`.
   - If `items_to_translate` is empty, still write `{}` to `<TRANSLATED_JSON_PATH>`.

```json
{
  "https://example.com/story": "中文标题",
  "https://x.com/ivanalog_com/status/123?s=20": {
    "title": "中文正文标题",
    "quoted_text_zh": "引用推文中文翻译"
  },
  "https://www.bloomberg.com/news/articles/example": {
    "title": "中文标题",
    "summary_zh": "中文摘要"
  }
}
```

9. Finalize outputs:

```bash
python3 /Users/x/.codex/skills/opencli-sequential-news-zh/scripts/run_incremental_news.py finalize --incremental-json <INCREMENTAL_JSON_PATH> --translated-json <TRANSLATED_JSON_PATH> --state-dir <STATE_DIR> --out-dir <WORKDIR>
```

Finalize recovery policy:

- If `finalize` succeeds, continue normally.
- If `finalize` fails, inspect stderr for a bracketed `FINALIZE_*` error code.
- For `FINALIZE_STATE_CHANGED_SINCE_PREPARE`, rerun `prepare` once using the same `current_json_path` stored in `incremental.json.paths.current_json_path`, the same `STATE_DIR`, and the same `INCREMENTAL_JSON_PATH`.
- After rerunning `prepare`, reuse the existing `translated.json` as a base, translate only newly missing `items_to_translate` fields, then rerun `finalize` once.
- Do not rerun the pipeline as part of finalize recovery. If the same `current.json` is rejected during the new `prepare`, stop and report that the current artifact is no longer usable against the latest state.
- Do not retry `FINALIZE_OUTPUT_EXISTS`, `FINALIZE_WRITE_FAILED`, bad artifact paths, bad JSON, bad metadata, bad state, or already-finalized runs.
- Bloomberg summary translation issues are handled before `finalize` by repairing `translated.json`; if still unresolved, `finalize` uses the original summary and records a warning instead of returning a failure.

Recoverable finalize codes:

- `FINALIZE_STATE_CHANGED_SINCE_PREPARE`: daily state changed after `prepare`; rerun `prepare` from the same `current.json`.

Non-recoverable finalize codes:

- `FINALIZE_BAD_ARTIFACT_PATH`: artifact paths are outside the run directory, mixed across run directories, or inconsistent with stored metadata.
- `FINALIZE_BAD_INCREMENTAL_JSON`: `incremental.json` is missing, invalid JSON, or not an object.
- `FINALIZE_BAD_TRANSLATED_JSON`: `translated.json` is missing, invalid JSON, or not an object.
- `FINALIZE_BAD_RUN_METADATA`: run identity, timezone, or timestamp metadata is missing or invalid.
- `FINALIZE_BAD_INCREMENTAL_METADATA`: date, run timestamp, paths, or state snapshot metadata is invalid.
- `FINALIZE_BAD_STATE`: the daily state file is invalid or cannot be parsed safely.
- `FINALIZE_RUN_ALREADY_FINALIZED`: this run id or generated timestamp has already been finalized.
- `FINALIZE_OUTPUT_EXISTS`: the target per-run `freshNews.md` already exists.
- `FINALIZE_WRITE_FAILED`: an output Markdown or state file could not be written.

10. Finalize writes exactly two user-facing Markdown files:
   - `dailyFreshNews_YYYY-MM-DD.md`: one rolling summary file per day.
   - `YYYY-MM-DD-HH-mm_freshNews.md`: one per-run fresh-news file.
   - Timezone: `Asia/Shanghai` unless user explicitly requests another timezone.
11. Hidden state is stored separately in the state directory, one JSON file per day.
12. Safety rules:
   - `prepare` and `finalize` now require run artifacts to live under `<STATE_DIR>/runs/<run-dir>/`.
   - `finalize` never overwrites an existing `YYYY-MM-DD-HH-mm_freshNews.md`.
   - If `prepare` sees a `current.json` older than the latest finalized run, it fails instead of returning a misleading `0 条新增`.
   - Recoverable `prepare` failures may trigger one clean retry from a new run directory; non-recoverable failures must remain hard stops.
   - If state changes after `prepare`, rerun `prepare` from the same `current.json`; do not force `finalize` and do not automatically rerun pipeline.

## State Schema Notes

- Daily state file path: `<STATE_DIR>/YYYY-MM-DD.json`.
- Top-level keys are daily aggregates and metadata, for example:
  - `date`, `timezone`, `section_order`
  - `today_seen_urls`, `today_first_seen_items`
  - `daily_errors`
  - `runs` (array of per-run summaries)
- Run artifact directory: `<STATE_DIR>/runs/<run-dir>/`.
- Pipeline payload now includes `run_id`, `started_at`, `finished_at`, and may include `current_json_path`.
- Per-run counters are stored under `runs[-1]` (latest run), not at top level.
  - Read `runs[-1].run_fresh_count` for this run's fresh count.
  - Read `runs[-1].daily_fresh_count` for current day cumulative fresh count.
  - Read `runs[-1].error_count` for this run error count.
  - Read `runs[-1].run_fresh_path` / `runs[-1].daily_fresh_path` for output files.
  - New runs also record audit fields such as `run_id`, `current_json_path`, `incremental_json_path`, `translated_json_path`, `prepared_at`, and `finalized_at`.
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
- Optional reliability fields are supported per source:
  - `retry_once`: retry the primary command once before recording failure.
  - `fallback_command`: secondary command when primary still fails.
  - `treat_empty_as_failure`: treat zero valid rows as failure for retry/fallback.
  - `min_valid_items`: minimum valid rows required for success when empty-check is enabled.
- Current policy in this skill:
  - News portal sources use `retry_once`; most also use `treat_empty_as_failure: true` and `min_valid_items: 1`.
  - `bbc_news` uses `opencli bbc news` as primary command with `retry_once`; no dedicated fallback chain.
  - Twitter sources use `retry_once` only; do not force empty-as-failure by default.

## Output Contract

For each output Markdown file:

- Emit full `## section（N条）` sections only for non-empty groups after filtering.
- Use display-friendly section names when available, such as `Reuters · World` or `TechCrunch`.
- Add a one-line summary blockquote under each non-empty section header:
  - Format: `> N条｜最新 ...｜最早 ...｜时间倒序`
- Separate adjacent non-empty sections with `---`.
- Do not emit standalone `（0条）` section headers for empty groups.
- Instead, append a summary section at the end:
  - `## 本次无更新的分组（X个）`
  - List each empty group as a bullet using its display name, or `- 无` when there are none.

Example non-empty section:

```markdown
## Reuters · World（3条）

> 3条｜最新 2026-04-09 10:00:00｜最早 2026-04-09 08:00:00｜时间倒序

### [中文标题](https://...)
- 发布时间：YYYY-MM-DD HH:MM:SS
```

Example empty-group summary:

```markdown
## 本次无更新的分组（2个）

- Bloomberg
- BBC
```

Constraints:
- Missing time must be `页面未显示`.
- Bloomberg summaries should be rendered in Chinese when translation is available. The translation map may use `summary` or `summary_zh`; `summary_zh` is preferred for clarity.
- If a non-Chinese Bloomberg summary is present but its translated summary is missing or still non-Chinese after one repair attempt, `finalize` must write the original source summary and record a warning in errors instead of failing.
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
5. Non-empty sections include display names and summary blockquotes; empty sections are grouped under `本次无更新的分组`.
6. Finalize writes `dailyFreshNews_YYYY-MM-DD.md` and `YYYY-MM-DD-HH-mm_freshNews.md`, not `*_fullNews.md`.
7. Reusing stale pipeline JSON or attempting to overwrite an existing run file must fail loudly.
