#!/usr/bin/env python3
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SKILL_ROOT = Path("/Users/x/.codex/skills/opencli-sequential-news-zh")
PIPELINE_SCRIPT = SKILL_ROOT / "scripts" / "run_news_pipeline.py"
INCREMENTAL_SCRIPT = SKILL_ROOT / "scripts" / "run_incremental_news.py"
TIMEZONE = "Asia/Shanghai"


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def read_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def load_incremental_module() -> object:
    spec = importlib.util.spec_from_file_location("run_incremental_news", INCREMENTAL_SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_state_payload(*, runs: list[dict], today_seen_urls: list[str] | None = None) -> dict:
    return {
        "date": "2026-04-09",
        "timezone": TIMEZONE,
        "section_order": [],
        "today_seen_urls": today_seen_urls or [],
        "today_first_seen_items": [],
        "daily_errors": [],
        "runs": runs,
    }


class NewsWorkflowSafetyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        self.state_dir = self.root / ".news_state"
        self.runs_root = self.state_dir / "runs"
        self.out_dir = self.root / "out"
        self.today_state_path = self.state_dir / "2026-04-09.json"

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def run_cmd(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, *args],
            capture_output=True,
            text=True,
        )

    def make_run_dir(self, name: str) -> Path:
        run_dir = self.runs_root / name
        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir

    def make_current_payload(
        self,
        *,
        run_id: str,
        started_at: str,
        finished_at: str,
        generated_at: str | None = None,
        deduped_items: list[dict] | None = None,
        errors: list[dict] | None = None,
    ) -> dict:
        return {
            "run_id": run_id,
            "started_at": started_at,
            "finished_at": finished_at,
            "generated_at": generated_at or finished_at,
            "timezone": TIMEZONE,
            "section_order": [],
            "grouped_items": {},
            "deduped_items": deduped_items or [],
            "errors": errors or [],
            "recovered_attempts": [],
            "stats": {
                "command_count": 1,
                "section_count": 0,
                "collected_before_dedup": len(deduped_items or []),
                "after_dedup": len(deduped_items or []),
                "error_count": len(errors or []),
                "recovered_count": 0,
            },
        }

    def make_incremental_payload(
        self,
        *,
        run_dir: Path,
        run_id: str,
        started_at: str,
        finished_at: str,
        latest_generated_at: str = "",
        latest_run_id: str = "",
        run_fresh_items: list[dict] | None = None,
    ) -> dict:
        incremental_path = run_dir / "incremental.json"
        return {
            "date": "2026-04-09",
            "run_id": run_id,
            "started_at": started_at,
            "finished_at": finished_at,
            "generated_at": finished_at,
            "timezone": TIMEZONE,
            "section_order": [],
            "run_file_timestamp": "2026-04-09-12-05",
            "current_run_items_raw": run_fresh_items or [],
            "current_run_first_seen_items_raw": run_fresh_items or [],
            "run_fresh_items_raw": run_fresh_items or [],
            "current_run_errors": [],
            "daily_errors": [],
            "items_to_translate": run_fresh_items or [],
            "paths": {
                "run_artifact_dir": str(run_dir),
                "current_json_path": str(run_dir / "current.json"),
                "incremental_json_path": str(incremental_path),
            },
            "state_snapshot": {
                "latest_finalized_run_id": latest_run_id,
                "latest_finalized_generated_at": latest_generated_at,
                "prepared_at": "2026-04-09 12:04:59",
            },
            "state": {
                "today_state_path": str(self.today_state_path),
                "yesterday_state_path": str(self.state_dir / "2026-04-08.json"),
            },
            "stats": {
                "current_run_count": len(run_fresh_items or []),
                "current_run_first_seen_count": len(run_fresh_items or []),
                "run_fresh_count": len(run_fresh_items or []),
                "daily_fresh_count": len(run_fresh_items or []),
                "current_error_count": 0,
                "daily_error_count": 0,
            },
        }

    def test_pipeline_writes_run_identity(self) -> None:
        run_dir = self.make_run_dir("manual-run")
        current_json = run_dir / "current.json"
        config_path = self.root / "commands.json"
        write_json(
            config_path,
            [
                {
                    "section": "world",
                    "command": [
                        sys.executable,
                        "-c",
                        (
                            "import json; "
                            "print(json.dumps([{'title':'Hello','url':'https://example.com/a',"
                            "'time':'2026-04-09 12:00:00'}]))"
                        ),
                    ],
                }
            ],
        )

        result = self.run_cmd(
            str(PIPELINE_SCRIPT),
            "--config",
            str(config_path),
            "--out-json",
            str(current_json),
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        payload = read_json(current_json)
        self.assertIn("run_id", payload)
        self.assertIn("started_at", payload)
        self.assertIn("finished_at", payload)
        self.assertEqual(payload["generated_at"], payload["finished_at"])
        self.assertEqual(
            Path(payload["current_json_path"]).resolve(),
            current_json.resolve(),
        )

    def test_prepare_requires_run_scoped_paths(self) -> None:
        current_json = self.state_dir / "tmp_current.json"
        incremental_json = self.state_dir / "tmp_incremental.json"
        write_json(
            current_json,
            self.make_current_payload(
                run_id="run-flat",
                started_at="2026-04-09 12:00:00",
                finished_at="2026-04-09 12:05:00",
            ),
        )

        result = self.run_cmd(
            str(INCREMENTAL_SCRIPT),
            "prepare",
            "--current-json",
            str(current_json),
            "--state-dir",
            str(self.state_dir),
            "--out-json",
            str(incremental_json),
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("must be stored under", result.stderr)
        self.assertIn("[PREPARE_BAD_ARTIFACT_PATH]", result.stderr)

    def test_prepare_rejects_stale_current_json(self) -> None:
        run_dir = self.make_run_dir("stale-run")
        current_json = run_dir / "current.json"
        incremental_json = run_dir / "incremental.json"
        write_json(
            self.today_state_path,
            make_state_payload(
                runs=[
                    {
                        "run_id": "latest-run",
                        "generated_at": "2026-04-09 12:30:00",
                        "run_fresh_path": "unused",
                        "daily_fresh_path": "unused",
                        "run_fresh_count": 1,
                        "daily_fresh_count": 1,
                        "error_count": 0,
                    }
                ]
            ),
        )
        write_json(
            current_json,
            self.make_current_payload(
                run_id="stale-run",
                started_at="2026-04-09 12:00:00",
                finished_at="2026-04-09 12:05:00",
            ),
        )

        result = self.run_cmd(
            str(INCREMENTAL_SCRIPT),
            "prepare",
            "--current-json",
            str(current_json),
            "--state-dir",
            str(self.state_dir),
            "--out-json",
            str(incremental_json),
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("not newer than the latest finalized run", result.stderr)
        self.assertIn("[PREPARE_STALE_CURRENT_JSON]", result.stderr)

    def test_prepare_marks_duplicate_run_id_with_error_code(self) -> None:
        run_dir = self.make_run_dir("duplicate-run-id")
        current_json = run_dir / "current.json"
        incremental_json = run_dir / "incremental.json"
        write_json(
            self.today_state_path,
            make_state_payload(
                runs=[
                    {
                        "run_id": "duplicate-run",
                        "generated_at": "2026-04-09 12:00:00",
                        "run_fresh_path": "unused",
                        "daily_fresh_path": "unused",
                        "run_fresh_count": 1,
                        "daily_fresh_count": 1,
                        "error_count": 0,
                    }
                ]
            ),
        )
        write_json(
            current_json,
            self.make_current_payload(
                run_id="duplicate-run",
                started_at="2026-04-09 12:01:00",
                finished_at="2026-04-09 12:05:00",
            ),
        )

        result = self.run_cmd(
            str(INCREMENTAL_SCRIPT),
            "prepare",
            "--current-json",
            str(current_json),
            "--state-dir",
            str(self.state_dir),
            "--out-json",
            str(incremental_json),
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("[PREPARE_RUN_ID_ALREADY_FINALIZED]", result.stderr)

    def test_prepare_marks_duplicate_generated_at_with_error_code(self) -> None:
        run_dir = self.make_run_dir("duplicate-generated-at")
        current_json = run_dir / "current.json"
        incremental_json = run_dir / "incremental.json"
        write_json(
            self.today_state_path,
            make_state_payload(
                runs=[
                    {
                        "run_id": "same-generated-at",
                        "generated_at": "2026-04-09 12:05:00",
                        "run_fresh_path": "unused",
                        "daily_fresh_path": "unused",
                        "run_fresh_count": 1,
                        "daily_fresh_count": 1,
                        "error_count": 0,
                    },
                    {
                        "run_id": "latest-run",
                        "generated_at": "2026-04-09 12:04:00",
                        "run_fresh_path": "unused",
                        "daily_fresh_path": "unused",
                        "run_fresh_count": 1,
                        "daily_fresh_count": 2,
                        "error_count": 0,
                    },
                ]
            ),
        )
        write_json(
            current_json,
            self.make_current_payload(
                run_id="new-run",
                started_at="2026-04-09 12:01:00",
                finished_at="2026-04-09 12:05:00",
            ),
        )

        result = self.run_cmd(
            str(INCREMENTAL_SCRIPT),
            "prepare",
            "--current-json",
            str(current_json),
            "--state-dir",
            str(self.state_dir),
            "--out-json",
            str(incremental_json),
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("[PREPARE_GENERATED_AT_ALREADY_FINALIZED]", result.stderr)

    def test_prepare_marks_unreadable_current_json_with_error_code(self) -> None:
        run_dir = self.make_run_dir("missing-current-json")
        current_json = run_dir / "current.json"
        incremental_json = run_dir / "incremental.json"

        result = self.run_cmd(
            str(INCREMENTAL_SCRIPT),
            "prepare",
            "--current-json",
            str(current_json),
            "--state-dir",
            str(self.state_dir),
            "--out-json",
            str(incremental_json),
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("[PREPARE_CURRENT_JSON_UNREADABLE]", result.stderr)

    def test_prepare_marks_bad_current_json_with_error_code(self) -> None:
        run_dir = self.make_run_dir("bad-current-json")
        current_json = run_dir / "current.json"
        incremental_json = run_dir / "incremental.json"
        write_json(current_json, [])

        result = self.run_cmd(
            str(INCREMENTAL_SCRIPT),
            "prepare",
            "--current-json",
            str(current_json),
            "--state-dir",
            str(self.state_dir),
            "--out-json",
            str(incremental_json),
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("[PREPARE_BAD_CURRENT_JSON]", result.stderr)

    def test_prepare_marks_bad_run_metadata_with_error_code(self) -> None:
        run_dir = self.make_run_dir("bad-metadata")
        current_json = run_dir / "current.json"
        incremental_json = run_dir / "incremental.json"
        write_json(
            current_json,
            self.make_current_payload(
                run_id="bad-metadata",
                started_at="2026-04-09 12:00:00",
                finished_at="2026-04-09 12:05:00",
                generated_at="2026-04-09 12:06:00",
            ),
        )

        result = self.run_cmd(
            str(INCREMENTAL_SCRIPT),
            "prepare",
            "--current-json",
            str(current_json),
            "--state-dir",
            str(self.state_dir),
            "--out-json",
            str(incremental_json),
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("[PREPARE_BAD_RUN_METADATA]", result.stderr)

    def test_prepare_marks_bad_state_with_error_code(self) -> None:
        run_dir = self.make_run_dir("bad-state")
        current_json = run_dir / "current.json"
        incremental_json = run_dir / "incremental.json"
        self.today_state_path.parent.mkdir(parents=True, exist_ok=True)
        self.today_state_path.write_text("{bad json", encoding="utf-8")
        write_json(
            current_json,
            self.make_current_payload(
                run_id="bad-state",
                started_at="2026-04-09 12:00:00",
                finished_at="2026-04-09 12:05:00",
            ),
        )

        result = self.run_cmd(
            str(INCREMENTAL_SCRIPT),
            "prepare",
            "--current-json",
            str(current_json),
            "--state-dir",
            str(self.state_dir),
            "--out-json",
            str(incremental_json),
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("[PREPARE_BAD_STATE]", result.stderr)

    def test_prepare_error_code_recoverability_table(self) -> None:
        module = load_incremental_module()
        recoverable_codes = [
            "PREPARE_STALE_CURRENT_JSON",
            "PREPARE_RUN_ID_ALREADY_FINALIZED",
            "PREPARE_GENERATED_AT_ALREADY_FINALIZED",
            "PREPARE_CURRENT_JSON_UNREADABLE",
        ]
        non_recoverable_codes = [
            "PREPARE_BAD_ARTIFACT_PATH",
            "PREPARE_BAD_CURRENT_JSON",
            "PREPARE_BAD_RUN_METADATA",
            "PREPARE_BAD_STATE",
            "PREPARE_WRITE_FAILED",
        ]

        for code in recoverable_codes:
            self.assertTrue(module.is_recoverable_prepare_error_code(code), code)
        for code in non_recoverable_codes:
            self.assertFalse(module.is_recoverable_prepare_error_code(code), code)

    def test_finalize_rejects_existing_run_file(self) -> None:
        run_dir = self.make_run_dir("finalize-run")
        incremental_json = run_dir / "incremental.json"
        translated_json = run_dir / "translated.json"
        self.out_dir.mkdir(parents=True, exist_ok=True)
        existing_run_file = self.out_dir / "2026-04-09-12-05_freshNews.md"
        existing_run_file.write_text("existing\n", encoding="utf-8")
        write_json(self.today_state_path, make_state_payload(runs=[]))
        write_json(
            incremental_json,
            self.make_incremental_payload(
                run_dir=run_dir,
                run_id="finalize-run",
                started_at="2026-04-09 12:00:00",
                finished_at="2026-04-09 12:05:00",
            ),
        )
        write_json(translated_json, {})

        result = self.run_cmd(
            str(INCREMENTAL_SCRIPT),
            "finalize",
            "--incremental-json",
            str(incremental_json),
            "--translated-json",
            str(translated_json),
            "--state-dir",
            str(self.state_dir),
            "--out-dir",
            str(self.out_dir),
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("Refusing to overwrite existing run file", result.stderr)
        self.assertIn("[FINALIZE_OUTPUT_EXISTS]", result.stderr)
        self.assertEqual(existing_run_file.read_text(encoding="utf-8"), "existing\n")

    def test_finalize_writes_new_daily_filename_and_records_it_in_state(self) -> None:
        run_dir = self.make_run_dir("new-daily-name")
        incremental_json = run_dir / "incremental.json"
        translated_json = run_dir / "translated.json"
        write_json(self.today_state_path, make_state_payload(runs=[]))
        write_json(
            incremental_json,
            self.make_incremental_payload(
                run_dir=run_dir,
                run_id="new-daily-name",
                started_at="2026-04-09 12:00:00",
                finished_at="2026-04-09 12:05:00",
            ),
        )
        write_json(translated_json, {})

        result = self.run_cmd(
            str(INCREMENTAL_SCRIPT),
            "finalize",
            "--incremental-json",
            str(incremental_json),
            "--translated-json",
            str(translated_json),
            "--state-dir",
            str(self.state_dir),
            "--out-dir",
            str(self.out_dir),
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        daily_path = self.out_dir / "dailyFreshNews_2026-04-09.md"
        old_daily_path = self.out_dir / "2026-04-09_dailyFreshNews.md"
        run_path = self.out_dir / "2026-04-09-12-05_freshNews.md"
        state_payload = read_json(self.today_state_path)

        self.assertTrue(daily_path.exists())
        self.assertFalse(old_daily_path.exists())
        self.assertTrue(run_path.exists())
        self.assertEqual(Path(payload["daily_fresh_path"]).resolve(), daily_path.resolve())
        self.assertEqual(
            Path(state_payload["runs"][0]["daily_fresh_path"]).resolve(),
            daily_path.resolve(),
        )

    def test_finalize_rejects_state_drift_since_prepare(self) -> None:
        run_dir = self.make_run_dir("drift-run")
        incremental_json = run_dir / "incremental.json"
        translated_json = run_dir / "translated.json"
        write_json(
            self.today_state_path,
            make_state_payload(
                runs=[
                    {
                        "run_id": "old-run",
                        "generated_at": "2026-04-09 10:00:00",
                        "run_fresh_path": "unused",
                        "daily_fresh_path": "unused",
                        "run_fresh_count": 1,
                        "daily_fresh_count": 1,
                        "error_count": 0,
                    },
                    {
                        "run_id": "newer-run",
                        "generated_at": "2026-04-09 11:00:00",
                        "run_fresh_path": "unused",
                        "daily_fresh_path": "unused",
                        "run_fresh_count": 1,
                        "daily_fresh_count": 2,
                        "error_count": 0,
                    },
                ]
            ),
        )
        write_json(
            incremental_json,
            self.make_incremental_payload(
                run_dir=run_dir,
                run_id="drift-run",
                started_at="2026-04-09 12:00:00",
                finished_at="2026-04-09 12:05:00",
                latest_generated_at="2026-04-09 10:00:00",
                latest_run_id="old-run",
            ),
        )
        write_json(translated_json, {})

        result = self.run_cmd(
            str(INCREMENTAL_SCRIPT),
            "finalize",
            "--incremental-json",
            str(incremental_json),
            "--translated-json",
            str(translated_json),
            "--state-dir",
            str(self.state_dir),
            "--out-dir",
            str(self.out_dir),
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("State changed since prepare", result.stderr)
        self.assertIn("[FINALIZE_STATE_CHANGED_SINCE_PREPARE]", result.stderr)

    def test_finalize_allows_untranslated_bloomberg_summary_with_warning(self) -> None:
        run_dir = self.make_run_dir("bloomberg-missing-summary")
        incremental_json = run_dir / "incremental.json"
        translated_json = run_dir / "translated.json"
        item = {
            "section": "bloomberg_main",
            "title": "Original Bloomberg Title",
            "raw_title": "Original Bloomberg Title",
            "time": "2026-04-09 12:00:00",
            "url": "https://www.bloomberg.com/news/articles/example-missing",
            "summary": "Original English summary",
        }
        payload = self.make_incremental_payload(
            run_dir=run_dir,
            run_id="bloomberg-missing-summary",
            started_at="2026-04-09 12:00:00",
            finished_at="2026-04-09 12:05:00",
            run_fresh_items=[item],
        )
        payload["section_order"] = ["bloomberg_main"]
        write_json(self.today_state_path, make_state_payload(runs=[]))
        write_json(incremental_json, payload)
        write_json(
            translated_json,
            {
                item["url"]: {
                    "title": "彭博标题",
                }
            },
        )

        result = self.run_cmd(
            str(INCREMENTAL_SCRIPT),
            "finalize",
            "--incremental-json",
            str(incremental_json),
            "--translated-json",
            str(translated_json),
            "--state-dir",
            str(self.state_dir),
            "--out-dir",
            str(self.out_dir),
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        run_markdown = (self.out_dir / "2026-04-09-12-05_freshNews.md").read_text(
            encoding="utf-8"
        )
        state_payload = read_json(self.today_state_path)
        self.assertIn("- 摘要：Original English summary", run_markdown)
        self.assertIn("Bloomberg summary 未翻译", run_markdown)
        self.assertIn("Bloomberg summary 未翻译", state_payload["daily_errors"][0]["error"])

    def test_finalize_uses_original_bloomberg_summary_when_translation_is_not_chinese(self) -> None:
        run_dir = self.make_run_dir("bloomberg-non-chinese-summary")
        incremental_json = run_dir / "incremental.json"
        translated_json = run_dir / "translated.json"
        item = {
            "section": "bloomberg_main",
            "title": "Original Bloomberg Title",
            "raw_title": "Original Bloomberg Title",
            "time": "2026-04-09 12:00:00",
            "url": "https://www.bloomberg.com/news/articles/example-non-chinese",
            "summary": "Original English summary",
        }
        payload = self.make_incremental_payload(
            run_dir=run_dir,
            run_id="bloomberg-non-chinese-summary",
            started_at="2026-04-09 12:00:00",
            finished_at="2026-04-09 12:05:00",
            run_fresh_items=[item],
        )
        payload["section_order"] = ["bloomberg_main"]
        write_json(self.today_state_path, make_state_payload(runs=[]))
        write_json(incremental_json, payload)
        write_json(
            translated_json,
            {
                item["url"]: {
                    "title": "彭博标题",
                    "summary_zh": "Bad English translation",
                }
            },
        )

        result = self.run_cmd(
            str(INCREMENTAL_SCRIPT),
            "finalize",
            "--incremental-json",
            str(incremental_json),
            "--translated-json",
            str(translated_json),
            "--state-dir",
            str(self.state_dir),
            "--out-dir",
            str(self.out_dir),
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        run_markdown = (self.out_dir / "2026-04-09-12-05_freshNews.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("- 摘要：Original English summary", run_markdown)
        self.assertNotIn("Bad English translation", run_markdown)
        self.assertIn("Bloomberg summary 翻译看起来仍非中文", run_markdown)

    def test_finalize_marks_bad_translated_json_with_error_code(self) -> None:
        run_dir = self.make_run_dir("bad-translated-json")
        incremental_json = run_dir / "incremental.json"
        translated_json = run_dir / "translated.json"
        write_json(self.today_state_path, make_state_payload(runs=[]))
        write_json(
            incremental_json,
            self.make_incremental_payload(
                run_dir=run_dir,
                run_id="bad-translated-json",
                started_at="2026-04-09 12:00:00",
                finished_at="2026-04-09 12:05:00",
            ),
        )
        write_json(translated_json, [])

        result = self.run_cmd(
            str(INCREMENTAL_SCRIPT),
            "finalize",
            "--incremental-json",
            str(incremental_json),
            "--translated-json",
            str(translated_json),
            "--state-dir",
            str(self.state_dir),
            "--out-dir",
            str(self.out_dir),
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("[FINALIZE_BAD_TRANSLATED_JSON]", result.stderr)

    def test_finalize_marks_already_finalized_run_with_error_code(self) -> None:
        run_dir = self.make_run_dir("already-finalized")
        incremental_json = run_dir / "incremental.json"
        translated_json = run_dir / "translated.json"
        write_json(
            self.today_state_path,
            make_state_payload(
                runs=[
                    {
                        "run_id": "already-finalized",
                        "generated_at": "2026-04-09 12:05:00",
                        "run_fresh_path": "unused",
                        "daily_fresh_path": "unused",
                        "run_fresh_count": 1,
                        "daily_fresh_count": 1,
                        "error_count": 0,
                    }
                ]
            ),
        )
        write_json(
            incremental_json,
            self.make_incremental_payload(
                run_dir=run_dir,
                run_id="already-finalized",
                started_at="2026-04-09 12:00:00",
                finished_at="2026-04-09 12:05:00",
                latest_generated_at="2026-04-09 12:05:00",
                latest_run_id="already-finalized",
            ),
        )
        write_json(translated_json, {})

        result = self.run_cmd(
            str(INCREMENTAL_SCRIPT),
            "finalize",
            "--incremental-json",
            str(incremental_json),
            "--translated-json",
            str(translated_json),
            "--state-dir",
            str(self.state_dir),
            "--out-dir",
            str(self.out_dir),
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("[FINALIZE_RUN_ALREADY_FINALIZED]", result.stderr)

    def test_finalize_error_code_recoverability_table(self) -> None:
        module = load_incremental_module()
        self.assertTrue(
            module.is_recoverable_finalize_error_code(
                "FINALIZE_STATE_CHANGED_SINCE_PREPARE"
            )
        )
        for code in [
            "FINALIZE_BAD_ARTIFACT_PATH",
            "FINALIZE_BAD_INCREMENTAL_JSON",
            "FINALIZE_BAD_TRANSLATED_JSON",
            "FINALIZE_BAD_RUN_METADATA",
            "FINALIZE_BAD_INCREMENTAL_METADATA",
            "FINALIZE_BAD_STATE",
            "FINALIZE_RUN_ALREADY_FINALIZED",
            "FINALIZE_OUTPUT_EXISTS",
            "FINALIZE_WRITE_FAILED",
        ]:
            self.assertFalse(module.is_recoverable_finalize_error_code(code), code)

    def test_finalize_help_no_longer_mentions_overwrite_flag(self) -> None:
        result = self.run_cmd(str(INCREMENTAL_SCRIPT), "finalize", "--help")
        self.assertEqual(result.returncode, 0)
        self.assertNotIn("--allow-overwrite-existing-run", result.stdout)


if __name__ == "__main__":
    unittest.main()
