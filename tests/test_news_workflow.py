#!/usr/bin/env python3
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
        deduped_items: list[dict] | None = None,
        errors: list[dict] | None = None,
    ) -> dict:
        return {
            "run_id": run_id,
            "started_at": started_at,
            "finished_at": finished_at,
            "generated_at": finished_at,
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
        self.assertEqual(existing_run_file.read_text(encoding="utf-8"), "existing\n")

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

    def test_finalize_help_no_longer_mentions_overwrite_flag(self) -> None:
        result = self.run_cmd(str(INCREMENTAL_SCRIPT), "finalize", "--help")
        self.assertEqual(result.returncode, 0)
        self.assertNotIn("--allow-overwrite-existing-run", result.stdout)


if __name__ == "__main__":
    unittest.main()
