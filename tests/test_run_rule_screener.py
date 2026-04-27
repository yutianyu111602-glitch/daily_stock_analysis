from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "run_rule_screener.py"
SPEC = importlib.util.spec_from_file_location("run_rule_screener_test_module", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
prepare_rule_screener_env = MODULE.prepare_rule_screener_env


class RunRuleScreenerEnvTestCase(unittest.TestCase):
    def test_prepare_rule_screener_env_sets_dynamic_defaults_when_missing(self) -> None:
        env = {
            "AIHUBMIX_KEY": "test-key",
            "RULE_SCREENER_PREFER_AIHUBMIX": "true",
            "RULE_SCREENER_AIHUBMIX_MODEL": "gpt-5-chat-latest",
        }
        with patch.dict(os.environ, env, clear=False):
            prepare_rule_screener_env()
            self.assertEqual(os.environ["RULE_SCREENER_DYNAMIC_MODE"], "true")
            self.assertEqual(os.environ["RULE_SCREENER_ALLOW_EMPTY_REPORT"], "false")
            self.assertEqual(os.environ["RULE_SCREENER_MANUAL_REVIEW_LIMIT"], "20")

    def test_prepare_rule_screener_env_preserves_existing_dynamic_defaults(self) -> None:
        env = {
            "AIHUBMIX_KEY": "test-key",
            "RULE_SCREENER_PREFER_AIHUBMIX": "true",
            "RULE_SCREENER_AIHUBMIX_MODEL": "gpt-5-chat-latest",
            "RULE_SCREENER_DYNAMIC_MODE": "custom-true",
            "RULE_SCREENER_ALLOW_EMPTY_REPORT": "custom-false",
            "RULE_SCREENER_MANUAL_REVIEW_LIMIT": "7",
        }
        with patch.dict(os.environ, env, clear=False):
            prepare_rule_screener_env()
            self.assertEqual(os.environ["RULE_SCREENER_DYNAMIC_MODE"], "custom-true")
            self.assertEqual(os.environ["RULE_SCREENER_ALLOW_EMPTY_REPORT"], "custom-false")
            self.assertEqual(os.environ["RULE_SCREENER_MANUAL_REVIEW_LIMIT"], "7")

    def test_prepare_rule_screener_env_disables_auto_relax_when_dynamic_mode_is_disabled(self) -> None:
        env = {
            "AIHUBMIX_KEY": "test-key",
            "RULE_SCREENER_DYNAMIC_MODE": "false",
            "RULE_SCREENER_AUTO_RELAX_IF_EMPTY": "true",
        }
        with patch.dict(os.environ, env, clear=False):
            prepare_rule_screener_env()
            self.assertEqual(os.environ["RULE_SCREENER_AUTO_RELAX_IF_EMPTY"], "false")

    def test_prepare_rule_screener_env_forces_aihubmix_model_when_enabled(self) -> None:
        env = {
            "AIHUBMIX_KEY": "test-key",
            "RULE_SCREENER_PREFER_AIHUBMIX": "true",
            "RULE_SCREENER_AIHUBMIX_MODEL": "gpt-5-chat-latest",
            "LITELLM_MODEL": "gemini/gemini-2.0-flash",
        }
        with patch.dict(os.environ, env, clear=False):
            prepare_rule_screener_env()
            self.assertEqual(os.environ["LITELLM_MODEL"], "openai/gpt-5-chat-latest")
            self.assertEqual(os.environ["OPENAI_MODEL"], "gpt-5-chat-latest")

    def test_prepare_rule_screener_env_removes_gemini_keys_when_disabled(self) -> None:
        env = {
            "AIHUBMIX_KEY": "test-key",
            "RULE_SCREENER_PREFER_AIHUBMIX": "true",
            "RULE_SCREENER_AIHUBMIX_MODEL": "gpt-5-chat-latest",
            "RULE_SCREENER_DISABLE_GEMINI": "true",
            "GEMINI_API_KEY": "gemini-key",
        }
        with patch.dict(os.environ, env, clear=False):
            prepare_rule_screener_env()
            self.assertNotIn("GEMINI_API_KEY", os.environ)
            self.assertEqual(os.environ["LITELLM_MODEL"], "openai/gpt-5-chat-latest")

    def test_extract_rule_screener_summary_parses_report_sections(self) -> None:
        report = """# A股规则选股日报 20260414

## 完整命中（2 只）

## 动态放宽命中（1 只）

## 技术候选池（3 只）

## 人工精选池（5 只）
"""

        summary = MODULE.extract_rule_screener_summary(report)
        self.assertEqual(summary["full"], 2)
        self.assertEqual(summary["relaxed"], 1)
        self.assertEqual(summary["technical"], 3)
        self.assertEqual(summary["manual"], 5)

    def test_extract_regime_debug_notes_returns_market_related_entries(self) -> None:
        notes = [
            "严格条件命中优先；严格档为 0 时，才会按市场状态进入动态放宽。",
            "市场环境：弱势日",
            "动态放宽：板块涨幅阈值：1.0 -> 0.8（弱势日放宽板块强度）",
            "技术候选池满足核心技术结构，板块强度仅作参考，不自动并入自选池。",
        ]

        extracted = MODULE.extract_regime_debug_notes(notes)
        self.assertEqual(
            extracted,
            [
                "市场环境：弱势日",
                "动态放宽：板块涨幅阈值：1.0 -> 0.8（弱势日放宽板块强度）",
            ],
        )

    def test_should_block_empty_report_follows_env_flag(self) -> None:
        with patch.dict(os.environ, {"RULE_SCREENER_ALLOW_EMPTY_REPORT": "false"}, clear=False):
            self.assertTrue(MODULE.should_block_empty_report(0))
            self.assertFalse(MODULE.should_block_empty_report(2))

        with patch.dict(os.environ, {"RULE_SCREENER_ALLOW_EMPTY_REPORT": "true"}, clear=False):
            self.assertFalse(MODULE.should_block_empty_report(0))

    def test_main_returns_nonzero_and_logs_regime_diagnostics_when_empty_report_is_blocked(self) -> None:
        fake_result = SimpleNamespace(
            trade_date="20260414",
            profile_name="动态放宽版",
            candidates=[],
            report="# A股规则选股日报 20260414\n\n## 结果\n- 今日未筛出符合条件的A股股票。\n",
            profile_notes=[
                "市场环境：弱势日",
                "动态放宽：板块涨幅阈值：1.0 -> 0.8（弱势日放宽板块强度）",
            ],
            buckets=None,
        )
        fake_args = SimpleNamespace(debug=False, no_notify=False, no_ai_review=False)

        with patch.dict(
            os.environ,
            {
                "RULE_SCREENER_ALLOW_EMPTY_REPORT": "false",
                "RULE_SCREENER_DEBUG_REGIME": "true",
            },
            clear=False,
        ), patch.object(MODULE, "parse_args", return_value=fake_args), \
            patch.object(MODULE, "setup_env"), \
            patch.object(MODULE, "get_config", return_value=SimpleNamespace(log_dir="logs")), \
            patch.object(MODULE, "setup_logging"), \
            patch.object(MODULE, "AshareRuleScreenerService") as service_cls, \
            patch.object(MODULE.logging, "getLogger") as get_logger:
            logger = get_logger.return_value
            service_cls.return_value.run.return_value = fake_result

            exit_code = MODULE.main()

        self.assertEqual(exit_code, 2)
        service_cls.return_value.run.assert_called_once_with(send_notification=True, ai_review=True)
        self.assertTrue(
            any(
                call.args and call.args[0] == "规则选股市场状态诊断: %s"
                for call in logger.info.call_args_list
            )
        )


if __name__ == "__main__":
    unittest.main()
