# -*- coding: utf-8 -*-
"""Regression tests for GitHub Actions workflow wiring."""

from pathlib import Path
import unittest


WORKFLOW_PATH = (
    Path(__file__).resolve().parents[1] / ".github" / "workflows" / "daily_analysis.yml"
)
RULE_SCREENER_WORKFLOW_PATH = (
    Path(__file__).resolve().parents[1] / ".github" / "workflows" / "rule_screener.yml"
)
CLOSE_COMBO_WORKFLOW_PATH = (
    Path(__file__).resolve().parents[1] / ".github" / "workflows" / "close_combo_push.yml"
)


class DailyAnalysisWorkflowTestCase(unittest.TestCase):
    def test_workflow_maps_a_share_data_and_search_keys(self) -> None:
        workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

        self.assertIn("TICKFLOW_API_KEY: ${{ secrets.TICKFLOW_API_KEY }}", workflow)
        self.assertIn("ANSPIRE_API_KEYS: ${{ secrets.ANSPIRE_API_KEYS }}", workflow)

    def test_intraday_profile_uses_detailed_report_settings(self) -> None:
        workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

        self.assertIn('REPORT_TYPE="full"', workflow)
        self.assertIn('NEWS_STRATEGY_PROFILE="short"', workflow)
        self.assertNotIn('REPORT_TYPE="brief"', workflow)
        self.assertNotIn('NEWS_STRATEGY_PROFILE="ultra_short"', workflow)

    def test_workflow_uses_four_scheduled_sessions_without_random_delay(self) -> None:
        workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

        self.assertIn("22 2 * * 1-5", workflow)
        self.assertIn("32 3 * * 1-5", workflow)
        self.assertIn("22 6 * * 1-5", workflow)
        self.assertIn("0 7 * * 1-5", workflow)
        self.assertNotIn("随机延迟（避免固定时间访问）", workflow)
        self.assertIn("SESSION_PROFILE=\"midday\"", workflow)
        self.assertIn("SESSION_PROFILE=\"close\"", workflow)
        self.assertIn("midday)", workflow)
        self.assertIn("close)", workflow)


class RuleScreenerWorkflowTestCase(unittest.TestCase):
    def test_rule_screener_workflow_exists_and_maps_required_env(self) -> None:
        workflow = RULE_SCREENER_WORKFLOW_PATH.read_text(encoding="utf-8")

        self.assertIn("TUSHARE_TOKEN: ${{ secrets.TUSHARE_TOKEN }}", workflow)
        self.assertIn("AIHUBMIX_KEY: ${{ secrets.AIHUBMIX_KEY }}", workflow)
        self.assertIn("SERVERCHAN3_SENDKEY: ${{ secrets.SERVERCHAN3_SENDKEY }}", workflow)
        self.assertIn("RULE_SCREENER_EXCLUDE_ST: ${{ vars.RULE_SCREENER_EXCLUDE_ST || 'true' }}", workflow)
        self.assertIn("RULE_SCREENER_ALLOW_FALLBACK: ${{ vars.RULE_SCREENER_ALLOW_FALLBACK || 'false' }}", workflow)
        self.assertIn('RULE_SCREENER_DYNAMIC_MODE: "${{ vars.RULE_SCREENER_DYNAMIC_MODE || \'true\' }}"', workflow)
        self.assertIn('RULE_SCREENER_ALLOW_EMPTY_REPORT: "${{ vars.RULE_SCREENER_ALLOW_EMPTY_REPORT || \'false\' }}"', workflow)
        self.assertIn('RULE_SCREENER_MANUAL_REVIEW_LIMIT: "${{ vars.RULE_SCREENER_MANUAL_REVIEW_LIMIT || \'15\' }}"', workflow)
        self.assertIn('RULE_SCREENER_FOCUS_POOL_LIMIT: "${{ vars.RULE_SCREENER_FOCUS_POOL_LIMIT || \'10\' }}"', workflow)
        self.assertIn('RULE_SCREENER_DEBUG_SECTOR: "${{ vars.RULE_SCREENER_DEBUG_SECTOR || \'true\' }}"', workflow)
        self.assertIn('RULE_SCREENER_DEBUG_REGIME: "${{ vars.RULE_SCREENER_DEBUG_REGIME || \'true\' }}"', workflow)
        self.assertIn("RULE_SCREENER_AUTO_RELAX_IF_EMPTY: ${{ vars.RULE_SCREENER_AUTO_RELAX_IF_EMPTY || 'true' }}", workflow)
        self.assertIn("RULE_SCREENER_MIN_PRIOR_RISE_PCT: ${{ vars.RULE_SCREENER_MIN_PRIOR_RISE_PCT || '20' }}", workflow)
        self.assertIn("RULE_SCREENER_MIN_VOLUME_RATIO: ${{ vars.RULE_SCREENER_MIN_VOLUME_RATIO || '1' }}", workflow)
        self.assertIn("RULE_SCREENER_MIN_TURNOVER_RATE: ${{ vars.RULE_SCREENER_MIN_TURNOVER_RATE || '3' }}", workflow)
        self.assertIn("RULE_SCREENER_MIN_SECTOR_CHANGE_PCT: ${{ vars.RULE_SCREENER_MIN_SECTOR_CHANGE_PCT || '1' }}", workflow)
        self.assertIn("RULE_SCREENER_MAX_BIAS_MA5_PCT: ${{ vars.RULE_SCREENER_MAX_BIAS_MA5_PCT || '9' }}", workflow)
        self.assertIn("RULE_SCREENER_SECTOR_TOP_N: ${{ vars.RULE_SCREENER_SECTOR_TOP_N || '5' }}", workflow)
        self.assertIn("RULE_SCREENER_AUTO_APPEND_TO_STOCK_LIST: ${{ vars.RULE_SCREENER_AUTO_APPEND_TO_STOCK_LIST || 'true' }}", workflow)
        self.assertIn("RULE_SCREENER_STOCK_POOL_REPO: ${{ vars.RULE_SCREENER_STOCK_POOL_REPO || github.repository }}", workflow)
        self.assertIn("GITHUB_TOKEN: ${{ github.token }}", workflow)
        self.assertIn("RULE_SCREENER_AI_REVIEW_LIMIT: ${{ vars.RULE_SCREENER_AI_REVIEW_LIMIT || '12' }}", workflow)
        self.assertIn("RULE_SCREENER_DISABLE_GEMINI: ${{ vars.RULE_SCREENER_DISABLE_GEMINI || 'true' }}", workflow)
        self.assertIn("RULE_SCREENER_CACHE_DIR: .cache/rule_screener_v2/tushare", workflow)
        self.assertIn("LITELLM_MODEL: ${{ vars.RULE_SCREENER_LITELLM_MODEL || 'openai/gpt-5-chat-latest' }}", workflow)
        self.assertIn("LITELLM_FALLBACK_MODELS: ${{ vars.RULE_SCREENER_LITELLM_FALLBACK_MODELS || '' }}", workflow)
        self.assertIn("GEMINI_MODEL: ${{ vars.GEMINI_MODEL || secrets.GEMINI_MODEL || '' }}", workflow)
        self.assertIn("GEMINI_MODEL_FALLBACK: ${{ vars.GEMINI_MODEL_FALLBACK || secrets.GEMINI_MODEL_FALLBACK || '' }}", workflow)
        self.assertIn("RULE_SCREENER_PREFER_AIHUBMIX: ${{ vars.RULE_SCREENER_PREFER_AIHUBMIX || 'true' }}", workflow)
        self.assertIn("RULE_SCREENER_AIHUBMIX_MODEL: ${{ vars.RULE_SCREENER_AIHUBMIX_MODEL || 'gpt-5-chat-latest' }}", workflow)
        self.assertIn("uses: actions/cache@v4", workflow)
        self.assertIn('cron: "20 2 * * 1-5"', workflow)
        self.assertIn('cron: "20 6 * * 1-5"', workflow)
        self.assertIn('if [ "${{ github.event_name }}" = "schedule" ]; then', workflow)
        self.assertIn('ARGS="$ARGS --no-ai-review"', workflow)
        self.assertIn("python scripts/run_rule_screener.py", workflow)


class CloseComboWorkflowTestCase(unittest.TestCase):
    def test_close_combo_workflow_exists_and_runs_combined_push_script(self) -> None:
        workflow = CLOSE_COMBO_WORKFLOW_PATH.read_text(encoding="utf-8")

        self.assertIn("15:15", workflow)
        self.assertIn("15 7 * * 1-5", workflow)
        self.assertIn("python scripts/run_close_combo_push.py", workflow)
        self.assertIn("SERVERCHAN3_SENDKEY: ${{ secrets.SERVERCHAN3_SENDKEY }}", workflow)
        self.assertIn("TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}", workflow)


if __name__ == "__main__":
    unittest.main()
