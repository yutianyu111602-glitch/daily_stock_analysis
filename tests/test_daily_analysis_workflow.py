# -*- coding: utf-8 -*-
"""Regression tests for GitHub Actions workflow wiring."""

from pathlib import Path
import unittest


WORKFLOW_PATH = (
    Path(__file__).resolve().parents[1] / ".github" / "workflows" / "daily_analysis.yml"
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


class RuleScreenerWorkflowTestCase(unittest.TestCase):
    def test_rule_screener_workflow_exists_and_maps_required_env(self) -> None:
        workflow_path = (
            Path(__file__).resolve().parents[1]
            / ".github"
            / "workflows"
            / "rule_screener.yml"
        )
        workflow = workflow_path.read_text(encoding="utf-8")

        self.assertIn("TUSHARE_TOKEN: ${{ secrets.TUSHARE_TOKEN }}", workflow)
        self.assertIn("AIHUBMIX_KEY: ${{ secrets.AIHUBMIX_KEY }}", workflow)
        self.assertIn("SERVERCHAN3_SENDKEY: ${{ secrets.SERVERCHAN3_SENDKEY }}", workflow)
        self.assertIn("RULE_SCREENER_EXCLUDE_ST: ${{ vars.RULE_SCREENER_EXCLUDE_ST || 'true' }}", workflow)
        self.assertIn("RULE_SCREENER_ALLOW_FALLBACK: ${{ vars.RULE_SCREENER_ALLOW_FALLBACK || 'false' }}", workflow)
        self.assertIn("RULE_SCREENER_AUTO_RELAX_IF_EMPTY: ${{ vars.RULE_SCREENER_AUTO_RELAX_IF_EMPTY || 'true' }}", workflow)
        self.assertIn("RULE_SCREENER_MAX_BIAS_MA5_PCT: ${{ vars.RULE_SCREENER_MAX_BIAS_MA5_PCT || '8' }}", workflow)
        self.assertIn("RULE_SCREENER_AUTO_APPEND_TO_STOCK_LIST: ${{ vars.RULE_SCREENER_AUTO_APPEND_TO_STOCK_LIST || 'true' }}", workflow)
        self.assertIn("RULE_SCREENER_STOCK_POOL_REPO: ${{ vars.RULE_SCREENER_STOCK_POOL_REPO || github.repository }}", workflow)
        self.assertIn("GITHUB_TOKEN: ${{ github.token }}", workflow)
        self.assertIn("RULE_SCREENER_DISABLE_GEMINI: ${{ vars.RULE_SCREENER_DISABLE_GEMINI || 'true' }}", workflow)
        self.assertIn("RULE_SCREENER_CACHE_DIR: .cache/rule_screener/tushare", workflow)
        self.assertIn("LITELLM_MODEL: ${{ vars.RULE_SCREENER_LITELLM_MODEL || 'openai/gpt-5-chat-latest' }}", workflow)
        self.assertIn("LITELLM_FALLBACK_MODELS: ${{ vars.RULE_SCREENER_LITELLM_FALLBACK_MODELS || '' }}", workflow)
        self.assertIn("GEMINI_MODEL: ${{ vars.GEMINI_MODEL || secrets.GEMINI_MODEL || '' }}", workflow)
        self.assertIn("GEMINI_MODEL_FALLBACK: ${{ vars.GEMINI_MODEL_FALLBACK || secrets.GEMINI_MODEL_FALLBACK || '' }}", workflow)
        self.assertIn("RULE_SCREENER_PREFER_AIHUBMIX: ${{ vars.RULE_SCREENER_PREFER_AIHUBMIX || 'true' }}", workflow)
        self.assertIn("RULE_SCREENER_AIHUBMIX_MODEL: ${{ vars.RULE_SCREENER_AIHUBMIX_MODEL || 'gpt-5-chat-latest' }}", workflow)
        self.assertIn("uses: actions/cache@v4", workflow)
        self.assertIn("python scripts/run_rule_screener.py", workflow)


if __name__ == "__main__":
    unittest.main()
