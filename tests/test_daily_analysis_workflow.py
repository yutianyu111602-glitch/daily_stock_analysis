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


if __name__ == "__main__":
    unittest.main()
