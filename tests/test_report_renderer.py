# -*- coding: utf-8 -*-
"""
===================================
Report Engine - Report renderer tests
===================================

Tests for Jinja2 report rendering and fallback behavior.
"""

import sys
import unittest
from unittest.mock import MagicMock

try:
    import litellm  # noqa: F401
except ModuleNotFoundError:
    sys.modules["litellm"] = MagicMock()

from src.analyzer import AnalysisResult
from src.services.report_renderer import render


def _make_result(
    code: str = "600519",
    name: str = "贵州茅台",
    sentiment_score: int = 72,
    operation_advice: str = "持有",
    analysis_summary: str = "稳健",
    decision_type: str = "hold",
    dashboard: dict = None,
    report_language: str = "zh",
) -> AnalysisResult:
    if dashboard is None:
        dashboard = {
            "core_conclusion": {"one_sentence": "持有观望"},
            "intelligence": {"risk_alerts": []},
            "battle_plan": {"sniper_points": {"stop_loss": "110"}},
        }
    return AnalysisResult(
        code=code,
        name=name,
        trend_prediction="看多",
        sentiment_score=sentiment_score,
        operation_advice=operation_advice,
        analysis_summary=analysis_summary,
        decision_type=decision_type,
        dashboard=dashboard,
        report_language=report_language,
    )


class TestReportRenderer(unittest.TestCase):
    """Report renderer tests."""

    def test_render_markdown_summary_only(self) -> None:
        """Markdown platform renders with summary_only."""
        r = _make_result()
        out = render("markdown", [r], summary_only=True)
        self.assertIsNotNone(out)
        self.assertIn("决策仪表盘", out)
        self.assertIn("贵州茅台", out)
        self.assertIn("持有", out)

    def test_render_markdown_full(self) -> None:
        """Markdown platform renders full report."""
        r = _make_result()
        out = render("markdown", [r], summary_only=False)
        self.assertIsNotNone(out)
        self.assertIn("核心结论", out)
        self.assertIn("作战计划", out)

    def test_render_wechat(self) -> None:
        """Wechat platform renders."""
        r = _make_result()
        out = render("wechat", [r])
        self.assertIsNotNone(out)
        self.assertIn("贵州茅台", out)

    def test_render_brief(self) -> None:
        """Brief platform renders 3-5 sentence summary."""
        r = _make_result()
        out = render("brief", [r])
        self.assertIsNotNone(out)
        self.assertIn("决策简报", out)
        self.assertIn("贵州茅台", out)

    def test_render_markdown_in_english(self) -> None:
        """Markdown renderer switches headings and summary labels for English reports."""
        r = _make_result(
            name="Kweichow Moutai",
            operation_advice="Buy",
            analysis_summary="Momentum remains constructive.",
            report_language="en",
        )
        out = render("markdown", [r], summary_only=True)
        self.assertIsNotNone(out)
        self.assertIn("Decision Dashboard", out)
        self.assertIn("Summary", out)
        self.assertIn("Buy", out)

    def test_render_markdown_market_snapshot_uses_template_context(self) -> None:
        """Market snapshot macro should render localized labels with template context."""
        r = _make_result(
            code="AAPL",
            name="Apple",
            operation_advice="Buy",
            report_language="en",
        )
        r.market_snapshot = {
            "close": "180.10",
            "prev_close": "178.25",
            "open": "179.00",
            "high": "181.20",
            "low": "177.80",
            "pct_chg": "+1.04%",
            "change_amount": "1.85",
            "amplitude": "1.91%",
            "volume": "1200000",
            "amount": "215000000",
            "price": "180.35",
            "volume_ratio": "1.2",
            "turnover_rate": "0.8%",
            "source": "polygon",
        }

        out = render("markdown", [r], summary_only=False)

        self.assertIsNotNone(out)
        self.assertIn("Market Snapshot", out)
        self.assertIn("Volume Ratio", out)

    def test_render_markdown_full_avoids_markdown_tables_for_cross_channel_readability(self) -> None:
        """Detailed markdown report should avoid markdown tables so chat apps keep alignment."""
        r = _make_result()
        r.market_snapshot = {
            "close": "17.83",
            "prev_close": "17.64",
            "open": "17.39",
            "high": "18.43",
            "low": "17.34",
            "pct_chg": "1.08%",
            "change_amount": "0.19",
            "amplitude": "6.18%",
            "volume": "1877.02万股",
            "amount": "3.36亿元",
            "price": "17.83",
            "volume_ratio": "1.62",
            "turnover_rate": "4.77%",
            "source": "tencent",
        }
        r.dashboard = {
            "core_conclusion": {
                "one_sentence": "建议小仓买入，关注短期趋势。",
                "position_advice": {
                    "no_position": "在17.50元附近小仓买入。",
                    "has_position": "保持持仓，关注止损位。",
                },
            },
            "data_perspective": {
                "price_position": {
                    "current_price": "17.83",
                    "ma5": "17.28",
                    "ma10": "16.91",
                    "ma20": "16.62",
                    "bias_ma5": "3.18",
                    "bias_status": "警戒",
                    "support_level": "17.28",
                    "resistance_level": "18.43",
                }
            },
            "battle_plan": {
                "sniper_points": {
                    "ideal_buy": "17.50",
                    "secondary_buy": "17.20",
                    "stop_loss": "16.90",
                    "take_profit": "18.60",
                }
            },
        }

        out = render("markdown", [r], summary_only=False)

        self.assertIsNotNone(out)
        self.assertNotIn("| 收盘 |", out)
        self.assertNotIn("| Price Metrics |", out)
        self.assertIn("- 收盘/昨收/开盘：17.83 / 17.64 / 17.39", out)
        self.assertIn("- 当前价/MA5/MA10/MA20：17.83 / 17.28 / 16.91 / 16.62", out)
        self.assertIn("- 理想买入点：17.50", out)

    def test_render_markdown_full_supports_conservative_decision_style(self) -> None:
        """Conservative style should rewrite signals for steadier investors."""
        r = _make_result(
            operation_advice="买入",
            analysis_summary="放量突破，可关注。",
            dashboard={
                "core_conclusion": {
                    "one_sentence": "建议回踩确认后再考虑参与。",
                    "position_advice": {
                        "no_position": "仅回踩确认后小仓试错，不追高。",
                        "has_position": "保留底仓，跌破止损位先减仓。",
                    },
                },
                "intelligence": {
                    "risk_alerts": ["短线涨幅偏大，追高性价比一般。"],
                },
                "battle_plan": {
                    "sniper_points": {
                        "ideal_buy": "17.50",
                        "stop_loss": "16.90",
                        "take_profit": "18.60",
                    }
                },
            },
        )

        out = render(
            "markdown",
            [r],
            summary_only=False,
            extra_context={"report_decision_style": "conservative"},
        )

        self.assertIsNotNone(out)
        self.assertIn("可小仓试错", out)
        self.assertIn("适合：有纪律的低吸型空仓者", out)
        self.assertIn("不适合：追高入场", out)

    def test_render_unknown_platform_returns_none(self) -> None:
        """Unknown platform returns None (caller fallback)."""
        r = _make_result()
        out = render("unknown_platform", [r])
        self.assertIsNone(out)

    def test_render_empty_results_returns_content(self) -> None:
        """Empty results still produces header."""
        out = render("markdown", [], summary_only=True)
        self.assertIsNotNone(out)
        self.assertIn("0", out)
