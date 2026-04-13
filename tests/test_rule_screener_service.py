from __future__ import annotations

from datetime import datetime, timedelta
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from src.services.rule_screener_service import (
    AshareRuleScreenerService,
    AshareRuleConfig,
    DynamicAdjustment,
    RuleScreeningCandidate,
    RuleScreeningBuckets,
    TurnoverSnapshot,
    _build_dynamic_rule_config,
    _build_sector_snapshot_from_tushare,
    _classify_market_regime,
    _filter_stock_universe,
    _merge_stock_codes,
    _split_stock_codes,
    apply_selection_rules,
    build_technical_candidate_pool,
    build_screening_report,
)


def _build_matching_history(code: str) -> pd.DataFrame:
    start = datetime(2026, 1, 2)
    closes = [
        9.4, 9.5, 9.6, 9.8, 9.9, 10.0, 10.1, 10.0, 9.9, 10.0,
        10.0, 10.2, 10.5, 10.8, 11.2, 11.6, 12.0, 12.4, 12.8, 13.3,
        13.8, 14.2, 14.8, 15.1, 15.5, 15.9, 16.4, 16.8, 17.1, 17.4,
        17.6, 17.9, 18.1, 18.4, 18.8, 19.0, 19.3, 19.6, 19.8, 20.1,
        19.4, 18.8, 18.1, 17.6, 17.2, 17.8, 18.3, 18.7, 18.1, 17.6,
        18.0, 18.6, 19.0, 19.4, 19.9, 20.2, 20.6, 21.0, 21.3, 21.7,
    ]
    volumes = [100 + i * 2 for i in range(54)] + [220, 260, 310, 360, 410, 520]
    rows = []
    for idx, close in enumerate(closes):
        trade_date = (start + timedelta(days=idx)).strftime("%Y%m%d")
        rows.append(
            {
                "code": code,
                "trade_date": trade_date,
                "open": round(close * 0.99, 2),
                "high": round(close * 1.02, 2),
                "low": round(close * 0.98, 2),
                "close": close,
                "volume": volumes[idx],
            }
        )
    return pd.DataFrame(rows)


def _build_candidate(
    code: str,
    *,
    name: str = "样本股票",
    sector_name: str = "样本板块",
    sector_change_pct: float = 3.1,
) -> RuleScreeningCandidate:
    return RuleScreeningCandidate(
        code=code,
        name=name,
        close=21.7,
        ma5=20.96,
        ma10=20.08,
        ma20=18.94,
        bias_ma5_pct=3.53,
        volume_ratio=1.86,
        turnover_rate=8.2,
        sector_name=sector_name,
        sector_change_pct=sector_change_pct,
        prior_rise_pct=34.8,
        abc_pattern_confirmed=True,
        notes=["放量站回20日线", "5/10/20日线多头排列"],
    )


def _build_service(
    *,
    config: AshareRuleConfig | None = None,
    daily_history: pd.DataFrame | None = None,
    latest_turnover: dict[str, float] | None = None,
) -> AshareRuleScreenerService:
    service = object.__new__(AshareRuleScreenerService)
    service.config = MagicMock()
    service.rule_config = config or AshareRuleConfig()
    service.notifier = MagicMock()
    service.fetcher_manager = MagicMock()
    service._load_trade_dates = MagicMock(return_value=["20260413", "20260201"])
    service._select_analysis_trade_dates = MagicMock(return_value=["20260413", "20260201"])
    service._load_stock_universe = MagicMock(return_value=pd.DataFrame())
    service._load_daily_history = MagicMock(
        return_value=daily_history if daily_history is not None else _build_matching_history("300565")
    )
    service._load_latest_turnover = MagicMock(
        return_value=latest_turnover if latest_turnover is not None else {"300565": 7.6}
    )
    service._select_technical_candidates = MagicMock()
    service._load_sector_snapshot = MagicMock()
    service._sync_candidates_to_stock_pool = MagicMock(return_value=["已自动加入自选池：300565"])
    service._build_ai_review_lines = MagicMock(return_value=["AI 复核摘要"])
    return service


class RuleScreenerServiceTestCase(unittest.TestCase):
    def test_apply_selection_rules_returns_candidate_when_all_rules_match(self) -> None:
        history = _build_matching_history("300490")
        config = AshareRuleConfig()

        matched = apply_selection_rules(
            daily_history=history,
            latest_turnover={"300490": 8.2},
            sector_snapshot={"300490": [{"name": "化工", "change_pct": 3.4}]},
            config=config,
        )

        self.assertEqual([item.code for item in matched], ["300490"])
        self.assertGreater(matched[0].prior_rise_pct, 20)
        self.assertTrue(matched[0].abc_pattern_confirmed)
        self.assertGreaterEqual(matched[0].sector_change_pct, 2.0)

    def test_apply_selection_rules_rejects_when_sector_is_not_strong_enough(self) -> None:
        history = _build_matching_history("300565")
        config = AshareRuleConfig()

        matched = apply_selection_rules(
            daily_history=history,
            latest_turnover={"300565": 7.6},
            sector_snapshot={"300565": [{"name": "化工", "change_pct": 1.3}]},
            config=config,
        )

        self.assertEqual(matched, [])

    def test_build_technical_candidate_pool_keeps_candidate_when_sector_is_weak(self) -> None:
        history = _build_matching_history("300565")
        config = AshareRuleConfig()

        matched = build_technical_candidate_pool(
            daily_history=history,
            latest_turnover={"300565": 7.6},
            sector_snapshot={"300565": [{"name": "化工", "change_pct": 1.3}]},
            config=config,
        )

        self.assertEqual([item.code for item in matched], ["300565"])
        self.assertIn("板块强度未达筛选阈值", matched[0].notes[-1])

    def test_classify_market_regime_returns_weak_when_breadth_is_poor(self) -> None:
        snapshot = {
            "index_change": {"sh": -0.8, "sz": -1.1, "cyb": -1.5},
            "up_count": 1200,
            "down_count": 3800,
            "limit_up": 35,
            "limit_down": 22,
            "sector_median": -0.6,
        }

        regime = _classify_market_regime(snapshot)

        self.assertEqual(regime, "weak")

    def test_classify_market_regime_supports_stats_wrapper_shape(self) -> None:
        snapshot = {
            "stats": {
                "index_change": {"sh": -0.8, "sz": -1.1, "cyb": -1.5},
                "up_count": 1200,
                "down_count": 3800,
                "limit_up_count": 35,
                "limit_down_count": 22,
                "sector_median": -0.6,
            }
        }

        regime = _classify_market_regime(snapshot)

        self.assertEqual(regime, "weak")

    def test_classify_market_regime_supports_existing_market_stats_shape_for_strong(self) -> None:
        snapshot = {
            "index_change": {"sh": 0.7, "sz": 0.9, "cyb": 1.1},
            "up_count": 3300,
            "down_count": 1700,
            "limit_up_count": 86,
            "limit_down_count": 5,
            "sector_changes": [
                {"name": "电力设备", "change_pct": 2.4},
                {"name": "基础化工", "change_pct": 1.8},
                {"name": "钢铁", "change_pct": 0.9},
            ],
        }

        regime = _classify_market_regime(snapshot)

        self.assertEqual(regime, "strong")

    def test_classify_market_regime_returns_neutral_when_inputs_are_mixed(self) -> None:
        snapshot = {
            "index_change": {"sh": 0.1, "sz": -0.1, "cyb": 0.2},
            "up_count": 2500,
            "down_count": 2400,
            "limit_up_count": 42,
            "limit_down_count": 18,
            "sector_changes": [
                {"name": "电力设备", "pct_change": 0.6},
                {"name": "基础化工", "pct_change": -0.1},
                {"name": "钢铁", "pct_change": 0.2},
            ],
        }

        regime = _classify_market_regime(snapshot)

        self.assertEqual(regime, "neutral")

    def test_classify_market_regime_preserves_explicit_zero_sector_median(self) -> None:
        snapshot = {
            "index_change": {"sh": 0.1, "sz": 0.0, "cyb": 0.2},
            "up_count": 2600,
            "down_count": 2400,
            "limit_up_count": 40,
            "limit_down_count": 16,
            "sector_median": 0.0,
            "sector_changes": [
                {"name": "电力设备", "change_pct": 2.6},
                {"name": "钢铁", "change_pct": 1.9},
                {"name": "传媒", "change_pct": 1.4},
            ],
        }

        regime = _classify_market_regime(snapshot)

        self.assertEqual(regime, "neutral")

    def test_classify_market_regime_ignores_non_numeric_nested_sector_metadata(self) -> None:
        snapshot = {
            "index_change": {"sh": 0.7, "sz": 0.8, "cyb": 0.9},
            "up_count": 3100,
            "down_count": 1800,
            "limit_up_count": 73,
            "limit_down_count": 4,
            "sector_rankings": {
                "leaders": [
                    {"name": "电力设备", "change_pct": 2.2},
                    {"name": "基础化工", "change_pct": 1.7},
                ],
                "metadata": {"source": "tushare", "note": "sample"},
            },
        }

        regime = _classify_market_regime(snapshot)

        self.assertEqual(regime, "strong")

    def test_classify_market_regime_ignores_numeric_sector_metadata(self) -> None:
        snapshot = {
            "index_change": {"sh": 0.7, "sz": 0.8, "cyb": 0.9},
            "up_count": 3100,
            "down_count": 1800,
            "limit_up_count": 73,
            "limit_down_count": 4,
            "sector_rankings": {
                "leaders": [
                    {"name": "电力设备", "change_pct": 2.2},
                    {"name": "基础化工", "change_pct": 1.7},
                ],
                "metadata": {"total": 80, "rank": 1},
            },
        }

        regime = _classify_market_regime(snapshot)

        self.assertEqual(regime, "strong")

    def test_build_dynamic_rule_config_strong_is_conservative(self) -> None:
        base = AshareRuleConfig()

        config, adjustments = _build_dynamic_rule_config(base, "strong")

        self.assertIsNot(config, base)
        self.assertEqual(config.min_volume_ratio, base.min_volume_ratio)
        self.assertEqual(config.min_turnover_rate, base.min_turnover_rate)
        self.assertEqual(config.min_sector_change_pct, 1.5)
        self.assertEqual(config.max_bias_ma5_pct, base.max_bias_ma5_pct)
        self.assertEqual(len(adjustments), 1)
        self.assertEqual(adjustments[0].name, "板块涨幅阈值")
        self.assertEqual(adjustments[0].from_value, 2.0)
        self.assertEqual(adjustments[0].to_value, 1.5)

    def test_build_dynamic_rule_config_neutral_relaxes_secondary_thresholds(self) -> None:
        base = AshareRuleConfig()

        config, adjustments = _build_dynamic_rule_config(base, "neutral")

        self.assertEqual(config.min_prior_rise_pct, 18.0)
        self.assertEqual(config.min_volume_ratio, 1.3)
        self.assertEqual(config.min_turnover_rate, 4.5)
        self.assertEqual(config.min_sector_change_pct, 1.2)
        self.assertEqual(config.max_bias_ma5_pct, 8.5)
        self.assertEqual(config.abc_min_pullback_pct, 4.0)
        self.assertEqual(config.abc_min_rebound_pct, 2.0)
        self.assertEqual(config.abc_min_c_leg_pct, 1.5)
        self.assertEqual(config.abc_min_c_retention_ratio, 0.86)
        self.assertEqual(config.abc_rebreak_buffer_pct, -0.2)
        self.assertEqual(len(adjustments), 10)
        self.assertEqual(
            [item.name for item in adjustments],
            [
                "前高前累计涨幅",
                "量比",
                "换手率",
                "板块涨幅阈值",
                "MA5乖离率",
                "ABC-A段回撤阈值",
                "ABC-B段反抽阈值",
                "ABC-C段回踩阈值",
                "ABC-C段保留比例",
                "ABC再突破缓冲",
            ],
        )

    def test_build_dynamic_rule_config_weak_relaxes_to_watchlist_floor(self) -> None:
        base = AshareRuleConfig()

        config, adjustments = _build_dynamic_rule_config(base, "weak")

        self.assertEqual(config.min_prior_rise_pct, 18.0)
        self.assertEqual(config.min_volume_ratio, 1.2)
        self.assertEqual(config.min_turnover_rate, 4.0)
        self.assertEqual(config.min_sector_change_pct, 0.8)
        self.assertEqual(config.max_bias_ma5_pct, 9.0)
        self.assertEqual(config.abc_min_pullback_pct, 3.5)
        self.assertEqual(config.abc_min_rebound_pct, 1.5)
        self.assertEqual(config.abc_min_c_leg_pct, 1.0)
        self.assertEqual(config.abc_min_c_retention_ratio, 0.82)
        self.assertEqual(config.abc_rebreak_buffer_pct, -0.4)
        self.assertEqual(len(adjustments), 10)

    def test_build_dynamic_rule_config_does_not_tighten_looser_base(self) -> None:
        base = AshareRuleConfig(
            min_prior_rise_pct=17.0,
            min_volume_ratio=1.1,
            min_turnover_rate=3.8,
            min_sector_change_pct=0.6,
            max_bias_ma5_pct=9.2,
            abc_min_pullback_pct=3.0,
            abc_min_rebound_pct=1.0,
            abc_min_c_leg_pct=0.8,
            abc_min_c_retention_ratio=0.8,
            abc_rebreak_buffer_pct=-0.5,
        )

        config, adjustments = _build_dynamic_rule_config(base, "neutral")

        self.assertEqual(config.min_prior_rise_pct, 17.0)
        self.assertEqual(config.min_volume_ratio, 1.1)
        self.assertEqual(config.min_turnover_rate, 3.8)
        self.assertEqual(config.min_sector_change_pct, 0.6)
        self.assertEqual(config.max_bias_ma5_pct, 9.2)
        self.assertEqual(config.abc_min_pullback_pct, 3.0)
        self.assertEqual(config.abc_min_rebound_pct, 1.0)
        self.assertEqual(config.abc_min_c_leg_pct, 0.8)
        self.assertEqual(config.abc_min_c_retention_ratio, 0.8)
        self.assertEqual(config.abc_rebreak_buffer_pct, -0.5)
        self.assertEqual(adjustments, [])

    def test_build_screening_report_handles_empty_candidates(self) -> None:
        report = build_screening_report(
            candidates=[],
            report_date="2026-04-13",
        )

        self.assertIn("2026-04-13", report)
        self.assertIn("未筛出符合条件的A股股票", report)
        self.assertIn("5 日线乖离率 < 8%", report)

    def test_build_screening_report_renders_layered_sections(self) -> None:
        report = build_screening_report(
            candidates=[],
            report_date="2026-04-13",
            grouped_candidates={
                "full": [_build_candidate("000001", name="平安银行", sector_name="银行")],
                "relaxed": [_build_candidate("000559", name="万向钱潮", sector_name="汽车零部件")],
                "technical": [_build_candidate("300490", name="华自科技", sector_name="基础化工", sector_change_pct=1.4)],
            },
            market_regime_label="震荡偏强",
            dynamic_adjustments=[
                "板块强度阈值 2.0% -> 1.5%（严格档无结果，进入动态放宽观察）",
            ],
        )

        self.assertIn("市场环境：震荡偏强", report)
        self.assertIn("板块强度阈值 2.0% -> 1.5%（严格档无结果，进入动态放宽观察）", report)
        self.assertIn("## 完整命中（1 只）", report)
        self.assertIn("## 动态放宽命中（1 只）", report)
        self.assertIn("## 技术候选池（1 只）", report)
        self.assertIn("平安银行 (000001)", report)
        self.assertIn("万向钱潮 (000559)", report)
        self.assertIn("华自科技 (300490)", report)
        self.assertNotIn("未筛出符合条件的A股股票", report)

    def test_build_screening_report_renders_layered_sections_from_dataclass(self) -> None:
        report = build_screening_report(
            candidates=[],
            report_date="2026-04-13",
            grouped_candidates=RuleScreeningBuckets(
                full_hits=[_build_candidate("000001", name="平安银行", sector_name="银行")],
                relaxed_hits=[_build_candidate("000559", name="万向钱潮", sector_name="汽车零部件")],
                technical_pool=[_build_candidate("300490", name="华自科技", sector_name="基础化工", sector_change_pct=1.4)],
            ),
        )

        self.assertIn("## 完整命中（1 只）", report)
        self.assertIn("## 动态放宽命中（1 只）", report)
        self.assertIn("## 技术候选池（1 只）", report)
        self.assertIn("平安银行 (000001)", report)
        self.assertIn("万向钱潮 (000559)", report)
        self.assertIn("华自科技 (300490)", report)

    def test_build_screening_report_renders_manual_review_pool(self) -> None:
        candidate = _build_candidate("600010", name="包钢股份", sector_name="钢铁", sector_change_pct=1.21)
        candidate.matched_condition_count = 7
        candidate.total_condition_count = 8
        candidate.failed_conditions = ["前高前累计涨幅未达到 20%"]
        candidate.notes.append("未满足条件：前高前累计涨幅未达到 20%")

        report = build_screening_report(
            candidates=[],
            report_date="2026-04-13",
            grouped_candidates=RuleScreeningBuckets(manual_review_pool=[candidate]),
        )

        self.assertIn("## 人工精选池（1 只）", report)
        self.assertIn("条件命中：7/8", report)
        self.assertIn("未满足条件：前高前累计涨幅未达到 20%", report)

    def test_build_screening_report_only_outputs_empty_copy_when_all_buckets_are_empty(self) -> None:
        empty_report = build_screening_report(
            candidates=[],
            report_date="2026-04-13",
            grouped_candidates={"full": [], "relaxed": [], "technical": []},
        )
        technical_only_report = build_screening_report(
            candidates=[],
            report_date="2026-04-13",
            grouped_candidates={
                "full": [],
                "relaxed": [],
                "technical": [_build_candidate("300490", name="华自科技", sector_name="基础化工", sector_change_pct=1.4)],
            },
        )

        self.assertIn("未筛出符合条件的A股股票", empty_report)
        self.assertNotIn("## 技术候选池", empty_report)
        self.assertNotIn("未筛出符合条件的A股股票", technical_only_report)
        self.assertIn("## 技术候选池（1 只）", technical_only_report)

    def test_build_screening_report_marks_sector_rule_as_reference_for_manual_review_only(self) -> None:
        report = build_screening_report(
            candidates=[],
            report_date="2026-04-13",
            grouped_candidates={
                "full": [],
                "relaxed": [],
                "technical": [],
                "manual": [_build_candidate("600010", name="包钢股份", sector_name="钢铁", sector_change_pct=1.21)],
            },
        )

        self.assertIn(
            "所属板块涨幅 > 2%（人工精选池中改为排序参考，不作硬性剔除）",
            report,
        )

    def test_build_screening_report_marks_sector_rule_as_reference_for_technical_pool_only(self) -> None:
        report = build_screening_report(
            candidates=[],
            report_date="2026-04-13",
            grouped_candidates={
                "full": [],
                "relaxed": [],
                "technical": [_build_candidate("300490", name="华自科技", sector_name="基础化工", sector_change_pct=1.4)],
            },
        )

        self.assertIn(
            "所属板块涨幅 > 2%（完整/放宽命中时适用；技术候选池仅供参考，不作硬性剔除）",
            report,
        )

    def test_build_screening_report_preserves_decimal_thresholds(self) -> None:
        report = build_screening_report(
            candidates=[],
            report_date="2026-04-13",
            grouped_candidates={
                "technical": [_build_candidate("300490", name="华自科技", sector_name="基础化工", sector_change_pct=0.8)],
            },
            rule_config=AshareRuleConfig(
                min_prior_rise_pct=18.0,
                min_volume_ratio=1.25,
                min_turnover_rate=4.25,
                min_sector_change_pct=0.8,
                max_bias_ma5_pct=8.55,
            ),
            dynamic_adjustments=[
                DynamicAdjustment(
                    name="量比",
                    from_value=1.5,
                    to_value=1.25,
                    reason="更细粒度放宽",
                )
            ],
        )

        self.assertIn("前期累计涨幅不少于 18%", report)
        self.assertIn("量比：1.5 -> 1.25（更细粒度放宽）", report)
        self.assertIn("量比 > 1.25，换手率 > 4.25%", report)
        self.assertIn("所属板块涨幅 > 0.8%（完整/放宽命中时适用；技术候选池仅供参考，不作硬性剔除）", report)
        self.assertIn("5 日线乖离率 < 8.55%", report)

    def test_build_screening_report_rejects_conflicting_candidate_sources(self) -> None:
        with self.assertRaises(ValueError):
            build_screening_report(
                candidates=[_build_candidate("000001", name="平安银行", sector_name="银行")],
                report_date="2026-04-13",
                grouped_candidates={"full": [_build_candidate("000559", name="万向钱潮", sector_name="汽车零部件")]},
            )

    def test_build_screening_report_falls_back_to_legacy_candidates_when_buckets_are_empty(self) -> None:
        report = build_screening_report(
            candidates=[_build_candidate("000001", name="平安银行", sector_name="银行")],
            report_date="2026-04-13",
            screening_buckets=RuleScreeningBuckets(),
        )

        self.assertIn("## 完整命中（1 只）", report)
        self.assertIn("平安银行 (000001)", report)

    def test_dynamic_adjustment_object_still_renders_with_numeric_values(self) -> None:
        report = build_screening_report(
            candidates=[],
            report_date="2026-04-13",
            grouped_candidates={"full": [_build_candidate("000001", name="平安银行", sector_name="银行")]},
            dynamic_adjustments=[
                DynamicAdjustment(
                    name="板块强度阈值",
                    from_value=2.0,
                    to_value=1.5,
                    reason="严格档无结果",
                )
            ],
        )

        self.assertIn("板块强度阈值：2.0 -> 1.5（严格档无结果）", report)

    def test_run_uses_market_regime_driven_dynamic_rule_config_when_strict_is_empty(self) -> None:
        service = _build_service(config=AshareRuleConfig(auto_relax_if_empty=True))
        service.fetcher_manager.get_market_stats.return_value = {
            "index_change": {"sh": -0.7, "sz": -0.8, "cyb": -1.2},
            "up_count": 1400,
            "down_count": 3600,
            "limit_up_count": 32,
            "limit_down_count": 18,
            "sector_median": -0.5,
        }
        service._select_technical_candidates.side_effect = [["300565"], ["300565"]]
        service._load_sector_snapshot.side_effect = [
            {"300565": [{"name": "化工", "change_pct": 0.9}]},
            {"300565": [{"name": "化工", "change_pct": 0.9}]},
        ]
        relaxed_candidate = _build_candidate("300565", name="中欣氟材", sector_name="化工", sector_change_pct=0.9)

        with patch("src.services.rule_screener_service.apply_selection_rules") as apply_rules, \
             patch("src.services.rule_screener_service._build_dynamic_rule_config") as build_dynamic:
            apply_rules.side_effect = [[], [relaxed_candidate]]
            build_dynamic.return_value = (
                AshareRuleConfig(
                    auto_relax_if_empty=False,
                    min_volume_ratio=1.2,
                    min_turnover_rate=4.0,
                    min_sector_change_pct=0.8,
                    max_bias_ma5_pct=9.0,
                ),
                [
                    DynamicAdjustment(
                        name="板块涨幅阈值",
                        from_value=2.0,
                        to_value=0.8,
                        reason="弱势日放宽板块强度",
                    )
                ],
            )

            result = service.run(send_notification=True, ai_review=False)

        build_dynamic.assert_called_once_with(service.rule_config, "weak")
        self.assertEqual([item.code for item in result.candidates], ["300565"])
        self.assertIn("动态放宽命中（1 只）", result.report)
        self.assertIn("市场环境：弱势日", result.report)
        self.assertIn("板块涨幅阈值：2.0 -> 0.8（弱势日放宽板块强度）", result.report)
        self.assertTrue(any("弱势日" in note for note in result.profile_notes))
        self.assertTrue(any("板块涨幅阈值" in note for note in result.profile_notes))
        service._sync_candidates_to_stock_pool.assert_called_once_with([relaxed_candidate])

    def test_resolve_market_regime_uses_indices_and_sector_rankings_from_fetcher_manager(self) -> None:
        service = _build_service(config=AshareRuleConfig(auto_relax_if_empty=True))
        service.fetcher_manager.get_market_stats.return_value = {
            "up_count": 3200,
            "down_count": 1800,
            "limit_up_count": 72,
            "limit_down_count": 4,
        }
        service.fetcher_manager.get_main_indices.return_value = [
            {"code": "000001", "change_pct": 0.7},
            {"code": "399001", "change_pct": 0.8},
            {"code": "399006", "change_pct": 0.9},
        ]
        service.fetcher_manager.get_sector_rankings.return_value = (
            [{"name": "电力设备", "change_pct": 2.4}, {"name": "基础化工", "change_pct": 1.8}],
            [{"name": "传媒", "change_pct": -0.3}, {"name": "计算机", "change_pct": -0.2}],
        )

        regime, label = service._resolve_market_regime()

        self.assertEqual(regime, "strong")
        self.assertEqual(label, "强势日")

    def test_run_uses_technical_pool_when_dynamic_relax_still_has_no_hits(self) -> None:
        service = _build_service(
            config=AshareRuleConfig(ai_review_limit=0, auto_relax_if_empty=True),
            daily_history=pd.concat(
                [_build_matching_history("300490"), _build_matching_history("300565")],
                ignore_index=True,
            ),
            latest_turnover={"300490": 8.2, "300565": 7.6},
        )
        service.fetcher_manager.get_market_stats.return_value = {
            "index_change": {"sh": 0.2, "sz": 0.0, "cyb": -0.1},
            "up_count": 2500,
            "down_count": 2400,
            "limit_up_count": 41,
            "limit_down_count": 17,
            "sector_median": 0.1,
        }
        service._select_technical_candidates.side_effect = [["300490", "300565"], ["300490", "300565"]]
        service._load_sector_snapshot.side_effect = [
            {
                "300490": [{"name": "化工", "change_pct": 0.7}],
                "300565": [{"name": "汽车", "change_pct": 0.6}],
            },
            {
                "300490": [{"name": "化工", "change_pct": 0.7}],
                "300565": [{"name": "汽车", "change_pct": 0.6}],
            },
        ]
        technical_candidates = [
            _build_candidate("300490", name="华自科技", sector_name="基础化工", sector_change_pct=0.7),
            _build_candidate("300565", name="科信技术", sector_name="汽车零部件", sector_change_pct=0.6),
        ]

        with patch("src.services.rule_screener_service.apply_selection_rules") as apply_rules, \
             patch("src.services.rule_screener_service.build_technical_candidate_pool", return_value=technical_candidates), \
             patch("src.services.rule_screener_service._build_dynamic_rule_config") as build_dynamic, \
             patch("src.core.pipeline.StockAnalysisPipeline") as pipeline_cls:
            apply_rules.side_effect = [[], []]
            build_dynamic.return_value = (
                AshareRuleConfig(
                    auto_relax_if_empty=False,
                    min_volume_ratio=1.3,
                    min_turnover_rate=4.5,
                    min_sector_change_pct=1.2,
                    max_bias_ma5_pct=8.5,
                ),
                [
                    DynamicAdjustment(
                        name="量比",
                        from_value=1.5,
                        to_value=1.3,
                        reason="中性日轻放宽",
                    )
                ],
            )
            pipeline = pipeline_cls.return_value
            pipeline.run.return_value = []

            result = service.run(send_notification=True, ai_review=True)

        self.assertEqual([item.code for item in result.candidates], ["300490", "300565"])
        self.assertIn("## 技术候选池（2 只）", result.report)
        self.assertNotIn("未筛出符合条件的A股股票", result.report)
        self.assertIn("市场环境：震荡日", result.report)
        self.assertIn("量比：1.5 -> 1.3（中性日轻放宽）", result.report)
        self.assertTrue(any("震荡日" in note for note in result.profile_notes))
        self.assertTrue(any("量比" in note for note in result.profile_notes))
        self.assertEqual(
            result.stock_pool_notes,
            ["当前为技术/人工精选候选名单，未自动并入自选池，请人工确认后再决定是否加入。"],
        )
        service._sync_candidates_to_stock_pool.assert_not_called()
        self.assertEqual(
            pipeline.run.call_args.kwargs["stock_codes"],
            ["300490", "300565"],
        )

    def test_run_reviews_all_technical_pool_candidates_even_when_limit_is_positive(self) -> None:
        service = _build_service(
            config=AshareRuleConfig(ai_review_limit=1, auto_relax_if_empty=True),
            daily_history=pd.concat(
                [_build_matching_history("300490"), _build_matching_history("300565")],
                ignore_index=True,
            ),
            latest_turnover={"300490": 8.2, "300565": 7.6},
        )
        service.fetcher_manager.get_market_stats.return_value = {
            "index_change": {"sh": 0.2, "sz": 0.0, "cyb": -0.1},
            "up_count": 2500,
            "down_count": 2400,
            "limit_up_count": 41,
            "limit_down_count": 17,
            "sector_median": 0.1,
        }
        service._select_technical_candidates.side_effect = [["300490", "300565"], ["300490", "300565"]]
        service._load_sector_snapshot.side_effect = [
            {
                "300490": [{"name": "化工", "change_pct": 0.7}],
                "300565": [{"name": "汽车", "change_pct": 0.6}],
            },
            {
                "300490": [{"name": "化工", "change_pct": 0.7}],
                "300565": [{"name": "汽车", "change_pct": 0.6}],
            },
        ]
        technical_candidates = [
            _build_candidate("300490", name="华自科技", sector_name="基础化工", sector_change_pct=0.7),
            _build_candidate("300565", name="科信技术", sector_name="汽车零部件", sector_change_pct=0.6),
        ]

        with patch("src.services.rule_screener_service.apply_selection_rules") as apply_rules, \
             patch("src.services.rule_screener_service.build_technical_candidate_pool", return_value=technical_candidates), \
             patch("src.services.rule_screener_service._build_dynamic_rule_config") as build_dynamic, \
             patch("src.core.pipeline.StockAnalysisPipeline") as pipeline_cls:
            apply_rules.side_effect = [[], []]
            build_dynamic.return_value = (
                AshareRuleConfig(
                    ai_review_limit=1,
                    auto_relax_if_empty=False,
                    min_volume_ratio=1.3,
                    min_turnover_rate=4.5,
                    min_sector_change_pct=1.2,
                    max_bias_ma5_pct=8.5,
                ),
                [
                    DynamicAdjustment(
                        name="量比",
                        from_value=1.5,
                        to_value=1.3,
                        reason="中性日轻放宽",
                    )
                ],
            )
            pipeline = pipeline_cls.return_value
            pipeline.run.return_value = []

            result = service.run(send_notification=True, ai_review=True)

        self.assertEqual([item.code for item in result.candidates], ["300490", "300565"])
        self.assertEqual(
            pipeline.run.call_args.kwargs["stock_codes"],
            ["300490", "300565"],
        )

    def test_run_uses_technical_pool_when_dynamic_relax_is_disabled(self) -> None:
        service = _build_service(
            config=AshareRuleConfig(auto_relax_if_empty=False),
            daily_history=_build_matching_history("300565"),
            latest_turnover={"300565": 7.6},
        )
        service.fetcher_manager.get_market_stats.return_value = {
            "index_change": {"sh": 0.0, "sz": -0.1, "cyb": -0.1},
            "up_count": 2400,
            "down_count": 2500,
            "limit_up_count": 38,
            "limit_down_count": 12,
            "sector_median": 0.0,
        }
        service._select_technical_candidates.return_value = ["300565"]
        service._load_sector_snapshot.return_value = {"300565": [{"name": "化工", "change_pct": 0.9}]}

        technical_candidates = [
            _build_candidate("300565", name="中欣氟材", sector_name="基础化工", sector_change_pct=0.9),
        ]

        with patch("src.services.rule_screener_service.apply_selection_rules", return_value=[]), \
             patch("src.services.rule_screener_service.build_technical_candidate_pool", return_value=technical_candidates), \
             patch("src.services.rule_screener_service._build_dynamic_rule_config") as build_dynamic:
            result = service.run(send_notification=False, ai_review=False)

        build_dynamic.assert_not_called()
        self.assertIn("技术候选池（1 只）", result.report)
        self.assertTrue(any("已禁用动态放宽" in note for note in result.profile_notes))

    def test_run_falls_back_to_manual_review_pool_when_all_rule_buckets_are_empty(self) -> None:
        service = _build_service(
            config=AshareRuleConfig(auto_relax_if_empty=True),
            daily_history=_build_matching_history("600010"),
            latest_turnover={"600010": 7.1},
        )
        service.fetcher_manager.get_market_stats.return_value = {
            "index_change": {"sh": 0.0, "sz": -0.1, "cyb": -0.1},
            "up_count": 2400,
            "down_count": 2500,
            "limit_up_count": 38,
            "limit_down_count": 12,
            "sector_median": 0.0,
        }
        service._select_technical_candidates.side_effect = [[], []]
        service._load_sector_snapshot.side_effect = [{}, {}, {"600010": [{"name": "钢铁", "change_pct": 1.21}]}]

        manual_candidate = _build_candidate("600010", name="包钢股份", sector_name="钢铁", sector_change_pct=1.21)
        manual_candidate.matched_condition_count = 7
        manual_candidate.total_condition_count = 8
        manual_candidate.failed_conditions = ["前高前累计涨幅未达到 20%"]

        with patch("src.services.rule_screener_service.apply_selection_rules", return_value=[]), \
             patch("src.services.rule_screener_service.build_manual_review_pool", return_value=[manual_candidate]), \
             patch("src.services.rule_screener_service._build_dynamic_rule_config") as build_dynamic:
            build_dynamic.return_value = (AshareRuleConfig(auto_relax_if_empty=False), [])
            result = service.run(send_notification=False, ai_review=False)

        self.assertEqual([item.code for item in result.candidates], ["600010"])
        self.assertIn("人工精选池（1 只）", result.report)
        self.assertTrue(any("人工精选池" in note for note in result.profile_notes))
        service._sync_candidates_to_stock_pool.assert_not_called()

    def test_run_skips_empty_notification_when_empty_reports_are_disabled(self) -> None:
        service = _build_service(
            config=AshareRuleConfig(auto_relax_if_empty=False, notify_when_empty=True),
            daily_history=_build_matching_history("300565"),
            latest_turnover={"300565": 7.6},
        )
        service.fetcher_manager.get_market_stats.return_value = {
            "index_change": {"sh": 0.0, "sz": -0.1, "cyb": -0.1},
            "up_count": 2400,
            "down_count": 2500,
            "limit_up_count": 38,
            "limit_down_count": 12,
            "sector_median": 0.0,
        }
        service._select_technical_candidates.return_value = []
        service._load_sector_snapshot.return_value = {}

        with patch.dict(os.environ, {"RULE_SCREENER_ALLOW_EMPTY_REPORT": "false"}, clear=False), \
             patch("src.services.rule_screener_service.apply_selection_rules", return_value=[]), \
             patch("src.services.rule_screener_service.build_manual_review_pool", return_value=[]):
            result = service.run(send_notification=True, ai_review=False)

        self.assertEqual(result.candidates, [])
        service.notifier.send.assert_not_called()

    def test_run_auto_syncs_strict_hits_without_entering_dynamic_relax(self) -> None:
        strict_candidates = [
            _build_candidate("300490", name="华自科技", sector_name="基础化工", sector_change_pct=3.4),
            _build_candidate("300565", name="科信技术", sector_name="汽车零部件", sector_change_pct=3.1),
        ]
        service = _build_service(
            config=AshareRuleConfig(auto_relax_if_empty=True),
            daily_history=pd.concat(
                [_build_matching_history("300490"), _build_matching_history("300565")],
                ignore_index=True,
            ),
            latest_turnover={"300490": 8.2, "300565": 7.6},
        )
        service.fetcher_manager.get_market_stats.return_value = {
            "index_change": {"sh": 0.7, "sz": 0.8, "cyb": 0.9},
            "up_count": 3200,
            "down_count": 1800,
            "limit_up_count": 72,
            "limit_down_count": 4,
            "sector_median": 0.6,
        }
        service._select_technical_candidates.return_value = ["300490", "300565"]
        service._load_sector_snapshot.return_value = {
            "300490": [{"name": "化工", "change_pct": 3.4}],
            "300565": [{"name": "汽车", "change_pct": 3.1}],
        }

        with patch("src.services.rule_screener_service.apply_selection_rules", return_value=strict_candidates), \
             patch("src.services.rule_screener_service._build_dynamic_rule_config") as build_dynamic:
            result = service.run(send_notification=True, ai_review=False)

        build_dynamic.assert_not_called()
        self.assertIn("完整命中（2 只）", result.report)
        service._sync_candidates_to_stock_pool.assert_called_once_with(strict_candidates)

    def test_run_uses_latest_completed_trade_date_instead_of_latest_open_session(self) -> None:
        strict_candidates = [
            _build_candidate("300565", name="科信技术", sector_name="基础化工", sector_change_pct=3.1),
        ]
        service = _build_service(
            config=AshareRuleConfig(auto_relax_if_empty=False),
            daily_history=_build_matching_history("300565"),
            latest_turnover={"300565": 7.6},
        )
        service._load_trade_dates = MagicMock(return_value=["20260414", "20260413", "20260410"])
        service._select_analysis_trade_dates = MagicMock(return_value=["20260413", "20260410"])
        service.fetcher_manager.get_market_stats.return_value = {
            "index_change": {"sh": 0.3, "sz": 0.1, "cyb": 0.0},
            "up_count": 2600,
            "down_count": 2200,
            "limit_up_count": 45,
            "limit_down_count": 10,
            "sector_median": 0.2,
        }
        service._select_technical_candidates.return_value = ["300565"]
        service._load_sector_snapshot.return_value = {
            "300565": [{"name": "基础化工", "change_pct": 3.1}],
        }

        with patch(
            "src.services.rule_screener_service.get_effective_trading_date",
            return_value=datetime(2026, 4, 13).date(),
        ), patch("src.services.rule_screener_service.apply_selection_rules", return_value=strict_candidates):
            result = service.run(send_notification=False, ai_review=False)

        service._select_analysis_trade_dates.assert_called_once_with(["20260414", "20260413", "20260410"], "20260413")
        service._load_latest_turnover.assert_called_once_with("20260413", trade_dates=["20260413", "20260410"])
        self.assertEqual(result.trade_date, "20260413")

    def test_run_reviews_all_display_candidates_when_ai_review_limit_is_unlimited(self) -> None:
        strict_candidates = [
            _build_candidate("300490", name="华自科技", sector_name="基础化工", sector_change_pct=3.4),
            _build_candidate("300565", name="科信技术", sector_name="汽车零部件", sector_change_pct=3.1),
        ]
        service = _build_service(
            config=AshareRuleConfig(ai_review_limit=0, auto_relax_if_empty=False),
            daily_history=pd.concat(
                [_build_matching_history("300490"), _build_matching_history("300565")],
                ignore_index=True,
            ),
            latest_turnover={"300490": 8.2, "300565": 7.6},
        )
        service.fetcher_manager.get_market_stats.return_value = {
            "index_change": {"sh": 0.7, "sz": 0.8, "cyb": 0.9},
            "up_count": 3200,
            "down_count": 1800,
            "limit_up_count": 72,
            "limit_down_count": 4,
            "sector_median": 0.6,
        }
        service._select_technical_candidates.return_value = ["300490", "300565"]
        service._load_sector_snapshot.return_value = {
            "300490": [{"name": "化工", "change_pct": 3.4}],
            "300565": [{"name": "汽车", "change_pct": 3.1}],
        }

        with patch("src.services.rule_screener_service.apply_selection_rules", return_value=strict_candidates), \
             patch("src.core.pipeline.StockAnalysisPipeline") as pipeline_cls:
            pipeline = pipeline_cls.return_value
            pipeline.run.return_value = []

            result = service.run(send_notification=False, ai_review=True)

        self.assertEqual(len(result.candidates), 2)
        self.assertEqual(
            pipeline.run.call_args.kwargs["stock_codes"],
            ["300490", "300565"],
        )

    def test_run_respects_ai_review_limit_when_positive(self) -> None:
        strict_candidates = [
            _build_candidate("300490", name="华自科技", sector_name="基础化工", sector_change_pct=3.4),
            _build_candidate("300565", name="科信技术", sector_name="汽车零部件", sector_change_pct=3.1),
        ]
        service = _build_service(
            config=AshareRuleConfig(ai_review_limit=1, auto_relax_if_empty=False),
            daily_history=pd.concat(
                [_build_matching_history("300490"), _build_matching_history("300565")],
                ignore_index=True,
            ),
            latest_turnover={"300490": 8.2, "300565": 7.6},
        )
        service.fetcher_manager.get_market_stats.return_value = {
            "index_change": {"sh": 0.7, "sz": 0.8, "cyb": 0.9},
            "up_count": 3200,
            "down_count": 1800,
            "limit_up_count": 72,
            "limit_down_count": 4,
            "sector_median": 0.6,
        }
        service._select_technical_candidates.return_value = ["300490", "300565"]
        service._load_sector_snapshot.return_value = {
            "300490": [{"name": "化工", "change_pct": 3.4}],
            "300565": [{"name": "汽车", "change_pct": 3.1}],
        }

        with patch("src.services.rule_screener_service.apply_selection_rules", return_value=strict_candidates), \
             patch("src.core.pipeline.StockAnalysisPipeline") as pipeline_cls:
            pipeline = pipeline_cls.return_value
            pipeline.run.return_value = []

            result = service.run(send_notification=False, ai_review=True)

        self.assertEqual(len(result.candidates), 2)
        self.assertEqual(
            pipeline.run.call_args.kwargs["stock_codes"],
            ["300490"],
        )

    def test_build_screening_report_contains_ai_review_summary(self) -> None:
        candidate = _build_candidate("000559", name="万向钱潮", sector_name="汽车零部件")

        report = build_screening_report(
            candidates=[candidate],
            report_date="2026-04-13",
            ai_review_lines=["000559 万向钱潮：偏强但需确认，适合小仓跟踪。"],
        )

        self.assertIn("000559", report)
        self.assertIn("汽车零部件", report)
        self.assertIn("AI复核", report)
        self.assertIn("偏强但需确认", report)

    def test_build_screening_report_includes_profile_notes(self) -> None:
        report = build_screening_report(
            candidates=[],
            report_date="2026-04-13",
            profile_name="轻度放宽版",
            profile_notes=["严格条件为 0 只，已自动切换。"],
        )

        self.assertIn("筛选档位", report)
        self.assertIn("轻度放宽版", report)
        self.assertIn("已自动切换", report)

    def test_build_screening_report_includes_stock_pool_notes(self) -> None:
        report = build_screening_report(
            candidates=[],
            report_date="2026-04-13",
            stock_pool_notes=["已自动加入自选池：300490, 600010"],
        )

        self.assertIn("自选池同步", report)
        self.assertIn("300490", report)

    def test_merge_stock_codes_preserves_existing_order_and_deduplicates(self) -> None:
        merged = _merge_stock_codes(["603601", "300490"], ["300490", "600010", "000559"])
        self.assertEqual(merged, ["603601", "300490", "600010", "000559"])

    def test_split_stock_codes_normalizes_values(self) -> None:
        self.assertEqual(_split_stock_codes("603601, 300490 ,600010"), ["603601", "300490", "600010"])

    def test_should_sync_stock_pool_only_when_notifications_enabled(self) -> None:
        service = object.__new__(AshareRuleScreenerService)
        self.assertTrue(service._should_sync_stock_pool(send_notification=True))
        self.assertFalse(service._should_sync_stock_pool(send_notification=False))

    def test_filter_stock_universe_excludes_st_and_recent_ipo(self) -> None:
        stock_list = pd.DataFrame(
            [
                {"code": "000001", "name": "平安银行", "market": "主板", "list_date": "19910403"},
                {"code": "000004", "name": "*ST国华", "market": "主板", "list_date": "19901201"},
                {"code": "301999", "name": "新股样本", "market": "创业板", "list_date": "20260320"},
            ]
        )

        filtered = _filter_stock_universe(
            stock_list,
            min_list_date_cutoff="20260201",
            exclude_st=True,
        )

        self.assertEqual(filtered["code"].tolist(), ["000001"])

    def test_build_sector_snapshot_from_tushare_uses_shenwan_l1_strength(self) -> None:
        index_member_df = pd.DataFrame(
            [
                {
                    "l1_code": "801880.SI",
                    "l1_name": "汽车",
                    "ts_code": "000559.SZ",
                    "name": "万向钱潮",
                    "in_date": "20040101",
                    "out_date": None,
                    "is_new": "Y",
                },
                {
                    "l1_code": "801030.SI",
                    "l1_name": "基础化工",
                    "ts_code": "300490.SZ",
                    "name": "华自科技",
                    "in_date": "20150101",
                    "out_date": None,
                    "is_new": "Y",
                },
            ]
        )
        sw_daily_df = pd.DataFrame(
            [
                {"ts_code": "801880.SI", "name": "汽车", "pct_change": 2.8},
                {"ts_code": "801030.SI", "name": "基础化工", "pct_change": 1.4},
            ]
        )

        snapshot = _build_sector_snapshot_from_tushare(
            index_member_df=index_member_df,
            sw_daily_df=sw_daily_df,
            candidate_codes=["000559", "300490"],
            trade_date="20260413",
        )

        self.assertEqual(snapshot["000559"][0]["name"], "汽车")
        self.assertAlmostEqual(snapshot["000559"][0]["change_pct"], 2.8)
        self.assertEqual(snapshot["300490"][0]["name"], "基础化工")
        self.assertAlmostEqual(snapshot["300490"][0]["change_pct"], 1.4)

    def test_call_tushare_cached_ignores_empty_cache_and_refetches(self) -> None:
        with TemporaryDirectory() as tmpdir:
            service = object.__new__(AshareRuleScreenerService)
            service.cache_dir = Path(tmpdir)
            api_dir = service.cache_dir / "daily_basic"
            api_dir.mkdir(parents=True, exist_ok=True)
            cache_file = api_dir / "20260413.pkl"
            pd.DataFrame().to_pickle(cache_file)

            expected = pd.DataFrame([{"ts_code": "300490.SZ", "turnover_rate": 7.84}])
            service.tushare_fetcher = MagicMock()
            service.tushare_fetcher._call_api_with_rate_limit.return_value = expected

            df = service._call_tushare_cached("daily_basic", cache_key="20260413", trade_date="20260413")

            self.assertEqual(df.to_dict(orient="records"), expected.to_dict(orient="records"))
            service.tushare_fetcher._call_api_with_rate_limit.assert_called_once_with("daily_basic", trade_date="20260413")
            self.assertEqual(pd.read_pickle(cache_file).to_dict(orient="records"), expected.to_dict(orient="records"))

    def test_call_tushare_cached_paginated_does_not_persist_empty_result(self) -> None:
        with TemporaryDirectory() as tmpdir:
            service = object.__new__(AshareRuleScreenerService)
            service.cache_dir = Path(tmpdir)
            service.tushare_fetcher = MagicMock()
            service.tushare_fetcher._call_api_with_rate_limit.return_value = pd.DataFrame()

            df = service._call_tushare_cached_paginated("index_member_all", cache_key="202604", fields="ts_code")

            self.assertTrue(df.empty)
            self.assertFalse((service.cache_dir / "index_member_all" / "202604.pkl").exists())
            service.tushare_fetcher._call_api_with_rate_limit.assert_called_once()

    def test_load_latest_turnover_falls_back_to_previous_trade_date_when_daily_basic_is_empty(self) -> None:
        service = object.__new__(AshareRuleScreenerService)
        service._load_trade_dates = MagicMock(return_value=["20260414", "20260411", "20260410"])
        service._call_tushare_cached = MagicMock(
            side_effect=lambda api_name, **kwargs: (
                pd.DataFrame()
                if kwargs["trade_date"] == "20260414"
                else pd.DataFrame([{"ts_code": "300490.SZ", "turnover_rate": 7.84}])
            )
        )

        snapshot = service._load_latest_turnover("20260414")

        self.assertIsInstance(snapshot, TurnoverSnapshot)
        self.assertEqual(snapshot.turnover_by_code, {"300490": 7.84})
        self.assertEqual(snapshot.source, "daily_basic:20260411")
        self.assertTrue(snapshot.is_partial)
        self.assertTrue(any("上一交易日" in note for note in snapshot.notes))

    def test_load_latest_turnover_returns_unknown_snapshot_when_current_and_previous_daily_basic_are_empty(self) -> None:
        service = object.__new__(AshareRuleScreenerService)
        service._load_trade_dates = MagicMock(return_value=["20260414", "20260411", "20260410"])
        service._call_tushare_cached = MagicMock(return_value=pd.DataFrame())

        snapshot = service._load_latest_turnover("20260414")

        self.assertIsInstance(snapshot, TurnoverSnapshot)
        self.assertEqual(snapshot.turnover_by_code, {})
        self.assertEqual(snapshot.source, "unknown")
        self.assertTrue(snapshot.is_partial)
        self.assertTrue(any("unknown" in note for note in snapshot.notes))

    def test_run_preserves_prefilter_turnover_note_when_open_data_fallback_is_used(self) -> None:
        service = _build_service(
            config=AshareRuleConfig(auto_relax_if_empty=False, allow_open_data_fallback=True),
            daily_history=_build_matching_history("300565"),
        )
        service._load_trade_dates.side_effect = RuntimeError("trade_cal unavailable")
        service._load_history_via_prefilter_fallback = MagicMock(
            return_value=(
                _build_matching_history("300565"),
                TurnoverSnapshot(
                    turnover_by_code={"300565": 7.6},
                    source="prefilter_snapshot:20260414",
                    is_partial=True,
                    notes=["已回退到开放快照预筛换手率数据，仅供人工判断。"],
                ),
                "20260414",
            )
        )
        service.fetcher_manager.get_market_stats.return_value = {
            "index_change": {"sh": 0.3, "sz": 0.2, "cyb": 0.1},
            "up_count": 2600,
            "down_count": 2200,
            "limit_up_count": 45,
            "limit_down_count": 10,
            "sector_median": 0.2,
        }
        service._select_technical_candidates.return_value = ["300565"]
        service._load_sector_snapshot.return_value = {"300565": [{"name": "基础化工", "change_pct": 3.1}]}

        with patch(
            "src.services.rule_screener_service.apply_selection_rules",
            return_value=[_build_candidate("300565", name="科信技术", sector_name="基础化工", sector_change_pct=3.1)],
        ):
            result = service.run(send_notification=False, ai_review=False)

        service._load_history_via_prefilter_fallback.assert_called_once()
        self.assertEqual([item.code for item in result.candidates], ["300565"])
        self.assertTrue(any("开放快照预筛换手率数据" in note for note in result.profile_notes))
        self.assertIn("开放快照预筛换手率数据", result.report)

    def test_run_keeps_technical_pool_and_marks_report_when_sector_data_is_missing(self) -> None:
        for missing_api in ("sw_daily", "index_member_all"):
            with self.subTest(missing_api=missing_api):
                service = _build_service(
                    config=AshareRuleConfig(auto_relax_if_empty=True),
                    daily_history=_build_matching_history("300565"),
                    latest_turnover={"300565": 7.6},
                )
                service.fetcher_manager.get_market_stats.return_value = {
                    "index_change": {"sh": 0.1, "sz": 0.0, "cyb": -0.1},
                    "up_count": 2400,
                    "down_count": 2300,
                    "limit_up_count": 40,
                    "limit_down_count": 12,
                    "sector_median": 0.0,
                }
                service._select_technical_candidates.side_effect = [["300565"], ["300565"]]
                service.tushare_fetcher = MagicMock()
                service.tushare_fetcher._convert_stock_code.side_effect = lambda code: f"{code}.SZ"
                service._load_sector_snapshot = AshareRuleScreenerService._load_sector_snapshot.__get__(
                    service,
                    AshareRuleScreenerService,
                )

                index_member_df = pd.DataFrame(
                    [
                        {
                            "l1_code": "801030.SI",
                            "l1_name": "基础化工",
                            "ts_code": "300565.SZ",
                            "name": "科信技术",
                            "in_date": "20150101",
                            "out_date": None,
                        }
                    ]
                )
                sw_daily_df = pd.DataFrame(
                    [
                        {"ts_code": "801030.SI", "name": "基础化工", "pct_change": 1.1},
                    ]
                )

                def fake_call_tushare_cached(api_name: str, **kwargs) -> pd.DataFrame:
                    if api_name == "index_member_all":
                        return pd.DataFrame() if missing_api == "index_member_all" else index_member_df
                    if api_name == "sw_daily":
                        return pd.DataFrame() if missing_api == "sw_daily" else sw_daily_df
                    raise AssertionError(f"unexpected api_name: {api_name}")

                service._call_tushare_cached = MagicMock(side_effect=fake_call_tushare_cached)

                result = service.run(send_notification=False, ai_review=False)

                self.assertEqual([item.code for item in result.candidates], ["300565"])
                self.assertIn("## 技术候选池（1 只）", result.report)
                self.assertIn("板块数据缺失，仅供人工判断", result.report)
                self.assertTrue(any("板块数据缺失，仅供人工判断" in note for note in result.profile_notes))
                self.assertTrue(
                    any("板块数据缺失，仅供人工判断" in note for candidate in result.candidates for note in candidate.notes)
                )

    def test_run_includes_data_notes_when_all_buckets_are_empty_for_missing_sector_data(self) -> None:
        for missing_api in ("sw_daily", "index_member_all"):
            with self.subTest(missing_api=missing_api):
                service = _build_service(
                    config=AshareRuleConfig(auto_relax_if_empty=True),
                    daily_history=_build_matching_history("300565"),
                    latest_turnover={"300565": 7.6},
                )
                service.fetcher_manager.get_market_stats.return_value = {
                    "index_change": {"sh": 0.1, "sz": 0.0, "cyb": -0.1},
                    "up_count": 2400,
                    "down_count": 2300,
                    "limit_up_count": 40,
                    "limit_down_count": 12,
                    "sector_median": 0.0,
                }
                service._select_technical_candidates.return_value = ["300565"]
                service.tushare_fetcher = MagicMock()
                service.tushare_fetcher._convert_stock_code.side_effect = lambda code: f"{code}.SZ"
                service._load_sector_snapshot = AshareRuleScreenerService._load_sector_snapshot.__get__(
                    service,
                    AshareRuleScreenerService,
                )

                index_member_df = pd.DataFrame(
                    [
                        {
                            "l1_code": "801030.SI",
                            "l1_name": "基础化工",
                            "ts_code": "300565.SZ",
                            "name": "科信技术",
                            "in_date": "20150101",
                            "out_date": None,
                        }
                    ]
                )
                sw_daily_df = pd.DataFrame(
                    [
                        {"ts_code": "801030.SI", "name": "基础化工", "pct_change": 1.1},
                    ]
                )

                def fake_call_tushare_cached(api_name: str, **kwargs) -> pd.DataFrame:
                    if api_name == "index_member_all":
                        return pd.DataFrame() if missing_api == "index_member_all" else index_member_df
                    if api_name == "sw_daily":
                        return pd.DataFrame() if missing_api == "sw_daily" else sw_daily_df
                    raise AssertionError(f"unexpected api_name: {api_name}")

                service._call_tushare_cached = MagicMock(side_effect=fake_call_tushare_cached)

                with patch("src.services.rule_screener_service.build_technical_candidate_pool", return_value=[]):
                    result = service.run(send_notification=False, ai_review=False)

                self.assertEqual([item.code for item in result.candidates], ["300565"])
                self.assertIn("人工精选池（1 只）", result.report)
                self.assertIn("板块数据缺失，仅供人工判断", result.report)
                self.assertIn("未满足条件：所属板块涨幅未达到 0.8%", result.report)
                self.assertTrue(any("板块数据缺失，仅供人工判断" in note for note in result.profile_notes))


if __name__ == "__main__":
    unittest.main()
