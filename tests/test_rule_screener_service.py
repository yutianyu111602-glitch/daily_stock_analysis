from __future__ import annotations

from datetime import datetime, timedelta
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
    _build_sector_snapshot_from_tushare,
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

    def test_run_uses_technical_pool_section_when_fallback_is_triggered(self) -> None:
        service = object.__new__(AshareRuleScreenerService)
        service.config = MagicMock()
        service.rule_config = AshareRuleConfig(auto_relax_if_empty=True)
        service.notifier = MagicMock()
        service._load_trade_dates = MagicMock(return_value=["20260413", "20260201"])
        service._load_stock_universe = MagicMock(return_value=pd.DataFrame())
        service._load_daily_history = MagicMock(return_value=_build_matching_history("300565"))
        service._load_latest_turnover = MagicMock(return_value={"300565": 7.6})
        service._select_technical_candidates = MagicMock(return_value=["300565"])
        service._load_sector_snapshot = MagicMock(
            side_effect=[
                {"300565": [{"name": "化工", "change_pct": 1.3}]},
                {"300565": [{"name": "化工", "change_pct": 1.3}]},
            ]
        )
        service._build_relaxed_rule_config = MagicMock(return_value=AshareRuleConfig(min_sector_change_pct=2.0))

        result = service.run(send_notification=False, ai_review=False)

        self.assertIn("## 技术候选池（1 只）", result.report)
        self.assertNotIn("## 完整命中（1 只）", result.report)

    def test_run_limits_ai_review_codes_outside_technical_pool_mode(self) -> None:
        service = object.__new__(AshareRuleScreenerService)
        service.config = MagicMock()
        service.rule_config = AshareRuleConfig(ai_review_limit=1, auto_relax_if_empty=False)
        service.notifier = MagicMock()
        service._load_trade_dates = MagicMock(return_value=["20260413", "20260201"])
        service._load_stock_universe = MagicMock(return_value=pd.DataFrame())
        service._load_daily_history = MagicMock(
            return_value=pd.concat(
                [_build_matching_history("300490"), _build_matching_history("300565")],
                ignore_index=True,
            )
        )
        service._load_latest_turnover = MagicMock(return_value={"300490": 8.2, "300565": 7.6})
        service._select_technical_candidates = MagicMock(return_value=["300490", "300565"])
        service._load_sector_snapshot = MagicMock(
            return_value={
                "300490": [{"name": "化工", "change_pct": 3.4}],
                "300565": [{"name": "汽车", "change_pct": 3.1}],
            }
        )
        service._build_ai_review_lines = MagicMock(return_value=["AI 复核摘要"])

        with patch("src.core.pipeline.StockAnalysisPipeline") as pipeline_cls:
            pipeline = pipeline_cls.return_value
            pipeline.run.return_value = []

            result = service.run(send_notification=False, ai_review=True)

        self.assertEqual(len(result.candidates), 2)
        self.assertEqual(
            pipeline.run.call_args.kwargs["stock_codes"],
            [result.candidates[0].code],
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


if __name__ == "__main__":
    unittest.main()
