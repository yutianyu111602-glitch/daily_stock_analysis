from __future__ import annotations

from datetime import datetime, timedelta
import unittest

import pandas as pd

from src.services.rule_screener_service import (
    AshareRuleScreenerService,
    AshareRuleConfig,
    RuleScreeningCandidate,
    _build_sector_snapshot_from_tushare,
    _filter_stock_universe,
    _merge_stock_codes,
    _split_stock_codes,
    apply_selection_rules,
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

    def test_build_screening_report_handles_empty_candidates(self) -> None:
        report = build_screening_report(
            candidates=[],
            report_date="2026-04-13",
        )

        self.assertIn("2026-04-13", report)
        self.assertIn("未筛出符合条件的A股股票", report)
        self.assertIn("5 日线乖离率 < 8%", report)

    def test_build_screening_report_contains_ai_review_summary(self) -> None:
        candidate = RuleScreeningCandidate(
            code="000559",
            name="万向钱潮",
            close=21.7,
            ma5=20.96,
            ma10=20.08,
            ma20=18.94,
            bias_ma5_pct=3.53,
            volume_ratio=1.86,
            turnover_rate=8.2,
            sector_name="汽车零部件",
            sector_change_pct=3.1,
            prior_rise_pct=34.8,
            abc_pattern_confirmed=True,
            notes=["放量站回20日线", "5/10/20日线多头排列"],
        )

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
            min_sector_change_pct=2.0,
        )

        self.assertEqual(snapshot["000559"][0]["name"], "汽车")
        self.assertAlmostEqual(snapshot["000559"][0]["change_pct"], 2.8)
        self.assertEqual(snapshot["300490"], [])


if __name__ == "__main__":
    unittest.main()
