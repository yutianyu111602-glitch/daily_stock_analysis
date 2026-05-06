from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from src.services.nl_rule_screener_service import parse_natural_language_rule


FATHER_RULE_TEXT = """
新规则 1、股价前期经过了约20%的上涨。2.经过了A-B-C浪的调整。
3、股价再次上涨，且站上20日均线。4、量比大于1、换手大于3%。
5、所在板块涨幅大于1%。6、5日线乖离率小于9%。
7、10日线20日线朝上。8，C浪的低点要高于A浪的低点。
9.B浪反弹要高于20日线。10.所在板块涨幅榜排名前五。
11.超大单，大单，中单是流入的。
10点半推送的换手率要大于3%，两点半推送的换手率要大于5%。
股票池按照10支，最好不要超过15只。
"""


class NaturalLanguageRuleParserTest(unittest.TestCase):
    def test_parse_father_rule_text_maps_supported_thresholds(self) -> None:
        profile = parse_natural_language_rule(FATHER_RULE_TEXT)

        self.assertEqual(profile.min_prior_rise_pct, 20.0)
        self.assertEqual(profile.min_volume_ratio, 1.0)
        self.assertEqual(profile.min_turnover_rate, 3.0)
        self.assertEqual(profile.morning_min_turnover_rate, 3.0)
        self.assertEqual(profile.afternoon_min_turnover_rate, 5.0)
        self.assertEqual(profile.min_sector_change_pct, 1.0)
        self.assertEqual(profile.max_bias_ma5_pct, 9.0)
        self.assertEqual(profile.sector_rank_top_n, 5)
        self.assertEqual(profile.limit, 10)
        self.assertTrue(profile.require_abc)
        self.assertTrue(profile.require_close_above_ma20)
        self.assertTrue(profile.require_ma10_ma20_up)
        self.assertTrue(profile.require_c_low_gt_a_low)
        self.assertTrue(profile.require_b_high_above_ma20)
        self.assertTrue(profile.require_capital_flow_all_positive)

    def test_to_rule_config_applies_session_specific_turnover(self) -> None:
        profile = parse_natural_language_rule(FATHER_RULE_TEXT)

        morning = profile.to_rule_config(session="morning")
        afternoon = profile.to_rule_config(session="afternoon")

        self.assertEqual(morning.min_turnover_rate, 3.0)
        self.assertEqual(afternoon.min_turnover_rate, 5.0)
        self.assertEqual(afternoon.min_volume_ratio, 1.0)
        self.assertEqual(afternoon.min_sector_change_pct, 1.0)
        self.assertEqual(afternoon.max_bias_ma5_pct, 9.0)
        self.assertEqual(afternoon.sector_rank_top_n, 5)

    def test_parser_keeps_defaults_for_sparse_text(self) -> None:
        profile = parse_natural_language_rule("帮我筛一下今天适合短线观察的A股，精选10只")
        config = profile.to_rule_config(session="morning")

        self.assertEqual(profile.limit, 10)
        self.assertEqual(config.min_prior_rise_pct, 20.0)
        self.assertEqual(config.min_volume_ratio, 1.0)
        self.assertEqual(config.min_turnover_rate, 3.0)

    def test_sparse_text_inherits_existing_environment_defaults(self) -> None:
        profile = parse_natural_language_rule("帮我筛一下今天适合短线观察的A股，精选10只")

        with patch.dict(
            os.environ,
            {
                "RULE_SCREENER_LOOKBACK_DAYS": "90",
                "RULE_SCREENER_MIN_VOLUME_RATIO": "1.2",
                "RULE_SCREENER_MIN_TURNOVER_RATE": "4",
                "RULE_SCREENER_SECTOR_TOP_N": "3",
            },
            clear=False,
        ):
            config = profile.to_rule_config(session="morning")

        self.assertEqual(config.lookback_days, 90)
        self.assertEqual(config.min_volume_ratio, 1.2)
        self.assertEqual(config.min_turnover_rate, 4.0)
        self.assertEqual(config.sector_rank_top_n, 3)


if __name__ == "__main__":
    unittest.main()
