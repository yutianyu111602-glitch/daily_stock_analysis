from __future__ import annotations

import os
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.services.nl_rule_screener_service import (
    looks_like_rule_screener_request,
    parse_natural_language_rule,
    parse_natural_language_rule_with_llm,
)


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

    def test_detects_rule_screener_requests_without_command_prefix(self) -> None:
        self.assertTrue(looks_like_rule_screener_request("按新规则筛一下，量比大于1，换手大于3，行业前五"))
        self.assertTrue(looks_like_rule_screener_request("ABC调整后，5日线乖离率小于9，超大单大单中单流入"))
        self.assertFalse(looks_like_rule_screener_request("帮我分析一下600519"))

    def test_llm_parser_maps_colloquial_voice_text_to_rule_profile(self) -> None:
        fake_response = SimpleNamespace(
            content=None,
            tool_calls=[
                SimpleNamespace(
                    name="run_ashare_rule_screener",
                    arguments={
                        "min_prior_rise_pct": 20,
                        "min_volume_ratio": None,
                        "min_turnover_rate": 3,
                        "morning_min_turnover_rate": 3,
                        "afternoon_min_turnover_rate": 5,
                        "min_sector_change_pct": 1,
                        "max_bias_ma5_pct": 9,
                        "sector_rank_top_n": 5,
                        "limit": 10,
                        "require_abc": True,
                        "require_close_above_ma20": True,
                        "require_ma10_ma20_up": True,
                        "require_c_low_gt_a_low": True,
                        "require_b_high_above_ma20": True,
                        "require_capital_flow_all_positive": True,
                    },
                )
            ],
        )
        config = SimpleNamespace(agent_litellm_model="", litellm_model="deepseek/deepseek-chat")

        with patch("src.agent.llm_adapter.LLMToolAdapter") as adapter_cls:
            adapter = adapter_cls.return_value
            adapter.call_with_tools.return_value = fake_response
            profile = parse_natural_language_rule_with_llm(
                "十点半给我整十来个，成交活跃点，换手不能太低三以上，板块得排前五，别离五日线太远九以内",
                config=config,
            )

        self.assertEqual(profile.min_turnover_rate, 3.0)
        self.assertEqual(profile.afternoon_min_turnover_rate, 5.0)
        self.assertEqual(profile.sector_rank_top_n, 5)
        self.assertEqual(profile.limit, 10)
        self.assertTrue(profile.require_capital_flow_all_positive)
        self.assertIn("deepseek", profile.parser_source)
        adapter.call_with_tools.assert_called_once()

    def test_llm_parser_defaults_to_deepseek_v4_pro(self) -> None:
        fake_response = SimpleNamespace(content='{"limit": 10}', tool_calls=[])
        config = SimpleNamespace(agent_litellm_model="", litellm_model="")

        with patch("src.agent.llm_adapter.LLMToolAdapter") as adapter_cls:
            adapter_cls.return_value.call_with_tools.return_value = fake_response
            profile = parse_natural_language_rule_with_llm("帮我筛一下短线A股", config=config)

        self.assertIn("deepseek/deepseek-v4-pro", profile.parser_source)


if __name__ == "__main__":
    unittest.main()
