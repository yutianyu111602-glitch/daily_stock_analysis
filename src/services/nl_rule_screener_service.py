from __future__ import annotations

import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from src.services.rule_screener_service import AshareRuleConfig


_CHINESE_NUMBERS = {
    "一": 1,
    "二": 2,
    "两": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
    "十": 10,
}


@dataclass
class NaturalLanguageRuleProfile:
    raw_text: str
    min_prior_rise_pct: Optional[float] = None
    min_volume_ratio: Optional[float] = None
    min_turnover_rate: Optional[float] = None
    morning_min_turnover_rate: Optional[float] = None
    afternoon_min_turnover_rate: Optional[float] = None
    min_sector_change_pct: Optional[float] = None
    max_bias_ma5_pct: Optional[float] = None
    sector_rank_top_n: Optional[int] = None
    limit: int = 10
    require_abc: bool = False
    require_close_above_ma20: bool = False
    require_ma10_ma20_up: bool = False
    require_c_low_gt_a_low: bool = False
    require_b_high_above_ma20: bool = False
    require_capital_flow_all_positive: bool = False

    def resolve_turnover_rate(self, session: str = "auto", default_turnover_rate: float = 3.0) -> float:
        resolved_session = resolve_session(session)
        if resolved_session == "afternoon" and self.afternoon_min_turnover_rate is not None:
            return self.afternoon_min_turnover_rate
        if resolved_session == "morning" and self.morning_min_turnover_rate is not None:
            return self.morning_min_turnover_rate
        if self.min_turnover_rate is not None:
            return self.min_turnover_rate
        return default_turnover_rate

    def to_rule_config(self, session: str = "auto", base_config: Optional[AshareRuleConfig] = None) -> AshareRuleConfig:
        base = base_config or _default_rule_config_from_env()
        return AshareRuleConfig(
            lookback_days=base.lookback_days,
            abc_window_days=base.abc_window_days,
            min_prior_rise_pct=self.min_prior_rise_pct if self.min_prior_rise_pct is not None else base.min_prior_rise_pct,
            min_volume_ratio=self.min_volume_ratio if self.min_volume_ratio is not None else base.min_volume_ratio,
            min_turnover_rate=self.resolve_turnover_rate(session, base.min_turnover_rate),
            min_sector_change_pct=self.min_sector_change_pct if self.min_sector_change_pct is not None else base.min_sector_change_pct,
            max_bias_ma5_pct=self.max_bias_ma5_pct if self.max_bias_ma5_pct is not None else base.max_bias_ma5_pct,
            ai_review_limit=base.ai_review_limit,
            sector_rank_top_n=self.sector_rank_top_n if self.sector_rank_top_n is not None else base.sector_rank_top_n,
            notify_when_empty=base.notify_when_empty,
            exclude_st=base.exclude_st,
            allow_open_data_fallback=base.allow_open_data_fallback,
            auto_relax_if_empty=base.auto_relax_if_empty,
            abc_min_pullback_pct=base.abc_min_pullback_pct,
            abc_min_rebound_pct=base.abc_min_rebound_pct,
            abc_min_c_leg_pct=base.abc_min_c_leg_pct,
            abc_min_c_retention_ratio=base.abc_min_c_retention_ratio,
            abc_rebreak_buffer_pct=base.abc_rebreak_buffer_pct,
        )

    def summary_lines(self, session: str = "auto") -> list[str]:
        config = self.to_rule_config(session=session)
        return [
            "自然语言规则已解析为安全白名单参数：",
            f"- 前期上涨：>= {config.min_prior_rise_pct:g}%",
            f"- 量比：> {config.min_volume_ratio:g}",
            f"- 换手率：> {config.min_turnover_rate:g}%（session={resolve_session(session)}）",
            f"- 板块涨幅：> {config.min_sector_change_pct:g}%",
            f"- 板块/行业排名：前 {config.sector_rank_top_n}",
            f"- MA5乖离率：< {config.max_bias_ma5_pct:g}%",
            f"- 推送展示上限：{self.limit} 只",
            f"- ABC结构：{'要求' if self.require_abc else '使用现有默认识别'}",
            f"- B浪高于MA20：{'要求' if self.require_b_high_above_ma20 else '使用现有默认识别'}",
            f"- C浪低点高于A浪低点：{'要求' if self.require_c_low_gt_a_low else '使用现有默认识别'}",
            f"- 大/中/超大单净流入：{'要求' if self.require_capital_flow_all_positive else '使用现有默认识别'}",
            "- 未显式提到的参数：继承 GitHub Vars / 环境变量里的现有默认规则",
        ]


def resolve_session(session: str = "auto") -> str:
    normalized = (session or "auto").strip().lower()
    if normalized in {"morning", "am", "10:30", "1030", "midday", "noon"}:
        return "morning"
    if normalized in {"afternoon", "pm", "14:30", "1430"}:
        return "afternoon"
    if normalized != "auto":
        return "morning"

    now = datetime.now(timezone(timedelta(hours=8))).time()
    if now.hour >= 13:
        return "afternoon"
    return "morning"


def parse_natural_language_rule(text: str) -> NaturalLanguageRuleProfile:
    normalized = _normalize_text(text)
    profile = NaturalLanguageRuleProfile(raw_text=text or "")
    profile.min_prior_rise_pct = _find_float(
        normalized,
        [
            r"(?:前期|前高前|累计|经过了?约?)\D{0,10}(\d+(?:\.\d+)?)\s*%?\D{0,10}上涨",
            r"上涨\D{0,8}(?:不少于|大于|超过|>=|>)\s*(\d+(?:\.\d+)?)\s*%",
        ],
    )
    profile.min_volume_ratio = _find_float(
        normalized,
        [r"量比(?:大于|超过|不低于|>=|>)\s*(\d+(?:\.\d+)?)"],
    )
    profile.min_turnover_rate = _find_float(
        normalized,
        [r"换手(?:率)?(?:大于|超过|不低于|>=|>)\s*(\d+(?:\.\d+)?)\s*%?"],
    )
    profile.morning_min_turnover_rate = _find_float(
        normalized,
        [
            r"(?:10点半|10:30|1030|上午).*?换手(?:率)?(?:要)?(?:大于|超过|不低于|>=|>)\s*(\d+(?:\.\d+)?)\s*%?",
        ],
    )
    profile.afternoon_min_turnover_rate = _find_float(
        normalized,
        [
            r"(?:2点半|两点半|14:30|1430|下午).*?换手(?:率)?(?:要)?(?:大于|超过|不低于|>=|>)\s*(\d+(?:\.\d+)?)\s*%?",
        ],
    )
    profile.min_sector_change_pct = _find_float(
        normalized,
        [
            r"(?:所在)?(?:板块|行业)\D{0,10}涨幅(?:大于|超过|不低于|>=|>)\s*(\d+(?:\.\d+)?)\s*%?",
        ],
    )
    profile.max_bias_ma5_pct = _find_float(
        normalized,
        [
            r"(?:5日线|五日线|MA5)\D{0,10}乖离率(?:小于|低于|不高于|<=|<)\s*(\d+(?:\.\d+)?)\s*%?",
        ],
    )
    profile.sector_rank_top_n = _find_sector_rank_top_n(normalized)
    profile.limit = _find_limit(normalized) or 10
    profile.require_abc = bool(re.search(r"A\s*[-－]?\s*B\s*[-－]?\s*C|ABC", normalized, re.IGNORECASE))
    profile.require_close_above_ma20 = bool(re.search(r"(?:站上|高于|突破)\s*20日均线|站上\s*MA20", normalized, re.IGNORECASE))
    profile.require_ma10_ma20_up = bool(re.search(r"(?:10日线|MA10).{0,8}(?:20日线|MA20).{0,8}(?:朝上|向上)", normalized, re.IGNORECASE))
    profile.require_c_low_gt_a_low = bool(re.search(r"C浪.{0,8}(?:低点).{0,8}(?:高于|大于).{0,8}A浪", normalized, re.IGNORECASE))
    profile.require_b_high_above_ma20 = bool(re.search(r"B浪.{0,8}(?:高于|站上|突破).{0,8}(?:20日线|20日均线|MA20)", normalized, re.IGNORECASE))
    profile.require_capital_flow_all_positive = bool(
        "超大单" in normalized
        and re.search(r"(?<!超)大单", normalized)
        and "中单" in normalized
        and re.search(r"(?:流入|净流入|为正)", normalized)
    )
    return profile


def _normalize_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", text or "")
    normalized = normalized.replace("％", "%")
    normalized = normalized.replace("，", ",").replace("。", ".").replace("、", ",")
    return normalized


def _default_rule_config_from_env() -> AshareRuleConfig:
    return AshareRuleConfig(
        lookback_days=int(os.getenv("RULE_SCREENER_LOOKBACK_DAYS", "60")),
        abc_window_days=int(os.getenv("RULE_SCREENER_ABC_WINDOW_DAYS", "20")),
        min_prior_rise_pct=float(os.getenv("RULE_SCREENER_MIN_PRIOR_RISE_PCT", "20")),
        min_volume_ratio=float(os.getenv("RULE_SCREENER_MIN_VOLUME_RATIO", "1")),
        min_turnover_rate=float(os.getenv("RULE_SCREENER_MIN_TURNOVER_RATE", "3")),
        min_sector_change_pct=float(os.getenv("RULE_SCREENER_MIN_SECTOR_CHANGE_PCT", "1")),
        max_bias_ma5_pct=float(os.getenv("RULE_SCREENER_MAX_BIAS_MA5_PCT", "9")),
        ai_review_limit=int(os.getenv("RULE_SCREENER_AI_REVIEW_LIMIT", "12")),
        sector_rank_top_n=int(os.getenv("RULE_SCREENER_SECTOR_TOP_N", "5")),
        exclude_st=os.getenv("RULE_SCREENER_EXCLUDE_ST", "true").lower() != "false",
        allow_open_data_fallback=os.getenv("RULE_SCREENER_ALLOW_FALLBACK", "false").lower() == "true",
        auto_relax_if_empty=os.getenv("RULE_SCREENER_AUTO_RELAX_IF_EMPTY", "true").lower() != "false",
    )


def _find_float(text: str, patterns: list[str]) -> Optional[float]:
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            return float(match.group(1))
    return None


def _find_sector_rank_top_n(text: str) -> Optional[int]:
    patterns = [
        r"(?:板块|行业).{0,12}(?:涨幅榜)?(?:排名)?前\s*([0-9一二三四五六七八九十两]+)",
        r"(?:板块|行业).{0,12}(?:排名|涨幅榜排名)\s*(?:前)?\s*([0-9一二三四五六七八九十两]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return _parse_int_token(match.group(1))
    return None


def _find_limit(text: str) -> Optional[int]:
    patterns = [
        r"(?:精选|推送|股票池|展示|前)\D{0,8}([0-9一二三四五六七八九十两]+)\s*(?:只|支|条|个)",
        r"不要超过\s*([0-9一二三四五六七八九十两]+)\s*(?:只|支|条|个)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            value = _parse_int_token(match.group(1))
            if value:
                return max(1, min(value, 15))
    return None


def _parse_int_token(value: str) -> Optional[int]:
    value = (value or "").strip()
    if value.isdigit():
        return int(value)
    if value in _CHINESE_NUMBERS:
        return _CHINESE_NUMBERS[value]
    if value.startswith("十") and len(value) == 2:
        return 10 + (_CHINESE_NUMBERS.get(value[1], 0) or 0)
    if len(value) == 2 and value.endswith("十"):
        return (_CHINESE_NUMBERS.get(value[0], 0) or 0) * 10
    if len(value) == 3 and value[1] == "十":
        return (_CHINESE_NUMBERS.get(value[0], 0) or 0) * 10 + (_CHINESE_NUMBERS.get(value[2], 0) or 0)
    return None
