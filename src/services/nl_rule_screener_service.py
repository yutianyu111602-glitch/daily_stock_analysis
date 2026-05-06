from __future__ import annotations

import os
import json
import copy
import re
import unicodedata
from dataclasses import dataclass, replace
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
    parser_source: str = "regex"

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
            f"- 解析方式：{self.parser_source}",
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


def parse_natural_language_rule_with_llm(
    text: str,
    *,
    config=None,
    model: Optional[str] = None,
    timeout: float = 12.0,
) -> NaturalLanguageRuleProfile:
    fallback_profile = parse_natural_language_rule(text)
    if os.getenv("RULE_SCREENER_NL_LLM_ENABLED", "true").strip().lower() == "false":
        return fallback_profile

    try:
        from src.config import get_config
        from src.agent.llm_adapter import LLMToolAdapter

        base_config = config or get_config()
        forced_model = model or os.getenv("RULE_SCREENER_NL_LLM_MODEL", "deepseek/deepseek-v4-pro").strip()
        try:
            llm_config = replace(base_config, agent_litellm_model=forced_model)
        except TypeError:
            llm_config = copy.copy(base_config)
            setattr(llm_config, "agent_litellm_model", forced_model)
        adapter = LLMToolAdapter(llm_config)
        response = adapter.call_with_tools(
            [
                {"role": "system", "content": _RULE_PARSE_PROMPT},
                {"role": "user", "content": text or ""},
            ],
            tools=_RULE_PARSE_TOOLS_SCHEMA,
            timeout=timeout,
        )
        payload = _extract_rule_payload_from_llm_response(response)
        merged = _merge_llm_rule_payload(fallback_profile, payload)
        merged.parser_source = f"deepseek:{forced_model}"
        return merged
    except Exception:
        fallback_profile.parser_source = "regex_fallback"
        return fallback_profile


_RULE_PARSE_PROMPT = """\
你是A股短线规则选股助手，负责把用户口语化、语音转文字后的表达解析成工具参数。
不要给投资建议，不要编股票代码，只抽取筛选规则。用户可能说得不标准，例如“来十个”“成交活跃点”“板块靠前”“别离五日线太远”“我爸那套”。
如果用户是在表达选股规则或筛选要求，请调用 run_ashare_rule_screener 工具，并只填写能从文本中确定的参数。

如果当前模型不能调用工具，才返回JSON对象，不要Markdown。字段如下，无法确定则填null或false：
{
  "min_prior_rise_pct": number|null,
  "min_volume_ratio": number|null,
  "min_turnover_rate": number|null,
  "morning_min_turnover_rate": number|null,
  "afternoon_min_turnover_rate": number|null,
  "min_sector_change_pct": number|null,
  "max_bias_ma5_pct": number|null,
  "sector_rank_top_n": integer|null,
  "limit": integer|null,
  "require_abc": boolean,
  "require_close_above_ma20": boolean,
  "require_ma10_ma20_up": boolean,
  "require_c_low_gt_a_low": boolean,
  "require_b_high_above_ma20": boolean,
  "require_capital_flow_all_positive": boolean
}

领域约定：
- “前期涨过约二十个点/涨幅二十左右” => min_prior_rise_pct=20。
- “成交活跃/量能放出来”如果没有数字，不要臆造量比；保留null。
- “换手三以上/换手不能太低三以上” => min_turnover_rate=3。
- “十点半/上午”附近的换手阈值填 morning_min_turnover_rate。
- “两点半/下午”附近的换手阈值填 afternoon_min_turnover_rate。
- “行业前五/板块靠前五名/板块涨幅榜前五” => sector_rank_top_n=5。
- “板块涨一个点以上/行业涨幅超过1%” => min_sector_change_pct=1。
- “别离五日线太远/五日线乖离九以内” => max_bias_ma5_pct=9。
- “ABC调整/三浪调整/A-B-C浪” => require_abc=true。
- “B浪过20日线/反弹站上二十线” => require_b_high_above_ma20=true。
- “C低比A低高/低点抬高” => require_c_low_gt_a_low=true。
- “大单超大单中单流入/主力资金流入” => require_capital_flow_all_positive=true。
- “十只/来十个/精选十个/不要超过十个” => limit=10；最大不要超过15。
"""

_RULE_PARSE_TOOLS_SCHEMA = [
    {
        "type": "function",
        "function": {
            "name": "run_ashare_rule_screener",
            "description": "Parse colloquial A-share short-term screening rules into safe rule-screener parameters.",
            "parameters": {
                "type": "object",
                "properties": {
                    "min_prior_rise_pct": {"type": "number", "description": "Minimum prior rise percentage."},
                    "min_volume_ratio": {"type": "number", "description": "Minimum volume ratio."},
                    "min_turnover_rate": {"type": "number", "description": "Minimum turnover rate percentage."},
                    "morning_min_turnover_rate": {"type": "number", "description": "Morning/10:30 turnover threshold."},
                    "afternoon_min_turnover_rate": {"type": "number", "description": "Afternoon/14:30 turnover threshold."},
                    "min_sector_change_pct": {"type": "number", "description": "Minimum sector or industry change percentage."},
                    "max_bias_ma5_pct": {"type": "number", "description": "Maximum MA5 bias percentage."},
                    "sector_rank_top_n": {"type": "integer", "description": "Top-N sector/industry ranking threshold."},
                    "limit": {"type": "integer", "description": "Maximum pushed candidate count, capped at 15."},
                    "require_abc": {"type": "boolean", "description": "Whether ABC-like pullback structure is required."},
                    "require_close_above_ma20": {"type": "boolean", "description": "Whether price must stand above MA20."},
                    "require_ma10_ma20_up": {"type": "boolean", "description": "Whether MA10 and MA20 should slope upward."},
                    "require_c_low_gt_a_low": {"type": "boolean", "description": "Whether C-wave low must be higher than A-wave low."},
                    "require_b_high_above_ma20": {"type": "boolean", "description": "Whether B-wave rebound must be above MA20."},
                    "require_capital_flow_all_positive": {"type": "boolean", "description": "Whether super-large, large, and medium order net flows should be positive."},
                },
                "required": [],
            },
        },
    }
]


def looks_like_rule_screener_request(text: str) -> bool:
    normalized = _normalize_text(text)
    if not normalized.strip():
        return False

    trigger_count = 0
    trigger_patterns = [
        r"规则选股|选股|筛选|筛股|股票池|候选池|精选",
        r"量比(?:大于|超过|不低于|>=|>)",
        r"换手(?:率)?(?:大于|超过|不低于|>=|>)",
        r"(?:板块|行业).{0,12}(?:前|排名|涨幅榜)",
        r"(?:5日线|五日线|MA5).{0,10}乖离率",
        r"A\s*[-－]?\s*B\s*[-－]?\s*C|ABC",
        r"B浪|C浪",
        r"超大单|大单|中单",
        r"我爸那套|老股民|候选|找.*股票|来.*只|来.*个",
    ]
    for pattern in trigger_patterns:
        if re.search(pattern, normalized, re.IGNORECASE):
            trigger_count += 1
    return trigger_count >= 2 or bool(re.search(r"规则选股|筛一下|筛选.*股票|选.*股票|找.*股票|我爸那套", normalized))


def _normalize_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", text or "")
    normalized = normalized.replace("％", "%")
    normalized = normalized.replace("，", ",").replace("。", ".").replace("、", ",")
    return normalized


def _loads_json_object(content: str) -> dict:
    text = (content or "").strip()
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)
    if not text.startswith("{"):
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            raise ValueError("LLM did not return a JSON object")
        text = match.group(0)
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("LLM JSON payload is not an object")
    return payload


def _extract_rule_payload_from_llm_response(response) -> dict:
    tool_calls = getattr(response, "tool_calls", None) or []
    for tool_call in tool_calls:
        name = getattr(tool_call, "name", "")
        arguments = getattr(tool_call, "arguments", None)
        if not name and hasattr(tool_call, "function"):
            function = getattr(tool_call, "function")
            name = getattr(function, "name", "")
            arguments = getattr(function, "arguments", arguments)
        if name != "run_ashare_rule_screener":
            continue
        if isinstance(arguments, dict):
            return arguments
        if isinstance(arguments, str):
            return _loads_json_object(arguments)
    return _loads_json_object(getattr(response, "content", "") or "")


def _merge_llm_rule_payload(profile: NaturalLanguageRuleProfile, payload: dict) -> NaturalLanguageRuleProfile:
    merged = NaturalLanguageRuleProfile(raw_text=profile.raw_text)
    numeric_fields = [
        "min_prior_rise_pct",
        "min_volume_ratio",
        "min_turnover_rate",
        "morning_min_turnover_rate",
        "afternoon_min_turnover_rate",
        "min_sector_change_pct",
        "max_bias_ma5_pct",
    ]
    for field_name in numeric_fields:
        setattr(
            merged,
            field_name,
            _coerce_float(payload.get(field_name), getattr(profile, field_name)),
        )
    merged.sector_rank_top_n = _coerce_int(payload.get("sector_rank_top_n"), profile.sector_rank_top_n)
    merged.limit = max(1, min(_coerce_int(payload.get("limit"), profile.limit) or 10, 15))
    for field_name in [
        "require_abc",
        "require_close_above_ma20",
        "require_ma10_ma20_up",
        "require_c_low_gt_a_low",
        "require_b_high_above_ma20",
        "require_capital_flow_all_positive",
    ]:
        setattr(merged, field_name, bool(payload.get(field_name) or getattr(profile, field_name)))
    return merged


def _coerce_float(value, fallback: Optional[float]) -> Optional[float]:
    if value is None or value == "":
        return fallback
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _coerce_int(value, fallback: Optional[int]) -> Optional[int]:
    if value is None or value == "":
        return fallback
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return fallback


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
