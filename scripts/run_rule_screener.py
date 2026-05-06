from __future__ import annotations

import argparse
import logging
import os
import re
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import setup_env, get_config
from src.logging_config import setup_logging
from src.services.rule_screener_service import AshareRuleScreenerService


def prepare_rule_screener_env() -> None:
    os.environ.setdefault("RULE_SCREENER_DYNAMIC_MODE", "true")
    os.environ.setdefault("RULE_SCREENER_ALLOW_EMPTY_REPORT", "false")
    os.environ.setdefault("RULE_SCREENER_MANUAL_REVIEW_LIMIT", "20")
    if os.getenv("RULE_SCREENER_DYNAMIC_MODE", "").strip().lower() == "false":
        os.environ["RULE_SCREENER_AUTO_RELAX_IF_EMPTY"] = "false"

    disable_gemini = os.getenv("RULE_SCREENER_DISABLE_GEMINI", "").strip().lower() == "true"
    disable_anthropic = os.getenv("RULE_SCREENER_DISABLE_ANTHROPIC", "").strip().lower() == "true"
    prefer_aihubmix_raw = os.getenv("RULE_SCREENER_PREFER_AIHUBMIX", "").strip().lower()
    prefer_aihubmix = prefer_aihubmix_raw == "true" or (
        prefer_aihubmix_raw == ""
        and bool(os.getenv("AIHUBMIX_KEY", "").strip())
    )

    if prefer_aihubmix and os.getenv("AIHUBMIX_KEY", "").strip():
        openai_model = (
            os.getenv("RULE_SCREENER_AIHUBMIX_MODEL", "").strip()
            or os.getenv("OPENAI_MODEL", "").strip()
            or "gpt-5-chat-latest"
        )
        os.environ["OPENAI_MODEL"] = openai_model
        os.environ["LITELLM_MODEL"] = (
            openai_model if "/" in openai_model else f"openai/{openai_model}"
        )
        if not disable_gemini and not os.getenv("LITELLM_FALLBACK_MODELS", "").strip():
            gemini_fallback = os.getenv("RULE_SCREENER_GEMINI_FALLBACK_MODEL", "gemini-2.0-flash").strip()
            if gemini_fallback:
                os.environ["LITELLM_FALLBACK_MODELS"] = (
                    gemini_fallback if "/" in gemini_fallback else f"gemini/{gemini_fallback}"
                )

    if disable_gemini:
        for key in (
            "GEMINI_API_KEY",
            "GEMINI_API_KEYS",
            "LLM_GEMINI_API_KEY",
            "LLM_GEMINI_API_KEYS",
        ):
            os.environ.pop(key, None)

    if disable_anthropic:
        for key in (
            "ANTHROPIC_API_KEY",
            "ANTHROPIC_API_KEYS",
            "LLM_ANTHROPIC_API_KEY",
            "LLM_ANTHROPIC_API_KEYS",
        ):
            os.environ.pop(key, None)


def extract_rule_screener_summary(report: str) -> dict[str, int]:
    summary = {"full": 0, "relaxed": 0, "technical": 0, "manual": 0}
    section_patterns = {
        "full": r"## 完整命中（(\d+) 只）",
        "relaxed": r"## 动态放宽命中（(\d+) 只）",
        "technical": r"## 技术候选池（(\d+) 只）",
        "manual": r"## 人工精选池（(\d+) 只）",
    }
    for key, pattern in section_patterns.items():
        match = re.search(pattern, report)
        if match:
            summary[key] = int(match.group(1))
    return summary


def extract_regime_debug_notes(profile_notes: list[str]) -> list[str]:
    return [
        note for note in (profile_notes or [])
        if "市场环境" in note or note.startswith("动态放宽：")
    ]


def should_block_empty_report(candidate_count: int) -> bool:
    allow_empty_report = os.getenv("RULE_SCREENER_ALLOW_EMPTY_REPORT", "false").strip().lower() == "true"
    return candidate_count == 0 and not allow_empty_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="A股规则选股 -> AI复核 -> 推送")
    parser.add_argument("--debug", action="store_true", help="输出调试日志")
    parser.add_argument("--no-notify", action="store_true", help="只生成结果，不推送")
    parser.add_argument("--no-ai-review", action="store_true", help="只做规则选股，不跑 AI 复核")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    setup_env()
    prepare_rule_screener_env()
    config = get_config()
    setup_logging(
        log_prefix="rule_screener",
        log_dir=config.log_dir,
        debug=args.debug,
    )

    logger = logging.getLogger(__name__)
    service = AshareRuleScreenerService(config=config)
    result = service.run(
        send_notification=not args.no_notify,
        ai_review=not args.no_ai_review,
    )

    summary = extract_rule_screener_summary(result.report)
    logger.info(
        "规则选股完成: trade_date=%s, profile=%s, full=%s, relaxed=%s, technical=%s, manual=%s, candidates=%s",
        result.trade_date,
        result.profile_name,
        summary["full"],
        summary["relaxed"],
        summary["technical"],
        summary["manual"],
        len(result.candidates),
    )
    if os.getenv("RULE_SCREENER_DEBUG_REGIME", "").strip().lower() == "true":
        regime_notes = extract_regime_debug_notes(result.profile_notes)
        logger.info("规则选股市场状态诊断: %s", " | ".join(regime_notes) if regime_notes else "无市场状态附注")
    sys.stdout.write(result.report + "\n")
    if should_block_empty_report(len(result.candidates)):
        logger.error(
            "规则选股输出为空且已禁用空报告: trade_date=%s, profile=%s",
            result.trade_date,
            result.profile_name,
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
