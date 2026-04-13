from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import setup_env, get_config
from src.logging_config import setup_logging
from src.services.rule_screener_service import AshareRuleScreenerService


def prepare_rule_screener_env() -> None:
    disable_gemini = os.getenv("RULE_SCREENER_DISABLE_GEMINI", "").strip().lower() == "true"
    disable_anthropic = os.getenv("RULE_SCREENER_DISABLE_ANTHROPIC", "").strip().lower() == "true"
    prefer_aihubmix = os.getenv("RULE_SCREENER_PREFER_AIHUBMIX", "").strip().lower() == "true"

    if prefer_aihubmix and os.getenv("AIHUBMIX_KEY", "").strip():
        openai_model = (
            os.getenv("RULE_SCREENER_AIHUBMIX_MODEL", "").strip()
            or os.getenv("OPENAI_MODEL", "").strip()
            or "gpt-5-chat-latest"
        )
        os.environ["OPENAI_MODEL"] = openai_model
        os.environ.setdefault(
            "LITELLM_MODEL",
            openai_model if "/" in openai_model else f"openai/{openai_model}",
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

    logger.info("规则选股完成: trade_date=%s, candidates=%s", result.trade_date, len(result.candidates))
    sys.stdout.write(result.report + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
