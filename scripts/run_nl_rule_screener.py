from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from src.config import get_config, setup_env
from src.logging_config import setup_logging
from src.services.nl_rule_screener_service import parse_natural_language_rule_with_llm
from src.services.rule_screener_service import AshareRuleScreenerService

try:
    from scripts.run_rule_screener import (
        extract_regime_debug_notes,
        extract_rule_screener_summary,
        prepare_rule_screener_env,
        should_block_empty_report,
    )
except ModuleNotFoundError:
    from run_rule_screener import (  # type: ignore
        extract_regime_debug_notes,
        extract_rule_screener_summary,
        prepare_rule_screener_env,
        should_block_empty_report,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="自然语言规则 -> A股规则选股 -> AI复核 -> 推送")
    parser.add_argument("rule_text", nargs="?", default="", help="自然语言选股规则")
    parser.add_argument("--rule-file", default="", help="从文本文件读取自然语言规则")
    parser.add_argument(
        "--session",
        choices=["auto", "morning", "midday", "afternoon"],
        default="auto",
        help="用于套用分时阈值，例如上午换手3%、下午换手5%",
    )
    parser.add_argument("--parse-only", action="store_true", help="只解析规则，不拉取行情、不推送")
    parser.add_argument("--debug", action="store_true", help="输出调试日志")
    parser.add_argument("--no-notify", action="store_true", help="只生成结果，不推送")
    parser.add_argument("--no-ai-review", action="store_true", help="只做规则选股，不跑 AI 复核")
    return parser.parse_args(argv)


def read_rule_text(args: argparse.Namespace) -> str:
    if args.rule_file:
        return Path(args.rule_file).read_text(encoding="utf-8").strip()
    rule_text = (args.rule_text or "").strip()
    if rule_text:
        return rule_text
    return os.getenv("RULE_SCREENER_NL_RULE_TEXT", "").strip()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    rule_text = read_rule_text(args)
    if not rule_text:
        raise SystemExit("必须提供自然语言规则文本，或设置 RULE_SCREENER_NL_RULE_TEXT")

    profile = parse_natural_language_rule_with_llm(rule_text)
    rule_config = profile.to_rule_config(session=args.session)
    os.environ["RULE_SCREENER_PUSH_CANDIDATE_LIMIT"] = str(profile.limit)
    os.environ["RULE_SCREENER_FOCUS_POOL_LIMIT"] = str(profile.limit)

    if args.parse_only:
        sys.stdout.write("\n".join(profile.summary_lines(session=args.session)) + "\n")
        return 0

    setup_env()
    prepare_rule_screener_env()
    config = get_config()
    setup_logging(
        log_prefix="nl_rule_screener",
        log_dir=config.log_dir,
        debug=args.debug,
    )

    logger = logging.getLogger(__name__)
    logger.info("自然语言规则解析完成: %s", " | ".join(profile.summary_lines(session=args.session)))
    service = AshareRuleScreenerService(config=config, rule_config=rule_config)
    result = service.run(
        send_notification=not args.no_notify,
        ai_review=not args.no_ai_review,
    )

    summary = extract_rule_screener_summary(result.report)
    logger.info(
        "自然语言规则选股完成: trade_date=%s, profile=%s, full=%s, relaxed=%s, technical=%s, manual=%s, candidates=%s",
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
        logger.error("自然语言规则选股输出为空且已禁用空报告: trade_date=%s", result.trade_date)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
