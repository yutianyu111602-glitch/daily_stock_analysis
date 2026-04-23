from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path
import os
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import get_config, setup_env
from src.core.market_review import run_market_review
from src.core.pipeline import StockAnalysisPipeline
from src.logging_config import setup_logging
from src.notification import NotificationService
from src.services.rule_screener_service import AshareRuleScreenerService


logger = logging.getLogger(__name__)


def prepare_close_combo_env() -> None:
    if os.getenv("AIHUBMIX_KEY", "").strip():
        openai_model = os.getenv("OPENAI_MODEL", "").strip() or "gpt-5-chat-latest"
        os.environ["OPENAI_MODEL"] = openai_model
        os.environ.setdefault(
            "LITELLM_MODEL",
            openai_model if "/" in openai_model else f"openai/{openai_model}",
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="收盘综合推送：大盘 + 自选股 + A股规则选股")
    parser.add_argument("--no-notify", action="store_true", help="只生成结果，不推送")
    parser.add_argument("--ai-review", action="store_true", help="启用规则选股 AI 复核")
    return parser.parse_args()


def build_combined_close_content(
    *,
    stock_report: str,
    market_report: str,
    screener_report: str,
    report_date: str,
) -> str:
    parts = [
        f"# 收盘综合推送 {report_date}",
        "",
        "## 大盘与自选股收盘总结",
        "",
        market_report.strip(),
        "",
        "---",
        "",
        stock_report.strip(),
        "",
        "---",
        "",
        "## A股规则选股",
        "",
        screener_report.strip(),
    ]
    return "\n".join(parts).strip() + "\n"


def main() -> int:
    args = parse_args()
    setup_env()
    prepare_close_combo_env()
    config = get_config()
    setup_logging(debug=False, log_dir=config.log_dir)

    notifier = NotificationService()
    pipeline = StockAnalysisPipeline(config=config, max_workers=1)

    logger.info("开始生成收盘综合推送")
    results = pipeline.run(
        stock_codes=config.stock_list,
        dry_run=False,
        send_notification=False,
        merge_notification=False,
    )
    stock_report = notifier.generate_aggregate_report(
        results,
        getattr(config, "report_type", "full"),
        report_date=datetime.now().strftime("%Y%m%d"),
    )
    market_report = run_market_review(
        notifier=notifier,
        analyzer=pipeline.analyzer,
        search_service=pipeline.search_service,
        send_notification=False,
        merge_notification=False,
        override_region=getattr(config, "market_review_region", "cn") or "cn",
    ) or "今日未生成大盘总结。"

    screener_result = AshareRuleScreenerService().run(
        send_notification=False,
        ai_review=args.ai_review,
    )

    report_date = screener_result.trade_date or datetime.now().strftime("%Y%m%d")
    combined_content = build_combined_close_content(
        stock_report=stock_report,
        market_report=market_report,
        screener_report=screener_result.report,
        report_date=report_date,
    )

    notifier.save_report_to_file(combined_content, f"close_combo_{report_date}.md")

    if args.no_notify:
        sys.stdout.buffer.write(combined_content.encode("utf-8", errors="replace"))
        return 0

    ok = notifier.send(combined_content, email_send_to_all=True)
    logger.info(
        "收盘综合推送完成: stocks=%s screener=%s send_ok=%s",
        len(results),
        len(screener_result.candidates),
        ok,
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
