# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import os
import threading
from typing import List, Optional

from bot.commands.base import BotCommand
from bot.models import BotMessage, BotResponse
from src.config import get_config
from src.services.nl_rule_screener_service import (
    NaturalLanguageRuleProfile,
    parse_natural_language_rule_with_llm,
    resolve_session,
)
from src.services.rule_screener_service import AshareRuleScreenerService

logger = logging.getLogger(__name__)


class RuleScreenerCommand(BotCommand):
    @property
    def name(self) -> str:
        return "rules"

    @property
    def aliases(self) -> List[str]:
        return ["r", "选股", "筛选", "筛股", "规则选股"]

    @property
    def description(self) -> str:
        return "按自然语言规则筛选A股候选池"

    @property
    def usage(self) -> str:
        return "/rules <自然语言选股规则>"

    def validate_args(self, args: List[str]) -> Optional[str]:
        if args and args[0].lower() in {"help", "帮助"}:
            return None
        return None

    def execute(self, message: BotMessage, args: List[str]) -> BotResponse:
        if args and args[0].lower() in {"help", "帮助"}:
            return BotResponse.markdown_response(self._help_text())

        rule_text = " ".join(args).strip()
        if not rule_text:
            rule_text = (
                "按默认短线规则筛选A股：前期约20%上涨，ABC调整后转强，量比大于1，"
                "换手大于3%，板块涨幅大于1%，行业前五，5日线乖离率小于9%，精选10只。"
            )
        session = _detect_session(rule_text)
        profile = parse_natural_language_rule_with_llm(rule_text)
        summary = "\n".join(profile.summary_lines(session=session))

        worker = threading.Thread(
            target=_run_rule_screener_background,
            args=(profile, session, message),
            daemon=True,
        )
        worker.start()

        return BotResponse.markdown_response(
            "✅ **规则选股任务已提交**\n\n"
            f"{summary}\n\n"
            "任务会在后台拉取行情并推送结果；如果候选较多，推送正文最多展示前10只。"
        )

    def _help_text(self) -> str:
        return "\n".join(
            [
                "📌 **规则选股命令**",
                "",
                "**用法**",
                "- `/rules 量比大于1，换手大于3，行业前五，精选10只`",
                "- `选股 ABC调整后，站上20日线，5日线乖离率小于9`",
                "- `筛选 下午换手大于5，板块涨幅大于1，行业前五`",
                "",
                "**说明**",
                "- 支持微信语音转文字后的自然语言。",
                "- 上午/10点半默认按上午阈值；下午/2点半默认按下午阈值。",
                "- 未说到的参数继承 GitHub Actions / 环境变量默认规则。",
            ]
        )


def _detect_session(rule_text: str) -> str:
    text = (rule_text or "").lower()
    if any(token in text for token in ("下午", "2点半", "两点半", "14:30", "1430")):
        return "afternoon"
    if any(token in text for token in ("上午", "10点半", "十点半", "10:30", "1030", "中午", "午盘")):
        return "morning"
    return resolve_session("auto")


def _run_rule_screener_background(
    profile: NaturalLanguageRuleProfile,
    session: str,
    message: BotMessage,
) -> None:
    try:
        os.environ["RULE_SCREENER_PUSH_CANDIDATE_LIMIT"] = str(profile.limit)
        os.environ["RULE_SCREENER_FOCUS_POOL_LIMIT"] = str(profile.limit)
        config = get_config()
        rule_config = profile.to_rule_config(session=session)
        service = AshareRuleScreenerService(config=config, rule_config=rule_config)
        ai_review = os.getenv("RULE_SCREENER_BOT_AI_REVIEW", "false").strip().lower() == "true"
        result = service.run(send_notification=True, ai_review=ai_review)
        logger.info(
            "[RuleScreenerCommand] 后台规则选股完成: user=%s session=%s trade_date=%s candidates=%s",
            message.user_id,
            session,
            result.trade_date,
            len(result.candidates),
        )
    except Exception as exc:
        logger.exception("[RuleScreenerCommand] 后台规则选股失败: %s", exc)
        try:
            from src.notification import NotificationService

            NotificationService().send(f"规则选股后台任务失败：{str(exc)[:300]}")
        except Exception:
            logger.exception("[RuleScreenerCommand] 失败通知发送失败")
