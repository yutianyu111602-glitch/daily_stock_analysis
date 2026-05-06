from __future__ import annotations

from datetime import datetime
import unittest
from unittest.mock import MagicMock, patch

from bot.commands.rule_screener import RuleScreenerCommand
from bot.models import BotMessage, ChatType
from src.services.nl_rule_screener_service import parse_natural_language_rule


def _make_message(content: str) -> BotMessage:
    return BotMessage(
        platform="wecom",
        message_id="m1",
        user_id="dad",
        user_name="dad",
        chat_id="c1",
        chat_type=ChatType.PRIVATE,
        content=content,
        raw_content=content,
        timestamp=datetime.now(),
    )


class RuleScreenerCommandTest(unittest.TestCase):
    def test_chinese_rule_command_is_parsed_without_slash_prefix(self) -> None:
        message = _make_message("选股 量比大于1，换手大于3")

        command, args = message.get_command_and_args("/")

        self.assertEqual(command, "rules")
        self.assertEqual(args, ["量比大于1，换手大于3"])

    def test_execute_starts_background_task_and_returns_parsed_summary(self) -> None:
        command = RuleScreenerCommand()
        message = _make_message("选股 量比大于1，换手大于3，行业前五，精选10只")

        with patch(
            "bot.commands.rule_screener.parse_natural_language_rule_with_llm",
            return_value=parse_natural_language_rule("量比大于1，换手大于3，行业前五，精选10只"),
        ), patch("bot.commands.rule_screener.threading.Thread") as thread_cls:
            thread = MagicMock()
            thread_cls.return_value = thread
            response = command.execute(message, ["量比大于1，换手大于3，行业前五，精选10只"])

        self.assertIn("规则选股任务已提交", response.text)
        self.assertIn("量比：> 1", response.text)
        self.assertIn("换手率：> 3%", response.text)
        thread.start.assert_called_once()

    def test_help_returns_usage_examples(self) -> None:
        response = RuleScreenerCommand().execute(_make_message("选股 help"), ["help"])

        self.assertIn("/rules", response.text)
        self.assertIn("微信语音转文字", response.text)


if __name__ == "__main__":
    unittest.main()
