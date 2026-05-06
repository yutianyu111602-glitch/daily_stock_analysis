from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import unittest
from unittest.mock import patch

from src.services.nl_rule_screener_service import parse_natural_language_rule


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "run_nl_rule_screener.py"
SPEC = importlib.util.spec_from_file_location("run_nl_rule_screener_test_module", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class RunNaturalLanguageRuleScreenerTest(unittest.TestCase):
    def test_parse_only_outputs_rule_summary_without_running_service(self) -> None:
        text = "量比大于1，换手大于3%，所在板块涨幅大于1%，行业前五，精选10只"

        with patch.dict(os.environ, {}, clear=False), \
             patch.object(
                 MODULE,
                 "parse_natural_language_rule_with_llm",
                 return_value=parse_natural_language_rule(text),
             ), \
             patch.object(MODULE, "AshareRuleScreenerService") as service_cls:
            exit_code = MODULE.main([text, "--session", "morning", "--parse-only"])
            self.assertEqual(os.environ["RULE_SCREENER_PUSH_CANDIDATE_LIMIT"], "10")
            self.assertEqual(os.environ["RULE_SCREENER_FOCUS_POOL_LIMIT"], "10")

        self.assertEqual(exit_code, 0)
        service_cls.assert_not_called()


if __name__ == "__main__":
    unittest.main()
