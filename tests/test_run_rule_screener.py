from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import unittest
from unittest.mock import patch

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "run_rule_screener.py"
SPEC = importlib.util.spec_from_file_location("run_rule_screener_test_module", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
prepare_rule_screener_env = MODULE.prepare_rule_screener_env


class RunRuleScreenerEnvTestCase(unittest.TestCase):
    def test_prepare_rule_screener_env_forces_aihubmix_model_when_enabled(self) -> None:
        env = {
            "AIHUBMIX_KEY": "test-key",
            "RULE_SCREENER_PREFER_AIHUBMIX": "true",
            "RULE_SCREENER_AIHUBMIX_MODEL": "gpt-5-chat-latest",
            "LITELLM_MODEL": "gemini/gemini-2.0-flash",
        }
        with patch.dict(os.environ, env, clear=False):
            prepare_rule_screener_env()
            self.assertEqual(os.environ["LITELLM_MODEL"], "openai/gpt-5-chat-latest")
            self.assertEqual(os.environ["OPENAI_MODEL"], "gpt-5-chat-latest")

    def test_prepare_rule_screener_env_removes_gemini_keys_when_disabled(self) -> None:
        env = {
            "AIHUBMIX_KEY": "test-key",
            "RULE_SCREENER_PREFER_AIHUBMIX": "true",
            "RULE_SCREENER_AIHUBMIX_MODEL": "gpt-5-chat-latest",
            "RULE_SCREENER_DISABLE_GEMINI": "true",
            "GEMINI_API_KEY": "gemini-key",
        }
        with patch.dict(os.environ, env, clear=False):
            prepare_rule_screener_env()
            self.assertNotIn("GEMINI_API_KEY", os.environ)
            self.assertEqual(os.environ["LITELLM_MODEL"], "openai/gpt-5-chat-latest")


if __name__ == "__main__":
    unittest.main()
