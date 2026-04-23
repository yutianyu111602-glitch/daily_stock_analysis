from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "run_close_combo_push.py"
SPEC = importlib.util.spec_from_file_location("run_close_combo_push_test_module", SCRIPT_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
build_combined_close_content = MODULE.build_combined_close_content


class RunCloseComboPushTestCase(unittest.TestCase):
    def test_build_combined_close_content_includes_all_sections(self) -> None:
        content = build_combined_close_content(
            stock_report="个股仪表盘",
            market_report="大盘总结",
            screener_report="# A股规则选股日报 20260415\n\n## 优先关注（前 10 只）\n\n1. 华自科技",
            report_date="20260415",
        )

        self.assertIn("# 收盘综合推送 20260415", content)
        self.assertIn("## 大盘与自选股收盘总结", content)
        self.assertIn("大盘总结", content)
        self.assertIn("个股仪表盘", content)
        self.assertIn("## A股规则选股", content)
        self.assertIn("优先关注（前 10 只）", content)


if __name__ == "__main__":
    unittest.main()
