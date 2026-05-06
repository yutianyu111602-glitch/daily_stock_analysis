# -*- coding: utf-8 -*-
"""Tests for the WSL2 WeChatBot gateway."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient

from bot.models import BotResponse


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "wechatbot_gateway.py"
SPEC = importlib.util.spec_from_file_location("wechatbot_gateway_under_test", MODULE_PATH)
assert SPEC and SPEC.loader
gateway = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gateway)
create_app = gateway.create_app


def test_healthz() -> None:
    client = TestClient(create_app())

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json()["ok"] is True


def test_message_dispatches_generic_wechat_payload(monkeypatch) -> None:
    monkeypatch.delenv("WECHATBOT_GATEWAY_TOKEN", raising=False)
    dispatch = AsyncMock(return_value=BotResponse.markdown_response("收到"))

    with patch.object(gateway, "_dispatch_bot_message", dispatch):
        client = TestClient(create_app())
        response = client.post(
            "/wechatbot/message",
            json={
                "text": "选股 量比大于1，换手大于3，行业前五",
                "from_user": "dad-wxid",
                "nickname": "dad",
                "chat_id": "dad-wxid",
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["reply"] == "收到"
    assert body["markdown"] is True
    message = dispatch.await_args.args[0]
    assert message.platform == "wechatbot"
    assert message.user_id == "dad-wxid"
    assert message.content.startswith("选股")


def test_message_accepts_nested_voice_transcript(monkeypatch) -> None:
    monkeypatch.delenv("WECHATBOT_GATEWAY_TOKEN", raising=False)
    dispatch = AsyncMock(return_value=BotResponse.text_response("ok"))

    with patch.object(gateway, "_dispatch_bot_message", dispatch):
        client = TestClient(create_app())
        response = client.post(
            "/wechatbot/message",
            json={
                "data": {
                    "text": "帮我按上午规则筛一下股票",
                    "user_id": "voice-user",
                    "chat_id": "room@chatroom",
                    "is_group": True,
                }
            },
        )

    assert response.status_code == 200
    message = dispatch.await_args.args[0]
    assert message.user_id == "voice-user"
    assert message.chat_id == "room@chatroom"
    assert message.chat_type.value == "group"


def test_message_requires_token_when_configured(monkeypatch) -> None:
    monkeypatch.setenv("WECHATBOT_GATEWAY_TOKEN", "secret-token")
    client = TestClient(create_app())

    denied = client.post("/wechatbot/message", json={"text": "help"})
    allowed = client.post(
        "/wechatbot/message",
        json={"text": "help"},
        headers={"X-Gateway-Token": "secret-token"},
    )

    assert denied.status_code == 401
    assert allowed.status_code != 401


def test_message_rejects_disallowed_user(monkeypatch) -> None:
    monkeypatch.delenv("WECHATBOT_GATEWAY_TOKEN", raising=False)
    monkeypatch.setenv("WECHATBOT_ALLOWED_USER_IDS", "dad-wxid")
    client = TestClient(create_app())

    response = client.post("/wechatbot/message", json={"text": "help", "from_user": "stranger"})

    assert response.status_code == 403


def test_message_rejects_oversized_content(monkeypatch) -> None:
    monkeypatch.delenv("WECHATBOT_GATEWAY_TOKEN", raising=False)
    monkeypatch.setenv("WECHATBOT_GATEWAY_MAX_CONTENT_CHARS", "5")
    client = TestClient(create_app())

    response = client.post("/wechatbot/message", json={"text": "123456"})

    assert response.status_code == 413


def test_github_rules_mode_triggers_workflow(monkeypatch) -> None:
    monkeypatch.setenv("WECHATBOT_GATEWAY_MODE", "github_rules")
    monkeypatch.setenv("WECHATBOT_GATEWAY_GITHUB_REPO", "owner/repo")
    monkeypatch.delenv("WECHATBOT_GATEWAY_TOKEN", raising=False)
    completed = MagicMock(returncode=0, stdout="", stderr="")

    with patch.object(gateway.subprocess, "run", return_value=completed) as run:
        client = TestClient(create_app())
        response = client.post(
            "/wechatbot/message",
            json={"text": "下午两点半规则，换手大于5，行业前五", "from_user": "dad"},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert "session=afternoon" in body["reply"]
    cmd = run.call_args.args[0]
    assert cmd[:3] == ["gh", "workflow", "run"]
    assert "owner/repo" in cmd
    assert "rule_text=下午两点半规则，换手大于5，行业前五" in cmd


def test_github_rules_failure_hides_internal_error(monkeypatch) -> None:
    monkeypatch.setenv("WECHATBOT_GATEWAY_MODE", "github_rules")
    monkeypatch.delenv("WECHATBOT_GATEWAY_TOKEN", raising=False)
    completed = MagicMock(returncode=1, stdout="", stderr="sensitive internal error")

    with patch.object(gateway.subprocess, "run", return_value=completed):
        client = TestClient(create_app())
        response = client.post("/wechatbot/message", json={"text": "选股 换手大于3", "from_user": "dad"})

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is False
    assert "sensitive" not in body["reply"]
    assert "查看网关日志" in body["reply"]


def test_dedicated_stock_gateway_rejects_non_stock_message(monkeypatch) -> None:
    monkeypatch.delenv("WECHATBOT_GATEWAY_TOKEN", raising=False)
    monkeypatch.setenv("WECHATBOT_GATEWAY_REQUIRE_STOCK_INTENT", "true")
    client = TestClient(create_app())

    response = client.post("/wechatbot/message", json={"text": "今天天气怎么样", "from_user": "dad"})

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is False
    assert "炒股专用网关" in body["reply"]


def test_dedicated_stock_gateway_accepts_stock_code(monkeypatch) -> None:
    monkeypatch.setenv("WECHATBOT_GATEWAY_MODE", "github_rules")
    monkeypatch.setenv("WECHATBOT_GATEWAY_REQUIRE_STOCK_INTENT", "true")
    monkeypatch.delenv("WECHATBOT_GATEWAY_TOKEN", raising=False)
    completed = MagicMock(returncode=0, stdout="", stderr="")

    with patch.object(gateway.subprocess, "run", return_value=completed):
        client = TestClient(create_app())
        response = client.post("/wechatbot/message", json={"text": "分析 600875", "from_user": "dad"})

    assert response.status_code == 200
    assert response.json()["ok"] is True


def test_message_rejects_missing_content(monkeypatch) -> None:
    monkeypatch.delenv("WECHATBOT_GATEWAY_TOKEN", raising=False)
    client = TestClient(create_app())

    response = client.post("/wechatbot/message", json={"user_id": "u1"})

    assert response.status_code == 400
