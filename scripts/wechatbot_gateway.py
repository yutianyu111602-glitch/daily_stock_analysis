# -*- coding: utf-8 -*-
"""Lightweight WSL2 gateway for personal WeChat bot integrations.

The gateway accepts generic JSON messages from OpenClaw/wechatbot adapters,
normalizes them into ``BotMessage``, and reuses the existing bot dispatcher.
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Optional

from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import setup_env  # noqa: E402
from src.logging_config import setup_logging  # noqa: E402

logger = logging.getLogger(__name__)

RULE_HELP_TEXT = (
    "可用命令：\n"
    "1. help / 帮助：查看说明\n"
    "2. 直接说选股规则：例如“选股 量比大于1，换手大于3，行业前五，精选10只”\n"
    "3. 说上午/十点半会按 morning；说下午/两点半会按 afternoon；不说则 auto"
)


class GatewayResponse(BaseModel):
    ok: bool
    reply: str = ""
    markdown: bool = False
    message_id: str = ""


def _deep_get(data: Mapping[str, Any], *paths: str) -> str:
    for path in paths:
        current: Any = data
        for part in path.split("."):
            if not isinstance(current, Mapping) or part not in current:
                current = None
                break
            current = current.get(part)
        if current is not None:
            text = str(current).strip()
            if text:
                return text
    return ""


def _extract_content(data: Mapping[str, Any]) -> str:
    return _deep_get(
        data,
        "content",
        "text",
        "message",
        "msg",
        "msg_text",
        "speech_text",
        "voice_text",
        "transcript",
        "data.content",
        "data.text",
        "data.message",
        "raw.Content",
        "raw.content",
        "payload.content",
        "payload.text",
    )


def _extract_bool(data: Mapping[str, Any], *paths: str) -> bool:
    value = _deep_get(data, *paths).lower()
    return value in {"1", "true", "yes", "y", "group", "chatroom"}


def _make_message(data: Mapping[str, Any]) -> Any:
    from bot.models import BotMessage, ChatType

    content = _extract_content(data)
    if not content:
        raise HTTPException(status_code=400, detail="missing message content")

    user_id = _deep_get(data, "user_id", "sender", "from_user", "from", "wxid", "data.user_id") or "wechat-user"
    user_name = _deep_get(data, "user_name", "nickname", "sender_name", "data.user_name") or user_id
    chat_id = _deep_get(data, "chat_id", "room_id", "conversation_id", "data.chat_id") or user_id
    is_group = _extract_bool(data, "is_group", "group", "chat_type", "data.is_group") or chat_id.endswith("@chatroom")
    message_id = _deep_get(data, "message_id", "msg_id", "id", "data.message_id") or str(uuid.uuid4())

    return BotMessage(
        platform="wechatbot",
        message_id=message_id,
        user_id=user_id,
        user_name=user_name,
        chat_id=chat_id,
        chat_type=ChatType.GROUP if is_group else ChatType.PRIVATE,
        content=content.strip(),
        raw_content=content.strip(),
        mentioned=_extract_bool(data, "mentioned", "is_at", "at_me", "data.mentioned"),
        timestamp=datetime.now(),
        raw_data=dict(data),
    )


async def _dispatch_bot_message(message: Any) -> Any:
    from bot.dispatcher import get_dispatcher

    return await get_dispatcher().dispatch_async(message)


def _detect_session(text: str) -> str:
    lowered = (text or "").lower()
    if any(token in lowered for token in ("下午", "2点半", "两点半", "14:30", "1430")):
        return "afternoon"
    if any(token in lowered for token in ("上午", "10点半", "十点半", "10:30", "1030", "中午", "午盘")):
        return "morning"
    return "auto"


def _github_rule_repo() -> str:
    return os.getenv("WECHATBOT_GATEWAY_GITHUB_REPO", "yutianyu111602-glitch/daily_stock_analysis").strip()


def _run_github_rule_workflow(rule_text: str) -> str:
    session = _detect_session(rule_text)
    ai_review = os.getenv("WECHATBOT_GATEWAY_GITHUB_AI_REVIEW", "false").strip().lower()
    ai_review = "true" if ai_review in {"1", "true", "yes", "on"} else "false"
    cmd = [
        "gh",
        "workflow",
        "run",
        "rule_screener.yml",
        "--repo",
        _github_rule_repo(),
        "-f",
        f"rule_text={rule_text}",
        "-f",
        f"session={session}",
        "-f",
        f"ai_review={ai_review}",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30, check=False)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "unknown error").strip()
        raise RuntimeError(f"GitHub Actions 触发失败：{detail[:300]}")
    return session


def _check_token(expected_token: str, provided_token: Optional[str]) -> None:
    if expected_token and provided_token != expected_token:
        raise HTTPException(status_code=401, detail="invalid gateway token")


def create_app() -> FastAPI:
    setup_env()
    app = FastAPI(title="Daily Stock Analysis WeChatBot Gateway")

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {"ok": True, "service": "wechatbot-gateway"}

    @app.post("/wechatbot/message", response_model=GatewayResponse)
    async def wechatbot_message(
        request: Request,
        x_gateway_token: Optional[str] = Header(default=None),
    ) -> GatewayResponse:
        expected_token = os.getenv("WECHATBOT_GATEWAY_TOKEN", "").strip()
        _check_token(expected_token, x_gateway_token)

        data = await request.json()
        if not isinstance(data, Mapping):
            raise HTTPException(status_code=400, detail="payload must be a JSON object")

        content = _extract_content(data)
        if not content:
            raise HTTPException(status_code=400, detail="missing message content")
        message_id = _deep_get(data, "message_id", "msg_id", "id", "data.message_id") or str(uuid.uuid4())
        mode = os.getenv("WECHATBOT_GATEWAY_MODE", "dispatcher").strip().lower()
        if content.strip().lower() in {"help", "/help", "帮助", "?"}:
            return GatewayResponse(ok=True, reply=RULE_HELP_TEXT, markdown=False, message_id=message_id)

        if mode == "github_rules":
            try:
                session = _run_github_rule_workflow(content.strip())
            except Exception as exc:
                logger.exception("[WeChatBotGateway] GitHub Actions trigger failed")
                return GatewayResponse(ok=False, reply=str(exc), markdown=False, message_id=message_id)
            return GatewayResponse(
                ok=True,
                reply=f"规则选股任务已提交到 GitHub Actions，session={session}。结果会走已配置的推送渠道。",
                markdown=False,
                message_id=message_id,
            )

        message = _make_message(data)
        logger.info("[WeChatBotGateway] message user=%s content=%s", message.user_id, message.content[:80])

        response = await _dispatch_bot_message(message)
        return GatewayResponse(
            ok=True,
            reply=response.text or "",
            markdown=response.markdown,
            message_id=message.message_id,
        )

    return app


app = create_app()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run WSL2 WeChatBot gateway")
    parser.add_argument("--host", default=os.getenv("WECHATBOT_GATEWAY_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("WECHATBOT_GATEWAY_PORT", "8765")))
    args = parser.parse_args()

    setup_logging(log_prefix="wechatbot_gateway")

    import uvicorn

    uvicorn.run("scripts.wechatbot_gateway:app", host=args.host, port=args.port, reload=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
