# -*- coding: utf-8 -*-
"""
Server酱3 发送提醒服务

职责：
1. 通过 Server酱3 API 发送 Server酱3 消息
"""
import logging
from typing import Optional
import requests
from datetime import datetime
import re
import time

from src.config import Config
from src.formatters import chunk_content_by_max_bytes


logger = logging.getLogger(__name__)


class Serverchan3Sender:
    
    def __init__(self, config: Config):
        """
        初始化 Server酱3 配置

        Args:
            config: 配置对象
        """
        self._serverchan3_sendkey = getattr(config, 'serverchan3_sendkey', None)
        self._serverchan3_max_bytes = getattr(config, 'serverchan3_max_bytes', 50000)
        
    def send_to_serverchan3(self, content: str, title: Optional[str] = None) -> bool:
        """
        推送消息到 Server酱3

        Server酱3 API 格式：
        POST https://sctapi.ftqq.com/{sendkey}.send
        或
        POST https://{num}.push.ft07.com/send/{sendkey}.send
        {
            "title": "消息标题",
            "desp": "消息内容",
            "options": {}
        }

        Server酱3 特点：
        - 国内推送服务，支持多家国产系统推送通道，可无后台推送
        - 简单易用的 API 接口

        Args:
            content: 消息内容（Markdown 格式）
            title: 消息标题（可选）

        Returns:
            是否发送成功
        """
        if not self._serverchan3_sendkey:
            logger.warning("Server酱3 SendKey 未配置，跳过推送")
            return False

        # 处理消息标题
        if title is None:
            date_str = datetime.now().strftime('%Y-%m-%d')
            title = f"📈 股票分析报告 - {date_str}"

        try:
            url = self._build_serverchan3_url()
            content_bytes = len(content.encode('utf-8'))
            if content_bytes > self._serverchan3_max_bytes:
                logger.info(
                    "Server酱3 消息内容超长(%s字节/%s字符)，将分批发送",
                    content_bytes,
                    len(content),
                )
                return self._send_serverchan3_chunked(url, content, title, self._serverchan3_max_bytes)

            return self._send_serverchan3_message(url, content, title)

        except Exception as e:
            logger.error(f"发送 Server酱3 消息失败: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return False

    def _build_serverchan3_url(self) -> str:
        sendkey = self._serverchan3_sendkey
        if sendkey.startswith('sctp'):
            return f"https://{sendkey}.push.ft07.com/send"
        return f"https://sctapi.ftqq.com/{sendkey}.send"

    def _send_serverchan3_message(self, url: str, content: str, title: str) -> bool:
        params = {
            'title': title,
            'desp': content,
            'short': self._build_short_summary(content),
            'options': {}
        }
        headers = {
            'Content-Type': 'application/json;charset=utf-8'
        }
        response = requests.post(url, json=params, headers=headers, timeout=10)

        if response.status_code == 200:
            result = response.json()
            logger.info(f"Server酱3 消息发送成功: {result}")
            return True

        logger.error(f"Server酱3 请求失败: HTTP {response.status_code}")
        logger.error(f"响应内容: {response.text}")
        return False

    def _build_short_summary(self, content: str, limit: int = 120) -> str:
        meaningful_lines = []
        for raw_line in content.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            line = re.sub(r"^[#>\-\*\d\.\)\s]+", "", line).strip()
            if not line:
                continue
            meaningful_lines.append(line)
            if len(meaningful_lines) >= 3:
                break
        summary = " | ".join(meaningful_lines)
        if len(summary) <= limit:
            return summary
        return summary[: max(0, limit - 1)].rstrip() + "…"

    def _send_serverchan3_chunked(self, url: str, content: str, title: str, max_bytes: int) -> bool:
        budget = max(1000, max_bytes - 2000)
        chunks = chunk_content_by_max_bytes(content, budget, add_page_marker=True)
        total_chunks = len(chunks)
        success_count = 0

        logger.info(f"Server酱3 分批发送：共 {total_chunks} 批")

        for i, chunk in enumerate(chunks):
            chunk_title = f"{title} ({i+1}/{total_chunks})" if total_chunks > 1 else title
            if self._send_serverchan3_message(url, chunk, chunk_title):
                success_count += 1
                logger.info(f"Server酱3 第 {i+1}/{total_chunks} 批发送成功")
            else:
                logger.error(f"Server酱3 第 {i+1}/{total_chunks} 批发送失败")

            if i < total_chunks - 1:
                time.sleep(1)

        return success_count == total_chunks

