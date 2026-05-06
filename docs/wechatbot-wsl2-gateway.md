# WSL2 WeChatBot 常驻网关

用途：让你爸在微信里发文字或语音转文字后的自然语言规则，由本机 WSL2 常驻服务接收，再复用项目内 `bot.dispatcher` 触发选股、分析和推送。

## 架构

```text
微信 / WeChatBot / OpenClaw
  -> POST http://127.0.0.1:8765/wechatbot/message
  -> scripts/wechatbot_gateway.py
  -> dispatcher 本地命令 或 GitHub Actions 规则选股
  -> 现有 Server酱 / Telegram / 企业微信通知
```

定时任务仍由 GitHub Actions 负责。这个网关只处理“你爸临时发消息询问或触发”的交互。

推荐你当前使用 `WECHATBOT_GATEWAY_MODE=github_rules`：微信临时规则直接触发 GitHub Actions，继续复用 GitHub Secrets、缓存和定时分析环境，WSL2 只负责收消息和提交任务。

## 启动

在 WSL2 中进入项目目录：

```bash
cd /mnt/c/code/githubstar/daily_stock_analysis
python -m venv .venv-wsl
source .venv-wsl/bin/activate
pip install "fastapi>=0.109.0" "uvicorn[standard]>=0.27.0" "python-dotenv>=1.0.0"
python scripts/wechatbot_gateway.py --host 127.0.0.1 --port 8765
```

如果使用 `WECHATBOT_GATEWAY_MODE=dispatcher` 在 WSL 本地直接跑完整分析，再执行 `pip install -r requirements.txt`。推荐先用 `github_rules`，减少 WSL 依赖和本地密钥暴露。

健康检查：

```bash
curl http://127.0.0.1:8765/healthz
```

## OpenClaw / WeChatBot 请求格式

最小 payload：

```json
{
  "text": "选股 量比大于1，换手大于3，行业前五，精选10只",
  "from_user": "dad-wxid",
  "nickname": "dad",
  "chat_id": "dad-wxid"
}
```

语音转文字后也可以用：

```json
{
  "data": {
    "text": "帮我按上午规则筛一下股票",
    "user_id": "dad-wxid",
    "chat_id": "dad-wxid"
  }
}
```

网关会返回：

```json
{
  "ok": true,
  "reply": "规则选股任务已提交...",
  "markdown": true,
  "message_id": "..."
}
```

## 鉴权

建议配置本地 token，避免任何本机其他进程误触发：

```bash
export WECHATBOT_GATEWAY_TOKEN="change-me"
```

请求时带 header：

```text
X-Gateway-Token: change-me
```

## systemd 用户服务

创建 `~/.config/systemd/user/dsa-wechatbot-gateway.service`：

```ini
[Unit]
Description=Daily Stock Analysis WeChatBot Gateway
After=network-online.target

[Service]
Type=simple
WorkingDirectory=/mnt/c/code/githubstar/daily_stock_analysis
Environment=WECHATBOT_GATEWAY_HOST=127.0.0.1
Environment=WECHATBOT_GATEWAY_PORT=8765
Environment=WECHATBOT_GATEWAY_MODE=github_rules
Environment=WECHATBOT_GATEWAY_GITHUB_REPO=yutianyu111602-glitch/daily_stock_analysis
Environment=WECHATBOT_GATEWAY_GITHUB_AI_REVIEW=false
EnvironmentFile=-/home/pc/openclaw-secrets/dsa-wechatbot-gateway.env
ExecStart=/mnt/c/code/githubstar/daily_stock_analysis/.venv-wsl/bin/python scripts/wechatbot_gateway.py
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
```

启动：

```bash
systemctl --user daemon-reload
systemctl --user enable --now dsa-wechatbot-gateway.service
systemctl --user status dsa-wechatbot-gateway.service
```

日志：

```bash
journalctl --user -u dsa-wechatbot-gateway.service -f
```
