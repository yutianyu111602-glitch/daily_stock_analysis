# Daily Stock Analysis Handoff

时间：2026-04-27
工作区：`C:\code\githubstar\daily_stock_analysis`
状态：规则选股、自然语言规则入口、通知推送和 GitHub Actions 自动化为当前主线。

## 一句话接手口径

这是一个覆盖 A 股、港股、美股的 AI 股票分析系统，主链路为数据抓取 -> 技术/资讯/基本面分析 -> LLM 生成报告 -> 多渠道通知，并同时提供 CLI、FastAPI、React Web、Electron 桌面端和 GitHub Actions 自动化入口。

## 先读入口

1. `AGENTS.md`：仓库协作规则、验证矩阵、目录边界和交付要求的唯一真源。
2. `README.md`：用户入门、核心能力、快速开始和部署概览。
3. `docs/full-guide.md`：高级配置、环境变量、数据源、通知和部署细节。
4. `docs/CHANGELOG.md`：版本历史；`[Unreleased]` 段要求扁平条目格式。
5. `.github/workflows/`：GitHub Actions 定时和手动触发入口。

## 当前事实

- 当前仓库是 Git 仓库，主分支 `main`，跟踪 `fork/main`。
- 规则选股主入口是 `scripts/run_rule_screener.py`。
- 收盘综合推送入口是 `scripts/run_close_combo_push.py`。
- 自然语言规则入口是 `scripts/run_nl_rule_screener.py` 和 `bot/commands/rule_screener.py`。
- 规则选股报告会推送候选池，不再执行内部模拟买卖。
- `PortfolioService` 仍保留，用于真实组合/事件/账本 API，不属于内部模拟交易。

## 已移除

- 内部模拟交易链路已移除。
- 不再支持旧的模拟交易开关。
- 不再自动创建模拟账户、自动买入、自动卖出或推送模拟交易报告。

## 注意事项

- 不读取、提交、复制或暴露 `.env`、密钥、token、账号等敏感信息。
- 修改数据源、报告结构、通知链路和调度逻辑前要跑相关测试。
- 修改 AI 协作治理资产时要运行 `python scripts/check_ai_assets.py`。
