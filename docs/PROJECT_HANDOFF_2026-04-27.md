# Daily Stock Analysis Detailed Handoff

时间：2026-04-27
工作区：`C:\code\githubstar\daily_stock_analysis`
状态：虚拟盘交易系统已实现并推送；原项目接手文档已更新。

## 一句话接手口径

这是一个覆盖 A 股、港股、美股的 AI 股票分析系统，主链路为数据抓取 -> 技术/资讯/基本面分析 -> LLM 生成报告 -> 多渠道通知，并同时提供 CLI、FastAPI、React Web、Electron 桌面端和 GitHub Actions 自动化入口。本次新增内部虚拟盘交易系统，规则选股后自动买卖并推送到所有已配置通知渠道。

## 先读入口

1. `AGENTS.md`：仓库协作规则、验证矩阵、目录边界和交付要求的唯一真源。
2. `docs/PROJECT_HANDOFF_2026-04-27.md`：本接手文档。
3. `docs/VIRTUAL_TRADING_HANDOFF_2026-04-27.md`：虚拟盘交易系统接手文档（本次新增功能）。
4. `README.md`：用户入门、核心能力、快速开始和部署概览。
5. `docs/full-guide.md`：高级配置、环境变量、数据源、通知和部署细节。
6. `docs/CHANGELOG.md`：版本历史；`[Unreleased]` 段要求扁平条目格式。
7. `review.md`：2026-03-19 的历史问题梳理，仅作背景参考。

## 当前事实

### Confirmed

- 当前仓库是 Git 仓库，分支 `main`，跟踪 `fork/main`。
- 虚拟盘交易系统已实现：`src/services/virtual_trading_service.py`（296行）。
- 测试通过：2 个虚拟盘测试 + 65 个规则选股测试 + 完整非网络测试套件 1575 通过。
- 虚拟盘通过 `VIRTUAL_TRADING_ENABLED=true` 控制，默认关闭，不影响现有流程。
- 本地 `.env` 已设为 `VIRTUAL_TRADING_ENABLED=true`。
- Git commit `055da34` 已推送远端。
- `AGENTS.md` 明确为仓库内 AI 协作规则唯一真源；`CLAUDE.md` 应为指向 `AGENTS.md` 的兼容入口。
- 关键运行入口存在：`main.py`、`server.py`、`api/`、`apps/dsa-web/`、`apps/dsa-desktop/`、`.github/workflows/`。
- 后端 CI 入口为 `scripts/ci_gate.sh`。
- 本地根目录存在 `.env`，接手者不要读取、提交或泄露其中内容。

### Unverified

- 未在实际交易日验证虚拟盘买卖执行（需要交易日 + 有效规则选股结果 + 联网）。
- 未验证 Server酱/Telegram 端到端推送虚拟盘交易报告。
- 未验证长时间多日连续买卖的现金追踪精度。
- 未验证 Web 依赖、lint、build 或 Playwright smoke。
- 未验证 Electron 构建、PyInstaller/后端打包产物或桌面 Release 工作流。

### Blocked

- 当前没有阻塞项。

## 模块状态

### docs-ssot

- 当前事实：`AGENTS.md` 是协作规则 SSOT；现有项目接手报告 + 虚拟盘接手报告。
- 已完成：新增 `docs/VIRTUAL_TRADING_HANDOFF_2026-04-27.md`，更新项目接手文档。
- 风险：无。
- 下一步：后续每次长任务结束时，追加日期化 handoff。

### backend-pipeline

- 当前事实：主 CLI 在 `main.py`；FastAPI 服务入口在 `server.py`。虚拟盘在 `scripts/run_rule_screener.py` 和 `scripts/run_close_combo_push.py` 接入。
- 已完成：虚拟盘买卖、记账、报告生成、通知推送完整闭环。
- 风险：配置、数据源 fallback、报告结构、通知链路和调度属于高风险区。
- 下一步：实际运行验证，观察交易日表现。

### data-providers

- 当前事实：`data_provider/` 负责多数据源适配与 fallback。虚拟盘不直接调用数据源。
- 已完成：本次未改数据源。
- 风险：单一数据源异常不应拖垮整体分析流程。
- 下一步：改数据源时重点验证字段标准化、失败降级。

### tests-verification

- 当前事实：全量非网络测试 1575 通过，1 跳过，1 偶发失败（Windows 文件锁，无关）。
- 已完成：2 个虚拟盘测试 + 回归修复（`test_run_rule_screener.py` mock 补 `buckets`）。
- 风险：无边界测试（空候选池、0 现金、重复 trade_uid）。
- 下一步：需要时补充参数化测试。

## 文件和产物

| Path | Status | Purpose |
| --- | --- | --- |
| `src/services/virtual_trading_service.py` | 新增 | 虚拟盘核心：买卖、记账、报告、通知 |
| `tests/test_virtual_trading_service.py` | 新增 | 2 个单元测试 |
| `src/services/rule_screener_service.py` | 修改 | `RuleScreeningRunResult.buckets` 字段 |
| `scripts/run_rule_screener.py` | 修改 | 虚拟盘接入入口 1 |
| `scripts/run_close_combo_push.py` | 修改 | 虚拟盘接入入口 2 |
| `tests/test_run_rule_screener.py` | 修改 | mock result 补 `buckets=None` |
| `.env.example` | 修改 | 新增 `VIRTUAL_TRADING_ENABLED` |
| `.env` | 修改 | 本地开启虚拟盘 |
| `docs/CHANGELOG.md` | 修改 | `[Unreleased]` 新功能条目 |
| `docs/VIRTUAL_TRADING_HANDOFF_2026-04-27.md` | 新增 | 虚拟盘接手报告 |
| `docs/PROJECT_HANDOFF_2026-04-27.md` | 更新 | 本文件，已补充虚拟盘信息 |

## 禁止动作

- 不读取、提交、复制或暴露 `.env`、密钥、token、账号等敏感信息。
- 不在不理解影响面的情况下修改配置语义、API/Schema、数据源 fallback。
- 不删除 `data/stock_analysis.db`（虚拟盘持仓数据存储在此）。
- 不把 `review.md` 的历史结论直接当作当前主线事实。

## 允许动作

- 读取代码、文档、workflow、测试和配置示例。
- 通过 `VIRTUAL_TRADING_ENABLED=false` 关闭虚拟盘。
- 在明确范围内做最小必要修改，并同步相关文档。

## 验证结果

- Passed: 1575 全量非网络测试（含 2 虚拟盘 + 65 规则选股 + 9 run_rule_screener）
- Passed: py_compile 所有改动文件
- Passed: `python scripts/check_ai_assets.py`
- Failed: 无（预存在的存储测试文件锁问题已标记为无关）
- Not run: 网络相关测试、实际交易日端到端验证

## 下一步

1. 等待下一个交易日，观察规则选股输出 + 虚拟盘自动执行
2. 检查 Server酱 / Telegram 是否收到虚拟盘交易报告
3. 需要时增加涨跌停检查、停牌检查
4. 需要时为 Web 管理后台增加虚拟盘持仓查看页面

## Git / PR 状态

- Branch: `main` (tracking `fork/main`)
- 最新 commit: `055da34` — feat: add internal virtual trading system with auto buy/sell and push
- Push 状态: 已推送到远端
- PR: 无

## 接手者注意

- 本仓库文档有中英双语内容；改中文文档时需评估英文文档是否同步。
- `docs/CHANGELOG.md` 的 `[Unreleased]` 段禁止新增分类标题，只追加扁平条目。
- 修改 AI 协作治理资产时要运行 `python scripts/check_ai_assets.py`。
- Windows 本地执行 shell 脚本可能受环境影响。
- 虚拟盘依赖 `PortfolioService` 准确性，若 Portfolio 账本漂移则虚拟盘持仓漂移。
- `_ensure_account` 通过 `owner_id="virtual-trading"` 查找账户，首次运行自动创建。
