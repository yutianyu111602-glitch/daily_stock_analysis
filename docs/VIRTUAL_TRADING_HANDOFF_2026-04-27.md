# 虚拟盘交易系统 — 详细接手报告

时间：2026-04-27
工作区：`C:\code\githubstar\daily_stock_analysis`
状态：实现完成，测试通过，已推送到远端。

## 一句话接手口径

内部虚拟盘交易系统已上线：规则选股后自动买卖（完整命中 20% 仓位、放宽命中 10%），规则失效卖出，复用 PortfolioService 记账，交易报告通过现有通知渠道（Server酱/Telegram/企微等）单独推送。`VIRTUAL_TRADING_ENABLED=true` 开启。

## 先读入口

1. `AGENTS.md` — 仓库协作规则 SSOT
2. `docs/PROJECT_HANDOFF_2026-04-27.md` — 项目级接手文档
3. `docs/VIRTUAL_TRADING_HANDOFF_2026-04-27.md` — 本报告
4. `docs/superpowers/specs/2026-04-14-ashare-dynamic-rule-screener-design.md` — 父亲战法设计文档

## 当前事实

### Confirmed

- 核心服务 `src/services/virtual_trading_service.py`（296 行）：纯 Python，无外部依赖，复用 `PortfolioService` 记账
- 测试 `tests/test_virtual_trading_service.py`（139 行）：2 个测试，覆盖买入和卖出核心路径
- 修改点：
  - `src/services/rule_screener_service.py`：`RuleScreeningRunResult` 新增 `buckets: Optional[RuleScreeningBuckets] = None`，`run()` 返回时填充 `buckets=grouped_candidates`
  - `scripts/run_rule_screener.py`：虚拟盘接入，`VIRTUAL_TRADING_ENABLED=true` 时执行
  - `scripts/run_close_combo_push.py`：同上
  - `tests/test_run_rule_screener.py`：mock result 补 `buckets=None`
  - `.env.example`：新增 `VIRTUAL_TRADING_ENABLED=false`
  - `.env`：本地已设 `VIRTUAL_TRADING_ENABLED=true`
  - `docs/CHANGELOG.md`：`[Unreleased]` 新增条目
- 测试结果：77 个直接相关 + 1575 个全量非网络测试通过
- Git commit `055da34` 已推送

### Unverified

- 未在实际行情日验证虚拟盘买卖（需要交易日 + 有效规则选股结果 + 联网）
- 未验证长时间运行后 PortfolioService 快照与实际持仓的一致性
- 未验证多日连续买卖后的现金追踪精度（浮点累计误差）
- 未验证 Server酱/Telegram 推送在实际运行中是否能收到虚拟盘交易报告
- `test_storage.py::test_save_daily_data_sqlite_concurrent` 偶发失败（Windows 文件锁，与本次改动无关）

### Blocked

- 无阻塞项。后续如有需求，可增加：Web UI 查看虚拟盘持仓、止损逻辑、ATR 仓位管理。

## 模块状态

### backend-pipeline

- 当前事实：
  - 虚拟盘在两个入口接入：
    1. `scripts/run_rule_screener.py` — 规则选股独立运行后
    2. `scripts/run_close_combo_push.py` — 收盘综合推送链路中
  - 两个入口都通过 `os.getenv("VIRTUAL_TRADING_ENABLED")` 控制，默认关闭
  - 虚拟盘通知使用独立的 `NotificationService()`（`run_rule_screener.py`）或复用已有实例（`run_close_combo_push.py`）
- 已完成：核心买卖逻辑、记账、报告生成、通知推送
- 风险：
  - **现金追踪不是持久化的**：`execute_from_screening_buckets` 中用局部变量 `cash` 追踪，卖出的金额会加回。但下次运行重新从 `PortfolioService.get_portfolio_snapshot()` 读取，依赖 PortfolioService 的账本准确性
  - **买入顺序依赖**：`tradable_candidates` 中先遍历 `full_hits` 再 `relaxed_hits`，若资金有限，完整命中优先
  - **trade_uid 去重**：格式 `virtual-{date}-{side}-{symbol}`，依赖 PortfolioService 的 `PortfolioConflictError` 去重
- 下一步：
  - 若需要保留历史现金余额，考虑将现金写入 CashLedger
  - 可增加 `--no-virtual-trading` CLI 参数供临时跳过

### data-providers

- 当前事实：虚拟盘不直接调用数据源，所有价格来自 `RuleScreeningCandidate.close`
- 已完成：通过 bucketed candidates 驱动
- 风险：若某股票次日停牌/涨跌停无成交，虚拟盘仍会以 `close` 价买入——实际不可买但虚拟盘无感知
- 下一步：可选增加涨跌停检查、停牌检查

### tests-verification

- 当前事实：
  - 2 个虚拟盘服务测试，使用 `FakePortfolioService`（模拟 5 个方法）
  - 1 个回归修复（`test_run_rule_screener.py` mock 补 `buckets` 字段）
  - 全量非网络测试：1575 通过，1 跳过，1 偶发失败（Windows 文件锁，无关）
- 已完成：核心买卖行为已锁定
- 风险：无边界测试（空候选池、0 现金、负价格、重复 trade_uid 冲突等）
- 下一步：若有复杂场景需求，可补充参数化测试

### docs-ssot

- 当前事实：
  - `AGENTS.md` 是 AI 协作 SSOT
  - 新增 `docs/VIRTUAL_TRADING_HANDOFF_2026-04-27.md`
  - `docs/CHANGELOG.md` 已更新 `[Unreleased]`
  - `.env.example` 已增加 `VIRTUAL_TRADING_ENABLED` 说明
- 已完成：文档同步
- 风险：无
- 下一步：`docs/PROJECT_HANDOFF_2026-04-27.md` 建议在下次更新时补充虚拟盘条目

## 文件和产物

| Path | Status | Purpose |
| --- | --- | --- |
| `src/services/virtual_trading_service.py` | **新增** | 虚拟盘核心：买卖、记账、报告、通知 |
| `tests/test_virtual_trading_service.py` | **新增** | 2 个单元测试 |
| `src/services/rule_screener_service.py` | **修改** | `RuleScreeningRunResult.buckets` 字段 |
| `scripts/run_rule_screener.py` | **修改** | 虚拟盘接入入口 1 |
| `scripts/run_close_combo_push.py` | **修改** | 虚拟盘接入入口 2 |
| `tests/test_run_rule_screener.py` | **修改** | mock result 补 `buckets=None` |
| `.env.example` | **修改** | 新增 `VIRTUAL_TRADING_ENABLED` |
| `.env` | **修改** | 本地开启虚拟盘 |
| `docs/CHANGELOG.md` | **修改** | `[Unreleased]` 新功能条目 |
| `docs/VIRTUAL_TRADING_HANDOFF_2026-04-27.md` | **新增** | 本接手报告 |

## 禁止动作

- 不要在 `.env` 中泄露密钥、token 或敏感信息
- 不要在交易日盘中手动删除 `data/stock_analysis.db`（虚拟盘持仓数据存储在此）
- 不要同时运行多个虚拟盘实例（同一个 account_id 可能产生重复交易记录）
- 不要修改 `trade_uid` 格式，除非同步更新 PortfolioService 的去重逻辑

## 允许动作

- 通过 `VIRTUAL_TRADING_ENABLED=false` 关闭虚拟盘
- 修改 `VirtualTradingConfig` 中的仓位比例、最大持仓数、费率
- 在 `FakePortfolioService` 基础上增加测试覆盖
- 阅读 `PortfolioService` 相关代码了解记账细节

## 验证结果

- Passed: 77 个直接相关测试（2 virtual_trading + 9 run_rule_screener + 1 run_close_combo_push + 65 rule_screener_service）
- Passed: 1575 个全量非网络测试
- Passed: py_compile 所有改动文件
- Passed: `python scripts/check_ai_assets.py`
- Failed: 无
- Not run: 网络相关测试、实际行情日测试、Server酱/Telegram 推送端到端验证

## 下一步

1. 等待下一个交易日，观察规则选股输出，确认虚拟盘是否自动执行
2. 检查 Server酱 和 Telegram 是否收到虚拟盘交易报告
3. 如有必要，增加涨跌停检查和停牌检查
4. 如有必要，为 Web 管理后台增加虚拟盘持仓查看页面

## Git / PR 状态

- Branch: `main` (tracking `fork/main`)
- 最新 commit: `055da34` — feat: add internal virtual trading system with auto buy/sell and push
- Push 状态: 已推送到 `https://github.com/yutianyu111602-glitch/daily_stock_analysis.git`
- PR: 无

## 接手者注意

- 虚拟盘完全依赖 `PortfolioService` 的准确性——如果 PortfolioService 的 `get_portfolio_snapshot` 返回的数据与真实交易记录不同步，虚拟盘持仓会漂移
- `_ensure_account` 通过 `owner_id="virtual-trading"` 查找账户；首次运行自动创建并注入 100000 CNY
- 虚拟盘报告单独推送，不与规则选股报告合并——这是设计选择，避免报告过长
- 交易费用估算：佣金 0.03%（最低 5 元），卖出印花税 0.1%（仅 A 股卖出单向征收）
