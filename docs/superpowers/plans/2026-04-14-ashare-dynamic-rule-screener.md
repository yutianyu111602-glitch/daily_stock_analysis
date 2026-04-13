# A股规则选股动态调参 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 A 股规则选股日报按市场状态动态调整辅助阈值、分层输出完整命中/动态放宽命中/技术候选池，并在数据缺失或板块不达标时仍然输出有价值的候选名单。

**Architecture:** 在 `src/services/rule_screener_service.py` 内新增“市场状态 -> 动态阈值 -> 分层筛选 -> 数据降级 -> 报告分层”主流程，保持核心战法固定，仅动态调整辅助条件。脚本与 workflow 只负责暴露环境变量、打印诊断和执行入口，不把策略逻辑散落到多个文件里。

**Tech Stack:** Python 3.11+, pandas, Tushare, pytest, GitHub Actions

---

## File Map

- Modify: `src/services/rule_screener_service.py`
  - 新增市场状态判断
  - 新增动态阈值计算
  - 新增三层结果模型
  - 新增空数据降级策略
  - 重构报告输出
- Modify: `scripts/run_rule_screener.py`
  - 输出新的市场状态和分层摘要
- Modify: `.github/workflows/rule_screener.yml`
  - 暴露新的环境变量与调试开关
- Modify: `tests/test_rule_screener_service.py`
  - 覆盖动态调参、分层输出、空数据降级
- Modify: `tests/test_daily_analysis_workflow.py`
  - 覆盖 workflow 默认环境变量
- Modify: `tests/test_run_rule_screener.py`
  - 覆盖脚本环境准备与运行口径

### Task 1: 为规则选股引入分层结果模型

**Files:**
- Modify: `src/services/rule_screener_service.py`
- Test: `tests/test_rule_screener_service.py`

- [ ] **Step 1: 先写失败测试，定义“分层结果”最小结构**

```python
def test_build_screening_report_renders_layered_sections() -> None:
    full_hit = RuleScreeningCandidate(...)
    relaxed_hit = RuleScreeningCandidate(...)
    technical_hit = RuleScreeningCandidate(...)

    report = build_screening_report(
        candidates=[full_hit, relaxed_hit, technical_hit],
        report_date="20260413",
        grouped_candidates={
            "full": [full_hit],
            "relaxed": [relaxed_hit],
            "technical": [technical_hit],
        },
        market_regime_label="弱势日",
        dynamic_adjustments=["板块涨幅阈值 2.0% -> 0.8%"],
    )

    assert "## 完整命中（1 只）" in report
    assert "## 动态放宽命中（1 只）" in report
    assert "## 技术候选池（1 只）" in report
```

- [ ] **Step 2: 运行测试，确认当前实现不支持分层结构**

Run: `pytest tests/test_rule_screener_service.py::RuleScreenerServiceTestCase::test_build_screening_report_renders_layered_sections -v`

Expected: FAIL，提示 `build_screening_report()` 不接受分层参数或报告中无三层章节。

- [ ] **Step 3: 新增结果数据结构**

在 `src/services/rule_screener_service.py` 增加：

```python
@dataclass
class DynamicAdjustment:
    name: str
    from_value: float
    to_value: float
    reason: str


@dataclass
class RuleScreeningBuckets:
    full_hits: List[RuleScreeningCandidate] = field(default_factory=list)
    relaxed_hits: List[RuleScreeningCandidate] = field(default_factory=list)
    technical_pool: List[RuleScreeningCandidate] = field(default_factory=list)
```

- [ ] **Step 4: 重构 `build_screening_report()`，支持三层输出**

关键要求：

```python
def build_screening_report(
    candidates: Sequence[RuleScreeningCandidate],
    report_date: str,
    *,
    grouped_candidates: Optional[Dict[str, Sequence[RuleScreeningCandidate]]] = None,
    market_regime_label: str = "",
    dynamic_adjustments: Optional[Sequence[str]] = None,
    ...
) -> str:
    ...
```

渲染规则：

- `grouped_candidates["full"]` 非空时输出 `## 完整命中`
- `grouped_candidates["relaxed"]` 非空时输出 `## 动态放宽命中`
- `grouped_candidates["technical"]` 非空时输出 `## 技术候选池`
- 三层都为空时，才输出“今日未筛出符合条件的A股股票”

- [ ] **Step 5: 运行本任务测试并修正格式**

Run: `pytest tests/test_rule_screener_service.py -k layered_sections -v`

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/services/rule_screener_service.py tests/test_rule_screener_service.py
git commit -m "feat: add layered A-share screener report buckets"
```

### Task 2: 引入市场状态判断与动态阈值生成

**Files:**
- Modify: `src/services/rule_screener_service.py`
- Test: `tests/test_rule_screener_service.py`

- [ ] **Step 1: 先写失败测试，定义三档市场状态**

```python
def test_build_market_regime_returns_weak_when_breadth_is_poor() -> None:
    snapshot = {
        "index_change": {"sh": -0.8, "sz": -1.1, "cyb": -1.5},
        "up_count": 1200,
        "down_count": 3800,
        "limit_up": 35,
        "limit_down": 22,
        "sector_median": -0.6,
    }
    regime = _classify_market_regime(snapshot)
    assert regime == "weak"
```

- [ ] **Step 2: 运行测试，确认当前没有市场状态逻辑**

Run: `pytest tests/test_rule_screener_service.py::RuleScreenerServiceTestCase::test_build_market_regime_returns_weak_when_breadth_is_poor -v`

Expected: FAIL，提示 `_classify_market_regime` 未定义。

- [ ] **Step 3: 新增市场状态枚举与动态配置函数**

在 `src/services/rule_screener_service.py` 增加：

```python
def _classify_market_regime(snapshot: Dict[str, Any]) -> str:
    ...


def _build_dynamic_rule_config(
    base: AshareRuleConfig,
    market_regime: str,
) -> tuple[AshareRuleConfig, List[DynamicAdjustment]]:
    ...
```

动态规则：

- `strong`: 只允许板块阈值 `2.0 -> 1.5`
- `neutral`: `量比 1.5 -> 1.3`、`换手率 5.0 -> 4.5`、`板块 2.0 -> 1.2`、`乖离率 8.0 -> 8.5`
- `weak`: `量比 1.5 -> 1.2`、`换手率 5.0 -> 4.0`、`板块 2.0 -> 0.8`、`乖离率 8.0 -> 9.0`

- [ ] **Step 4: 为动态阈值补全 3 条测试**

```python
def test_build_dynamic_rule_config_strong_is_conservative() -> None: ...
def test_build_dynamic_rule_config_neutral_relaxes_secondary_thresholds() -> None: ...
def test_build_dynamic_rule_config_weak_relaxes_to_watchlist_floor() -> None: ...
```

- [ ] **Step 5: 运行相关测试**

Run: `pytest tests/test_rule_screener_service.py -k "market_regime or dynamic_rule_config" -v`

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/services/rule_screener_service.py tests/test_rule_screener_service.py
git commit -m "feat: add market regime driven rule config"
```

### Task 3: 重构筛选流程，按 Level 0 / Level 1 / 技术候选池分层

**Files:**
- Modify: `src/services/rule_screener_service.py`
- Test: `tests/test_rule_screener_service.py`

- [ ] **Step 1: 先写失败测试，锁住“技术形态多但最终不应为 0”**

```python
def test_run_uses_technical_pool_when_sector_gate_blocks_everything() -> None:
    service = _build_fake_service(...)
    result = service.run(send_notification=False, ai_review=False)

    assert result.profile_name.endswith("技术候选池")
    assert len(result.candidates) > 0
    assert "因板块强度条件当日未命中" in "\n".join(result.profile_notes)
```

- [ ] **Step 2: 运行测试，确认当前行为会回空**

Run: `pytest tests/test_rule_screener_service.py::RuleScreenerServiceTestCase::test_run_uses_technical_pool_when_sector_gate_blocks_everything -v`

Expected: FAIL 或行为不完整。

- [ ] **Step 3: 把 `run()` 主流程拆成显式三层**

建议拆出辅助函数：

```python
def _run_full_hit_stage(...): ...
def _run_relaxed_stage(...): ...
def _run_technical_pool_stage(...): ...
def _collect_bucketed_candidates(...): ...
```

执行顺序：

1. `Level 0` 原始规则
2. 若过少，按市场状态生成 `Level 1` 动态配置
3. 若仍过少，输出 `Level 2` 技术候选池

产出：

- `RuleScreeningBuckets`
- `profile_notes`
- `market_regime_label`
- `dynamic_adjustment_lines`

- [ ] **Step 4: 明确“哪些票自动入池，哪些不入池”**

规则：

- `full_hits` 自动同步 `STOCK_LIST`
- `relaxed_hits` 自动同步 `STOCK_LIST`
- `technical_pool` 默认不自动同步

代码要求：

```python
stock_pool_sync_candidates = buckets.full_hits + buckets.relaxed_hits
display_candidates = buckets.full_hits + buckets.relaxed_hits + buckets.technical_pool
```

- [ ] **Step 5: AI 复核覆盖最终展示名单，而不是仅前 N 只**

```python
review_codes = [candidate.code for candidate in display_candidates]
```

若要限流，只在配置里控制 `max_review_count`，默认覆盖全部展示名单。

- [ ] **Step 6: 运行本任务测试**

Run: `pytest tests/test_rule_screener_service.py -k "technical_pool or auto_sync or ai_review" -v`

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/services/rule_screener_service.py tests/test_rule_screener_service.py
git commit -m "feat: add staged A-share screener fallback flow"
```

### Task 4: 修复空数据导致的整批清零

**Files:**
- Modify: `src/services/rule_screener_service.py`
- Test: `tests/test_rule_screener_service.py`

- [ ] **Step 1: 先写失败测试，锁住 `daily_basic` 空表不应导致整批归零**

```python
def test_empty_daily_basic_falls_back_to_previous_trade_date_or_unknown_turnover() -> None:
    service = _build_fake_service_with_empty_daily_basic(...)
    turnover = service._load_latest_turnover("20260414")
    assert turnover["_meta"]["source"] in {"previous_trade_date", "unknown"}
```

- [ ] **Step 2: 扩展 `_load_latest_turnover()` 返回更多元信息**

不要只返回 `Dict[str, float]`，改成：

```python
@dataclass
class TurnoverSnapshot:
    values: Dict[str, float]
    source: str
    is_partial: bool = False
```

降级顺序：

1. 当日 `daily_basic`
2. 上一交易日 `daily_basic`
3. 若仍为空，则 `source="unknown"`，允许股票进入技术候选池，但在 notes 中标记

- [ ] **Step 3: 同样处理板块数据缺失**

新增行为：

- `index_member_all`/`sw_daily` 空表时，不直接导致 `sector=0`
- 记录 `sector_data_available=False`
- 在技术候选池层继续输出，并在 notes 中增加 `板块数据缺失，仅供人工判断`

- [ ] **Step 4: 为缓存与降级加测试**

```python
def test_empty_sw_daily_keeps_technical_pool_with_data_warning() -> None: ...
def test_empty_daily_basic_uses_previous_trade_date_cache() -> None: ...
```

- [ ] **Step 5: 运行本任务测试**

Run: `pytest tests/test_rule_screener_service.py -k "daily_basic or sw_daily or cache" -v`

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/services/rule_screener_service.py tests/test_rule_screener_service.py
git commit -m "fix: degrade gracefully when batch market data is missing"
```

### Task 5: 暴露新的脚本与 workflow 配置

**Files:**
- Modify: `scripts/run_rule_screener.py`
- Modify: `.github/workflows/rule_screener.yml`
- Test: `tests/test_daily_analysis_workflow.py`
- Test: `tests/test_run_rule_screener.py`

- [ ] **Step 1: 先写 workflow 失败测试，锁住新环境变量**

```python
def test_rule_screener_workflow_sets_dynamic_mode_defaults() -> None:
    workflow = _load_rule_screener_workflow()
    env = workflow["jobs"]["rule-screener"]["steps"][...]["env"]
    assert env["RULE_SCREENER_DYNAMIC_MODE"] == "true"
    assert env["RULE_SCREENER_ALLOW_EMPTY_REPORT"] == "false"
```

- [ ] **Step 2: 在脚本中增加动态模式环境准备**

示例：

```python
def prepare_rule_screener_env() -> None:
    ...
    os.environ.setdefault("RULE_SCREENER_DYNAMIC_MODE", "true")
    os.environ.setdefault("RULE_SCREENER_ALLOW_EMPTY_REPORT", "false")
```

- [ ] **Step 3: 在 workflow 中加入调试开关与默认值**

建议增加：

```yaml
RULE_SCREENER_DYNAMIC_MODE: "true"
RULE_SCREENER_ALLOW_EMPTY_REPORT: "false"
RULE_SCREENER_DEBUG_SECTOR: "true"
RULE_SCREENER_DEBUG_REGIME: "true"
```

- [ ] **Step 4: 增加脚本日志摘要**

脚本完成时打印：

```python
logger.info(
    "规则选股完成: trade_date=%s, profile=%s, full=%s, relaxed=%s, technical=%s",
    ...
)
```

- [ ] **Step 5: 运行相关测试**

Run: `pytest tests/test_daily_analysis_workflow.py tests/test_run_rule_screener.py -v`

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add scripts/run_rule_screener.py .github/workflows/rule_screener.yml tests/test_daily_analysis_workflow.py tests/test_run_rule_screener.py
git commit -m "chore: wire dynamic rule screener workflow defaults"
```

### Task 6: 端到端回归与历史样本验证

**Files:**
- Modify: `tests/test_rule_screener_service.py`
- Modify: `tests/test_run_rule_screener.py`
- Modify: `src/services/rule_screener_service.py`（仅在必要时修补）

- [ ] **Step 1: 写历史样本回归测试，覆盖 20260413**

```python
def test_historical_20260413_never_returns_empty_when_technical_candidates_exist() -> None:
    result = _replay_trade_date("20260413")
    assert result.profile_name in {"轻度放宽版", "轻度放宽版（技术候选池）"}
    assert len(result.candidates) > 0
```

- [ ] **Step 2: 运行全量测试**

Run: `pytest tests/test_rule_screener_service.py tests/test_daily_analysis_workflow.py tests/test_run_rule_screener.py -q`

Expected: 全绿

- [ ] **Step 3: 运行本地无推送回归**

Run: `python scripts/run_rule_screener.py --no-notify`

Expected:

- 报告出现 `市场状态`
- 报告出现 `动态调参说明`
- 至少出现一个非空章节：`完整命中`、`动态放宽命中`、`技术候选池`

- [ ] **Step 4: 触发远端 workflow 验证**

Run:

```bash
gh workflow run rule_screener.yml --repo yutianyu111602-glitch/daily_stock_analysis
gh run list --workflow rule_screener.yml --repo yutianyu111602-glitch/daily_stock_analysis --limit 1
gh run view <run-id> --repo yutianyu111602-glitch/daily_stock_analysis --log
```

Expected:

- GitHub Actions 报告不再出现“技术形态命中很多，但最终 0 只”
- 若完整命中仍少，至少出现 `技术候选池`
- 只有 `完整命中` / `动态放宽命中` 自动加入自选池

- [ ] **Step 5: Commit**

```bash
git add src/services/rule_screener_service.py tests/test_rule_screener_service.py tests/test_run_rule_screener.py
git commit -m "test: cover dynamic A-share screener end-to-end behavior"
```
