from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from math import floor
from typing import Any, Optional, Sequence

from data_provider.base import normalize_stock_code
from src.services.portfolio_service import PortfolioConflictError, PortfolioService
from src.services.rule_screener_service import RuleScreeningBuckets, RuleScreeningCandidate


@dataclass
class VirtualTradingConfig:
    enabled: bool = False
    account_name: str = "AI虚拟盘"
    owner_id: str = "virtual-trading"
    initial_cash: float = 100000.0
    max_positions: int = 5
    full_position_pct: float = 0.20
    relaxed_position_pct: float = 0.10
    lot_size: int = 100
    fee_rate: float = 0.0003
    min_fee: float = 5.0
    sell_tax_rate: float = 0.001
    sell_when_rule_miss: bool = True


@dataclass
class VirtualTradingRunResult:
    enabled: bool
    executed: bool
    account_id: Optional[int]
    report: str
    trades: list[dict[str, Any]] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)


class VirtualTradingService:
    """Internal paper-trading executor backed by the existing portfolio ledger."""

    def __init__(
        self,
        *,
        portfolio_service: Optional[PortfolioService] = None,
        notifier: Optional[Any] = None,
        config: Optional[VirtualTradingConfig] = None,
    ) -> None:
        self.portfolio_service = portfolio_service or PortfolioService()
        self.notifier = notifier
        self.config = config or VirtualTradingConfig()

    def execute_from_screening_buckets(
        self,
        *,
        buckets: RuleScreeningBuckets,
        trade_date: str | date,
        send_notification: bool = True,
    ) -> VirtualTradingRunResult:
        if not self.config.enabled:
            return VirtualTradingRunResult(
                enabled=False,
                executed=False,
                account_id=None,
                report="虚拟盘未启用。",
            )

        as_of = _coerce_date(trade_date)
        account_id = self._ensure_account(as_of)
        snapshot = self.portfolio_service.get_portfolio_snapshot(account_id=account_id, as_of=as_of)
        account_snapshot = (snapshot.get("accounts") or [{}])[0]
        cash = float(account_snapshot.get("total_cash") or snapshot.get("total_cash") or 0.0)
        positions = _positions_by_symbol(account_snapshot.get("positions") or [])

        trades: list[dict[str, Any]] = []
        skipped: list[str] = []
        tradable_candidates = list(buckets.full_hits) + list(buckets.relaxed_hits)
        observation_codes = {candidate.code for candidate in list(buckets.technical_pool) + list(buckets.manual_review_pool)}
        tradable_codes = {candidate.code for candidate in tradable_candidates}
        valid_codes = tradable_codes | observation_codes

        if self.config.sell_when_rule_miss:
            for symbol, position in list(positions.items()):
                if symbol in valid_codes:
                    continue
                quantity = float(position.get("quantity") or 0.0)
                price = float(position.get("last_price") or position.get("avg_cost") or 0.0)
                if quantity <= 0 or price <= 0:
                    skipped.append(f"{symbol} 持仓缺少可用数量或价格，跳过卖出。")
                    continue
                trade = self._record_trade(
                    account_id=account_id,
                    symbol=symbol,
                    trade_date=as_of,
                    side="sell",
                    quantity=quantity,
                    price=price,
                    reason="规则失效卖出：不再属于完整/放宽命中或技术观察池。",
                )
                if trade:
                    trades.append(trade)
                    cash += quantity * price - float(trade["fee"]) - float(trade["tax"])
                    positions.pop(symbol, None)

        for candidate in tradable_candidates:
            symbol = normalize_stock_code(candidate.code)
            if symbol in positions:
                skipped.append(f"{symbol} 已持仓，今日不重复买入。")
                continue
            if len(positions) >= self.config.max_positions:
                skipped.append(f"{symbol} 超过最多 {self.config.max_positions} 只持仓限制，跳过买入。")
                continue
            pct = self.config.full_position_pct if candidate in buckets.full_hits else self.config.relaxed_position_pct
            budget = min(self.config.initial_cash * pct, cash)
            quantity = self._calc_lot_quantity(budget=budget, price=float(candidate.close))
            if quantity <= 0:
                skipped.append(f"{symbol} 可用现金不足一手，跳过买入。")
                continue
            amount = quantity * float(candidate.close)
            fee = self._calc_fee(amount)
            if amount + fee > cash:
                quantity = self._calc_lot_quantity(budget=max(cash - fee, 0.0), price=float(candidate.close))
                amount = quantity * float(candidate.close)
                fee = self._calc_fee(amount)
            if quantity <= 0 or amount + fee > cash:
                skipped.append(f"{symbol} 扣除费用后现金不足，跳过买入。")
                continue
            tier = "完整命中" if candidate in buckets.full_hits else "动态放宽命中"
            trade = self._record_trade(
                account_id=account_id,
                symbol=symbol,
                trade_date=as_of,
                side="buy",
                quantity=quantity,
                price=float(candidate.close),
                reason=f"{tier}买入：你爸规则命中，AI复核结果随规则选股报告同步查看。",
            )
            if trade:
                trade["name"] = candidate.name
                trade["tier"] = tier
                trades.append(trade)
                cash -= amount + fee
                positions[symbol] = {"symbol": symbol, "quantity": quantity, "last_price": candidate.close}

        for candidate in list(buckets.technical_pool) + list(buckets.manual_review_pool):
            skipped.append(f"{candidate.code} {candidate.name}：技术/人工候选，观察不买。")

        report = self._build_report(
            trade_date=as_of,
            trades=trades,
            skipped=skipped,
            remaining_cash=cash,
        )
        if send_notification and self.notifier is not None:
            self.notifier.send(report)

        return VirtualTradingRunResult(
            enabled=True,
            executed=bool(trades or skipped),
            account_id=account_id,
            report=report,
            trades=trades,
            skipped=skipped,
        )

    def _ensure_account(self, as_of: date) -> int:
        for account in self.portfolio_service.list_accounts(include_inactive=False):
            if account.get("owner_id") == self.config.owner_id or account.get("name") == self.config.account_name:
                return int(account["id"])

        account = self.portfolio_service.create_account(
            name=self.config.account_name,
            broker="virtual",
            market="cn",
            base_currency="CNY",
            owner_id=self.config.owner_id,
        )
        account_id = int(account["id"])
        self.portfolio_service.record_cash_ledger(
            account_id=account_id,
            event_date=as_of,
            direction="in",
            amount=float(self.config.initial_cash),
            currency="CNY",
            note="虚拟盘初始资金",
        )
        return account_id

    def _record_trade(
        self,
        *,
        account_id: int,
        symbol: str,
        trade_date: date,
        side: str,
        quantity: float,
        price: float,
        reason: str,
    ) -> Optional[dict[str, Any]]:
        amount = quantity * price
        fee = self._calc_fee(amount)
        tax = amount * self.config.sell_tax_rate if side == "sell" else 0.0
        trade_uid = f"virtual-{trade_date.isoformat()}-{side}-{symbol}"
        try:
            row = self.portfolio_service.record_trade(
                account_id=account_id,
                symbol=symbol,
                trade_date=trade_date,
                side=side,
                quantity=quantity,
                price=price,
                fee=fee,
                tax=tax,
                market="cn",
                currency="CNY",
                trade_uid=trade_uid,
                note=reason,
            )
        except PortfolioConflictError:
            return None
        return {
            "id": row.get("id"),
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "price": price,
            "amount": amount,
            "fee": fee,
            "tax": tax,
            "reason": reason,
        }

    def _calc_lot_quantity(self, *, budget: float, price: float) -> int:
        if budget <= 0 or price <= 0:
            return 0
        lots = floor((budget / price) / self.config.lot_size)
        return int(lots * self.config.lot_size)

    def _calc_fee(self, amount: float) -> float:
        if amount <= 0:
            return 0.0
        return round(max(amount * self.config.fee_rate, self.config.min_fee), 4)

    def _build_report(
        self,
        *,
        trade_date: date,
        trades: Sequence[dict[str, Any]],
        skipped: Sequence[str],
        remaining_cash: float,
    ) -> str:
        lines = [
            f"# AI虚拟盘交易报告（{trade_date.isoformat()}）",
            "",
            f"- 初始资金：{self.config.initial_cash:.2f} CNY",
            f"- 预估剩余现金：{remaining_cash:.2f} CNY",
            f"- 今日成交：{len(trades)} 笔",
            "",
            "## 成交明细",
        ]
        if trades:
            for trade in trades:
                side_label = "买入" if trade["side"] == "buy" else "卖出"
                lines.append(
                    f"- {side_label} {trade['symbol']} {trade.get('name', '')} "
                    f"{trade['quantity']:.0f} 股 @ {trade['price']:.2f}，"
                    f"金额 {trade['amount']:.2f}，原因：{trade['reason']}"
                )
        else:
            lines.append("- 今日无成交。")

        lines.extend(["", "## 跳过/观察"])
        if skipped:
            lines.extend(f"- {item}" for item in skipped)
        else:
            lines.append("- 无。")
        lines.extend(["", "仅为内部虚拟盘记录，不构成投资建议。"])
        return "\n".join(lines)


def _coerce_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").date()
    return date.fromisoformat(text)


def _positions_by_symbol(positions: Sequence[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for position in positions:
        symbol = normalize_stock_code(str(position.get("symbol") or ""))
        if symbol:
            result[symbol] = dict(position)
    return result
