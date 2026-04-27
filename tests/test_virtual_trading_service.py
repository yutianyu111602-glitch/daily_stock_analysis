from __future__ import annotations

from datetime import date

from src.services.rule_screener_service import RuleScreeningBuckets, RuleScreeningCandidate
from src.services.virtual_trading_service import VirtualTradingConfig, VirtualTradingService


def _candidate(code: str, *, name: str = "样本股票", close: float = 21.7) -> RuleScreeningCandidate:
    return RuleScreeningCandidate(
        code=code,
        name=name,
        close=close,
        change_pct=3.2,
        ma5=20.8,
        ma10=20.1,
        ma20=19.4,
        bias_ma5_pct=4.3,
        volume_ratio=1.8,
        turnover_rate=6.2,
        sector_name="样本板块",
        sector_change_pct=2.6,
        prior_rise_pct=31.0,
        abc_pattern_confirmed=True,
    )


class FakePortfolioService:
    def __init__(self, *, cash: float = 100000.0, positions: list[dict] | None = None):
        self.accounts: list[dict] = []
        self.cash = cash
        self.positions = positions or []
        self.cash_events: list[dict] = []
        self.trades: list[dict] = []

    def list_accounts(self, include_inactive: bool = False) -> list[dict]:
        return list(self.accounts)

    def create_account(self, **kwargs) -> dict:
        account = {
            "id": 1,
            "name": kwargs["name"],
            "broker": kwargs.get("broker"),
            "market": kwargs["market"],
            "base_currency": kwargs["base_currency"],
            "owner_id": kwargs.get("owner_id"),
        }
        self.accounts.append(account)
        return account

    def record_cash_ledger(self, **kwargs) -> dict:
        self.cash_events.append(kwargs)
        self.cash += float(kwargs["amount"])
        return {"id": len(self.cash_events)}

    def get_portfolio_snapshot(self, **kwargs) -> dict:
        return {
            "total_cash": self.cash,
            "accounts": [
                {
                    "account_id": kwargs["account_id"],
                    "total_cash": self.cash,
                    "positions": list(self.positions),
                }
            ],
        }

    def record_trade(self, **kwargs) -> dict:
        self.trades.append(kwargs)
        amount = float(kwargs["quantity"]) * float(kwargs["price"])
        if kwargs["side"] == "buy":
            self.cash -= amount + float(kwargs.get("fee") or 0) + float(kwargs.get("tax") or 0)
        else:
            self.cash += amount - float(kwargs.get("fee") or 0) - float(kwargs.get("tax") or 0)
        return {"id": len(self.trades)}


def test_virtual_trading_buys_full_and_relaxed_hits_but_skips_technical_pool() -> None:
    portfolio = FakePortfolioService()
    service = VirtualTradingService(
        portfolio_service=portfolio,
        config=VirtualTradingConfig(enabled=True, initial_cash=100000.0),
    )
    buckets = RuleScreeningBuckets(
        full_hits=[_candidate("300001", name="完整命中", close=20.0)],
        relaxed_hits=[_candidate("300002", name="放宽命中", close=10.0)],
        technical_pool=[_candidate("300003", name="技术候选", close=5.0)],
    )

    result = service.execute_from_screening_buckets(
        buckets=buckets,
        trade_date="20260413",
        send_notification=False,
    )

    assert result.executed
    assert [trade["symbol"] for trade in portfolio.trades] == ["300001", "300002"]
    assert all(trade["side"] == "buy" for trade in portfolio.trades)
    assert "技术候选" in result.report
    assert "观察不买" in result.report


def test_virtual_trading_sells_position_when_father_rule_is_lost() -> None:
    portfolio = FakePortfolioService(
        cash=30000.0,
        positions=[
            {
                "symbol": "300009",
                "quantity": 1000,
                "last_price": 12.5,
            }
        ],
    )
    portfolio.accounts.append(
        {
            "id": 1,
            "name": "AI虚拟盘",
            "broker": "virtual",
            "market": "cn",
            "base_currency": "CNY",
            "owner_id": "virtual-trading",
        }
    )
    service = VirtualTradingService(
        portfolio_service=portfolio,
        config=VirtualTradingConfig(enabled=True, sell_when_rule_miss=True),
    )

    result = service.execute_from_screening_buckets(
        buckets=RuleScreeningBuckets(full_hits=[_candidate("300001", close=20.0)]),
        trade_date=date(2026, 4, 13),
        send_notification=False,
    )

    sell_trades = [trade for trade in portfolio.trades if trade["side"] == "sell"]
    assert len(sell_trades) == 1
    assert sell_trades[0]["symbol"] == "300009"
    assert sell_trades[0]["quantity"] == 1000
    assert "规则失效卖出" in result.report
