from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union

import pandas as pd
import requests

from data_provider.base import is_st_stock, normalize_stock_code
from src.config import Config, get_config
from src.core.trading_calendar import get_effective_trading_date
from src.report_language import (
    localize_decision_display_advice,
    normalize_decision_style,
    normalize_report_language,
)

if TYPE_CHECKING:
    from data_provider import DataFetcherManager
    from data_provider.tushare_fetcher import TushareFetcher
    from src.notification import NotificationService


logger = logging.getLogger(__name__)


@dataclass
class AshareRuleConfig:
    lookback_days: int = 60
    abc_window_days: int = 20
    min_prior_rise_pct: float = 20.0
    min_volume_ratio: float = 1.0
    min_turnover_rate: float = 3.0
    min_sector_change_pct: float = 1.0
    max_bias_ma5_pct: float = 9.0
    ai_review_limit: int = 12
    sector_rank_top_n: int = 5
    notify_when_empty: bool = True
    exclude_st: bool = True
    allow_open_data_fallback: bool = False
    auto_relax_if_empty: bool = True
    abc_min_pullback_pct: float = 5.0
    abc_min_rebound_pct: float = 3.0
    abc_min_c_leg_pct: float = 2.0
    abc_min_c_retention_ratio: float = 0.90
    abc_rebreak_buffer_pct: float = 0.0


@dataclass
class RuleScreeningCandidate:
    code: str
    name: str
    close: float
    change_pct: float
    ma5: float
    ma10: float
    ma20: float
    bias_ma5_pct: float
    volume_ratio: float
    turnover_rate: float
    sector_name: str
    sector_change_pct: float
    prior_rise_pct: float
    abc_pattern_confirmed: bool
    sector_rank: int = 0
    abc_a_low_price: float = 0.0
    abc_b_high_price: float = 0.0
    abc_c_low_price: float = 0.0
    abc_b_high_ma20: float = 0.0
    capital_flow_known: bool = False
    super_large_net_inflow: float = 0.0
    large_net_inflow: float = 0.0
    medium_net_inflow: float = 0.0
    father_priority_score: float = 0.0
    matched_condition_count: int = 0
    total_condition_count: int = 0
    failed_conditions: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


@dataclass
class ABCPatternMatch:
    confirmed: bool
    prior_rise_pct: float
    a_low_price: float = 0.0
    b_high_price: float = 0.0
    c_low_price: float = 0.0
    b_high_ma20: float = 0.0
    c_low_higher_than_a_low: bool = False
    b_high_above_ma20: bool = False


@dataclass
class DynamicAdjustment:
    name: str
    from_value: float
    to_value: float
    reason: str

    def to_report_line(self) -> str:
        line = f"{self.name}：{_format_adjustment_value(self.from_value)} -> {_format_adjustment_value(self.to_value)}"
        if self.reason:
            line = f"{line}（{self.reason}）"
        return line


@dataclass
class RuleScreeningBuckets:
    full_hits: List[RuleScreeningCandidate] = field(default_factory=list)
    relaxed_hits: List[RuleScreeningCandidate] = field(default_factory=list)
    technical_pool: List[RuleScreeningCandidate] = field(default_factory=list)
    manual_review_pool: List[RuleScreeningCandidate] = field(default_factory=list)

    def is_empty(self) -> bool:
        return not (self.full_hits or self.relaxed_hits or self.technical_pool or self.manual_review_pool)

    @property
    def full_matches(self) -> List[RuleScreeningCandidate]:
        return self.full_hits

    @property
    def relaxed_matches(self) -> List[RuleScreeningCandidate]:
        return self.relaxed_hits

    @property
    def technical_candidates(self) -> List[RuleScreeningCandidate]:
        return self.technical_pool

    @property
    def manual_review_candidates(self) -> List[RuleScreeningCandidate]:
        return self.manual_review_pool


@dataclass
class TurnoverSnapshot:
    turnover_by_code: Dict[str, float] = field(default_factory=dict)
    source: str = "daily_basic"
    is_partial: bool = False
    notes: List[str] = field(default_factory=list)


@dataclass
class CapitalFlowSnapshot:
    flow_by_code: Dict[str, Dict[str, float]] = field(default_factory=dict)
    source: str = "moneyflow"
    is_partial: bool = False
    notes: List[str] = field(default_factory=list)


@dataclass
class SectorSnapshotLoadResult:
    snapshot: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    source: str = "tushare"
    is_partial: bool = False
    notes: List[str] = field(default_factory=list)


@dataclass
class RuleScreeningRunResult:
    trade_date: str
    candidates: List[RuleScreeningCandidate]
    ai_review_lines: List[str]
    report: str
    profile_name: str
    profile_notes: List[str]
    stock_pool_notes: List[str]


@dataclass
class RuleScreeningStageResult:
    config: AshareRuleConfig
    technical_candidate_codes: List[str] = field(default_factory=list)
    sector_snapshot: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    candidates: List[RuleScreeningCandidate] = field(default_factory=list)
    data_notes: List[str] = field(default_factory=list)


def _coerce_turnover_snapshot(
    latest_turnover: Optional[Union[Dict[str, float], TurnoverSnapshot]],
) -> TurnoverSnapshot:
    if isinstance(latest_turnover, TurnoverSnapshot):
        return latest_turnover
    if not latest_turnover:
        return TurnoverSnapshot(source="unknown", is_partial=True)

    turnover_by_code: Dict[str, float] = {}
    for code, value in latest_turnover.items():
        if value is None or pd.isna(value):
            continue
        turnover_by_code[normalize_stock_code(code)] = float(value)
    return TurnoverSnapshot(turnover_by_code=turnover_by_code)


def _coerce_capital_flow_snapshot(
    capital_flow_snapshot: Optional[Union[Dict[str, Dict[str, float]], CapitalFlowSnapshot]],
) -> CapitalFlowSnapshot:
    if isinstance(capital_flow_snapshot, CapitalFlowSnapshot):
        return capital_flow_snapshot
    if not capital_flow_snapshot:
        return CapitalFlowSnapshot(source="unknown", is_partial=True)

    normalized: Dict[str, Dict[str, float]] = {}
    for code, payload in capital_flow_snapshot.items():
        if not isinstance(payload, dict):
            continue
        normalized_code = normalize_stock_code(code)
        normalized[normalized_code] = {
            "super_large_net_inflow": float(payload.get("super_large_net_inflow") or 0.0),
            "large_net_inflow": float(payload.get("large_net_inflow") or 0.0),
            "medium_net_inflow": float(payload.get("medium_net_inflow") or 0.0),
        }
    return CapitalFlowSnapshot(flow_by_code=normalized)


def _coerce_sector_snapshot_result(
    sector_snapshot: Union[Dict[str, List[Dict[str, Any]]], SectorSnapshotLoadResult],
    candidate_codes: Sequence[str],
) -> SectorSnapshotLoadResult:
    if isinstance(sector_snapshot, SectorSnapshotLoadResult):
        return sector_snapshot

    normalized_snapshot = {
        normalize_stock_code(code): list((sector_snapshot or {}).get(normalize_stock_code(code), []) or [])
        for code in candidate_codes
    }
    return SectorSnapshotLoadResult(snapshot=normalized_snapshot)


def _append_unique_notes(target: List[str], notes: Optional[Sequence[str]]) -> None:
    for note in notes or []:
        if note and note not in target:
            target.append(str(note))


def _normalize_sector_name(name: str) -> str:
    return "".join(ch for ch in str(name).strip().lower() if ch.isalnum() or "\u4e00" <= ch <= "\u9fff")


def _sector_name_matches(left: str, right: str) -> bool:
    left_n = _normalize_sector_name(left)
    right_n = _normalize_sector_name(right)
    return bool(left_n and right_n and (left_n == right_n or left_n in right_n or right_n in left_n))


def _prepare_indicator_frame(daily_history: pd.DataFrame) -> pd.DataFrame:
    if daily_history.empty:
        return daily_history.copy()

    df = daily_history.copy()
    if "code" not in df.columns:
        raise ValueError("daily_history must include code column")

    df["trade_date"] = df["trade_date"].astype(str)
    df["code"] = df["code"].astype(str).map(normalize_stock_code)
    df = df.sort_values(["code", "trade_date"]).reset_index(drop=True)

    for col in ("close", "open", "high", "low", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    grouped = df.groupby("code", group_keys=False)
    df["ma5"] = grouped["close"].transform(lambda s: s.rolling(5, min_periods=5).mean())
    df["ma10"] = grouped["close"].transform(lambda s: s.rolling(10, min_periods=10).mean())
    df["ma20"] = grouped["close"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    df["avg_volume_5"] = grouped["volume"].transform(lambda s: s.rolling(5, min_periods=5).mean().shift(1))
    df["volume_ratio"] = (df["volume"] / df["avg_volume_5"]).replace([pd.NA, pd.NaT], None)
    df["bias_ma5_pct"] = ((df["close"] - df["ma5"]) / df["ma5"] * 100.0).abs()
    df["pct_chg"] = grouped["close"].transform(lambda s: s.pct_change() * 100.0).fillna(0.0)
    return df


def _compute_prior_rise_pct(closes: Sequence[float], peak_index: int) -> float:
    if peak_index <= 0:
        return 0.0
    history = list(closes[: peak_index + 1])
    if not history:
        return 0.0
    floor_price = min(history)
    peak_price = history[peak_index]
    if floor_price <= 0:
        return 0.0
    return (peak_price / floor_price - 1.0) * 100.0


def _detect_abc_pattern(
    closes: Sequence[float],
    *,
    ma20_values: Optional[Sequence[float]] = None,
    abc_window_days: int,
    min_pullback_pct: float = 5.0,
    min_rebound_pct: float = 3.0,
    min_c_leg_pct: float = 2.0,
    min_c_retention_ratio: float = 0.90,
    rebreak_buffer_pct: float = 0.0,
) -> ABCPatternMatch:
    values = list(float(v) for v in closes if pd.notna(v))
    if ma20_values is None:
        ma20_series = [float("nan")] * len(values)
    else:
        ma20_series = [
            float(v) if v is not None and pd.notna(v) else float("nan")
            for v in list(ma20_values)[: len(values)]
        ]
        if len(ma20_series) < len(values):
            ma20_series.extend([float("nan")] * (len(values) - len(ma20_series)))
    if len(values) < max(abc_window_days, 12):
        return ABCPatternMatch(confirmed=False, prior_rise_pct=0.0)

    window = values[-abc_window_days:]
    ma20_window = ma20_series[-abc_window_days:]
    current = window[-1]
    if len(window) < 8:
        return ABCPatternMatch(confirmed=False, prior_rise_pct=0.0)

    turning_points = list(range(1, len(window) - 1))
    local_mins = [
        idx for idx in turning_points
        if window[idx] <= window[idx - 1] and window[idx] <= window[idx + 1]
    ]
    local_maxs = [
        idx for idx in turning_points
        if window[idx] >= window[idx - 1] and window[idx] >= window[idx + 1]
    ]
    search_end = max(len(window) - 6, 1)
    fallback_peak_index = max(range(search_end), key=lambda idx: window[idx])
    peak_candidates = sorted(
        {
            idx
            for idx in ([fallback_peak_index] + local_maxs)
            if idx < len(window) - 4
        },
        reverse=True,
    )

    def prior_rise_for_peak(peak_index: int) -> float:
        global_peak_index = len(values) - abc_window_days + peak_index
        return _compute_prior_rise_pct(values, global_peak_index)

    fallback_prior_rise = prior_rise_for_peak(fallback_peak_index)
    best_partial: Optional[tuple[int, ABCPatternMatch]] = None

    for peak_index in peak_candidates:
        peak_price = window[peak_index]
        post_peak = window[peak_index + 1 :]
        if len(post_peak) < 4:
            continue

        low_a_candidates = [idx for idx in local_mins if idx > peak_index]
        for low_a_index in low_a_candidates:
            rebound_candidates = [idx for idx in local_maxs if idx > low_a_index]
            for rebound_index in rebound_candidates:
                low_c_candidates = [idx for idx in local_mins if idx > rebound_index]
                for low_c_index in low_c_candidates:
                    low_a_price = window[low_a_index]
                    rebound_price = window[rebound_index]
                    low_c_price = window[low_c_index]
                    rebound_ma20 = (
                        float(ma20_window[rebound_index])
                        if rebound_index < len(ma20_window) and pd.notna(ma20_window[rebound_index])
                        else float("nan")
                    )
                    pullback_pct = (peak_price - low_a_price) / peak_price * 100 if peak_price else 0.0
                    rebound_pct = (rebound_price - low_a_price) / low_a_price * 100 if low_a_price else 0.0
                    c_leg_pct = (rebound_price - low_c_price) / rebound_price * 100 if rebound_price else 0.0
                    c_low_higher_than_a_low = low_c_price > low_a_price
                    b_high_above_ma20 = pd.notna(rebound_ma20) and rebound_price > rebound_ma20
                    rebreak_ok = current >= rebound_price * (1 + rebreak_buffer_pct / 100.0)
                    current_up_ok = current > window[-2]

                    confirmed = all(
                        [
                            pullback_pct >= min_pullback_pct,
                            rebound_pct >= min_rebound_pct,
                            c_leg_pct >= min_c_leg_pct,
                            low_c_price >= low_a_price * min_c_retention_ratio,
                            rebreak_ok,
                            current_up_ok,
                            c_low_higher_than_a_low,
                            b_high_above_ma20,
                        ]
                    )
                    result = ABCPatternMatch(
                        confirmed=confirmed,
                        prior_rise_pct=prior_rise_for_peak(peak_index),
                        a_low_price=low_a_price,
                        b_high_price=rebound_price,
                        c_low_price=low_c_price,
                        b_high_ma20=float(rebound_ma20) if pd.notna(rebound_ma20) else 0.0,
                        c_low_higher_than_a_low=c_low_higher_than_a_low,
                        b_high_above_ma20=b_high_above_ma20,
                    )
                    if confirmed:
                        return result
                    score = sum(
                        1
                        for passed in (
                            pullback_pct >= min_pullback_pct,
                            rebound_pct >= min_rebound_pct,
                            c_leg_pct >= min_c_leg_pct,
                            low_c_price >= low_a_price * min_c_retention_ratio,
                            rebreak_ok,
                            current_up_ok,
                            c_low_higher_than_a_low,
                            b_high_above_ma20,
                        )
                        if passed
                    )
                    if best_partial is None or score > best_partial[0]:
                        best_partial = (score, result)

    if best_partial is not None:
        partial = best_partial[1]
        partial.prior_rise_pct = partial.prior_rise_pct or fallback_prior_rise
        return partial

    return ABCPatternMatch(confirmed=False, prior_rise_pct=fallback_prior_rise)


def _pick_strong_sector(
    sector_snapshot: Dict[str, List[Dict[str, Any]]],
    code: str,
    min_sector_change_pct: float,
) -> tuple[str, float]:
    boards = sector_snapshot.get(code, []) or []
    best_name = ""
    best_change = 0.0
    for board in boards:
        change_pct = float(board.get("change_pct") or 0.0)
        if change_pct >= min_sector_change_pct and change_pct > best_change:
            best_change = change_pct
            best_name = str(board.get("name") or "")
    return best_name, best_change


def _pick_best_sector(
    sector_snapshot: Dict[str, List[Dict[str, Any]]],
    code: str,
) -> tuple[str, float, int]:
    boards = sector_snapshot.get(code, []) or []
    if not boards:
        return "", 0.0, 0
    best_board = max(
        boards,
        key=lambda item: (
            -(int(item.get("rank") or 0) or 10_000),
            float(item.get("change_pct") or 0.0),
        ),
    )
    return (
        str(best_board.get("name") or ""),
        float(best_board.get("change_pct") or 0.0),
        int(best_board.get("rank") or 0),
    )


def _build_candidate(
    group: pd.DataFrame,
    latest_turnover: Union[Dict[str, float], TurnoverSnapshot],
    sector_snapshot: Dict[str, List[Dict[str, Any]]],
    capital_flow_snapshot: Optional[Union[Dict[str, Dict[str, float]], CapitalFlowSnapshot]],
    config: AshareRuleConfig,
    *,
    optional_checks: Optional[Sequence[str]] = None,
) -> Optional[RuleScreeningCandidate]:
    evaluation = _evaluate_candidate_conditions(
        group=group,
        latest_turnover=latest_turnover,
        sector_snapshot=sector_snapshot,
        capital_flow_snapshot=capital_flow_snapshot,
        config=config,
    )
    if evaluation is None:
        return None

    checks = dict(evaluation["checks"])
    ignored_checks = set(optional_checks or [])
    required_keys = [key for key in checks.keys() if key not in ignored_checks]
    if not all(checks[key] for key in required_keys):
        return None

    notes = [
        f"前高前累计涨幅 {evaluation['prior_rise_pct']:.1f}%",
        "ABC 调整后重新转强",
        f"收盘站上 MA20，现价 {evaluation['close']:.2f} / MA20 {evaluation['ma20']:.2f}",
        f"当日涨幅 {evaluation['change_pct']:+.2f}%",
        f"量比 {evaluation['volume_ratio']:.2f}，换手率 {evaluation['turnover_rate']:.2f}%",
        f"{evaluation['sector_name'] or '板块数据暂缺'} 涨幅 {evaluation['sector_change_pct']:.2f}%（涨幅榜第 {evaluation['sector_rank'] or '-'} 名）",
        f"MA5 乖离率 {evaluation['bias_ma5_pct']:.2f}% ，10日线/20日线保持朝上",
        (
            f"A低点 {evaluation['abc_a_low_price']:.2f}，"
            f"B高点 {evaluation['abc_b_high_price']:.2f}（MA20 {evaluation['abc_b_high_ma20']:.2f}），"
            f"C低点 {evaluation['abc_c_low_price']:.2f}"
        ),
        "C浪低点高于A浪低点；B浪反弹高于20日线",
    ]
    if evaluation["capital_flow_known"]:
        notes.append(
            "资金流向："
            f"超大单 {evaluation['super_large_net_inflow']:+.2f}，"
            f"大单 {evaluation['large_net_inflow']:+.2f}，"
            f"中单 {evaluation['medium_net_inflow']:+.2f}"
        )
        if not checks["capital_flow_ok"]:
            notes.append("资金流向未完全满足：超大单/大单/中单净流入未同时为正")
    else:
        notes.append("资金流向数据暂缺，规则11本次仅作参考")
    if not evaluation["turnover_known"]:
        notes.append("换手率数据缺失，仅供人工判断")
    if not sector_snapshot.get(evaluation["code"]):
        notes.append("板块数据缺失，仅供人工判断")
    if not checks["sector_ok"]:
        notes.append(f"板块强度未达筛选阈值 {config.min_sector_change_pct:.2f}%")

    return RuleScreeningCandidate(
        code=evaluation["code"],
        name=evaluation["name"],
        close=round(evaluation["close"], 2),
        change_pct=round(evaluation["change_pct"], 2),
        ma5=round(evaluation["ma5"], 2),
        ma10=round(evaluation["ma10"], 2),
        ma20=round(evaluation["ma20"], 2),
        bias_ma5_pct=round(evaluation["bias_ma5_pct"], 2),
        volume_ratio=round(evaluation["volume_ratio"], 2),
        turnover_rate=round(evaluation["turnover_rate"], 2),
        sector_name=evaluation["sector_name"],
        sector_change_pct=round(evaluation["sector_change_pct"], 2),
        prior_rise_pct=round(evaluation["prior_rise_pct"], 2),
        abc_pattern_confirmed=evaluation["abc_pattern_confirmed"],
        sector_rank=int(evaluation["sector_rank"]),
        abc_a_low_price=round(evaluation["abc_a_low_price"], 2),
        abc_b_high_price=round(evaluation["abc_b_high_price"], 2),
        abc_c_low_price=round(evaluation["abc_c_low_price"], 2),
        abc_b_high_ma20=round(evaluation["abc_b_high_ma20"], 2),
        capital_flow_known=bool(evaluation["capital_flow_known"]),
        super_large_net_inflow=round(evaluation["super_large_net_inflow"], 2),
        large_net_inflow=round(evaluation["large_net_inflow"], 2),
        medium_net_inflow=round(evaluation["medium_net_inflow"], 2),
        matched_condition_count=evaluation["matched_condition_count"],
        total_condition_count=evaluation["total_condition_count"],
        failed_conditions=list(evaluation["failed_conditions"]),
        notes=notes,
    )


def _filter_stock_universe(
    stock_list_df: pd.DataFrame,
    *,
    min_list_date_cutoff: str,
    exclude_st: bool,
) -> pd.DataFrame:
    if stock_list_df is None or stock_list_df.empty:
        return pd.DataFrame(columns=["code", "name", "market", "list_date"])

    df = stock_list_df.copy()
    df["code"] = df["code"].astype(str).map(normalize_stock_code)
    df["name"] = df["name"].astype(str)
    df["list_date"] = df["list_date"].astype(str)
    df = df[df["list_date"] <= str(min_list_date_cutoff)]
    if exclude_st:
        df = df[~df["name"].map(is_st_stock)]
    return df.reset_index(drop=True)


def _build_sector_snapshot_from_tushare(
    *,
    index_member_df: pd.DataFrame,
    sw_daily_df: pd.DataFrame,
    candidate_codes: Sequence[str],
    trade_date: str,
) -> Dict[str, List[Dict[str, Any]]]:
    snapshot: Dict[str, List[Dict[str, Any]]] = {normalize_stock_code(code): [] for code in candidate_codes}
    if index_member_df is None or index_member_df.empty or sw_daily_df is None or sw_daily_df.empty:
        return snapshot

    sector_df = sw_daily_df.copy()
    sector_df["ts_code"] = sector_df["ts_code"].astype(str)
    sector_df["pct_change"] = pd.to_numeric(sector_df["pct_change"], errors="coerce").fillna(0.0)
    sector_df = sector_df.sort_values("pct_change", ascending=False).reset_index(drop=True)
    sector_df["rank"] = sector_df.index + 1
    sector_map = {
        row.ts_code: {
            "name": str(row.name),
            "change_pct": float(row.pct_change),
            "rank": int(row.rank),
        }
    for row in sector_df.itertuples(index=False)
    }

    member_df = index_member_df.copy()
    member_df["stock_code"] = member_df["ts_code"].astype(str).str.split(".").str[0].map(normalize_stock_code)
    member_df["in_date"] = member_df["in_date"].astype(str)
    member_df["out_date"] = member_df["out_date"].astype(str)
    active_df = member_df[
        (member_df["in_date"] <= str(trade_date))
        & (
            member_df["out_date"].isin(["None", "nan", "NaT", ""])
            | (member_df["out_date"] >= str(trade_date))
        )
    ]

    for code in snapshot:
        rows = active_df[active_df["stock_code"] == code]
        matched: List[Dict[str, Any]] = []
        for row in rows.itertuples(index=False):
            sector = sector_map.get(str(row.l1_code))
            if sector is None:
                continue
            matched.append(
                {
                    "name": str(row.l1_name) or sector["name"],
                    "change_pct": sector["change_pct"],
                    "rank": int(sector.get("rank") or 0),
                }
            )
        matched.sort(key=lambda item: ((item.get("rank") or 10_000), -(item.get("change_pct") or 0.0)))
        snapshot[code] = matched[:1]
    return snapshot


def _merge_index_member_frames(frames: Sequence[pd.DataFrame]) -> pd.DataFrame:
    valid_frames = [frame for frame in frames if frame is not None and not frame.empty]
    if not valid_frames:
        return pd.DataFrame()
    merged = pd.concat(valid_frames, ignore_index=True)
    merged = merged.drop_duplicates(subset=["ts_code", "l1_code", "in_date", "out_date"], keep="last")
    return merged.reset_index(drop=True)


def _split_stock_codes(raw_value: Optional[str]) -> List[str]:
    if not raw_value:
        return []
    parts = [normalize_stock_code(part) for part in str(raw_value).split(",")]
    return [part for part in parts if part]


def _merge_stock_codes(existing_codes: Sequence[str], new_codes: Sequence[str]) -> List[str]:
    merged: List[str] = []
    seen = set()
    for code in list(existing_codes) + list(new_codes):
        normalized = normalize_stock_code(code)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        merged.append(normalized)
    return merged


def _count_sector_matched_codes(
    sector_snapshot: Dict[str, List[Dict[str, Any]]],
    min_sector_change_pct: float,
    sector_rank_top_n: Optional[int] = None,
) -> int:
    return sum(
        1
        for boards in sector_snapshot.values()
        if any(
            float(board.get("change_pct") or 0.0) >= min_sector_change_pct
            and (
                sector_rank_top_n is None
                or (0 < int(board.get("rank") or 0) <= sector_rank_top_n)
            )
            for board in boards
        )
    )


def _extract_snapshot_float(snapshot: Dict[str, Any], *keys: str) -> float:
    for key in keys:
        value = snapshot.get(key)
        if value is not None and pd.notna(value):
            return float(value)
    return 0.0


def _extract_snapshot_optional_float(snapshot: Dict[str, Any], *keys: str) -> Optional[float]:
    for key in keys:
        value = snapshot.get(key)
        if value is not None and pd.notna(value):
            return float(value)
    return None


def _extract_sector_change_values(snapshot: Dict[str, Any]) -> List[float]:
    raw_values = snapshot.get("sector_changes")
    if raw_values is None:
        raw_values = snapshot.get("sector_rankings")
    if raw_values is None:
        raw_values = snapshot.get("sector_snapshot")

    if raw_values is None:
        return []

    values: List[float] = []

    def collect(item: Any) -> None:
        if item is None:
            return
        if isinstance(item, dict):
            if "change_pct" in item and item.get("change_pct") is not None and pd.notna(item.get("change_pct")):
                values.append(float(item["change_pct"]))
                return
            if "pct_change" in item and item.get("pct_change") is not None and pd.notna(item.get("pct_change")):
                values.append(float(item["pct_change"]))
                return
            for nested in item.values():
                if isinstance(nested, (dict, list, tuple, set)):
                    collect(nested)
            return
        if isinstance(item, (list, tuple, set)):
            for nested in item:
                collect(nested)
            return
        try:
            if item is not None and pd.notna(item):
                values.append(float(item))
        except (TypeError, ValueError):
            return

    collect(raw_values)
    return values


def _format_rule_threshold(value: float) -> str:
    return f"{float(value):.2f}".rstrip("0").rstrip(".")


def _format_adjustment_value(value: float) -> str:
    formatted = f"{float(value):.2f}".rstrip("0")
    if formatted.endswith("."):
        formatted += "0"
    return formatted


def _condition_label_map(config: AshareRuleConfig) -> Dict[str, str]:
    return {
        "close_gt_ma20": "收盘未重新站上20日均线",
        "ma_bull": "10日线、20日线未保持朝上",
        "bias_ok": f"MA5乖离率未回到 {_format_rule_threshold(config.max_bias_ma5_pct)}% 以内",
        "volume_ok": f"量比未达到 {_format_rule_threshold(config.min_volume_ratio)}",
        "turnover_ok": f"换手率未达到 {_format_rule_threshold(config.min_turnover_rate)}%",
        "prior_rise_ok": f"前高前累计涨幅未达到 {_format_rule_threshold(config.min_prior_rise_pct)}%",
        "abc_ok": "ABC 式调整后的再转强尚未完全确认",
        "abc_c_low_ok": "C浪低点未高于A浪低点",
        "abc_b_high_ma20_ok": "B浪反弹未站上20日线",
        "sector_ok": f"所属板块涨幅未达到 {_format_rule_threshold(config.min_sector_change_pct)}%",
        "sector_rank_ok": f"所属板块未进入涨幅榜前 {config.sector_rank_top_n}",
        "capital_flow_ok": "超大单/大单/中单净流入未同时为正",
    }


def _evaluate_candidate_conditions(
    group: pd.DataFrame,
    latest_turnover: Union[Dict[str, float], TurnoverSnapshot],
    sector_snapshot: Dict[str, List[Dict[str, Any]]],
    capital_flow_snapshot: Optional[Union[Dict[str, Dict[str, float]], CapitalFlowSnapshot]],
    config: AshareRuleConfig,
) -> Optional[Dict[str, Any]]:
    group = group.sort_values("trade_date").reset_index(drop=True)
    if len(group) < max(config.lookback_days, 20):
        return None

    latest = group.iloc[-1]
    if pd.isna(latest.get("ma20")) or pd.isna(latest.get("volume_ratio")) or pd.isna(latest.get("bias_ma5_pct")):
        return None

    code = str(latest["code"])
    close = float(latest["close"])
    ma5 = float(latest["ma5"])
    ma10 = float(latest["ma10"])
    ma20 = float(latest["ma20"])
    prev_ma10 = float(group.iloc[-2]["ma10"]) if len(group) >= 2 and pd.notna(group.iloc[-2].get("ma10")) else ma10
    prev_ma20 = float(group.iloc[-2]["ma20"]) if len(group) >= 2 and pd.notna(group.iloc[-2].get("ma20")) else ma20
    bias_ma5_pct = float(latest["bias_ma5_pct"])
    volume_ratio = float(latest["volume_ratio"])
    change_pct = float(latest.get("pct_chg") or 0.0)
    turnover_snapshot = _coerce_turnover_snapshot(latest_turnover)
    turnover_known = code in turnover_snapshot.turnover_by_code
    turnover_rate = float(turnover_snapshot.turnover_by_code.get(code) or 0.0)
    capital_flow = _coerce_capital_flow_snapshot(capital_flow_snapshot)
    flow_payload = capital_flow.flow_by_code.get(code, {})
    capital_flow_known = bool(flow_payload)
    super_large_net_inflow = float(flow_payload.get("super_large_net_inflow") or 0.0)
    large_net_inflow = float(flow_payload.get("large_net_inflow") or 0.0)
    medium_net_inflow = float(flow_payload.get("medium_net_inflow") or 0.0)
    sector_name, sector_change_pct, sector_rank = _pick_best_sector(sector_snapshot=sector_snapshot, code=code)
    abc_match = _detect_abc_pattern(
        group["close"].tolist(),
        ma20_values=group["ma20"].tolist(),
        abc_window_days=config.abc_window_days,
        min_pullback_pct=config.abc_min_pullback_pct,
        min_rebound_pct=config.abc_min_rebound_pct,
        min_c_leg_pct=config.abc_min_c_leg_pct,
        min_c_retention_ratio=config.abc_min_c_retention_ratio,
        rebreak_buffer_pct=config.abc_rebreak_buffer_pct,
    )
    prior_rise_pct = abc_match.prior_rise_pct

    checks = {
        "close_gt_ma20": close > ma20,
        "ma_bull": ma10 >= prev_ma10 and ma20 >= prev_ma20,
        "bias_ok": bias_ma5_pct < config.max_bias_ma5_pct,
        "volume_ok": volume_ratio >= config.min_volume_ratio,
        "turnover_ok": turnover_rate >= config.min_turnover_rate if turnover_known else True,
        "sector_ok": sector_change_pct >= config.min_sector_change_pct,
        "sector_rank_ok": 0 < sector_rank <= config.sector_rank_top_n,
        "prior_rise_ok": prior_rise_pct >= config.min_prior_rise_pct,
        "abc_ok": abc_match.confirmed,
        "abc_c_low_ok": abc_match.c_low_higher_than_a_low,
        "abc_b_high_ma20_ok": abc_match.b_high_above_ma20,
        "capital_flow_ok": (
            super_large_net_inflow > 0 and large_net_inflow > 0 and medium_net_inflow > 0
            if capital_flow_known
            else False
        ),
    }

    return {
        "code": code,
        "name": str(latest.get("name") or code),
        "close": close,
        "change_pct": change_pct,
        "ma5": ma5,
        "ma10": ma10,
        "ma20": ma20,
        "bias_ma5_pct": bias_ma5_pct,
        "volume_ratio": volume_ratio,
        "turnover_known": turnover_known,
        "turnover_rate": turnover_rate,
        "capital_flow_known": capital_flow_known,
        "super_large_net_inflow": super_large_net_inflow,
        "large_net_inflow": large_net_inflow,
        "medium_net_inflow": medium_net_inflow,
        "sector_name": sector_name,
        "sector_change_pct": sector_change_pct,
        "sector_rank": sector_rank,
        "prior_rise_pct": prior_rise_pct,
        "abc_pattern_confirmed": abc_match.confirmed,
        "abc_a_low_price": abc_match.a_low_price,
        "abc_b_high_price": abc_match.b_high_price,
        "abc_c_low_price": abc_match.c_low_price,
        "abc_b_high_ma20": abc_match.b_high_ma20,
        "checks": checks,
        "matched_condition_count": sum(1 for passed in checks.values() if passed),
        "total_condition_count": len(checks),
        "failed_conditions": [
            label
            for key, label in _condition_label_map(config).items()
            if not checks.get(key, False)
        ],
        "data_notes": list(turnover_snapshot.notes),
    }


def _classify_market_regime(snapshot: Dict[str, Any]) -> str:
    if isinstance(snapshot.get("stats"), dict):
        merged_snapshot = dict(snapshot["stats"])
        merged_snapshot.update({key: value for key, value in snapshot.items() if key != "stats"})
        snapshot = merged_snapshot

    index_change = snapshot.get("index_change") or {}
    if isinstance(index_change, dict):
        index_values = [
            float(value)
            for key, value in index_change.items()
            if key in {"sh", "sz", "cyb"} and value is not None and pd.notna(value)
        ]
    elif isinstance(index_change, (list, tuple, set)):
        index_values = [float(value) for value in index_change if value is not None and pd.notna(value)]
    elif index_change is None or pd.isna(index_change):
        index_values = []
    else:
        index_values = [float(index_change)]
    if not index_values:
        for key in ("sh_change_pct", "sz_change_pct", "cyb_change_pct", "sh_pct_change", "sz_pct_change", "cyb_pct_change"):
            value = snapshot.get(key)
            if value is not None and pd.notna(value):
                index_values.append(float(value))

    avg_index_change = sum(index_values) / len(index_values) if index_values else 0.0
    up_count = _extract_snapshot_float(snapshot, "up_count")
    down_count = _extract_snapshot_float(snapshot, "down_count")
    limit_up = _extract_snapshot_float(snapshot, "limit_up", "limit_up_count")
    limit_down = _extract_snapshot_float(snapshot, "limit_down", "limit_down_count")
    sector_median_value = _extract_snapshot_optional_float(snapshot, "sector_median")
    sector_median = sector_median_value if sector_median_value is not None else 0.0
    if sector_median_value is None:
        sector_changes = _extract_sector_change_values(snapshot)
        if sector_changes:
            sector_median = float(pd.Series(sector_changes).median())

    breadth_total = up_count + down_count
    breadth_balance = (up_count - down_count) / breadth_total if breadth_total else 0.0
    limit_balance = limit_up - limit_down

    if (
        avg_index_change <= -0.5
        or (down_count > up_count and sector_median <= -0.3)
        or (breadth_balance <= -0.2 and sector_median <= 0.0)
    ):
        return "weak"

    if (
        avg_index_change >= 0.5
        and up_count >= down_count
        and sector_median >= 0.2
        and limit_balance >= 0
    ):
        return "strong"

    return "neutral"


def _rank_candidates_for_father(
    candidates: Sequence[RuleScreeningCandidate],
) -> List[RuleScreeningCandidate]:
    def clamp(value: float, minimum: float = 0.0, maximum: float = 100.0) -> float:
        return max(minimum, min(maximum, value))

    def score_turnover(turnover_rate: float) -> float:
        return clamp(12.0 - abs(turnover_rate - 5.5) * 2.2, maximum=12.0)

    def score_volume_ratio(volume_ratio: float) -> float:
        return clamp(10.0 - abs(volume_ratio - 1.6) * 6.0, maximum=10.0)

    def score_prior_rise(prior_rise_pct: float) -> float:
        return clamp(12.0 - abs(prior_rise_pct - 30.0) * 0.35, maximum=12.0)

    def score_change_pct(change_pct: float) -> float:
        return clamp(10.0 - abs(change_pct - 4.0) * 1.8, maximum=10.0)

    def score_ma20_proximity(close: float, ma20: float) -> float:
        if ma20 <= 0:
            return 0.0
        premium_pct = (close / ma20 - 1.0) * 100.0
        return clamp(14.0 - abs(premium_pct - 3.0) * 2.5, maximum=14.0)

    def score_sector_rank(sector_rank: int) -> float:
        if sector_rank <= 0:
            return 0.0
        return clamp(24.0 - min(sector_rank, 120) * 0.18, maximum=24.0)

    def score_sector_strength(sector_change_pct: float) -> float:
        return clamp(max(sector_change_pct, 0.0) * 3.5, maximum=14.0)

    def score_abc_structure(candidate: RuleScreeningCandidate) -> float:
        score = 0.0
        if candidate.abc_a_low_price > 0 and candidate.abc_c_low_price > 0:
            c_support_pct = (candidate.abc_c_low_price / candidate.abc_a_low_price - 1.0) * 100.0
            score += clamp(c_support_pct * 1.5, maximum=8.0)
        if candidate.abc_b_high_ma20 > 0 and candidate.abc_b_high_price > 0:
            b_breakout_pct = (candidate.abc_b_high_price / candidate.abc_b_high_ma20 - 1.0) * 100.0
            score += clamp(b_breakout_pct * 1.2, maximum=8.0)
        return score

    def score_capital_flow(candidate: RuleScreeningCandidate) -> float:
        if not candidate.capital_flow_known:
            return 0.0
        positives = sum(
            1
            for value in (
                candidate.super_large_net_inflow,
                candidate.large_net_inflow,
                candidate.medium_net_inflow,
            )
            if value > 0
        )
        raw_strength = max(candidate.super_large_net_inflow, 0.0) + max(candidate.large_net_inflow, 0.0) + max(candidate.medium_net_inflow, 0.0)
        return clamp(positives * 3.0 + raw_strength * 0.15, maximum=12.0)

    ranked: List[RuleScreeningCandidate] = []
    for candidate in candidates:
        total_score = (
            score_sector_rank(int(candidate.sector_rank))
            + score_sector_strength(candidate.sector_change_pct)
            + score_turnover(candidate.turnover_rate)
            + score_volume_ratio(candidate.volume_ratio)
            + score_change_pct(candidate.change_pct)
            + score_ma20_proximity(candidate.close, candidate.ma20)
            + score_prior_rise(candidate.prior_rise_pct)
            + score_abc_structure(candidate)
            + score_capital_flow(candidate)
        )
        candidate.father_priority_score = round(total_score, 2)
        ranked.append(candidate)

    return sorted(
        ranked,
        key=lambda item: (
            item.father_priority_score,
            -(item.sector_rank or 10_000),
            item.sector_change_pct,
            -abs(item.change_pct - 4.0),
            -abs(item.turnover_rate - 5.5),
            -abs(item.volume_ratio - 1.6),
            -abs(item.prior_rise_pct - 30.0),
        ),
        reverse=True,
    )


def _build_dynamic_rule_config(
    base: AshareRuleConfig,
    market_regime: str,
) -> tuple[AshareRuleConfig, List[DynamicAdjustment]]:
    config = replace(base)
    adjustments: List[DynamicAdjustment] = []

    def apply_adjustment(
        attr_name: str,
        target_value: float,
        name: str,
        reason: str,
        *,
        direction: str,
    ) -> None:
        from_value = float(getattr(config, attr_name))
        if direction == "loosen_min":
            to_value = min(from_value, target_value)
        elif direction == "loosen_max":
            to_value = max(from_value, target_value)
        else:
            raise ValueError(f"Unsupported adjustment direction: {direction}")
        if from_value == to_value:
            return
        setattr(config, attr_name, to_value)
        adjustments.append(
            DynamicAdjustment(
                name=name,
                from_value=from_value,
                to_value=to_value,
                reason=reason,
            )
        )

    normalized_regime = (market_regime or "").strip().lower()
    if normalized_regime == "strong":
        apply_adjustment("min_prior_rise_pct", 19.0, "前高前累计涨幅", "强势日保留20%主升浪口径，仅轻微放宽", direction="loosen_min")
    elif normalized_regime == "neutral":
        apply_adjustment("min_prior_rise_pct", 18.5, "前高前累计涨幅", "中性日保留主升浪容错", direction="loosen_min")
        apply_adjustment("min_volume_ratio", 0.95, "量比", "中性日轻放宽", direction="loosen_min")
        apply_adjustment("min_turnover_rate", 2.5, "换手率", "中性日轻放宽", direction="loosen_min")
        apply_adjustment("max_bias_ma5_pct", 9.5, "MA5乖离率", "中性日轻放宽", direction="loosen_max")
    elif normalized_regime == "weak":
        apply_adjustment("min_prior_rise_pct", 18.0, "前高前累计涨幅", "弱势日保留强势股主升浪容错", direction="loosen_min")
        apply_adjustment("min_volume_ratio", 0.9, "量比", "弱势日优先保留量能", direction="loosen_min")
        apply_adjustment("min_turnover_rate", 2.0, "换手率", "弱势日优先保留换手", direction="loosen_min")
        apply_adjustment("max_bias_ma5_pct", 10.5, "MA5乖离率", "弱势日允许更大回撤", direction="loosen_max")

    return config, adjustments


def _sample_records(df: Optional[pd.DataFrame], columns: Sequence[str], limit: int = 3) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    existing = [column for column in columns if column in df.columns]
    if not existing:
        return []
    return df.loc[:, existing].head(limit).to_dict(orient="records")


def apply_selection_rules(
    daily_history: pd.DataFrame,
    latest_turnover: Union[Dict[str, float], TurnoverSnapshot],
    sector_snapshot: Dict[str, List[Dict[str, Any]]],
    capital_flow_snapshot: Optional[Union[Dict[str, Dict[str, float]], CapitalFlowSnapshot]] = None,
    config: Optional[AshareRuleConfig] = None,
    *,
    optional_checks: Optional[Sequence[str]] = None,
) -> List[RuleScreeningCandidate]:
    config = config or AshareRuleConfig()
    prepared = _prepare_indicator_frame(daily_history)
    if prepared.empty:
        return []
    active_optional_checks = list(optional_checks or [])
    if not _coerce_capital_flow_snapshot(capital_flow_snapshot).flow_by_code and "capital_flow_ok" not in active_optional_checks:
        active_optional_checks.append("capital_flow_ok")

    candidates: List[RuleScreeningCandidate] = []
    for code, group in prepared.groupby("code"):
        candidate = _build_candidate(
            group=group,
            latest_turnover=latest_turnover,
            sector_snapshot=sector_snapshot,
            capital_flow_snapshot=capital_flow_snapshot,
            config=config,
            optional_checks=active_optional_checks,
        )
        if candidate is not None:
            candidates.append(candidate)

    return sorted(
        candidates,
        key=lambda item: (
            item.sector_rank or 10_000,
            -item.sector_change_pct,
            -item.volume_ratio,
            -item.turnover_rate,
            -item.prior_rise_pct,
        ),
    )


def build_technical_candidate_pool(
    daily_history: pd.DataFrame,
    latest_turnover: Union[Dict[str, float], TurnoverSnapshot],
    sector_snapshot: Dict[str, List[Dict[str, Any]]],
    capital_flow_snapshot: Optional[Union[Dict[str, Dict[str, float]], CapitalFlowSnapshot]] = None,
    config: Optional[AshareRuleConfig] = None,
) -> List[RuleScreeningCandidate]:
    config = config or AshareRuleConfig()
    prepared = _prepare_indicator_frame(daily_history)
    if prepared.empty:
        return []

    candidates: List[RuleScreeningCandidate] = []
    for _, group in prepared.groupby("code"):
        candidate = _build_candidate(
            group=group,
            latest_turnover=latest_turnover,
            sector_snapshot=sector_snapshot,
            capital_flow_snapshot=capital_flow_snapshot,
            config=config,
            optional_checks=("sector_ok", "sector_rank_ok", "capital_flow_ok"),
        )
        if candidate is not None:
            candidates.append(candidate)

    return _rank_candidates_for_father(candidates)


def build_manual_review_pool(
    daily_history: pd.DataFrame,
    latest_turnover: Union[Dict[str, float], TurnoverSnapshot],
    sector_snapshot: Dict[str, List[Dict[str, Any]]],
    capital_flow_snapshot: Optional[Union[Dict[str, Dict[str, float]], CapitalFlowSnapshot]] = None,
    config: Optional[AshareRuleConfig] = None,
    *,
    limit: int = 15,
) -> List[RuleScreeningCandidate]:
    config = config or AshareRuleConfig()
    prepared = _prepare_indicator_frame(daily_history)
    if prepared.empty:
        return []

    candidates: List[RuleScreeningCandidate] = []
    for _, group in prepared.groupby("code"):
        evaluation = _evaluate_candidate_conditions(
            group=group,
            latest_turnover=latest_turnover,
            sector_snapshot=sector_snapshot,
            capital_flow_snapshot=capital_flow_snapshot,
            config=config,
        )
        if evaluation is None:
            continue

        checks = dict(evaluation["checks"])
        core_pass_count = sum(1 for key in ("close_gt_ma20", "ma_bull", "prior_rise_ok", "abc_ok") if checks.get(key))
        if core_pass_count < 3:
            continue

        notes = [
            f"前高前累计涨幅 {evaluation['prior_rise_pct']:.1f}%",
            f"当日涨幅 {evaluation['change_pct']:+.2f}%",
            f"收盘/MA20：{evaluation['close']:.2f} / {evaluation['ma20']:.2f}",
            f"量比/换手率：{evaluation['volume_ratio']:.2f} / {evaluation['turnover_rate']:.2f}%",
            (
                f"板块：{evaluation['sector_name'] or '暂无板块数据'}"
                f"（{evaluation['sector_change_pct']:+.2f}%，第 {evaluation['sector_rank'] or '-'} 名）"
            ),
            (
                f"ABC：A低点 {evaluation['abc_a_low_price']:.2f}，"
                f"B高点 {evaluation['abc_b_high_price']:.2f}（MA20 {evaluation['abc_b_high_ma20']:.2f}），"
                f"C低点 {evaluation['abc_c_low_price']:.2f}"
            ),
            f"命中条件 {evaluation['matched_condition_count']}/{evaluation['total_condition_count']} 项",
        ]
        if evaluation["capital_flow_known"]:
            notes.append(
                "资金流向："
                f"超大单 {evaluation['super_large_net_inflow']:+.2f}，"
                f"大单 {evaluation['large_net_inflow']:+.2f}，"
                f"中单 {evaluation['medium_net_inflow']:+.2f}"
            )
        else:
            notes.append("资金流向数据暂缺，规则11本次仅作参考")
        if evaluation["failed_conditions"]:
            notes.append(f"未满足条件：{'；'.join(evaluation['failed_conditions'])}")
        if not evaluation["turnover_known"]:
            notes.append("换手率数据缺失，仅供人工判断")
        if not sector_snapshot.get(evaluation["code"]):
            notes.append("板块数据缺失，仅供人工判断")

        candidates.append(
            RuleScreeningCandidate(
                code=evaluation["code"],
                name=evaluation["name"],
                close=round(evaluation["close"], 2),
                change_pct=round(evaluation["change_pct"], 2),
                ma5=round(evaluation["ma5"], 2),
                ma10=round(evaluation["ma10"], 2),
                ma20=round(evaluation["ma20"], 2),
                bias_ma5_pct=round(evaluation["bias_ma5_pct"], 2),
                volume_ratio=round(evaluation["volume_ratio"], 2),
                turnover_rate=round(evaluation["turnover_rate"], 2),
                sector_name=evaluation["sector_name"],
                sector_change_pct=round(evaluation["sector_change_pct"], 2),
                prior_rise_pct=round(evaluation["prior_rise_pct"], 2),
                abc_pattern_confirmed=evaluation["abc_pattern_confirmed"],
                sector_rank=int(evaluation["sector_rank"]),
                abc_a_low_price=round(evaluation["abc_a_low_price"], 2),
                abc_b_high_price=round(evaluation["abc_b_high_price"], 2),
                abc_c_low_price=round(evaluation["abc_c_low_price"], 2),
                abc_b_high_ma20=round(evaluation["abc_b_high_ma20"], 2),
                capital_flow_known=bool(evaluation["capital_flow_known"]),
                super_large_net_inflow=round(evaluation["super_large_net_inflow"], 2),
                large_net_inflow=round(evaluation["large_net_inflow"], 2),
                medium_net_inflow=round(evaluation["medium_net_inflow"], 2),
                matched_condition_count=evaluation["matched_condition_count"],
                total_condition_count=evaluation["total_condition_count"],
                failed_conditions=list(evaluation["failed_conditions"]),
                notes=notes,
            )
        )

    return _rank_candidates_for_father(sorted(
        candidates,
        key=lambda item: (
            item.matched_condition_count,
            item.abc_pattern_confirmed,
            item.sector_change_pct,
            item.volume_ratio,
            item.turnover_rate,
            item.prior_rise_pct,
        ),
        reverse=True,
    ))[: max(int(limit), 0)]


def build_screening_report(
    candidates: Sequence[RuleScreeningCandidate],
    report_date: str,
    *,
    ai_review_lines: Optional[Sequence[str]] = None,
    profile_name: str = "严格版",
    profile_notes: Optional[Sequence[str]] = None,
    stock_pool_notes: Optional[Sequence[str]] = None,
    rule_config: Optional[AshareRuleConfig] = None,
    grouped_candidates: Optional[Union[RuleScreeningBuckets, Dict[str, Sequence[RuleScreeningCandidate]]]] = None,
    market_regime_label: str = "",
    dynamic_adjustments: Optional[Sequence[Union[DynamicAdjustment, str]]] = None,
    screening_buckets: Optional[RuleScreeningBuckets] = None,
) -> str:
    def normalize_grouped_candidates(
        value: Optional[Union[RuleScreeningBuckets, Dict[str, Sequence[RuleScreeningCandidate]]]],
    ) -> RuleScreeningBuckets:
        if value is None:
            return RuleScreeningBuckets(full_hits=list(candidates))
        if isinstance(value, RuleScreeningBuckets):
            return value
        return RuleScreeningBuckets(
            full_hits=list(value.get("full", []) or []),
            relaxed_hits=list(value.get("relaxed", []) or []),
            technical_pool=list(value.get("technical", []) or []),
            manual_review_pool=list(value.get("manual", []) or []),
        )

    def has_candidates(value: Optional[Union[RuleScreeningBuckets, Dict[str, Sequence[RuleScreeningCandidate]]]]) -> bool:
        if value is None:
            return False
        if isinstance(value, RuleScreeningBuckets):
            return not value.is_empty()
        return any(value.get(key) for key in ("full", "relaxed", "technical", "manual"))

    def append_candidate_section(
        section_title: str,
        section_candidates: Sequence[RuleScreeningCandidate],
    ) -> None:
        if not section_candidates:
            return

        lines.extend(
            [
                f"## {section_title}（{len(section_candidates)} 只）",
                "",
            ]
        )
        for idx, candidate in enumerate(section_candidates, start=1):
            candidate_lines = [
                f"{idx}. {candidate.name} ({candidate.code})",
                (
                    f"   - 板块：{candidate.sector_name or '暂无板块数据'}"
                    f"（{candidate.sector_change_pct:+.2f}%，第 {candidate.sector_rank or '-'} 名）"
                ),
                f"   - 现价/MA5/MA10/MA20：{candidate.close:.2f} / {candidate.ma5:.2f} / {candidate.ma10:.2f} / {candidate.ma20:.2f}",
                f"   - 当日涨幅/量比/换手率：{candidate.change_pct:+.2f}% / {candidate.volume_ratio:.2f} / {candidate.turnover_rate:.2f}%",
                f"   - 前高前累计涨幅：{candidate.prior_rise_pct:.2f}%",
            ]
            if candidate.capital_flow_known:
                candidate_lines.append(
                    "   - 资金流向："
                    f"超大单 {candidate.super_large_net_inflow:+.2f} / "
                    f"大单 {candidate.large_net_inflow:+.2f} / "
                    f"中单 {candidate.medium_net_inflow:+.2f}"
                )
            if candidate.total_condition_count:
                candidate_lines.append(
                    f"   - 条件命中：{candidate.matched_condition_count}/{candidate.total_condition_count}"
                )
            if candidate.failed_conditions:
                candidate_lines.append(f"   - 未满足条件：{'；'.join(candidate.failed_conditions)}")
            candidate_lines.append(f"   - 规则说明：{'；'.join(candidate.notes)}")
            lines.extend(candidate_lines)
        lines.append("")

    def append_focus_section(section_candidates: Sequence[RuleScreeningCandidate]) -> None:
        focus_limit = max(1, min(int(os.getenv("RULE_SCREENER_FOCUS_POOL_LIMIT", "10")), 15))
        focus_candidates = _rank_candidates_for_father(list(section_candidates))[:focus_limit]
        if not focus_candidates:
            return
        displayed_focus_count = len(focus_candidates)

        lines.extend(
            [
                f"## 优先关注（前 {displayed_focus_count} 只）",
                "",
                "排序依据：行业涨幅排名、行业涨幅、当日涨幅、量比、换手率、ABC 结构质量、资金流向。",
                "",
            ]
        )
        for idx, candidate in enumerate(focus_candidates, start=1):
            ma20_premium_pct = ((candidate.close / candidate.ma20 - 1.0) * 100.0) if candidate.ma20 > 0 else 0.0
            c_support_pct = ((candidate.abc_c_low_price / candidate.abc_a_low_price - 1.0) * 100.0) if candidate.abc_a_low_price > 0 else 0.0
            b_breakout_pct = ((candidate.abc_b_high_price / candidate.abc_b_high_ma20 - 1.0) * 100.0) if candidate.abc_b_high_ma20 > 0 else 0.0
            lines.extend(
                [
                    f"{idx}. {candidate.name} ({candidate.code}) | 优先分 {candidate.father_priority_score:.1f}",
                    (
                        f"   - 板块强度：{candidate.sector_name or '暂无板块数据'}"
                        f"（{candidate.sector_change_pct:+.2f}%，第 {candidate.sector_rank or '-'} 名）"
                    ),
                    (
                        f"   - 价量位置：现价 {candidate.close:.2f}，高于MA20 {ma20_premium_pct:.2f}%"
                        f"，当日涨幅 {candidate.change_pct:+.2f}%"
                        f"，量比 {candidate.volume_ratio:.2f}，换手率 {candidate.turnover_rate:.2f}%"
                    ),
                    (
                        f"   - 结构质量：前高前涨幅 {candidate.prior_rise_pct:.2f}%"
                        f"，C高于A {c_support_pct:.2f}%"
                        f"，B高于MA20 {b_breakout_pct:.2f}%"
                    ),
                ]
            )
            if candidate.capital_flow_known:
                lines.append(
                    "   - 资金流向："
                    f"超大单 {candidate.super_large_net_inflow:+.2f}，"
                    f"大单 {candidate.large_net_inflow:+.2f}，"
                    f"中单 {candidate.medium_net_inflow:+.2f}"
                )
        lines.append("")

    rule_config = rule_config or AshareRuleConfig()
    if candidates and (has_candidates(grouped_candidates) or has_candidates(screening_buckets)):
        raise ValueError("build_screening_report received conflicting candidate sources")
    bucket_source = grouped_candidates if grouped_candidates is not None else screening_buckets
    if has_candidates(bucket_source):
        grouped_candidates = normalize_grouped_candidates(bucket_source)
    elif candidates:
        grouped_candidates = RuleScreeningBuckets(full_hits=list(candidates))
    else:
        grouped_candidates = normalize_grouped_candidates(bucket_source)
    sector_rule_line = (
        f"- 所属板块涨幅 > {_format_rule_threshold(rule_config.min_sector_change_pct)}%"
        f"，且涨幅榜排名前 {rule_config.sector_rank_top_n}"
    )
    if grouped_candidates.technical_pool and not grouped_candidates.full_hits and not grouped_candidates.relaxed_hits:
        sector_rule_line = (
            f"- 所属板块涨幅 > {_format_rule_threshold(rule_config.min_sector_change_pct)}%"
            f"，且涨幅榜排名前 {rule_config.sector_rank_top_n}"
            "（完整/放宽命中时适用；技术候选池仅供参考，不作硬性剔除）"
        )
    elif grouped_candidates.manual_review_pool and grouped_candidates.is_empty() is False and not (
        grouped_candidates.full_hits or grouped_candidates.relaxed_hits or grouped_candidates.technical_pool
    ):
        sector_rule_line = (
            f"- 所属板块涨幅 > {_format_rule_threshold(rule_config.min_sector_change_pct)}%"
            f"，且涨幅榜排名前 {rule_config.sector_rank_top_n}"
            "（人工精选池中改为排序参考，不作硬性剔除）"
        )
    lines = [
        f"# A股规则选股日报 {report_date}",
        "",
        "## 筛选档位",
        f"- 本次结果：{profile_name}",
    ]
    if profile_notes:
        lines.extend(f"- {note}" for note in profile_notes)
    if market_regime_label and not any(str(note).startswith("市场环境：") for note in (profile_notes or [])):
        lines.append(f"- 市场环境：{market_regime_label}")
    if dynamic_adjustments:
        rendered_adjustments = [
            adjustment.to_report_line() if isinstance(adjustment, DynamicAdjustment) else str(adjustment)
            for adjustment in dynamic_adjustments
        ]
        existing_profile_notes = {str(note).removeprefix("动态放宽：") for note in (profile_notes or [])}
        lines.extend(
            f"- {adjustment}"
            for adjustment in rendered_adjustments
            if adjustment not in existing_profile_notes
        )
    lines.extend([
        "",
        "## 策略条件",
        f"- 前期累计涨幅不少于 {_format_rule_threshold(rule_config.min_prior_rise_pct)}%",
        "- 经过 ABC 式调整后再度转强",
        "- 收盘重新站上 20 日均线",
        f"- 量比 > {_format_rule_threshold(rule_config.min_volume_ratio)}，换手率 > {_format_rule_threshold(rule_config.min_turnover_rate)}%",
        sector_rule_line,
        f"- 5 日线乖离率 < {_format_rule_threshold(rule_config.max_bias_ma5_pct)}%",
        "- 10 日线、20 日线保持朝上",
        "- C浪低点高于A浪低点",
        "- B浪反弹高于20日线",
        "- 超大单、大单、中单净流入为正",
        "",
    ])

    if grouped_candidates.is_empty():
        lines.extend(["## 结果", "- 今日未筛出符合条件的A股股票。"])
        if stock_pool_notes:
            lines.extend(["", "## 自选池同步"])
            lines.extend(f"- {line}" for line in stock_pool_notes)
        return "\n".join(lines)

    append_focus_section(grouped_candidates.technical_pool)
    append_candidate_section("完整命中", grouped_candidates.full_hits)
    append_candidate_section("动态放宽命中", grouped_candidates.relaxed_hits)
    append_candidate_section("技术候选池", grouped_candidates.technical_pool)
    append_candidate_section("人工精选池", grouped_candidates.manual_review_pool)

    if ai_review_lines:
        lines.extend(["", "## AI复核"])
        lines.extend(f"- {line}" for line in ai_review_lines)

    if stock_pool_notes:
        lines.extend(["", "## 自选池同步"])
        lines.extend(f"- {line}" for line in stock_pool_notes)

    return "\n".join(lines)


class AshareRuleScreenerService:
    def __init__(
        self,
        config: Optional[Config] = None,
        rule_config: Optional[AshareRuleConfig] = None,
        fetcher_manager: Optional["DataFetcherManager"] = None,
        tushare_fetcher: Optional["TushareFetcher"] = None,
        notifier: Optional["NotificationService"] = None,
    ) -> None:
        from data_provider import DataFetcherManager
        from src.notification import NotificationService

        self.config = config or get_config()
        self.rule_config = rule_config or AshareRuleConfig(
            lookback_days=int(os.getenv("RULE_SCREENER_LOOKBACK_DAYS", "60")),
            abc_window_days=int(os.getenv("RULE_SCREENER_ABC_WINDOW_DAYS", "20")),
            min_prior_rise_pct=float(os.getenv("RULE_SCREENER_MIN_PRIOR_RISE_PCT", "20")),
            min_volume_ratio=float(os.getenv("RULE_SCREENER_MIN_VOLUME_RATIO", "1")),
            max_bias_ma5_pct=float(os.getenv("RULE_SCREENER_MAX_BIAS_MA5_PCT", "9")),
            ai_review_limit=int(os.getenv("RULE_SCREENER_AI_REVIEW_LIMIT", "12")),
            min_turnover_rate=float(os.getenv("RULE_SCREENER_MIN_TURNOVER_RATE", "3")),
            min_sector_change_pct=float(os.getenv("RULE_SCREENER_MIN_SECTOR_CHANGE_PCT", "1")),
            sector_rank_top_n=int(os.getenv("RULE_SCREENER_SECTOR_TOP_N", "5")),
            exclude_st=os.getenv("RULE_SCREENER_EXCLUDE_ST", "true").lower() != "false",
            allow_open_data_fallback=os.getenv("RULE_SCREENER_ALLOW_FALLBACK", "false").lower() == "true",
            auto_relax_if_empty=os.getenv("RULE_SCREENER_AUTO_RELAX_IF_EMPTY", "true").lower() != "false",
        )
        self.fetcher_manager = fetcher_manager or DataFetcherManager()
        self.tushare_fetcher = tushare_fetcher or self._resolve_tushare_fetcher()
        self.notifier = notifier or NotificationService()
        self.cache_dir = Path(os.getenv("RULE_SCREENER_CACHE_DIR", ".cache/rule_screener_v2/tushare"))
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _resolve_tushare_fetcher(self) -> "TushareFetcher":
        from data_provider.tushare_fetcher import TushareFetcher

        for fetcher in getattr(self.fetcher_manager, "_fetchers", []):
            if isinstance(fetcher, TushareFetcher):
                return fetcher
        return TushareFetcher()

    def _cache_file(self, api_name: str, cache_key: str) -> Path:
        safe_key = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(cache_key))
        cache_root = getattr(self, "cache_dir", Path(os.getenv("RULE_SCREENER_CACHE_DIR", ".cache/rule_screener_v2/tushare")))
        if not isinstance(cache_root, Path):
            cache_root = Path(str(cache_root))
        cache_root.mkdir(parents=True, exist_ok=True)
        self.cache_dir = cache_root
        api_dir = cache_root / api_name
        api_dir.mkdir(parents=True, exist_ok=True)
        return api_dir / f"{safe_key}.pkl"

    def _call_tushare_cached(self, api_name: str, *, cache_key: str, **kwargs) -> pd.DataFrame:
        cache_file = self._cache_file(api_name, cache_key)
        if cache_file.exists():
            cached_df = pd.read_pickle(cache_file)
            if isinstance(cached_df, pd.DataFrame) and not cached_df.empty:
                return cached_df
            logger.warning("检测到 %s 的空缓存 %s，已忽略并重新拉取。", api_name, cache_file)
        df = self.tushare_fetcher._call_api_with_rate_limit(api_name, **kwargs)
        cached_df = pd.DataFrame() if df is None else df
        if not cached_df.empty:
            cached_df.to_pickle(cache_file)
        elif cache_file.exists():
            cache_file.unlink(missing_ok=True)
        return cached_df

    def _call_tushare_cached_paginated(
        self,
        api_name: str,
        *,
        cache_key: str,
        page_size: int = 2000,
        **kwargs,
    ) -> pd.DataFrame:
        cache_file = self._cache_file(api_name, cache_key)
        if cache_file.exists():
            cached_df = pd.read_pickle(cache_file)
            if isinstance(cached_df, pd.DataFrame) and not cached_df.empty:
                return cached_df
            logger.warning("检测到 %s 的空缓存 %s，已忽略并重新拉取。", api_name, cache_file)
        fetcher = getattr(self, "tushare_fetcher", None)
        if fetcher is None or not hasattr(fetcher, "_call_api_with_rate_limit"):
            return pd.DataFrame()

        frames: List[pd.DataFrame] = []
        offset = 0
        while True:
            page_kwargs = dict(kwargs)
            page_kwargs.update({"offset": offset, "limit": page_size})
            df = fetcher._call_api_with_rate_limit(api_name, **page_kwargs)
            if df is None or df.empty:
                break
            frames.append(df)
            if len(df) < page_size:
                break
            offset += page_size

        merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if not merged.empty:
            merged.to_pickle(cache_file)
        elif cache_file.exists():
            cache_file.unlink(missing_ok=True)
        return merged

    def _build_relaxed_rule_config(self) -> AshareRuleConfig:
        return AshareRuleConfig(
            lookback_days=self.rule_config.lookback_days,
            abc_window_days=self.rule_config.abc_window_days,
            min_prior_rise_pct=max(14.0, self.rule_config.min_prior_rise_pct - 2.0),
            min_volume_ratio=max(0.9, self.rule_config.min_volume_ratio - 0.1),
            min_turnover_rate=max(2.5, self.rule_config.min_turnover_rate - 0.5),
            min_sector_change_pct=self.rule_config.min_sector_change_pct,
            max_bias_ma5_pct=self.rule_config.max_bias_ma5_pct + 1.0,
            ai_review_limit=self.rule_config.ai_review_limit,
            sector_rank_top_n=self.rule_config.sector_rank_top_n,
            notify_when_empty=self.rule_config.notify_when_empty,
            exclude_st=self.rule_config.exclude_st,
            allow_open_data_fallback=self.rule_config.allow_open_data_fallback,
            auto_relax_if_empty=False,
            abc_min_pullback_pct=self.rule_config.abc_min_pullback_pct,
            abc_min_rebound_pct=self.rule_config.abc_min_rebound_pct,
            abc_min_c_leg_pct=self.rule_config.abc_min_c_leg_pct,
            abc_min_c_retention_ratio=self.rule_config.abc_min_c_retention_ratio,
            abc_rebreak_buffer_pct=self.rule_config.abc_rebreak_buffer_pct,
        )

    def _load_trade_dates(self) -> List[str]:
        requested_trade_dates = self.rule_config.lookback_days + 5
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=max(self.rule_config.lookback_days * 3, 180))).strftime("%Y%m%d")
        df_cal = self._call_tushare_cached(
            "trade_cal",
            cache_key=f"{start_date}_{end_date}",
            exchange="SSE",
            start_date=start_date,
            end_date=end_date,
            fields="cal_date,is_open",
        )
        if df_cal is None or df_cal.empty:
            raise RuntimeError("无法从 Tushare 获取交易日历，规则选股无法继续")
        trade_dates = sorted(
            df_cal[df_cal["is_open"] == 1]["cal_date"].astype(str).tolist(),
            reverse=True,
        )
        if len(trade_dates) < self.rule_config.lookback_days:
            raise RuntimeError(
                f"Tushare 交易日历数量不足：需要至少 {self.rule_config.lookback_days} 个交易日，实际 {len(trade_dates)}"
            )
        return trade_dates[: requested_trade_dates]

    def _resolve_latest_completed_trade_date(self, trade_dates: Sequence[str]) -> str:
        if not trade_dates:
            raise RuntimeError("trade_dates 为空，无法确定规则选股目标交易日")

        effective_trade_date = get_effective_trading_date("cn").strftime("%Y%m%d")
        for trade_date in trade_dates:
            if str(trade_date) <= effective_trade_date:
                return str(trade_date)
        return str(trade_dates[0])

    def _select_analysis_trade_dates(
        self,
        trade_dates: Sequence[str],
        latest_trade_date: str,
    ) -> List[str]:
        completed_trade_dates = [str(trade_date) for trade_date in trade_dates if str(trade_date) <= str(latest_trade_date)]
        if len(completed_trade_dates) < self.rule_config.lookback_days:
            raise RuntimeError(
                "规则选股可用的已完成交易日数量不足："
                f"需要至少 {self.rule_config.lookback_days} 个，实际 {len(completed_trade_dates)}"
            )
        return completed_trade_dates[: self.rule_config.lookback_days]

    def _load_stock_universe(self, min_list_date_cutoff: str) -> pd.DataFrame:
        stock_list_df = self._call_tushare_cached(
            "stock_basic",
            cache_key=datetime.now().strftime("%Y%m"),
            exchange="",
            list_status="L",
            fields="ts_code,name,market,list_date",
        )
        if stock_list_df is None or stock_list_df.empty:
            raise RuntimeError("未获取到 Tushare 股票列表，规则选股无法继续")
        stock_list_df = stock_list_df.copy()
        stock_list_df["code"] = stock_list_df["ts_code"].astype(str).str.split(".").str[0]
        return _filter_stock_universe(
            stock_list_df[["code", "name", "market", "list_date"]],
            min_list_date_cutoff=min_list_date_cutoff,
            exclude_st=self.rule_config.exclude_st,
        )

    def _load_daily_history(self, trade_dates: Sequence[str], stock_universe: pd.DataFrame) -> pd.DataFrame:
        frames: List[pd.DataFrame] = []
        allowed_codes = set(stock_universe["code"].astype(str).map(normalize_stock_code))
        for trade_date in reversed(list(trade_dates)):
            df = self._call_tushare_cached(
                "daily",
                cache_key=trade_date,
                trade_date=trade_date,
                fields="ts_code,trade_date,open,high,low,close,vol",
            )
            if df is None or df.empty:
                continue
            work_df = df.copy()
            work_df["code"] = work_df["ts_code"].astype(str).str.split(".").str[0]
            work_df["code"] = work_df["code"].map(normalize_stock_code)
            work_df = work_df[work_df["code"].isin(allowed_codes)]
            if work_df.empty:
                continue
            work_df["volume"] = pd.to_numeric(work_df["vol"], errors="coerce")
            frames.append(work_df[["code", "trade_date", "open", "high", "low", "close", "volume"]])

        if not frames:
            raise RuntimeError("未获取到任何日线数据，规则选股无法继续")
        merged = pd.concat(frames, ignore_index=True)
        names = stock_universe[["code", "name"]].copy()
        names["code"] = names["code"].astype(str).map(normalize_stock_code)
        merged["code"] = merged["code"].astype(str).map(normalize_stock_code)
        merged = merged.merge(names, on="code", how="left")
        return merged

    def _load_latest_turnover(
        self,
        trade_date: str,
        *,
        trade_dates: Optional[Sequence[str]] = None,
    ) -> TurnoverSnapshot:
        fallback_trade_date: Optional[str] = None
        resolved_trade_dates = [str(item) for item in (trade_dates or []) if item]
        if not resolved_trade_dates:
            try:
                resolved_trade_dates = [str(item) for item in self._load_trade_dates() if item]
            except Exception as exc:
                logger.warning("换手率回退时获取交易日失败，将直接使用 unknown: %s", exc)
                resolved_trade_dates = []

        if trade_date in resolved_trade_dates:
            trade_date_index = resolved_trade_dates.index(trade_date)
            if trade_date_index + 1 < len(resolved_trade_dates):
                fallback_trade_date = resolved_trade_dates[trade_date_index + 1]
        else:
            earlier_trade_dates = sorted((item for item in resolved_trade_dates if item < trade_date), reverse=True)
            if earlier_trade_dates:
                fallback_trade_date = earlier_trade_dates[0]

        candidate_trade_dates = [trade_date]
        if fallback_trade_date and fallback_trade_date != trade_date:
            candidate_trade_dates.append(fallback_trade_date)

        for candidate_trade_date in candidate_trade_dates:
            df = self._call_tushare_cached(
                "daily_basic",
                cache_key=candidate_trade_date,
                trade_date=candidate_trade_date,
                fields="ts_code,turnover_rate",
            )
            if df is None or df.empty:
                continue

            work_df = df.copy()
            work_df["code"] = work_df["ts_code"].astype(str).str.split(".").str[0].map(normalize_stock_code)
            work_df["turnover_rate"] = pd.to_numeric(work_df["turnover_rate"], errors="coerce").fillna(0.0)
            notes: List[str] = []
            is_partial = candidate_trade_date != trade_date
            if is_partial:
                notes.append(
                    f"daily_basic({trade_date}) 为空，已回退到上一交易日 {candidate_trade_date} 的换手率数据。"
                )
            return TurnoverSnapshot(
                turnover_by_code=dict(zip(work_df["code"], work_df["turnover_rate"])),
                source=f"daily_basic:{candidate_trade_date}",
                is_partial=is_partial,
                notes=notes,
            )

        logger.warning("daily_basic 当前与上一交易日均为空，换手率条件将降级为参考项")
        return TurnoverSnapshot(
            turnover_by_code={},
            source="unknown",
            is_partial=True,
            notes=[f"daily_basic({trade_date}) 及上一交易日均为空，换手率以 unknown 处理，仅供人工判断。"],
        )

    def _load_capital_flow_snapshot(
        self,
        trade_date: str,
        candidate_codes: Sequence[str],
        *,
        trade_dates: Optional[Sequence[str]] = None,
    ) -> CapitalFlowSnapshot:
        normalized_codes = [normalize_stock_code(code) for code in candidate_codes if code]
        if not normalized_codes:
            return CapitalFlowSnapshot(source="moneyflow", is_partial=True)

        fallback_trade_date: Optional[str] = None
        resolved_trade_dates = [str(item) for item in (trade_dates or []) if item]
        if trade_date in resolved_trade_dates:
            trade_date_index = resolved_trade_dates.index(trade_date)
            if trade_date_index + 1 < len(resolved_trade_dates):
                fallback_trade_date = resolved_trade_dates[trade_date_index + 1]
        else:
            earlier_trade_dates = sorted((item for item in resolved_trade_dates if item < trade_date), reverse=True)
            if earlier_trade_dates:
                fallback_trade_date = earlier_trade_dates[0]

        candidate_trade_dates = [trade_date]
        if fallback_trade_date and fallback_trade_date != trade_date:
            candidate_trade_dates.append(fallback_trade_date)

        for candidate_trade_date in candidate_trade_dates:
            df = self._call_tushare_cached_paginated(
                "moneyflow",
                cache_key=candidate_trade_date,
                trade_date=candidate_trade_date,
                fields="ts_code,buy_elg_amount,sell_elg_amount,buy_lg_amount,sell_lg_amount,buy_md_amount,sell_md_amount",
            )
            if df is None or df.empty:
                continue

            work_df = df.copy()
            work_df["code"] = work_df["ts_code"].astype(str).str.split(".").str[0].map(normalize_stock_code)
            work_df = work_df[work_df["code"].isin(set(normalized_codes))]
            if work_df.empty:
                continue

            for column in ("buy_elg_amount", "sell_elg_amount", "buy_lg_amount", "sell_lg_amount", "buy_md_amount", "sell_md_amount"):
                work_df[column] = pd.to_numeric(work_df[column], errors="coerce").fillna(0.0)

            notes: List[str] = []
            is_partial = candidate_trade_date != trade_date
            if is_partial:
                notes.append(
                    f"moneyflow({trade_date}) 为空，已回退到上一交易日 {candidate_trade_date} 的资金流向数据。"
                )

            flow_by_code: Dict[str, Dict[str, float]] = {}
            for row in work_df.itertuples(index=False):
                flow_by_code[str(row.code)] = {
                    "super_large_net_inflow": float(row.buy_elg_amount) - float(row.sell_elg_amount),
                    "large_net_inflow": float(row.buy_lg_amount) - float(row.sell_lg_amount),
                    "medium_net_inflow": float(row.buy_md_amount) - float(row.sell_md_amount),
                }
            return CapitalFlowSnapshot(
                flow_by_code=flow_by_code,
                source=f"moneyflow:{candidate_trade_date}",
                is_partial=is_partial,
                notes=notes,
            )

        logger.warning("moneyflow 当前与上一交易日均为空，资金流向条件将降级为参考项")
        return CapitalFlowSnapshot(
            flow_by_code={},
            source="unknown",
            is_partial=True,
            notes=[f"moneyflow({trade_date}) 及上一交易日均为空，规则11降级为参考项，仅供人工判断。"],
        )

    def _load_market_snapshot_fallback(self) -> pd.DataFrame:
        logger.warning("规则选股回退到开放快照模式：先用全市场实时快照预筛，再补候选历史K线")
        df = None
        code_col = "股票代码"
        name_col = "股票名称"
        volume_ratio_col = "量比"
        turnover_col = "换手率"

        try:
            import efinance as ef

            df = ef.stock.get_realtime_quotes()
            code_col = "股票代码" if "股票代码" in df.columns else "code"
            name_col = "股票名称" if "股票名称" in df.columns else "name"
            volume_ratio_col = "量比" if "量比" in df.columns else "volume_ratio"
            turnover_col = "换手率" if "换手率" in df.columns else "turnover_rate"
        except ModuleNotFoundError:
            import akshare as ak

            logger.warning("本地缺少 efinance，改用 ak.stock_zh_a_spot_em() 作为开放快照源")
            df = ak.stock_zh_a_spot_em()
            code_col = "代码"
            name_col = "名称"
            volume_ratio_col = "量比"
            turnover_col = "换手率"

        if df is None or df.empty:
            raise RuntimeError("开放快照模式未获取到全市场实时行情")

        work_df = df[[code_col, name_col, volume_ratio_col, turnover_col]].copy()
        work_df.columns = ["code", "name", "volume_ratio", "turnover_rate"]
        work_df["code"] = work_df["code"].astype(str).str.extract(r"(\d{6})", expand=False)
        work_df = work_df.dropna(subset=["code"])
        work_df["code"] = work_df["code"].map(normalize_stock_code)
        work_df["volume_ratio"] = pd.to_numeric(work_df["volume_ratio"], errors="coerce").fillna(0.0)
        work_df["turnover_rate"] = pd.to_numeric(work_df["turnover_rate"], errors="coerce").fillna(0.0)
        return work_df

    def _load_history_via_prefilter_fallback(self) -> tuple[pd.DataFrame, TurnoverSnapshot, str]:
        snapshot = self._load_market_snapshot_fallback()
        trade_date = datetime.now().strftime("%Y%m%d")
        fallback_note = "已回退到开放快照预筛换手率数据，仅供人工判断。"
        filtered = snapshot[
            (snapshot["volume_ratio"] >= self.rule_config.min_volume_ratio)
            & (snapshot["turnover_rate"] >= self.rule_config.min_turnover_rate)
        ].copy()
        if filtered.empty:
            return (
                pd.DataFrame(columns=["code", "trade_date", "open", "high", "low", "close", "volume", "name"]),
                TurnoverSnapshot(
                    turnover_by_code={},
                    source=f"prefilter_snapshot:{trade_date}",
                    is_partial=True,
                    notes=[fallback_note],
                ),
                trade_date,
            )

        fetch_limit = int(os.getenv("RULE_SCREENER_HISTORY_FETCH_LIMIT", "120"))
        filtered = filtered.sort_values(["volume_ratio", "turnover_rate"], ascending=False).head(fetch_limit)

        frames: List[pd.DataFrame] = []
        latest_turnover = TurnoverSnapshot(
            turnover_by_code=dict(zip(filtered["code"], filtered["turnover_rate"])),
            source=f"prefilter_snapshot:{trade_date}",
            is_partial=True,
            notes=[fallback_note],
        )

        for row in filtered.itertuples(index=False):
            code = normalize_stock_code(row.code)
            try:
                history_df, _ = self.fetcher_manager.get_daily_data(code, days=self.rule_config.lookback_days)
            except Exception as exc:
                logger.debug("预筛候选 %s 历史K线获取失败，跳过: %s", code, exc)
                continue
            if history_df is None or history_df.empty:
                continue

            work_df = history_df.copy()
            work_df["code"] = code
            work_df["name"] = row.name
            if "volume" not in work_df.columns and "vol" in work_df.columns:
                work_df["volume"] = work_df["vol"]
            frames.append(work_df[["code", "trade_date", "open", "high", "low", "close", "volume", "name"]])
            if "trade_date" in work_df.columns and not work_df.empty:
                trade_date = str(work_df["trade_date"].astype(str).iloc[-1])

        if not frames:
            return pd.DataFrame(columns=["code", "trade_date", "open", "high", "low", "close", "volume", "name"]), latest_turnover, trade_date

        return pd.concat(frames, ignore_index=True), latest_turnover, trade_date

    def _load_sector_snapshot(
        self,
        candidate_codes: Sequence[str],
        trade_date: str,
        min_sector_change_pct: Optional[float] = None,
    ) -> SectorSnapshotLoadResult:
        empty_snapshot = {normalize_stock_code(code): [] for code in candidate_codes}
        if not candidate_codes:
            return SectorSnapshotLoadResult(snapshot=empty_snapshot)
        sector_threshold = (
            self.rule_config.min_sector_change_pct
            if min_sector_change_pct is None
            else float(min_sector_change_pct)
        )

        membership_frames: List[pd.DataFrame] = []
        for code in candidate_codes:
            ts_code = self.tushare_fetcher._convert_stock_code(code)
            membership_frames.append(
                self._call_tushare_cached(
                    "index_member_all",
                    cache_key=f"{datetime.now().strftime('%Y%m')}_{ts_code}",
                    ts_code=ts_code,
                )
            )
        index_member_df = _merge_index_member_frames(membership_frames)
        sw_daily_df = self._call_tushare_cached(
            "sw_daily",
            cache_key=trade_date,
            trade_date=trade_date,
            fields="ts_code,name,pct_change",
        )
        debug_sector = os.getenv("RULE_SCREENER_DEBUG_SECTOR", "false").lower() == "true"
        if debug_sector:
            member_l1_codes = (
                set(index_member_df["l1_code"].dropna().astype(str).tolist())
                if index_member_df is not None and not index_member_df.empty and "l1_code" in index_member_df.columns
                else set()
            )
            sw_sector_codes = (
                set(sw_daily_df["ts_code"].dropna().astype(str).tolist())
                if sw_daily_df is not None and not sw_daily_df.empty and "ts_code" in sw_daily_df.columns
                else set()
            )
            overlap_codes = sorted(member_l1_codes & sw_sector_codes)[:5]
            logger.info(
                "板块数据诊断: candidates=%s trade_date=%s threshold=%.2f index_member_rows=%s sw_daily_rows=%s member_l1=%s sw_codes=%s overlap=%s",
                len(candidate_codes),
                trade_date,
                sector_threshold,
                0 if index_member_df is None else len(index_member_df),
                0 if sw_daily_df is None else len(sw_daily_df),
                len(member_l1_codes),
                len(sw_sector_codes),
                overlap_codes,
            )
            logger.info(
                "板块数据样例: index_member_cols=%s sw_daily_cols=%s index_member_sample=%s sw_daily_sample=%s",
                [] if index_member_df is None else list(index_member_df.columns),
                [] if sw_daily_df is None else list(sw_daily_df.columns),
                _sample_records(index_member_df, ["ts_code", "l1_code", "l1_name", "in_date", "out_date"]),
                _sample_records(sw_daily_df, ["ts_code", "name", "pct_change"]),
            )
        if index_member_df is None or index_member_df.empty:
            logger.warning("index_member_all 返回为空，板块强度条件将无法命中")
            return SectorSnapshotLoadResult(
                snapshot=empty_snapshot,
                source="index_member_all:empty",
                is_partial=True,
                notes=["板块数据缺失，仅供人工判断：index_member_all 返回为空，技术候选池继续输出，板块强度条件已降级为参考项。"],
            )
        if sw_daily_df is None or sw_daily_df.empty:
            logger.warning("sw_daily 返回为空，板块强度条件将无法命中")
            return SectorSnapshotLoadResult(
                snapshot=empty_snapshot,
                source=f"sw_daily:{trade_date}:empty",
                is_partial=True,
                notes=["板块数据缺失，仅供人工判断：sw_daily 返回为空，技术候选池继续输出，板块强度条件已降级为参考项。"],
            )
        return SectorSnapshotLoadResult(
            snapshot=_build_sector_snapshot_from_tushare(
                index_member_df=index_member_df,
                sw_daily_df=sw_daily_df,
                candidate_codes=candidate_codes,
                trade_date=trade_date,
            ),
            source=f"sw_daily:{trade_date}",
        )

    def _select_technical_candidates(
        self,
        daily_history: pd.DataFrame,
        latest_turnover: Union[Dict[str, float], TurnoverSnapshot],
        config: Optional[AshareRuleConfig] = None,
    ) -> List[str]:
        active_config = config or self.rule_config
        prepared = _prepare_indicator_frame(daily_history)
        candidate_codes: List[str] = []
        for code, group in prepared.groupby("code"):
            candidate = _build_candidate(
                group=group,
                latest_turnover=latest_turnover,
                sector_snapshot={
                    code: [
                        {
                            "name": "placeholder",
                            "change_pct": active_config.min_sector_change_pct,
                            "rank": 1,
                        }
                    ]
                },
                capital_flow_snapshot=None,
                config=active_config,
                optional_checks=("sector_ok", "sector_rank_ok", "capital_flow_ok"),
            )
            if candidate is not None:
                candidate_codes.append(code)
        return candidate_codes

    def _build_ai_review_lines(self, results: Sequence[Any]) -> List[str]:
        report_language = normalize_report_language(getattr(self.config, "report_language", "zh"))
        decision_style = normalize_decision_style(getattr(self.config, "report_decision_style", "standard"))
        lines: List[str] = []
        for result in results:
            display_name = getattr(result, "stock_name", "") or getattr(result, "name", "") or result.code
            operation_advice = localize_decision_display_advice(
                getattr(result, "operation_advice", ""),
                report_language,
                decision_style,
            )
            summary = getattr(result, "analysis_summary", "") or "无额外 AI 结论"
            lines.append(
                f"{display_name}({result.code})：{operation_advice}；{summary}"
            )
        return lines

    def _github_headers(self, token: str) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def _get_repo_variable(self, repo: str, token: str, name: str) -> Optional[str]:
        url = f"https://api.github.com/repos/{repo}/actions/variables/{name}"
        response = requests.get(url, headers=self._github_headers(token), timeout=30)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        payload = response.json()
        return payload.get("value")

    def _upsert_repo_variable(self, repo: str, token: str, name: str, value: str) -> None:
        headers = self._github_headers(token)
        variable_url = f"https://api.github.com/repos/{repo}/actions/variables/{name}"
        payload = {"name": name, "value": value}

        existing = requests.get(variable_url, headers=headers, timeout=30)
        if existing.status_code == 404:
            create_url = f"https://api.github.com/repos/{repo}/actions/variables"
            response = requests.post(create_url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            return

        existing.raise_for_status()
        response = requests.patch(variable_url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()

    def _sync_candidates_to_stock_pool(self, candidates: Sequence[RuleScreeningCandidate]) -> List[str]:
        if os.getenv("RULE_SCREENER_AUTO_APPEND_TO_STOCK_LIST", "true").strip().lower() == "false":
            return []
        if not candidates:
            return []

        token = (os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN") or "").strip()
        repo = (
            os.getenv("RULE_SCREENER_STOCK_POOL_REPO")
            or os.getenv("GITHUB_REPOSITORY")
            or ""
        ).strip()
        if not token or not repo:
            logger.warning("规则选股命中后未自动加入自选池：缺少 GITHUB_TOKEN/GH_TOKEN 或目标仓库配置")
            return ["命中股票未自动加入自选池：缺少 GitHub 仓库或 Token 配置。"]

        candidate_codes = [candidate.code for candidate in candidates]
        try:
            current_raw = self._get_repo_variable(repo, token, "STOCK_LIST")
            existing_codes = _split_stock_codes(current_raw)
            merged_codes = _merge_stock_codes(existing_codes, candidate_codes)
            added_codes = [code for code in candidate_codes if code not in set(existing_codes)]
            if merged_codes != existing_codes:
                self._upsert_repo_variable(repo, token, "STOCK_LIST", ",".join(merged_codes))
                logger.info("规则选股命中股票已自动并入自选池: repo=%s, added=%s", repo, ",".join(added_codes))
                return [f"已自动加入自选池：{', '.join(added_codes)}"]
            return ["命中股票均已存在于自选池，无需重复加入。"]
        except Exception as exc:
            logger.warning("规则选股命中后同步自选池失败: %s", exc)
            return [f"命中股票自动加入自选池失败：{exc}"]

    def _should_sync_stock_pool(self, *, send_notification: bool) -> bool:
        return send_notification

    def _select_ai_review_candidates(
        self,
        grouped_candidates: RuleScreeningBuckets,
        *,
        technical_review_limit: int,
    ) -> tuple[List[RuleScreeningCandidate], List[str]]:
        review_candidates: List[RuleScreeningCandidate] = []
        review_notes: List[str] = []
        seen_codes: set[str] = set()

        def append_unique(candidates: Sequence[RuleScreeningCandidate]) -> None:
            for candidate in candidates:
                if candidate.code in seen_codes:
                    continue
                seen_codes.add(candidate.code)
                review_candidates.append(candidate)

        core_candidates = list(grouped_candidates.full_hits) + list(grouped_candidates.relaxed_hits)
        append_unique(core_candidates)

        if grouped_candidates.technical_pool:
            if technical_review_limit > 0:
                technical_review_candidates = list(grouped_candidates.technical_pool[:technical_review_limit])
                append_unique(technical_review_candidates)
                if len(grouped_candidates.technical_pool) > len(technical_review_candidates):
                    review_notes.append(
                        "AI复核范围：完整命中/动态放宽命中全部保留；"
                        f"技术候选池仅复核前 {len(technical_review_candidates)} 只，其余仅展示硬指标供人工精选。"
                    )
                else:
                    review_notes.append(
                        "AI复核范围：完整命中/动态放宽命中全部保留；技术候选池已全部纳入 AI 复核。"
                    )
            else:
                append_unique(grouped_candidates.technical_pool)
                review_notes.append("AI复核范围：完整命中/动态放宽命中与技术候选池全部纳入 AI 复核。")
        elif grouped_candidates.manual_review_pool:
            if technical_review_limit > 0:
                manual_review_candidates = list(grouped_candidates.manual_review_pool[:technical_review_limit])
                append_unique(manual_review_candidates)
                if len(grouped_candidates.manual_review_pool) > len(manual_review_candidates):
                    review_notes.append(
                        f"AI复核范围：人工精选池仅复核前 {len(manual_review_candidates)} 只，其余仅展示硬指标供人工精选。"
                    )
                else:
                    review_notes.append("AI复核范围：人工精选池已全部纳入 AI 复核。")
            else:
                append_unique(grouped_candidates.manual_review_pool)
                review_notes.append("AI复核范围：人工精选池全部纳入 AI 复核。")

        return review_candidates, review_notes

    def _resolve_market_regime(self) -> tuple[str, str]:
        snapshot: Dict[str, Any] = {}
        fetcher_manager = getattr(self, "fetcher_manager", None)
        if fetcher_manager is not None and hasattr(fetcher_manager, "get_market_stats"):
            try:
                raw_snapshot = fetcher_manager.get_market_stats() or {}
                if isinstance(raw_snapshot, dict):
                    snapshot = raw_snapshot
            except Exception as exc:
                logger.warning("获取市场状态快照失败，按中性市场处理: %s", exc)

        if fetcher_manager is not None and hasattr(fetcher_manager, "get_main_indices"):
            try:
                raw_indices = fetcher_manager.get_main_indices(region="cn")
                if isinstance(raw_indices, list):
                    index_change: Dict[str, float] = {}
                    for item in raw_indices:
                        if not isinstance(item, dict):
                            continue
                        code = str(item.get("code") or "")
                        change_pct = item.get("change_pct")
                        if change_pct is None or pd.isna(change_pct):
                            continue
                        if code in {"000001", "000001.SH", "sh000001"}:
                            index_change["sh"] = float(change_pct)
                        elif code in {"399001", "399001.SZ", "sz399001"}:
                            index_change["sz"] = float(change_pct)
                        elif code in {"399006", "399006.SZ", "sz399006"}:
                            index_change["cyb"] = float(change_pct)
                    if index_change:
                        snapshot["index_change"] = index_change
            except Exception as exc:
                logger.warning("获取主要指数快照失败，继续按现有市场统计判定: %s", exc)

        if fetcher_manager is not None and hasattr(fetcher_manager, "get_sector_rankings"):
            try:
                raw_rankings = fetcher_manager.get_sector_rankings(
                    n=max(int(self.rule_config.sector_rank_top_n), 20)
                )
                if (
                    isinstance(raw_rankings, tuple)
                    and len(raw_rankings) == 2
                    and isinstance(raw_rankings[0], list)
                    and isinstance(raw_rankings[1], list)
                ):
                    top_sectors, bottom_sectors = raw_rankings
                    snapshot["sector_rankings"] = {
                        "top": top_sectors,
                        "bottom": bottom_sectors,
                    }
            except Exception as exc:
                logger.warning("获取板块排行失败，继续按现有市场统计判定: %s", exc)

        market_regime = _classify_market_regime(snapshot)
        regime_label = {
            "strong": "强势日",
            "neutral": "震荡日",
            "weak": "弱势日",
        }.get(market_regime, "震荡日")
        return market_regime, regime_label

    def _evaluate_screening_stage(
        self,
        *,
        daily_history: pd.DataFrame,
        latest_turnover: Union[Dict[str, float], TurnoverSnapshot],
        trade_date: str,
        config: AshareRuleConfig,
        stage_name: str,
    ) -> RuleScreeningStageResult:
        technical_candidate_codes = self._select_technical_candidates(
            daily_history=daily_history,
            latest_turnover=latest_turnover,
            config=config,
        )
        capital_flow_snapshot = self._load_capital_flow_snapshot(
            trade_date,
            technical_candidate_codes,
        )
        sector_snapshot_result = _coerce_sector_snapshot_result(
            self._load_sector_snapshot(
                technical_candidate_codes,
                trade_date,
                config.min_sector_change_pct,
            ),
            technical_candidate_codes,
        )
        sector_snapshot = sector_snapshot_result.snapshot
        scoped_history = daily_history[daily_history["code"].isin(technical_candidate_codes)]
        optional_checks: List[str] = []
        if not capital_flow_snapshot.flow_by_code:
            optional_checks.append("capital_flow_ok")
        candidates = apply_selection_rules(
            daily_history=scoped_history,
            latest_turnover=latest_turnover,
            sector_snapshot=sector_snapshot,
            capital_flow_snapshot=capital_flow_snapshot,
            config=config,
            optional_checks=optional_checks,
        )
        logger.info(
            "规则选股%s统计: technical=%s, sector=%s, capital_flow=%s, final=%s",
            stage_name,
            len(technical_candidate_codes),
            _count_sector_matched_codes(
                sector_snapshot,
                config.min_sector_change_pct,
                config.sector_rank_top_n,
            ),
            sum(
                1
                for flow in capital_flow_snapshot.flow_by_code.values()
                if flow.get("super_large_net_inflow", 0.0) > 0
                and flow.get("large_net_inflow", 0.0) > 0
                and flow.get("medium_net_inflow", 0.0) > 0
            ),
            len(candidates),
        )
        return RuleScreeningStageResult(
            config=config,
            technical_candidate_codes=technical_candidate_codes,
            sector_snapshot=sector_snapshot,
            candidates=candidates,
            data_notes=list(sector_snapshot_result.notes) + list(capital_flow_snapshot.notes),
        )

    def run(
        self,
        *,
        send_notification: bool = True,
        ai_review: bool = True,
    ) -> RuleScreeningRunResult:
        try:
            trade_dates = self._load_trade_dates()
            latest_trade_date = self._resolve_latest_completed_trade_date(trade_dates)
            analysis_trade_dates = self._select_analysis_trade_dates(trade_dates, latest_trade_date)
            stock_universe = self._load_stock_universe(min_list_date_cutoff=analysis_trade_dates[-1])
            daily_history = self._load_daily_history(analysis_trade_dates, stock_universe)
            latest_turnover = self._load_latest_turnover(latest_trade_date, trade_dates=analysis_trade_dates)
        except Exception as exc:
            if not self.rule_config.allow_open_data_fallback:
                raise
            logger.warning("Tushare 批量模式不可用，改走预筛降级链路: %s", exc)
            daily_history, latest_turnover, latest_trade_date = self._load_history_via_prefilter_fallback()

        grouped_candidates = RuleScreeningBuckets()
        active_config = self.rule_config
        profile_name = "严格版"
        market_regime, market_regime_label = self._resolve_market_regime()
        dynamic_adjustments: List[DynamicAdjustment] = []
        latest_turnover_snapshot = _coerce_turnover_snapshot(latest_turnover)
        profile_notes: List[str] = [
            "严格条件命中优先；严格档为 0 时，才会按市场状态进入动态放宽。",
            f"市场环境：{market_regime_label}",
        ]
        _append_unique_notes(profile_notes, latest_turnover_snapshot.notes)

        strict_stage = self._evaluate_screening_stage(
            daily_history=daily_history,
            latest_turnover=latest_turnover_snapshot,
            trade_date=latest_trade_date,
            config=self.rule_config,
            stage_name="严格版",
        )
        profile_notes.append(
            "严格版诊断：技术形态命中 "
            f"{len(strict_stage.technical_candidate_codes)} 只，板块强度命中 "
            f"{_count_sector_matched_codes(strict_stage.sector_snapshot, self.rule_config.min_sector_change_pct, self.rule_config.sector_rank_top_n)} 只，"
            f"最终入选 {len(strict_stage.candidates)} 只。"
        )
        final_stage = strict_stage
        relaxed_stage: Optional[RuleScreeningStageResult] = None
        _append_unique_notes(profile_notes, strict_stage.data_notes)
        if strict_stage.candidates:
            grouped_candidates.full_hits = list(strict_stage.candidates)
        else:
            if self.rule_config.auto_relax_if_empty:
                active_config, dynamic_adjustments = _build_dynamic_rule_config(self.rule_config, market_regime)
                profile_name = "动态放宽版"
                profile_notes.append(f"严格条件为 0，已按 {market_regime_label} 进入动态放宽。")
                profile_notes.extend(
                    f"动态放宽：{adjustment.to_report_line()}" for adjustment in dynamic_adjustments
                )
                relaxed_stage = self._evaluate_screening_stage(
                    daily_history=daily_history,
                    latest_turnover=latest_turnover_snapshot,
                    trade_date=latest_trade_date,
                    config=active_config,
                    stage_name="动态放宽版",
                )
                final_stage = relaxed_stage
                _append_unique_notes(profile_notes, relaxed_stage.data_notes)
                profile_notes.append(
                    f"{profile_name}诊断：技术形态命中 "
                    f"{len(relaxed_stage.technical_candidate_codes)} 只，板块强度命中 "
                    f"{_count_sector_matched_codes(relaxed_stage.sector_snapshot, active_config.min_sector_change_pct, active_config.sector_rank_top_n)} 只，"
                    f"最终入选 {len(relaxed_stage.candidates)} 只。"
                )
                if relaxed_stage.candidates:
                    grouped_candidates.relaxed_hits = list(relaxed_stage.candidates)
            else:
                relaxed_stage = RuleScreeningStageResult(
                    config=active_config,
                    technical_candidate_codes=list(strict_stage.technical_candidate_codes),
                    sector_snapshot=dict(strict_stage.sector_snapshot),
                    candidates=[],
                    data_notes=list(strict_stage.data_notes),
                )
                profile_notes.append("严格条件为 0，且已禁用动态放宽；继续输出技术候选池供人工精选。")
                _append_unique_notes(profile_notes, relaxed_stage.data_notes)

        stage_for_technical_pool = relaxed_stage or strict_stage
        if stage_for_technical_pool.technical_candidate_codes:
            technical_capital_flow_snapshot = self._load_capital_flow_snapshot(
                latest_trade_date,
                stage_for_technical_pool.technical_candidate_codes,
                trade_dates=analysis_trade_dates if 'analysis_trade_dates' in locals() else None,
            )
            technical_candidates = build_technical_candidate_pool(
                daily_history=daily_history[
                    daily_history["code"].isin(stage_for_technical_pool.technical_candidate_codes)
                ],
                latest_turnover=latest_turnover_snapshot,
                sector_snapshot=stage_for_technical_pool.sector_snapshot,
                capital_flow_snapshot=technical_capital_flow_snapshot,
                config=stage_for_technical_pool.config,
            )
            selected_codes = {
                candidate.code for candidate in list(grouped_candidates.full_hits) + list(grouped_candidates.relaxed_hits)
            }
            technical_candidates = [
                candidate for candidate in technical_candidates
                if candidate.code not in selected_codes
            ]
            if technical_candidates:
                active_config = stage_for_technical_pool.config
                final_stage = stage_for_technical_pool
                grouped_candidates.technical_pool = list(technical_candidates)
                if not grouped_candidates.full_hits and not grouped_candidates.relaxed_hits:
                    profile_name = f"{profile_name}（技术候选池）"
                    profile_notes.append(
                        f"动态放宽仍为 0，已回退到技术候选池，共列出 {len(technical_candidates)} 只股票并全部展示。"
                    )
                else:
                    profile_notes.append(
                        f"除完整/动态放宽命中外，另补充技术候选池 {len(technical_candidates)} 只，供人工精选。"
                    )
                profile_notes.append(
                    "技术候选池满足核心技术结构，板块强度仅作参考，不自动并入自选池。"
                )
                _append_unique_notes(profile_notes, stage_for_technical_pool.data_notes)
                logger.info("规则选股补充技术候选池: candidates=%s", len(technical_candidates))

        if grouped_candidates.is_empty():
                manual_review_config = self._build_relaxed_rule_config()
                all_candidate_codes = sorted(set(daily_history["code"].astype(str).map(normalize_stock_code).tolist()))
                manual_sector_snapshot_result = _coerce_sector_snapshot_result(
                    self._load_sector_snapshot(
                        all_candidate_codes,
                        latest_trade_date,
                        manual_review_config.min_sector_change_pct,
                    ),
                    all_candidate_codes,
                )
                manual_review_pool = build_manual_review_pool(
                    daily_history=daily_history,
                    latest_turnover=latest_turnover_snapshot,
                    sector_snapshot=manual_sector_snapshot_result.snapshot,
                    capital_flow_snapshot=self._load_capital_flow_snapshot(
                        latest_trade_date,
                        all_candidate_codes,
                        trade_dates=analysis_trade_dates if 'analysis_trade_dates' in locals() else None,
                    ),
                    config=manual_review_config,
                    limit=int(os.getenv("RULE_SCREENER_MANUAL_REVIEW_LIMIT", "15")),
                )
                _append_unique_notes(profile_notes, manual_sector_snapshot_result.notes)
                if manual_review_pool:
                    grouped_candidates.manual_review_pool = list(manual_review_pool)
                    active_config = manual_review_config
                    profile_name = f"{profile_name}（人工精选池）"
                    profile_notes.append(
                        f"完整命中、动态放宽命中、技术候选池均为空，已改为人工精选池并展示 {len(manual_review_pool)} 只接近命中股票。"
                    )
                    profile_notes.append("人工精选池按命中条件数量排序，未满足条件会逐条列出，供人工精选，不自动并入自选池。")
                    logger.info("规则选股已回退到人工精选池: candidates=%s", len(manual_review_pool))

        stock_pool_sync_candidates = list(grouped_candidates.full_hits) + list(grouped_candidates.relaxed_hits)
        display_candidates = (
            stock_pool_sync_candidates
            + list(grouped_candidates.technical_pool)
            + list(grouped_candidates.manual_review_pool)
        )

        ai_review_lines: List[str] = []
        if display_candidates and ai_review:
            from src.core.pipeline import StockAnalysisPipeline

            review_limit = max(int(active_config.ai_review_limit), 0)
            review_candidates, review_notes = self._select_ai_review_candidates(
                grouped_candidates,
                technical_review_limit=review_limit,
            )
            _append_unique_notes(profile_notes, review_notes)
            review_codes = [candidate.code for candidate in review_candidates]
            pipeline = StockAnalysisPipeline(
                config=self.config,
                max_workers=min(4, len(review_codes)) or 1,
            )
            ai_results = pipeline.run(
                stock_codes=review_codes,
                dry_run=False,
                send_notification=False,
                merge_notification=False,
            )
            ai_review_lines = self._build_ai_review_lines(ai_results)

        stock_pool_notes: List[str] = []
        if stock_pool_sync_candidates and self._should_sync_stock_pool(send_notification=send_notification):
            stock_pool_notes.extend(self._sync_candidates_to_stock_pool(stock_pool_sync_candidates))
        if grouped_candidates.technical_pool or grouped_candidates.manual_review_pool:
            stock_pool_notes.append("当前为技术/人工精选候选名单，未自动并入自选池，请人工确认后再决定是否加入。")

        report = build_screening_report(
            candidates=[],
            report_date=latest_trade_date,
            ai_review_lines=ai_review_lines,
            profile_name=profile_name,
            profile_notes=profile_notes,
            stock_pool_notes=stock_pool_notes,
            rule_config=active_config,
            grouped_candidates=grouped_candidates,
            market_regime_label=market_regime_label,
            dynamic_adjustments=dynamic_adjustments,
        )

        allow_empty_report = os.getenv("RULE_SCREENER_ALLOW_EMPTY_REPORT", "true").strip().lower() == "true"
        should_send_empty_report = self.rule_config.notify_when_empty and allow_empty_report
        if send_notification and (display_candidates or should_send_empty_report):
            self.notifier.send(report)
        elif send_notification and self.rule_config.notify_when_empty and not display_candidates:
            logger.warning("规则选股为空且 RULE_SCREENER_ALLOW_EMPTY_REPORT=false，已跳过空报告推送。")

        return RuleScreeningRunResult(
            trade_date=latest_trade_date,
            candidates=display_candidates,
            ai_review_lines=ai_review_lines,
            report=report,
            profile_name=profile_name,
            profile_notes=profile_notes,
            stock_pool_notes=stock_pool_notes,
        )
