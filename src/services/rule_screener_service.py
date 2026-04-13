from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence

import pandas as pd
import requests

from data_provider.base import is_st_stock, normalize_stock_code
from src.config import Config, get_config
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
    min_volume_ratio: float = 1.5
    min_turnover_rate: float = 5.0
    min_sector_change_pct: float = 2.0
    max_bias_ma5_pct: float = 8.0
    ai_review_limit: int = 8
    sector_rank_top_n: int = 80
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
    abc_window_days: int,
    *,
    min_pullback_pct: float = 5.0,
    min_rebound_pct: float = 3.0,
    min_c_leg_pct: float = 2.0,
    min_c_retention_ratio: float = 0.90,
    rebreak_buffer_pct: float = 0.0,
) -> tuple[bool, float]:
    values = list(float(v) for v in closes if pd.notna(v))
    if len(values) < max(abc_window_days, 12):
        return False, 0.0

    window = values[-abc_window_days:]
    current = window[-1]
    if len(window) < 8:
        return False, 0.0

    search_end = max(len(window) - 8, 1)
    peak_index = max(range(search_end), key=lambda idx: window[idx])
    peak_price = window[peak_index]
    post_peak = window[peak_index + 1 :]
    if len(post_peak) < 6:
        return False, _compute_prior_rise_pct(values, len(values) - abc_window_days + peak_index)

    turning_points = list(range(1, len(window) - 1))
    local_mins = [
        idx for idx in turning_points
        if window[idx] <= window[idx - 1] and window[idx] <= window[idx + 1]
    ]
    local_maxs = [
        idx for idx in turning_points
        if window[idx] >= window[idx - 1] and window[idx] >= window[idx + 1]
    ]

    low_a_index = next((idx for idx in local_mins if idx > peak_index), None)
    if low_a_index is None:
        return False, _compute_prior_rise_pct(values, len(values) - abc_window_days + peak_index)
    rebound_index = next((idx for idx in local_maxs if idx > low_a_index), None)
    if rebound_index is None:
        return False, _compute_prior_rise_pct(values, len(values) - abc_window_days + peak_index)
    low_c_index = next((idx for idx in local_mins if idx > rebound_index), None)
    if low_c_index is None:
        return False, _compute_prior_rise_pct(values, len(values) - abc_window_days + peak_index)

    low_a_price = window[low_a_index]
    rebound_price = window[rebound_index]
    low_c_price = window[low_c_index]
    pullback_pct = (peak_price - low_a_price) / peak_price * 100 if peak_price else 0.0
    rebound_pct = (rebound_price - low_a_price) / low_a_price * 100 if low_a_price else 0.0
    c_leg_pct = (rebound_price - low_c_price) / rebound_price * 100 if rebound_price else 0.0

    confirmed = all(
        [
            pullback_pct >= min_pullback_pct,
            rebound_pct >= min_rebound_pct,
            c_leg_pct >= min_c_leg_pct,
            low_c_price >= low_a_price * min_c_retention_ratio,
            current >= rebound_price * (1 + rebreak_buffer_pct / 100.0),
            current > window[-2],
        ]
    )

    global_peak_index = len(values) - abc_window_days + peak_index
    prior_rise_pct = _compute_prior_rise_pct(values, global_peak_index)
    return confirmed, prior_rise_pct


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


def _build_candidate(
    group: pd.DataFrame,
    latest_turnover: Dict[str, float],
    sector_snapshot: Dict[str, List[Dict[str, Any]]],
    config: AshareRuleConfig,
) -> Optional[RuleScreeningCandidate]:
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
    bias_ma5_pct = float(latest["bias_ma5_pct"])
    volume_ratio = float(latest["volume_ratio"])
    turnover_rate = float(latest_turnover.get(code) or 0.0)
    sector_name, sector_change_pct = _pick_strong_sector(
        sector_snapshot=sector_snapshot,
        code=code,
        min_sector_change_pct=config.min_sector_change_pct,
    )
    abc_pattern_confirmed, prior_rise_pct = _detect_abc_pattern(
        group["close"].tolist(),
        abc_window_days=config.abc_window_days,
        min_pullback_pct=config.abc_min_pullback_pct,
        min_rebound_pct=config.abc_min_rebound_pct,
        min_c_leg_pct=config.abc_min_c_leg_pct,
        min_c_retention_ratio=config.abc_min_c_retention_ratio,
        rebreak_buffer_pct=config.abc_rebreak_buffer_pct,
    )

    checks = [
        close > ma20,
        ma5 > ma10 > ma20,
        bias_ma5_pct < config.max_bias_ma5_pct,
        volume_ratio >= config.min_volume_ratio,
        turnover_rate >= config.min_turnover_rate,
        sector_change_pct >= config.min_sector_change_pct,
        prior_rise_pct >= config.min_prior_rise_pct,
        abc_pattern_confirmed,
    ]
    if not all(checks):
        return None

    notes = [
        f"前高前累计涨幅 {prior_rise_pct:.1f}%",
        "ABC 调整后重新转强",
        f"收盘站上 MA20，现价 {close:.2f} / MA20 {ma20:.2f}",
        f"量比 {volume_ratio:.2f}，换手率 {turnover_rate:.2f}%",
        f"{sector_name} 涨幅 {sector_change_pct:.2f}%",
        f"MA5 乖离率 {bias_ma5_pct:.2f}% ，均线多头排列",
    ]
    name = str(latest.get("name") or code)

    return RuleScreeningCandidate(
        code=code,
        name=name,
        close=round(close, 2),
        ma5=round(ma5, 2),
        ma10=round(ma10, 2),
        ma20=round(ma20, 2),
        bias_ma5_pct=round(bias_ma5_pct, 2),
        volume_ratio=round(volume_ratio, 2),
        turnover_rate=round(turnover_rate, 2),
        sector_name=sector_name,
        sector_change_pct=round(sector_change_pct, 2),
        prior_rise_pct=round(prior_rise_pct, 2),
        abc_pattern_confirmed=abc_pattern_confirmed,
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
    min_sector_change_pct: float,
) -> Dict[str, List[Dict[str, Any]]]:
    snapshot: Dict[str, List[Dict[str, Any]]] = {normalize_stock_code(code): [] for code in candidate_codes}
    if index_member_df is None or index_member_df.empty or sw_daily_df is None or sw_daily_df.empty:
        return snapshot

    sector_df = sw_daily_df.copy()
    sector_df["ts_code"] = sector_df["ts_code"].astype(str)
    sector_df["pct_change"] = pd.to_numeric(sector_df["pct_change"], errors="coerce").fillna(0.0)
    sector_map = {
        row.ts_code: {
            "name": str(row.name),
            "change_pct": float(row.pct_change),
        }
        for row in sector_df.itertuples(index=False)
        if float(row.pct_change) >= min_sector_change_pct
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
                }
            )
        matched.sort(key=lambda item: item["change_pct"], reverse=True)
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


def apply_selection_rules(
    daily_history: pd.DataFrame,
    latest_turnover: Dict[str, float],
    sector_snapshot: Dict[str, List[Dict[str, Any]]],
    config: Optional[AshareRuleConfig] = None,
) -> List[RuleScreeningCandidate]:
    config = config or AshareRuleConfig()
    prepared = _prepare_indicator_frame(daily_history)
    if prepared.empty:
        return []

    candidates: List[RuleScreeningCandidate] = []
    for code, group in prepared.groupby("code"):
        candidate = _build_candidate(
            group=group,
            latest_turnover=latest_turnover,
            sector_snapshot=sector_snapshot,
            config=config,
        )
        if candidate is not None:
            candidates.append(candidate)

    return sorted(
        candidates,
        key=lambda item: (
            item.sector_change_pct,
            item.volume_ratio,
            item.turnover_rate,
            item.prior_rise_pct,
        ),
        reverse=True,
    )


def build_screening_report(
    candidates: Sequence[RuleScreeningCandidate],
    report_date: str,
    ai_review_lines: Optional[Sequence[str]] = None,
    profile_name: str = "严格版",
    profile_notes: Optional[Sequence[str]] = None,
    stock_pool_notes: Optional[Sequence[str]] = None,
    rule_config: Optional[AshareRuleConfig] = None,
) -> str:
    rule_config = rule_config or AshareRuleConfig()
    lines = [
        f"# A股规则选股日报 {report_date}",
        "",
        "## 筛选档位",
        f"- 本次结果：{profile_name}",
    ]
    if profile_notes:
        lines.extend(f"- {note}" for note in profile_notes)
    lines.extend([
        "",
        "## 策略条件",
        f"- 前期累计涨幅不少于 {rule_config.min_prior_rise_pct:.0f}%",
        "- 经过 ABC 式调整后再度转强",
        "- 收盘重新站上 20 日均线",
        f"- 量比 > {rule_config.min_volume_ratio:.1f}，换手率 > {rule_config.min_turnover_rate:.1f}%",
        f"- 所属板块涨幅 > {rule_config.min_sector_change_pct:.0f}%",
        f"- 5 日线乖离率 < {rule_config.max_bias_ma5_pct:.0f}%",
        "- MA5 > MA10 > MA20",
        "",
    ])

    if not candidates:
        lines.extend(["## 结果", "- 今日未筛出符合条件的A股股票。"])
        if stock_pool_notes:
            lines.extend(["", "## 自选池同步"])
            lines.extend(f"- {line}" for line in stock_pool_notes)
        return "\n".join(lines)

    lines.extend(
        [
            f"## 命中结果（{len(candidates)} 只）",
            "",
        ]
    )

    for idx, candidate in enumerate(candidates, start=1):
        lines.extend(
            [
                f"{idx}. {candidate.name} ({candidate.code})",
                f"   - 板块：{candidate.sector_name}（{candidate.sector_change_pct:+.2f}%）",
                f"   - 现价/MA5/MA10/MA20：{candidate.close:.2f} / {candidate.ma5:.2f} / {candidate.ma10:.2f} / {candidate.ma20:.2f}",
                f"   - 量比/换手率：{candidate.volume_ratio:.2f} / {candidate.turnover_rate:.2f}%",
                f"   - 前高前累计涨幅：{candidate.prior_rise_pct:.2f}%",
                f"   - 规则说明：{'；'.join(candidate.notes)}",
            ]
        )

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
            max_bias_ma5_pct=float(os.getenv("RULE_SCREENER_MAX_BIAS_MA5_PCT", "8")),
            ai_review_limit=int(os.getenv("RULE_SCREENER_AI_REVIEW_LIMIT", "8")),
            sector_rank_top_n=int(os.getenv("RULE_SCREENER_SECTOR_TOP_N", "80")),
            exclude_st=os.getenv("RULE_SCREENER_EXCLUDE_ST", "true").lower() != "false",
            allow_open_data_fallback=os.getenv("RULE_SCREENER_ALLOW_FALLBACK", "false").lower() == "true",
            auto_relax_if_empty=os.getenv("RULE_SCREENER_AUTO_RELAX_IF_EMPTY", "true").lower() != "false",
        )
        self.fetcher_manager = fetcher_manager or DataFetcherManager()
        self.tushare_fetcher = tushare_fetcher or self._resolve_tushare_fetcher()
        self.notifier = notifier or NotificationService()
        self.cache_dir = Path(os.getenv("RULE_SCREENER_CACHE_DIR", ".cache/rule_screener/tushare"))
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _resolve_tushare_fetcher(self) -> "TushareFetcher":
        from data_provider.tushare_fetcher import TushareFetcher

        for fetcher in getattr(self.fetcher_manager, "_fetchers", []):
            if isinstance(fetcher, TushareFetcher):
                return fetcher
        return TushareFetcher()

    def _cache_file(self, api_name: str, cache_key: str) -> Path:
        safe_key = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(cache_key))
        api_dir = self.cache_dir / api_name
        api_dir.mkdir(parents=True, exist_ok=True)
        return api_dir / f"{safe_key}.pkl"

    def _call_tushare_cached(self, api_name: str, *, cache_key: str, **kwargs) -> pd.DataFrame:
        cache_file = self._cache_file(api_name, cache_key)
        if cache_file.exists():
            return pd.read_pickle(cache_file)
        df = self.tushare_fetcher._call_api_with_rate_limit(api_name, **kwargs)
        cached_df = pd.DataFrame() if df is None else df
        cached_df.to_pickle(cache_file)
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
            return pd.read_pickle(cache_file)

        frames: List[pd.DataFrame] = []
        offset = 0
        while True:
            page_kwargs = dict(kwargs)
            page_kwargs.update({"offset": offset, "limit": page_size})
            df = self.tushare_fetcher._call_api_with_rate_limit(api_name, **page_kwargs)
            if df is None or df.empty:
                break
            frames.append(df)
            if len(df) < page_size:
                break
            offset += page_size

        merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        merged.to_pickle(cache_file)
        return merged

    def _build_relaxed_rule_config(self) -> AshareRuleConfig:
        return AshareRuleConfig(
            lookback_days=self.rule_config.lookback_days,
            abc_window_days=self.rule_config.abc_window_days + 5,
            min_prior_rise_pct=max(15.0, self.rule_config.min_prior_rise_pct - 2.0),
            min_volume_ratio=max(1.2, self.rule_config.min_volume_ratio - 0.2),
            min_turnover_rate=max(4.0, self.rule_config.min_turnover_rate - 1.0),
            min_sector_change_pct=max(1.0, self.rule_config.min_sector_change_pct - 1.0),
            max_bias_ma5_pct=self.rule_config.max_bias_ma5_pct + 1.0,
            ai_review_limit=self.rule_config.ai_review_limit,
            sector_rank_top_n=self.rule_config.sector_rank_top_n,
            notify_when_empty=self.rule_config.notify_when_empty,
            exclude_st=self.rule_config.exclude_st,
            allow_open_data_fallback=self.rule_config.allow_open_data_fallback,
            auto_relax_if_empty=False,
            abc_min_pullback_pct=max(4.0, self.rule_config.abc_min_pullback_pct - 1.0),
            abc_min_rebound_pct=max(2.0, self.rule_config.abc_min_rebound_pct - 1.0),
            abc_min_c_leg_pct=max(1.5, self.rule_config.abc_min_c_leg_pct - 0.5),
            abc_min_c_retention_ratio=max(0.85, self.rule_config.abc_min_c_retention_ratio - 0.05),
            abc_rebreak_buffer_pct=-0.2,
        )

    def _load_trade_dates(self) -> List[str]:
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
        return trade_dates[: self.rule_config.lookback_days]

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

    def _load_latest_turnover(self, trade_date: str) -> Dict[str, float]:
        df = self._call_tushare_cached(
            "daily_basic",
            cache_key=trade_date,
            trade_date=trade_date,
            fields="ts_code,turnover_rate",
        )
        if df is None or df.empty:
            logger.warning("daily_basic 返回为空，换手率规则将全部视作不满足")
            return {}
        df = df.copy()
        df["code"] = df["ts_code"].astype(str).str.split(".").str[0].map(normalize_stock_code)
        df["turnover_rate"] = pd.to_numeric(df["turnover_rate"], errors="coerce").fillna(0.0)
        return dict(zip(df["code"], df["turnover_rate"]))

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

    def _load_history_via_prefilter_fallback(self) -> tuple[pd.DataFrame, Dict[str, float], str]:
        snapshot = self._load_market_snapshot_fallback()
        filtered = snapshot[
            (snapshot["volume_ratio"] >= self.rule_config.min_volume_ratio)
            & (snapshot["turnover_rate"] >= self.rule_config.min_turnover_rate)
        ].copy()
        if filtered.empty:
            return pd.DataFrame(columns=["code", "trade_date", "open", "high", "low", "close", "volume", "name"]), {}, datetime.now().strftime("%Y%m%d")

        fetch_limit = int(os.getenv("RULE_SCREENER_HISTORY_FETCH_LIMIT", "120"))
        filtered = filtered.sort_values(["volume_ratio", "turnover_rate"], ascending=False).head(fetch_limit)

        frames: List[pd.DataFrame] = []
        latest_turnover = dict(zip(filtered["code"], filtered["turnover_rate"]))
        trade_date = datetime.now().strftime("%Y%m%d")

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
    ) -> Dict[str, List[Dict[str, Any]]]:
        if not candidate_codes:
            return {normalize_stock_code(code): [] for code in candidate_codes}
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
        if index_member_df is None or index_member_df.empty:
            logger.warning("index_member_all 返回为空，板块强度条件将无法命中")
            return {normalize_stock_code(code): [] for code in candidate_codes}
        if sw_daily_df is None or sw_daily_df.empty:
            logger.warning("sw_daily 返回为空，板块强度条件将无法命中")
            return {normalize_stock_code(code): [] for code in candidate_codes}
        return _build_sector_snapshot_from_tushare(
            index_member_df=index_member_df,
            sw_daily_df=sw_daily_df,
            candidate_codes=candidate_codes,
            trade_date=trade_date,
            min_sector_change_pct=sector_threshold,
        )

    def _select_technical_candidates(
        self,
        daily_history: pd.DataFrame,
        latest_turnover: Dict[str, float],
    ) -> List[str]:
        prepared = _prepare_indicator_frame(daily_history)
        candidate_codes: List[str] = []
        for code, group in prepared.groupby("code"):
            candidate = _build_candidate(
                group=group,
                latest_turnover=latest_turnover,
                sector_snapshot={code: [{"name": "placeholder", "change_pct": self.rule_config.min_sector_change_pct}]},
                config=self.rule_config,
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

    def run(
        self,
        *,
        send_notification: bool = True,
        ai_review: bool = True,
    ) -> RuleScreeningRunResult:
        try:
            trade_dates = self._load_trade_dates()
            latest_trade_date = trade_dates[0]
            stock_universe = self._load_stock_universe(min_list_date_cutoff=trade_dates[-1])
            daily_history = self._load_daily_history(trade_dates, stock_universe)
            latest_turnover = self._load_latest_turnover(latest_trade_date)
        except Exception as exc:
            if not self.rule_config.allow_open_data_fallback:
                raise
            logger.warning("Tushare 批量模式不可用，改走预筛降级链路: %s", exc)
            daily_history, latest_turnover, latest_trade_date = self._load_history_via_prefilter_fallback()

        active_config = self.rule_config
        profile_name = "严格版"
        profile_notes: List[str] = ["严格条件命中优先；仅当严格档为 0 只时，才会启用轻度放宽版。"]

        technical_candidate_codes = self._select_technical_candidates(
            daily_history=daily_history,
            latest_turnover=latest_turnover,
        )
        sector_snapshot = self._load_sector_snapshot(
            technical_candidate_codes,
            latest_trade_date,
            active_config.min_sector_change_pct,
        )
        candidates = apply_selection_rules(
            daily_history=daily_history[daily_history["code"].isin(technical_candidate_codes)],
            latest_turnover=latest_turnover,
            sector_snapshot=sector_snapshot,
            config=active_config,
        )

        if not candidates and self.rule_config.auto_relax_if_empty:
            active_config = self._build_relaxed_rule_config()
            profile_name = "轻度放宽版"
            profile_notes = [
                "严格条件当日筛出 0 只，已自动切换到轻度放宽版。",
                f"放宽项：前高前涨幅 >= {active_config.min_prior_rise_pct:.1f}%，量比 >= {active_config.min_volume_ratio:.1f}，换手率 >= {active_config.min_turnover_rate:.1f}%，板块涨幅 >= {active_config.min_sector_change_pct:.1f}%，MA5 乖离率 < {active_config.max_bias_ma5_pct:.1f}%。",
                "ABC 调整识别同步做了轻微放宽；该名单是候选观察池，不等于直接买入信号。",
            ]
            technical_candidate_codes = []
            prepared = _prepare_indicator_frame(daily_history)
            for code, group in prepared.groupby("code"):
                candidate = _build_candidate(
                    group=group,
                    latest_turnover=latest_turnover,
                    sector_snapshot={code: [{"name": "placeholder", "change_pct": active_config.min_sector_change_pct}]},
                    config=active_config,
                )
                if candidate is not None:
                    technical_candidate_codes.append(code)
            sector_snapshot = self._load_sector_snapshot(
                technical_candidate_codes,
                latest_trade_date,
                active_config.min_sector_change_pct,
            )
            candidates = apply_selection_rules(
                daily_history=daily_history[daily_history["code"].isin(technical_candidate_codes)],
                latest_turnover=latest_turnover,
                sector_snapshot=sector_snapshot,
                config=active_config,
            )

        ai_review_lines: List[str] = []
        if candidates and ai_review:
            from src.core.pipeline import StockAnalysisPipeline

            review_codes = [candidate.code for candidate in candidates[: active_config.ai_review_limit]]
            pipeline = StockAnalysisPipeline(
                config=self.config,
                max_workers=min(2, len(review_codes)) or 1,
            )
            ai_results = pipeline.run(
                stock_codes=review_codes,
                dry_run=False,
                send_notification=False,
                merge_notification=False,
            )
            ai_review_lines = self._build_ai_review_lines(ai_results)

        stock_pool_notes = self._sync_candidates_to_stock_pool(candidates) if self._should_sync_stock_pool(
            send_notification=send_notification
        ) else []

        report = build_screening_report(
            candidates=candidates,
            report_date=latest_trade_date,
            ai_review_lines=ai_review_lines,
            profile_name=profile_name,
            profile_notes=profile_notes,
            stock_pool_notes=stock_pool_notes,
            rule_config=active_config,
        )

        if send_notification and (candidates or self.rule_config.notify_when_empty):
            self.notifier.send(report)

        return RuleScreeningRunResult(
            trade_date=latest_trade_date,
            candidates=candidates,
            ai_review_lines=ai_review_lines,
            report=report,
            profile_name=profile_name,
            profile_notes=profile_notes,
            stock_pool_notes=stock_pool_notes,
        )
