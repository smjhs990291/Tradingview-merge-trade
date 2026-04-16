import argparse
import io
import math
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import numpy as np

try:
    import tkinter as tk
    from tkinter import filedialog
except Exception:  # pragma: no cover
    tk = None
    filedialog = None


@dataclass(frozen=True)
class FileMeta:
    source_file: str
    symbol: Optional[str]
    timeframe: Optional[str]
    point_value: Optional[float]
    initial_capital: Optional[float]


TRADE_SHEET_NAMES = ["交易清單", "List of trades"]
ATTR_SHEET_NAMES = ["屬性", "Properties"]
PERF_SHEET_NAMES = ["績效", "Performance"]


def _timeframe_to_minutes(timeframe: Optional[str]) -> Optional[int]:
    if not timeframe:
        return None

    tf = str(timeframe).strip()
    m = re.search(r"(\d+)", tf)
    if not m:
        return None

    n = int(m.group(1))
    if "分鐘" in tf or "minutes" in tf:
        return n
    if "小時" in tf or "hours" in tf:
        return n * 60
    if "天" in tf or "days" in tf:
        return n * 60 * 24
    return None


def _safe_float(v) -> Optional[float]:
    try:
        if pd.isna(v):
            return None
        return float(v)
    except Exception:
        return None


def _read_first_available_sheet(xlsx_path: str, sheet_names: List[str]) -> pd.DataFrame:
    try:
        xl = pd.ExcelFile(xlsx_path)
        available = set(xl.sheet_names)
        for name in sheet_names:
            if name in available:
                return pd.read_excel(xl, sheet_name=name)
    except Exception:
        pass
    return pd.DataFrame()


def _read_attributes(xlsx_path: str) -> Dict[str, object]:
    df = _read_first_available_sheet(xlsx_path, ATTR_SHEET_NAMES)
    if "name" not in df.columns or "value" not in df.columns:
        return {}

    df = df[["name", "value"]].copy()
    df["name"] = df["name"].astype(str).str.strip()

    result: Dict[str, object] = {}
    for _, row in df.iterrows():
        k = str(row["name"]).strip()
        result[k] = row["value"]
    return result


def _read_initial_capital(xlsx_path: str) -> Optional[float]:
    df = _read_first_available_sheet(xlsx_path, PERF_SHEET_NAMES)
    if "Unnamed: 0" not in df.columns:
        return None

    label_series = df["Unnamed: 0"].astype(str).str.strip()
    row = df.loc[label_series.isin(["初始資本", "初始資金", "Initial capital", "Initial equity"])]
    if row.empty:
        return None

    for candidate_col in ["全部 USD", "全部USD", "全部", "All USD", "AllUSD", "All"]:
        if candidate_col in df.columns:
            return _safe_float(row.iloc[0][candidate_col])

    for col in df.columns:
        if "USD" in str(col):
            return _safe_float(row.iloc[0][col])

    return None


def read_file_meta(xlsx_path: str) -> FileMeta:
    attrs = _read_attributes(xlsx_path)

    # Try Chinese keys then English keys
    symbol = attrs.get("商品") or attrs.get("Symbol")
    symbol = str(symbol).strip() if symbol is not None and not pd.isna(symbol) else None

    timeframe = attrs.get("時間週期") or attrs.get("Timeframe")
    timeframe = str(timeframe).strip() if timeframe is not None and not pd.isna(timeframe) else None

    point_value = _safe_float(attrs.get("點值") or attrs.get("Point value"))
    initial_capital = _read_initial_capital(xlsx_path)

    return FileMeta(
        source_file=os.path.basename(xlsx_path),
        symbol=symbol,
        timeframe=timeframe,
        point_value=point_value,
        initial_capital=initial_capital,
    )


def read_trade_list(xlsx_path: str, meta: FileMeta) -> pd.DataFrame:
    df = _read_first_available_sheet(xlsx_path, TRADE_SHEET_NAMES)
    df = df.copy()

    df.columns = [str(c).strip() for c in df.columns]

    rename_map = {
        "Date and time": "日期/時間",
        "Date & time": "日期/時間",
        "Datetime": "日期/時間",
        "MFE USD": "上漲幅度 USD",
        "MFE %": "上漲幅度 %",
        "MAE USD": "回撤 USD",
        "MAE %": "回撤 %",
        "累 積損益 %": "累積損益 %",
        "累積損益%": "累積損益 %",
        # English mappings
        "Trade #": "交易 #",
        "Type": "類型",
        "Signal": "訊號",
        "Price USD": "價格 USD",
        "Position size (qty)": "持倉大小(數量)",
        "Position size (value)": "持倉大小(值)",
        "Net P&L USD": "淨損益 USD",
        "Net P&L %": "淨損益 %",
        "Favorable excursion USD": "上漲幅度 USD",
        "Favorable excursion %": "上漲幅度 %",
        "Adverse excursion USD": "回撤 USD",
        "Adverse excursion %": "回撤 %",
        "Cumulative P&L USD": "累積損益 USD",
        "Cumulative P&L %": "累積損益 %"
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    if "日期/時間" in df.columns:
        df["日期/時間"] = pd.to_datetime(df["日期/時間"], errors="coerce")

    numeric_cols = [
        "價格 USD",
        "持倉大小(數量)",
        "持倉大小(值)",
        "淨損益 USD",
        "淨損益 %",
        "上漲幅度 USD",
        "上漲幅度 %",
        "回撤 USD",
        "回撤 %",
        "累積損益 USD",
        "累積損益 %",
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df.insert(0, "來源檔案", meta.source_file)
    df.insert(1, "商品", meta.symbol)
    df.insert(2, "時間週期", meta.timeframe)
    df.insert(3, "點值", meta.point_value)
    df.insert(4, "初始資本", meta.initial_capital)

    if "交易 #" in df.columns:
        df.insert(5, "交易鍵", df["商品"].astype(str) + "#" + df["交易 #"].astype(str))
    else:
        df.insert(5, "交易鍵", df["商品"].astype(str) + "#")

    return df


def is_closed_trade_row(trade_type: object) -> bool:
    if trade_type is None or pd.isna(trade_type):
        return False
    t = str(trade_type)
    return "出場" in t or "Exit" in t


def is_entry_trade_row(trade_type: object) -> bool:
    if trade_type is None or pd.isna(trade_type):
        return False
    t = str(trade_type)
    return "進場" in t or "Entry" in t


def trade_direction(trade_type: object) -> Optional[str]:
    if trade_type is None or pd.isna(trade_type):
        return None
    t = str(trade_type).lower()
    if "做多" in t or "long" in t:
        return "long"
    if "做空" in t or "short" in t:
        return "short"
    return None


def _format_days(delta: pd.Timedelta) -> str:
    if pd.isna(delta):
        return ""
    days = int(max(0, math.floor(delta.total_seconds() / 86400)))
    return f"{days}天"


def _equity_curve_from_closed_trades(closed_df: pd.DataFrame, initial_capital: float) -> pd.DataFrame:
    g = closed_df.copy()
    if "日期/時間" not in g.columns:
        return pd.DataFrame(columns=["日期/時間", "pnl", "equity"])

    g = g.copy()
    g["_row_order"] = range(len(g))
    g["日期/時間"] = pd.to_datetime(g["日期/時間"], errors="coerce")
    g = g.sort_values(["日期/時間", "_row_order"], na_position="last")
    if g["日期/時間"].notna().any():
        base_time = pd.Timestamp(g["日期/時間"].dropna().max())
    else:
        base_time = pd.Timestamp("1970-01-01")

    missing_mask = g["日期/時間"].isna()
    if missing_mask.any():
        # Fill missing timestamps with synthetic, strictly increasing timestamps.
        # This ensures these trades are not silently excluded from drawdown/runup.
        fill_times = [base_time + pd.Timedelta(seconds=i + 1) for i in range(int(missing_mask.sum()))]
        g.loc[missing_mask, "日期/時間"] = fill_times

    g["pnl"] = pd.to_numeric(g.get("淨損益 USD"), errors="coerce").fillna(0.0)

    # Multiple trades can share the same timestamp. Aggregate pnl by timestamp
    # to guarantee a unique DatetimeIndex for resampling/risk metrics.
    pnl_by_time = g.groupby("日期/時間", dropna=False)["pnl"].sum().sort_index()
    equity = pnl_by_time.cumsum() + float(initial_capital)
    out = pd.DataFrame({"日期/時間": pnl_by_time.index, "pnl": pnl_by_time.values, "equity": equity.values})
    return out


def _sharpe_sortino_from_equity(equity_curve: pd.DataFrame) -> Tuple[Optional[float], Optional[float]]:
    if equity_curve.empty:
        return None, None

    s = equity_curve.set_index("日期/時間")["equity"].sort_index()
    if s.index.has_duplicates:
        s = s.groupby(level=0).last()
    daily = s.resample("1D").ffill()
    rets = daily.pct_change().dropna()
    if rets.empty:
        return None, None

    mean = float(rets.mean())
    std = float(rets.std(ddof=0))
    if std == 0:
        sharpe = None
    else:
        sharpe = (mean / std) * math.sqrt(365)

    downside = rets[rets < 0]
    if downside.empty:
        sortino = None
    else:
        dstd = float(downside.std(ddof=0))
        sortino = None if dstd == 0 else (mean / dstd) * math.sqrt(365)

    return sharpe, sortino


def _drawdown_episodes(equity_curve: pd.DataFrame) -> Tuple[List[Dict[str, object]], float, float]:
    if equity_curve.empty:
        return [], 0.0, 0.0

    s = equity_curve.set_index("日期/時間")["equity"].sort_index()
    peak_val = float(s.iloc[0])
    peak_time = s.index[0]
    in_dd = False
    dd_min_val = peak_val
    dd_min_time = peak_time
    episodes: List[Dict[str, object]] = []
    max_dd_val = 0.0
    max_dd_pct = 0.0

    for t, v in s.items():
        v = float(v)
        if v >= peak_val:
            if in_dd:
                depth = peak_val - dd_min_val
                episodes.append(
                    {
                        "peak_time": peak_time,
                        "recover_time": t,
                        "depth": depth,
                        "peak_equity": peak_val,
                        "duration": t - peak_time,
                    }
                )
                if depth > max_dd_val:
                    max_dd_val = depth
                    # CORRECTIVE: Relative drawdown calculation
                    max_dd_pct = (depth / peak_val * 100.0) if peak_val > 0 else 0.0
            peak_val = v
            peak_time = t
            in_dd = False
            dd_min_val = v
            dd_min_time = t
            continue

        if not in_dd:
            in_dd = True
            dd_min_val = v
            dd_min_time = t
        else:
            if v < dd_min_val:
                dd_min_val = v
                dd_min_time = t

    if in_dd:
        depth = peak_val - dd_min_val
        episodes.append(
            {
                "peak_time": peak_time,
                "recover_time": s.index[-1],
                "depth": depth,
                "peak_equity": peak_val,
                "duration": s.index[-1] - peak_time,
            }
        )
        if depth > max_dd_val:
            max_dd_val = depth
            # CORRECTIVE: Relative drawdown calculation
            max_dd_pct = (depth / peak_val * 100.0) if peak_val > 0 else 0.0

    return episodes, float(max_dd_val), float(max_dd_pct)


def _runup_episodes(equity_curve: pd.DataFrame) -> Tuple[List[Dict[str, object]], float, float]:
    if equity_curve.empty:
        return [], 0.0, 0.0

    s = equity_curve.set_index("日期/時間")["equity"].sort_index()
    trough_val = float(s.iloc[0])
    trough_time = s.index[0]
    peak_val = trough_val
    peak_time = trough_time
    episodes: List[Dict[str, object]] = []
    max_ru_val = 0.0
    max_ru_pct = 0.0

    for t, v in s.items():
        v = float(v)
        if v >= peak_val:
            peak_val = v
            peak_time = t

        if v < trough_val:
            if peak_val > trough_val:
                rise = peak_val - trough_val
                episodes.append(
                    {
                        "trough_time": trough_time,
                        "peak_time": peak_time,
                        "rise": rise,
                        "duration": peak_time - trough_time,
                        "peak_equity": peak_val,
                        "trough_equity": trough_val,
                    }
                )
                if rise > max_ru_val:
                    max_ru_val = rise
                    max_ru_pct = (rise / trough_val * 100.0) if trough_val != 0 else 0.0
            trough_val = v
            trough_time = t
            peak_val = v
            peak_time = t

    if peak_val > trough_val:
        rise = peak_val - trough_val
        episodes.append(
            {
                "trough_time": trough_time,
                "peak_time": peak_time,
                "rise": rise,
                "duration": peak_time - trough_time,
                "peak_equity": peak_val,
                "trough_equity": trough_val,
            }
        )
        if rise > max_ru_val:
            max_ru_val = rise
            max_ru_pct = (rise / trough_val * 100.0) if trough_val != 0 else 0.0

    return episodes, float(max_ru_val), float(max_ru_pct)


def _buy_and_hold_from_prices(trades_df: pd.DataFrame, initial_capital: float) -> Tuple[Optional[float], Optional[float]]:
    if trades_df.empty:
        return None, None
    if "日期/時間" not in trades_df.columns or "價格 USD" not in trades_df.columns:
        return None, None
    g = trades_df.dropna(subset=["日期/時間"]).sort_values("日期/時間")
    if g.empty:
        return None, None
    first_price = pd.to_numeric(g.iloc[0]["價格 USD"], errors="coerce")
    last_price = pd.to_numeric(g.iloc[-1]["價格 USD"], errors="coerce")
    if pd.isna(first_price) or pd.isna(last_price) or float(first_price) == 0.0:
        return None, None
    pct = (float(last_price) / float(first_price) - 1.0) * 100.0
    usd = float(initial_capital) * (pct / 100.0)
    return usd, pct


def _compute_trade_bars(trades_df: pd.DataFrame) -> pd.Series:
    if trades_df.empty:
        return pd.Series(dtype="float64")
    if "日期/時間" not in trades_df.columns or "類型" not in trades_df.columns:
        return pd.Series([float("nan")] * len(trades_df), index=trades_df.index)

    entries = trades_df[trades_df["類型"].apply(is_entry_trade_row)].copy()
    exits = trades_df[trades_df["類型"].apply(is_closed_trade_row)].copy()

    entry_time_by_key = entries.dropna(subset=["交易鍵", "日期/時間"]).groupby("交易鍵")["日期/時間"].min()
    minutes_by_key = (
        trades_df.dropna(subset=["交易鍵"]).groupby("交易鍵")["時間週期"].first().apply(_timeframe_to_minutes)
    )

    bars = pd.Series([float("nan")] * len(exits), index=exits.index, dtype="float64")
    for idx, row in exits.iterrows():
        key = row.get("交易鍵")
        exit_time = row.get("日期/時間")
        entry_time = entry_time_by_key.get(key)
        tf_min = minutes_by_key.get(key)
        if pd.isna(exit_time) or entry_time is None or pd.isna(entry_time) or tf_min is None or tf_min <= 0:
            continue
        delta_min = (pd.Timestamp(exit_time) - pd.Timestamp(entry_time)).total_seconds() / 60.0
        bars.loc[idx] = math.floor(delta_min / tf_min) + 1

    return bars


def _subset_stats(exit_df: pd.DataFrame, initial_capital: float) -> Dict[str, object]:
    pnl = pd.to_numeric(exit_df.get("淨損益 USD"), errors="coerce").fillna(0.0)
    pnl_pct = pd.to_numeric(exit_df.get("淨損益 %"), errors="coerce")

    gross_profit = float(pnl[pnl > 0].sum())
    gross_loss = float(abs(pnl[pnl < 0].sum()))
    net_profit = float(pnl.sum())
    trades = int(len(exit_df))
    wins = int((pnl > 0).sum())
    losses = int((pnl < 0).sum())
    win_rate = (wins / trades * 100.0) if trades else 0.0

    avg_trade = (net_profit / trades) if trades else 0.0
    avg_trade_pct = float(pnl_pct.mean()) if trades and pnl_pct is not None else float("nan")

    avg_win = float(pnl[pnl > 0].mean()) if wins else 0.0
    avg_win_pct = float(pnl_pct[pnl > 0].mean()) if wins else float("nan")

    avg_loss = float(abs(pnl[pnl < 0].mean())) if losses else 0.0
    avg_loss_pct = float(abs(pnl_pct[pnl < 0].mean())) if losses else float("nan")

    win_loss_ratio = (avg_win / avg_loss) if avg_loss else (float("inf") if avg_win else 0.0)
    profit_factor = (gross_profit / gross_loss) if gross_loss else (float("inf") if gross_profit else 0.0)

    max_win = float(pnl.max()) if trades else 0.0
    max_loss = float(abs(pnl.min())) if trades else 0.0
    max_win_pct = float(pnl_pct.max()) if trades and pnl_pct is not None else float("nan")
    max_loss_pct = float(abs(pnl_pct.min())) if trades and pnl_pct is not None else float("nan")

    # POSITION-VALUE BASED METRICS (Prop Firm Efficiency)
    # Formula: (Net P&L / Position Value) * 100
    pos_val_raw = exit_df.get("持倉大小(值)")
    if pos_val_raw is not None and isinstance(pos_val_raw, pd.Series):
        pos_val = pd.to_numeric(pos_val_raw, errors="coerce")
        if not pos_val.isna().all():
            # Avoid division by zero
            valid_pos = (pos_val != 0)
            pnl_val_pct = (pnl[valid_pos] / pos_val[valid_pos] * 100.0)
            avg_pos_efficiency = float(pnl_val_pct.mean()) if not pnl_val_pct.empty else 0.0
        else:
            avg_pos_efficiency = float("nan")
    else:
        avg_pos_efficiency = float("nan")

    return {
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "net_profit": net_profit,
        "net_profit_pct": (net_profit / initial_capital * 100.0) if initial_capital else float("nan"),
        "gross_profit": gross_profit,
        "gross_profit_pct": (gross_profit / initial_capital * 100.0) if initial_capital else float("nan"),
        "gross_loss": gross_loss,
        "gross_loss_pct": (gross_loss / initial_capital * 100.0) if initial_capital else float("nan"),
        "win_rate": win_rate,
        "avg_trade": avg_trade,
        "avg_trade_pct": avg_trade_pct,
        "avg_win": avg_win,
        "avg_win_pct": avg_win_pct,
        "avg_loss": avg_loss,
        "avg_loss_pct": avg_loss_pct,
        "win_loss_ratio": win_loss_ratio,
        "profit_factor": profit_factor,
        "max_win": max_win,
        "max_win_pct": max_win_pct,
        "max_loss": max_loss,
        "max_loss_pct": max_loss_pct,
        "avg_pos_efficiency": avg_pos_efficiency, # NEW
    }


def build_report_tables(
    merged_trades: pd.DataFrame,
    base_attrs: Optional[pd.DataFrame],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    if merged_trades.empty:
        empty = pd.DataFrame()
        return empty, empty, empty, empty, empty

    original_trade_cols = [
        "交易 #",
        "類型",
        "日期/時間",
        "訊號",
        "價格 USD",
        "持倉大小(數量)",
        "持倉大小(值)",
        "淨損益 USD",
        "淨損益 %",
        "上漲幅度 USD",
        "上漲幅度 %",
        "回撤 USD",
        "回撤 %",
        "累積損益 USD",
        "累積損益 %",
    ]

    trade_list_out = merged_trades.copy()
    if "日期/時間" in trade_list_out.columns:
        trade_list_out["日期/時間"] = pd.to_datetime(trade_list_out["日期/時間"], errors="coerce")
    
    # Global sort to ensure the final list is strictly chronological
    trade_list_out = trade_list_out.sort_values("日期/時間", na_position="last").copy()
    
    if "交易 #" in trade_list_out.columns and "商品" in trade_list_out.columns:
        trade_list_out["交易 #"] = trade_list_out["商品"].astype(str) + "-" + trade_list_out["交易 #"].astype(str)
    
    # Recompute cumulative metrics for the Excel tab to avoid "Reset" jumps
    temp_initial_capital = float(pd.to_numeric(merged_trades.get("初始資本"), errors="coerce").dropna().unique().sum())
    if not temp_initial_capital:
        temp_initial_capital = float(pd.to_numeric(merged_trades.get("初始資本"), errors="coerce").fillna(0.0).max())
        
    if "淨損益 USD" in trade_list_out.columns:
        pnl_series = pd.to_numeric(trade_list_out["淨損益 USD"], errors="coerce").fillna(0.0)
        trade_list_out["累積損益 USD"] = pnl_series.cumsum()
        if temp_initial_capital:
            trade_list_out["累積損益 %"] = (trade_list_out["累積損益 USD"] / temp_initial_capital * 100.0)

    trade_list_out = trade_list_out[[c for c in original_trade_cols if c in trade_list_out.columns]].copy()

    entries = merged_trades[merged_trades["類型"].apply(is_entry_trade_row)].copy()
    exits = merged_trades[merged_trades["類型"].apply(is_closed_trade_row)].copy()
    if "日期/時間" in exits.columns:
        exits["日期/時間"] = pd.to_datetime(exits["日期/時間"], errors="coerce")

    open_keys = set(entries.get("交易鍵", pd.Series(dtype=str)).dropna().unique()) - set(
        exits.get("交易鍵", pd.Series(dtype=str)).dropna().unique()
    )
    initial_capital = float(pd.to_numeric(merged_trades.get("初始資本"), errors="coerce").dropna().unique().sum())
    if not initial_capital:
        initial_capital = float(pd.to_numeric(merged_trades.get("初始資本"), errors="coerce").fillna(0.0).max())
    open_entries = entries[entries.get("交易鍵").isin(open_keys)].copy() if open_keys else pd.DataFrame()
    unrealized = float(pd.to_numeric(open_entries.get("淨損益 USD"), errors="coerce").fillna(0.0).sum()) if not open_entries.empty else 0.0
    unrealized_pct = (unrealized / initial_capital * 100.0) if initial_capital else float("nan")
    open_trades = int(len(open_keys))

    exits = exits.copy()
    if "日期/時間" in exits.columns:
        exits["日期/時間"] = pd.to_datetime(exits["日期/時間"], errors="coerce")
    
    # GLOBAL RE-SORT to fix temporal continuity
    exits = exits.sort_values("日期/時間", na_position="last").copy()
    
    # RE-CALCULATE CUMULATIVE METRICS (Fix Reset Issue)
    pnl_usd = pd.to_numeric(exits.get("淨損益 USD"), errors="coerce").fillna(0.0)
    pos_value = pd.to_numeric(exits.get("持倉大小(值)"), errors="coerce")
    
    exits["點值"] = pd.to_numeric(exits.get("點值"), errors="coerce").fillna(0.0)
    exits["累積損益 USD"] = pnl_usd.cumsum()
    exits["累積損益 %"] = (exits["累積損益 USD"] / initial_capital * 100.0) if initial_capital else 0.0
    
    # RE-CALCULATE Trade-level % based on Position Value (User Request)
    if pos_value is not None:
        exits["淨損益 %"] = (pnl_usd / pos_value * 100.0).fillna(0.0)

    exits["方向"] = exits["類型"].apply(trade_direction)
    exit_all = exits
    exit_long = exits[exits["方向"] == "long"]
    exit_short = exits[exits["方向"] == "short"]

    stats_all = _subset_stats(exit_all, initial_capital)
    stats_long = _subset_stats(exit_long, initial_capital)
    stats_short = _subset_stats(exit_short, initial_capital)

    if "持倉大小(數量)" in merged_trades.columns:
        contracts_series = pd.to_numeric(merged_trades["持倉大小(數量)"], errors="coerce")
        max_contracts_all = float(contracts_series.abs().max())
        
        trade_types = merged_trades.get("類型", pd.Series(dtype=str)).astype(str)
        max_contracts_long = float(contracts_series[trade_types.str.contains("做多", na=False)].abs().max())
        max_contracts_short = float(contracts_series[trade_types.str.contains("做空", na=False)].abs().max())
    else:
        max_contracts_all = float("nan")
        max_contracts_long = float("nan")
        max_contracts_short = float("nan")

    bnh_usd_total = 0.0
    bnh_cap_total = 0.0
    for sym, g in merged_trades.groupby("商品", dropna=False):
        cap = float(pd.to_numeric(g.get("初始資本"), errors="coerce").dropna().max() or 0.0)
        if cap <= 0:
            continue
        usd, _pct = _buy_and_hold_from_prices(g, cap)
        if usd is None:
            continue
        bnh_usd_total += float(usd)
        bnh_cap_total += cap
    bnh_pct_total = (bnh_usd_total / bnh_cap_total * 100.0) if bnh_cap_total else float("nan")

    equity_curve = _equity_curve_from_closed_trades(exit_all, initial_capital)
    dd_episodes, max_dd_usd, max_dd_pct = _drawdown_episodes(equity_curve)
    ru_episodes, max_ru_usd, max_ru_pct = _runup_episodes(equity_curve)

    avg_dd_usd = float(pd.Series([e["depth"] for e in dd_episodes]).mean()) if dd_episodes else 0.0
    avg_dd_dur = _format_days(pd.Series([e["duration"] for e in dd_episodes]).mean()) if dd_episodes else "0天"
    avg_dd_pct = (
        float(
            pd.Series(
                [
                    (float(e["depth"]) / float(e["peak_equity"]) * 100.0)
                    for e in dd_episodes
                    if e.get("peak_equity")
                ]
            ).mean()
        )
        if dd_episodes
        else float("nan")
    )

    avg_ru_usd = float(pd.Series([e["rise"] for e in ru_episodes]).mean()) if ru_episodes else 0.0
    avg_ru_dur = _format_days(pd.Series([e["duration"] for e in ru_episodes]).mean()) if ru_episodes else "0天"
    avg_ru_pct = float(pd.Series([e["rise"] / e.get("trough_equity", float("nan")) * 100.0 for e in ru_episodes if e.get("trough_equity")]).mean()) if ru_episodes else float("nan")

    missing_exit_datetimes = int(exit_all["日期/時間"].isna().sum()) if "日期/時間" in exit_all.columns else int(len(exit_all))
    if "日期/時間" in exit_all.columns and exit_all["日期/時間"].notna().any():
        exit_dt_min = exit_all["日期/時間"].min()
        exit_dt_max = exit_all["日期/時間"].max()
        exit_dt_range_str = f"{exit_dt_min} — {exit_dt_max}"
    else:
        exit_dt_range_str = ""

    if "商品" in exit_all.columns and "日期/時間" in exit_all.columns:
        missing_by_symbol = (
            exit_all.groupby("商品", dropna=False)["日期/時間"].apply(lambda s: int(s.isna().sum())).to_dict()
        )
        missing_by_symbol_str = ", ".join([f"{k}:{v}" for k, v in missing_by_symbol.items()])
    else:
        missing_by_symbol_str = ""

    # Sharpe/Sortino require a real time series; if timestamps are incomplete, do not compute.
    if missing_exit_datetimes > 0:
        sharpe, sortino = None, None
    else:
        sharpe, sortino = _sharpe_sortino_from_equity(equity_curve)

    bars = _compute_trade_bars(merged_trades)
    exit_all_bars = bars.reindex(exit_all.index)
    exit_win_bars = exit_all_bars[pd.to_numeric(exit_all.get("淨損益 USD"), errors="coerce") > 0]
    exit_loss_bars = exit_all_bars[pd.to_numeric(exit_all.get("淨損益 USD"), errors="coerce") < 0]

    avg_bars_all = float(exit_all_bars.mean()) if not exit_all_bars.empty else float("nan")
    avg_bars_win = float(exit_win_bars.mean()) if not exit_win_bars.empty else float("nan")
    avg_bars_loss = float(exit_loss_bars.mean()) if not exit_loss_bars.empty else float("nan")

    exit_long_bars = bars.reindex(exit_long.index)
    exit_short_bars = bars.reindex(exit_short.index)

    perf_rows = [
        ("初始資本", initial_capital, float("nan"), float("nan"), float("nan"), float("nan"), float("nan")),
        ("未實現盈虧", unrealized, unrealized_pct, float("nan"), float("nan"), float("nan"), float("nan")),
        ("淨利", stats_all["net_profit"], stats_all["net_profit_pct"], stats_long["net_profit"], stats_long["net_profit_pct"], stats_short["net_profit"], stats_short["net_profit_pct"]),
        ("毛利", stats_all["gross_profit"], stats_all["gross_profit_pct"], stats_long["gross_profit"], stats_long["gross_profit_pct"], stats_short["gross_profit"], stats_short["gross_profit_pct"]),
        ("總損失", stats_all["gross_loss"], stats_all["gross_loss_pct"], stats_long["gross_loss"], stats_long["gross_loss_pct"], stats_short["gross_loss"], stats_short["gross_loss_pct"]),
        ("已支付佣金", 0.0, float("nan"), 0.0, float("nan"), 0.0, float("nan")),
        ("買入並持有報酬率", bnh_usd_total if bnh_cap_total else float("nan"), bnh_pct_total, float("nan"), float("nan"), float("nan"), float("nan")),
        ("最大持倉合約數", max_contracts_all, float("nan"), max_contracts_long, float("nan"), max_contracts_short, float("nan")),
        ("平均股票上漲持續時間", avg_ru_dur, float("nan"), float("nan"), float("nan"), float("nan"), float("nan")),
        ("平均股票上漲", avg_ru_usd, avg_ru_pct, float("nan"), float("nan"), float("nan"), float("nan")),
        ("最大資產漲幅", max_ru_usd, max_ru_pct, float("nan"), float("nan"), float("nan"), float("nan")),
        ("平均股票回撤持續時間", avg_dd_dur, float("nan"), float("nan"), float("nan"), float("nan"), float("nan")),
        ("平均股票回撤", avg_dd_usd, avg_dd_pct, float("nan"), float("nan"), float("nan"), float("nan")),
        ("最大資產回撤", max_dd_usd, max_dd_pct, float("nan"), float("nan"), float("nan"), float("nan")),
    ]

    perf = pd.DataFrame(
        perf_rows,
        columns=["Unnamed: 0", "全部 USD", "全部 %", "看多 USD", "看多 %", "看空 USD", "看空 %"],
    )

    analysis_rows = [
        ("總交易量", stats_all["trades"], float("nan"), stats_long["trades"], float("nan"), stats_short["trades"], float("nan")),
        ("總未平倉交易", open_trades, float("nan"), float("nan"), float("nan"), float("nan"), float("nan")),
        ("獲利交易", stats_all["wins"], float("nan"), stats_long["wins"], float("nan"), stats_short["wins"], float("nan")),
        ("虧損交易", stats_all["losses"], float("nan"), stats_long["losses"], float("nan"), stats_short["losses"], float("nan")),
        ("勝率", float("nan"), stats_all["win_rate"], float("nan"), stats_long["win_rate"], float("nan"), stats_short["win_rate"]),
        ("平均盈虧", stats_all["avg_trade"], stats_all["avg_trade_pct"], stats_long["avg_trade"], stats_long["avg_trade_pct"], stats_short["avg_trade"], stats_short["avg_trade_pct"]),
        ("平均獲利交易", stats_all["avg_win"], stats_all["avg_win_pct"], stats_long["avg_win"], stats_long["avg_win_pct"], stats_short["avg_win"], stats_short["avg_win_pct"]),
        ("平均虧損交易", stats_all["avg_loss"], stats_all["avg_loss_pct"], stats_long["avg_loss"], stats_long["avg_loss_pct"], stats_short["avg_loss"], stats_short["avg_loss_pct"]),
        ("平均獲利/平均虧損", stats_all["win_loss_ratio"], float("nan"), stats_long["win_loss_ratio"], float("nan"), stats_short["win_loss_ratio"], float("nan")),
        ("最大獲利交易", stats_all["max_win"], float("nan"), stats_long["max_win"], float("nan"), stats_short["max_win"], float("nan")),
        ("最大獲利交易百分比", float("nan"), stats_all["max_win_pct"], float("nan"), stats_long["max_win_pct"], float("nan"), stats_short["max_win_pct"]),
        ("最大虧損交易", stats_all["max_loss"], float("nan"), stats_long["max_loss"], float("nan"), stats_short["max_loss"], float("nan")),
        ("最大虧損交易百分比", float("nan"), stats_all["max_loss_pct"], float("nan"), stats_long["max_loss_pct"], float("nan"), stats_short["max_loss_pct"]),
        ("交易的平均K線數", avg_bars_all, float("nan"), float(exit_long_bars.mean()) if not exit_long_bars.empty else float("nan"), float("nan"), float(exit_short_bars.mean()) if not exit_short_bars.empty else float("nan"), float("nan")),
        ("獲利交易的平均K線數", avg_bars_win, float("nan"), float(exit_long_bars[pd.to_numeric(exit_long.get("淨損益 USD"), errors="coerce") > 0].mean()) if not exit_long_bars.empty else float("nan"), float("nan"), float(exit_short_bars[pd.to_numeric(exit_short.get("淨損益 USD"), errors="coerce") > 0].mean()) if not exit_short_bars.empty else float("nan"), float("nan")),
        ("虧損交易的平均K線數", avg_bars_loss, float("nan"), float(exit_long_bars[pd.to_numeric(exit_long.get("淨損益 USD"), errors="coerce") < 0].mean()) if not exit_long_bars.empty else float("nan"), float("nan"), float(exit_short_bars[pd.to_numeric(exit_short.get("淨損益 USD"), errors="coerce") < 0].mean()) if not exit_short_bars.empty else float("nan"), float("nan")),
    ]

    analysis = pd.DataFrame(
        analysis_rows,
        columns=["Unnamed: 0", "全部 USD", "全部 %", "看多 USD", "看多 %", "看空 USD", "看空 %"],
    )

    risk_rows = [
        ("夏普比率", round(sharpe, 3) if sharpe is not None else float("nan"), float("nan"), float("nan"), float("nan"), float("nan"), float("nan")),
        ("索提諾比率", round(sortino, 3) if sortino is not None else float("nan"), float("nan"), float("nan"), float("nan"), float("nan"), float("nan")),
        ("獲利因子", round(stats_all["profit_factor"], 3), float("nan"), round(stats_long["profit_factor"], 3), float("nan"), round(stats_short["profit_factor"], 3), float("nan")),
        ("持倉平均回報 (Efficiency)", float("nan"), round(stats_all["avg_pos_efficiency"], 4), float("nan"), float("nan"), float("nan"), float("nan")),
        ("追加保證金", 0.0, float("nan"), 0.0, float("nan"), 0.0, float("nan")),
    ]
    risk = pd.DataFrame(
        risk_rows,
        columns=["Unnamed: 0", "全部 USD", "全部 %", "看多 USD", "看多 %", "看空 USD", "看空 %"],
    )

    trade_range = merged_trades.dropna(subset=["日期/時間"]).sort_values("日期/時間")["日期/時間"]
    if not trade_range.empty:
        start = trade_range.iloc[0]
        end = trade_range.iloc[-1]
        trade_range_str = f"{start.year}年{start.month:02d}月{start.day:02d}日, {start.hour:02d}:{start.minute:02d} — {end.year}年{end.month:02d}月{end.day:02d}日, {end.hour:02d}:{end.minute:02d}"
    else:
        trade_range_str = ""

    symbols = merged_trades.get("商品", pd.Series(dtype=str)).dropna().unique().tolist()
    symbol_str = ",".join([str(s) for s in symbols]) if symbols else ""
    timeframes = merged_trades.get("時間週期", pd.Series(dtype=str)).dropna().unique().tolist()
    timeframe_str = timeframes[0] if len(timeframes) == 1 else "混合"
    point_values = pd.to_numeric(merged_trades.get("點值"), errors="coerce").dropna().unique().tolist()
    point_value_str = str(point_values[0]) if len(point_values) == 1 else ""
    source_files = merged_trades.get("來源檔案", pd.Series(dtype=str)).dropna().unique().tolist()

    if base_attrs is not None and "name" in base_attrs.columns and "value" in base_attrs.columns:
        attrs = base_attrs[["name", "value"]].copy()
        attrs["name"] = attrs["name"].astype(str).str.strip()
        attrs.loc[attrs["name"] == "交易範圍", "value"] = trade_range_str
        attrs.loc[attrs["name"] == "回測範圍", "value"] = trade_range_str
        attrs.loc[attrs["name"] == "商品", "value"] = symbol_str
        attrs.loc[attrs["name"] == "時間週期", "value"] = timeframe_str
        attrs.loc[attrs["name"] == "點值", "value"] = point_value_str
    else:
        attrs = pd.DataFrame(
            [
                {"name": "交易範圍", "value": trade_range_str},
                {"name": "回測範圍", "value": trade_range_str},
                {"name": "商品", "value": symbol_str},
                {"name": "時間週期", "value": timeframe_str},
                {"name": "點值", "value": point_value_str},
            ]
        )

    attrs = pd.concat(
        [
            attrs,
            pd.DataFrame(
                [
                    {"name": "出場交易 日期/時間 缺失筆數", "value": missing_exit_datetimes},
                    {"name": "出場交易 日期/時間 缺失(商品)", "value": missing_by_symbol_str},
                    {"name": "出場交易 日期/時間 範圍", "value": exit_dt_range_str},
                ]
            ),
        ],
        ignore_index=True,
    )

    if missing_exit_datetimes > 0:
        warning = (
            f"警告：出場交易缺失 日期/時間 {missing_exit_datetimes} 筆；"
            "夏普/索提諾未計算；最大回撤/上漲使用交易順序（含合成時間戳）計算。"
        )
        attrs = pd.concat([attrs, pd.DataFrame([{ "name": "資料品質警告", "value": warning }])], ignore_index=True)

    return perf, analysis, risk, trade_list_out, attrs, equity_curve, stats_all


def _max_drawdown_from_pnl(pnl_series: pd.Series) -> float:
    pnl_series = pd.to_numeric(pnl_series, errors="coerce").fillna(0.0)
    equity = pnl_series.cumsum()
    peak = equity.cummax()
    drawdown = equity - peak
    return float(abs(drawdown.min()))


def summarize_closed_trades(closed_df: pd.DataFrame, group_key: str) -> pd.DataFrame:
    if closed_df.empty:
        return pd.DataFrame(
            columns=[
                group_key,
                "交易筆數",
                "總淨利USD",
                "毛利USD",
                "毛損USD",
                "獲利因子",
                "勝率%",
                "平均每筆USD",
                "最大回撤USD",
            ]
        )

    rows: List[Dict[str, object]] = []

    for key, g in closed_df.groupby(group_key, dropna=False):
        pnl = pd.to_numeric(g.get("淨損益 USD"), errors="coerce").fillna(0.0)

        gross_profit = float(pnl[pnl > 0].sum())
        gross_loss = float(pnl[pnl < 0].sum())
        net_profit = float(pnl.sum())

        wins = int((pnl > 0).sum())
        trades = int(pnl.notna().sum())
        win_rate = (wins / trades * 100.0) if trades else 0.0

        profit_factor = (gross_profit / abs(gross_loss)) if gross_loss != 0 else (float("inf") if gross_profit > 0 else 0.0)
        avg_trade = (net_profit / trades) if trades else 0.0
        max_dd = _max_drawdown_from_pnl(pnl)

        rows.append(
            {
                group_key: key,
                "交易筆數": trades,
                "總淨利USD": net_profit,
                "毛利USD": gross_profit,
                "毛損USD": gross_loss,
                "獲利因子": profit_factor,
                "勝率%": win_rate,
                "平均每筆USD": avg_trade,
                "最大回撤USD": max_dd,
            }
        )

    out = pd.DataFrame(rows)
    return out.sort_values([group_key])


def generate_quant_audit_reports(merged_df: pd.DataFrame, output_dir: str):
    """
    量化審計專用模組：強制計算投資組合相關性與 MFE/MAE 效率。
    """
    print("==================================================")
    print("[Quant Auditor] INITIATING STATISTICAL PROCESSING")
    print("==================================================")
    
    # 1. 欄位容錯處理
    col_date = '日期/時間' if '日期/時間' in merged_df.columns else 'Date and time'
    col_pnl_pct = '淨損益 %' if '淨損益 %' in merged_df.columns else 'Net P&L %'
    col_mae_pct = '回撤 %' if '回撤 %' in merged_df.columns else 'Adverse excursion %'
    col_trade_id = '交易 #' if '交易 #' in merged_df.columns else 'Trade #'
    
    if col_date not in merged_df.columns or col_pnl_pct not in merged_df.columns:
        print("[Quant Auditor ERROR] Required columns missing. Aborting analysis.")
        print(f"Available columns: {merged_df.columns.tolist()}")
        return

    df = merged_df.copy()
    
    # 確保資料型態正確
    df[col_date] = pd.to_datetime(df[col_date], errors='coerce')
    df[col_pnl_pct] = pd.to_numeric(df[col_pnl_pct].astype(str).str.replace('%', ''), errors='coerce').fillna(0)
    df[col_mae_pct] = pd.to_numeric(df[col_mae_pct].astype(str).str.replace('%', ''), errors='coerce').fillna(0)
    
    # 提取商品代碼 (Symbol)
    # 優先從 '商品' 欄位讀取，若無則從 '交易 #' 提取 (相容 TV 原始格式)
    if '商品' in df.columns:
        df['Symbol'] = df['商品'].astype(str).str.strip()
    
    if 'Symbol' not in df.columns or (df['Symbol'] == 'Unknown').all():
        df['Symbol'] = df[col_trade_id].astype(str).apply(
            lambda x: x.split('-')[0].split(':')[-1] if '-' in x else 'Unknown'
        )

    # ---------------------------------------------------------
    # REPORT 1: 每日收益率相關性矩陣 (Daily Returns Correlation)
    # ---------------------------------------------------------
    print("[Quant Auditor] Calculating Pearson Correlation Matrix...")
    df['Date_Only'] = df[col_date].dt.date
    daily_returns = df.groupby(['Date_Only', 'Symbol'])[col_pnl_pct].sum().reset_index()
    
    # 建立時間序列矩陣
    pivot_returns = daily_returns.pivot(index='Date_Only', columns='Symbol', values=col_pnl_pct)
    pivot_returns = pivot_returns.fillna(0) # 無交易日視為 0 收益
    
    # 計算相關性
    corr_matrix = pivot_returns.corr(method='pearson')
    
    # ---------------------------------------------------------
    # REPORT 2: MAE 止損效率分析 (Microstructure MAE Profile)
    # ---------------------------------------------------------
    print("[Quant Auditor] Profiling Maximum Adverse Excursion (MAE) for Winning Trades...")
    winning_trades = df[df[col_pnl_pct] > 0]
    
    mae_stats = []
    for symbol, group in winning_trades.groupby('Symbol'):
        mae_series = np.abs(group[col_mae_pct]) # 取絕對值衡量深度
        mae_stats.append({
            'Symbol': symbol,
            'Total_Win_Trades': len(group),
            'Avg_MAE_of_Wins_%': round(mae_series.mean(), 3),
            'Median_MAE_of_Wins_%': round(mae_series.median(), 3),
            '90th_Percentile_MAE_%': round(mae_series.quantile(0.90), 3)
        })
    mae_df = pd.DataFrame(mae_stats)

    # ---------------------------------------------------------
    # FILE EXPORT
    # ---------------------------------------------------------
    # 解析原本的匯出路徑，產生同目錄下的新檔案
    base_dir = os.path.dirname(output_dir)
    base_name = os.path.basename(output_dir).replace('.xlsx', '')
    
    corr_path = os.path.join(base_dir, f"{base_name}_Correlation_Matrix.csv")
    mae_path = os.path.join(base_dir, f"{base_name}_MAE_Optimization.csv")
    
    corr_matrix.to_csv(corr_path)
    mae_df.to_csv(mae_path, index=False)
    
    print("==================================================")
    print(f"[*] Audit Report 1 Generated: {corr_path}")
    print(f"[*] Audit Report 2 Generated: {mae_path}")
    print("==================================================")

    return corr_matrix, mae_df


def discover_excel_files(paths: Iterable[str]) -> List[str]:
    files: List[str] = []
    for p in paths:
        if os.path.isdir(p):
            for name in os.listdir(p):
                if name.lower().endswith(".xlsx") and not name.startswith("~$"):
                    files.append(os.path.join(p, name))
        else:
            if p.lower().endswith(".xlsx") and os.path.exists(p):
                files.append(p)

    seen = set()
    deduped: List[str] = []
    for f in files:
        norm = os.path.normcase(os.path.abspath(f))
        if norm in seen:
            continue
        seen.add(norm)
        deduped.append(f)
    return deduped


def _gui_select_input_files() -> List[str]:
    if tk is None or filedialog is None:
        raise SystemExit("tkinter is not available. Please use --inputs instead.")

    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)

    files = filedialog.askopenfilenames(
        title="選取要合併的 Excel 交易紀錄檔",
        filetypes=[("Excel files", "*.xlsx")],
    )

    try:
        root.destroy()
    except Exception:
        pass

    return [str(f) for f in files]


def _gui_select_output_file() -> Optional[str]:
    if tk is None or filedialog is None:
        raise SystemExit("tkinter is not available. Please use --output instead.")

    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)

    out = filedialog.asksaveasfilename(
        title="選擇輸出報表位置",
        defaultextension=".xlsx",
        filetypes=[("Excel files", "*.xlsx")],
    )

    try:
        root.destroy()
    except Exception:
        pass

    return str(out) if out else None


def merge_and_export(excel_files: List[str], output_path: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    all_trades: List[pd.DataFrame] = []

    for f in excel_files:
        meta = read_file_meta(f)
        trades = read_trade_list(f, meta)
        all_trades.append(trades)

    merged = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()

    base_attrs = _read_first_available_sheet(excel_files[0], ATTR_SHEET_NAMES) if excel_files else None
    perf, analysis, risk, trade_list_out, attrs, equity_curve, stats_all = build_report_tables(merged, base_attrs)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        perf.to_excel(writer, index=False, sheet_name="績效")
        analysis.to_excel(writer, index=False, sheet_name="交易分析")
        risk.to_excel(writer, index=False, sheet_name="風險 績效比")
        trade_list_out.to_excel(writer, index=False, sheet_name="交易清單")
        attrs.to_excel(writer, index=False, sheet_name="屬性")

    generate_quant_audit_reports(merged, output_path)

    return merged, analysis, risk


def merge_and_export_to_bytes(excel_files: List[str]) -> Tuple[bytes, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    all_trades: List[pd.DataFrame] = []

    for f in excel_files:
        meta = read_file_meta(f)
        trades = read_trade_list(f, meta)
        all_trades.append(trades)

    merged = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()
    base_attrs = _read_first_available_sheet(excel_files[0], ATTR_SHEET_NAMES) if excel_files else None
    perf, analysis, risk, trade_list_out, attrs, equity_curve, stats_all = build_report_tables(merged, base_attrs)

    # Generate audit reports as well
    corr_matrix, mae_df = generate_quant_audit_reports(merged, "memory_buffered_report.xlsx")

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        perf.to_excel(writer, index=False, sheet_name="績效")
        analysis.to_excel(writer, index=False, sheet_name="交易分析")
        risk.to_excel(writer, index=False, sheet_name="風險 績效比")
        trade_list_out.to_excel(writer, index=False, sheet_name="交易清單")
        attrs.to_excel(writer, index=False, sheet_name="屬性")

    return buf.getvalue(), perf, corr_matrix, mae_df, equity_curve, stats_all


def main() -> int:
    parser = argparse.ArgumentParser(description="Merge multiple TradingView-style excel trade reports and compute summary metrics.")
    parser.add_argument(
        "--inputs",
        nargs="+",
        required=False,
        help="Excel files or folders containing Excel files.",
    )
    parser.add_argument(
        "--output",
        required=False,
        help="Output Excel path (e.g. merged_report.xlsx)",
    )
    parser.add_argument(
        "--gui",
        action="store_true",
        help="Use GUI to select input files and output path.",
    )

    args = parser.parse_args()

    if args.gui or not args.inputs:
        selected_files = _gui_select_input_files()
        if not selected_files:
            raise SystemExit("No .xlsx files selected")
        excel_files = discover_excel_files(selected_files)
        output_path = args.output or _gui_select_output_file()
        if not output_path:
            raise SystemExit("No output path selected")
    else:
        excel_files = discover_excel_files(args.inputs)
        output_path = args.output

    if not excel_files:
        raise SystemExit("No .xlsx files found in inputs")

    if not output_path:
        raise SystemExit("Missing --output (or use --gui)")

    merged, analysis, risk = merge_and_export(excel_files, output_path)

    print(f"Imported files: {len(excel_files)}")
    print(f"Merged rows (all): {len(merged)}")
    if not analysis.empty:
        total_row = analysis.loc[analysis["Unnamed: 0"].astype(str).str.strip() == "總交易量"]
        if not total_row.empty and "全部 USD" in total_row.columns:
            closed_trades_count = int(pd.to_numeric(total_row.iloc[0]['全部 USD'], errors='coerce') or 0)
            print(f"Closed trades count: {closed_trades_count}")

    print(f"\nSaved: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
