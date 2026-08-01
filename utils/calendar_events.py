"""Shared calendar/session event resolution for semantic strategies.

This module intentionally has no single-asset or portfolio assumptions.  It
turns a dataframe time axis plus a calendar node into a boolean mask that can be
reused by the current single-series runtime and future multi-asset workflows.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import numpy as np
import pandas as pd


WEEKDAY_ALIASES = {
    "monday": 0,
    "mon": 0,
    "tuesday": 1,
    "tue": 1,
    "wednesday": 2,
    "wed": 2,
    "thursday": 3,
    "thu": 3,
    "friday": 4,
    "fri": 4,
    "saturday": 5,
    "sat": 5,
    "sunday": 6,
    "sun": 6,
}

WEEKDAY_NAMES = {
    0: "monday",
    1: "tuesday",
    2: "wednesday",
    3: "thursday",
    4: "friday",
    5: "saturday",
    6: "sunday",
}


class CalendarConditionMaterializer:
    """Resolve calendar predicate nodes against a dataframe time axis.

    The class name is kept for the current single-asset backtester, but the
    contract is intentionally broader:

    - ``materialize`` returns a boolean mask for signal-style engines.
    - ``trigger_sessions`` returns resolved tradable sessions for rebalance or
      portfolio engines.
    - ``event_frame`` returns an auditable row table that can be persisted by
      any caller.
    """

    def __init__(
        self,
        data: pd.DataFrame,
        *,
        repo_root: Optional[Path] = None,
        time_column: str = "Time",
    ) -> None:
        self.data = data
        self.repo_root = repo_root or Path.cwd()
        self.time_column = time_column
        self._dates = self._resolve_dates()
        self._normalized_dates = self._dates.dt.normalize()
        self._available_dates = set(self._normalized_dates.dropna())
        self._available_dates_sorted = sorted(self._available_dates)

    def materialize(self, node: Dict[str, Any]) -> np.ndarray:
        op = str((node or {}).get("op", "")).strip().lower()
        if op == "calendar.month_in":
            return self._month_in(node)
        if op == "calendar.weekday_eq":
            return self._weekday_eq(node)
        if op == "calendar.every_session":
            return self._every_session()
        if op == "calendar.first_session":
            return self._first_session()
        if op == "calendar.month_start":
            return self._period_boundary(node, period_freq="M", boundary="start")
        if op == "calendar.month_end":
            return self._period_boundary(node, period_freq="M", boundary="end")
        if op == "calendar.quarter_start":
            return self._period_boundary(node, period_freq="Q", boundary="start")
        if op == "calendar.quarter_end":
            return self._period_boundary(node, period_freq="Q", boundary="end")
        if op == "calendar.year_start":
            return self._period_boundary(node, period_freq="Y", boundary="start")
        if op == "calendar.year_end":
            return self._period_boundary(node, period_freq="Y", boundary="end")
        if op == "calendar.last_weekday_of_month":
            return self._last_weekday_of_month(node)
        if op == "calendar.nth_weekday_of_month":
            return self._nth_weekday_of_month(node)
        if op == "calendar.event_date":
            return self._event_date(node)
        if op == "calendar.session_offset_from_month_end":
            return self._session_offset_from_month_end(node)
        return np.zeros(len(self._dates), dtype=np.bool_)

    def trigger_sessions(self, node: Dict[str, Any]) -> List[pd.Timestamp]:
        """Return resolved tradable sessions for a calendar node.

        This is the common entry point for future multi-asset workflows such as
        monthly rebalance or event-date portfolio selection.  It returns only
        sessions that exist on the caller's supplied time axis.
        """
        rows = self._calendar_node_rows(node)
        sessions: List[pd.Timestamp] = []
        seen: set[pd.Timestamp] = set()
        for row in rows:
            if not row.get("triggered") or not row.get("resolved_session_date"):
                continue
            session = pd.Timestamp(row["resolved_session_date"]).normalize()
            if session in seen:
                continue
            seen.add(session)
            sessions.append(session)
        return sessions

    def event_frame(
        self,
        node: Any,
        *,
        combo: Optional[Dict[str, Any]] = None,
        backtest_id: Optional[str] = None,
        strategy_id: Optional[str] = None,
        event_role: str = "signal",
    ) -> pd.DataFrame:
        """Return an auditable event table for signal or rebalance workflows."""
        return pd.DataFrame(
            self.audit_rows(
                node,
                combo=combo,
                backtest_id=backtest_id,
                strategy_id=strategy_id,
                event_role=event_role,
            )
        )

    def audit_rows(
        self,
        node: Any,
        *,
        combo: Optional[Dict[str, Any]] = None,
        backtest_id: Optional[str] = None,
        strategy_id: Optional[str] = None,
        event_role: str = "signal",
    ) -> List[Dict[str, Any]]:
        """Return auditable calendar target/trigger rows for a node tree."""
        rows: List[Dict[str, Any]] = []
        self._collect_audit_rows(
            node,
            rows,
            combo=combo or {},
            backtest_id=backtest_id,
            strategy_id=strategy_id,
            event_role=event_role,
        )
        return rows

    @staticmethod
    def is_calendar_op(op: str) -> bool:
        return str(op or "").strip().lower().startswith("calendar.")

    def _resolve_dates(self) -> pd.Series:
        if isinstance(self.data.index, pd.DatetimeIndex):
            raw = pd.Series(self.data.index, index=self.data.index)
        elif self.time_column in self.data.columns:
            raw = pd.to_datetime(self.data[self.time_column], errors="coerce")
        else:
            raw = pd.Series(pd.NaT, index=self.data.index)
        return pd.to_datetime(raw, errors="coerce").dt.tz_localize(None)

    def _month_in(self, node: Dict[str, Any]) -> np.ndarray:
        months = self._int_set(node.get("months"))
        if not months:
            return np.zeros(len(self._dates), dtype=np.bool_)
        return self._dates.dt.month.isin(months).fillna(False).to_numpy(dtype=np.bool_)

    def _weekday_eq(self, node: Dict[str, Any]) -> np.ndarray:
        weekday = self._weekday_code(node.get("weekday"))
        if weekday is None:
            return np.zeros(len(self._dates), dtype=np.bool_)
        return (self._dates.dt.weekday == weekday).fillna(False).to_numpy(dtype=np.bool_)

    def _period_boundary(
        self,
        node: Dict[str, Any],
        *,
        period_freq: str,
        boundary: str,
    ) -> np.ndarray:
        targets = self._period_boundary_targets(
            node,
            period_freq=period_freq,
            boundary=boundary,
        )
        return self._normalized_dates.isin(targets).fillna(False).to_numpy(dtype=np.bool_)

    def _last_weekday_of_month(self, node: Dict[str, Any]) -> np.ndarray:
        weekday = self._weekday_code(node.get("weekday"))
        if weekday is None:
            return np.zeros(len(self._dates), dtype=np.bool_)
        months = self._int_set(node.get("months"))
        targets = self._calendar_weekday_targets(weekday=weekday, months=months, ordinal=-1)
        return self._normalized_dates.isin(targets).fillna(False).to_numpy(dtype=np.bool_)

    def _nth_weekday_of_month(self, node: Dict[str, Any]) -> np.ndarray:
        weekday = self._weekday_code(node.get("weekday"))
        ordinal = self._ordinal_value(node.get("ordinal"))
        if weekday is None or ordinal is None or ordinal == 0:
            return np.zeros(len(self._dates), dtype=np.bool_)
        months = self._int_set(node.get("months"))
        targets = self._calendar_weekday_targets(weekday=weekday, months=months, ordinal=ordinal)
        return self._normalized_dates.isin(targets).fillna(False).to_numpy(dtype=np.bool_)

    def _calendar_weekday_targets(
        self,
        *,
        weekday: int,
        months: set[int],
        ordinal: int,
    ) -> set[pd.Timestamp]:
        targets: set[pd.Timestamp] = set()
        for period in self._month_periods():
            if months and int(period.month) not in months:
                continue
            target = self._calendar_weekday_target_for_period(
                period=period,
                weekday=weekday,
                ordinal=ordinal,
            )
            if target.month == period.month:
                targets.add(target)
        return targets

    def _period_boundary_targets(
        self,
        node: Dict[str, Any],
        *,
        period_freq: str,
        boundary: str,
    ) -> set[pd.Timestamp]:
        months = self._int_set(node.get("months"))
        dates = self._normalized_dates.dropna().drop_duplicates().sort_values()
        if dates.empty:
            return set()
        frame = pd.DataFrame({"date": dates})
        frame["period"] = frame["date"].dt.to_period(period_freq)
        grouped = frame.groupby("period", sort=True)["date"]
        selected = grouped.min() if boundary == "start" else grouped.max()
        targets: set[pd.Timestamp] = set()
        for target in selected.tolist():
            target = pd.Timestamp(target).normalize()
            if months and int(target.month) not in months:
                continue
            targets.add(target)
        return targets

    def _event_date(self, node: Dict[str, Any]) -> np.ndarray:
        event_dates = self._event_dates(node)
        if not event_dates:
            return np.zeros(len(self._dates), dtype=np.bool_)
        targets = {
            pd.Timestamp(row["resolved_session_date"]).normalize()
            for row in self._event_date_audit_rows(node)
            if row.get("triggered") and row.get("resolved_session_date")
        }
        return self._normalized_dates.isin(targets).fillna(False).to_numpy(dtype=np.bool_)

    def _session_offset_from_month_end(self, node: Dict[str, Any]) -> np.ndarray:
        offset = self._session_offset_value(node)
        mask = np.zeros(len(self._dates), dtype=np.bool_)
        dates = self._normalized_dates.tolist()
        start = 0
        while start < len(dates):
            if pd.isna(dates[start]):
                start += 1
                continue
            month = pd.Timestamp(dates[start]).to_period("M")
            end = start + 1
            while (
                end < len(dates)
                and not pd.isna(dates[end])
                and pd.Timestamp(dates[end]).to_period("M") == month
            ):
                end += 1
            target = end - 1 + offset
            if start <= target < end:
                mask[target] = True
            start = end
        return mask

    def _session_offset_from_month_end_audit_rows(
        self, node: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        offset = self._session_offset_value(node)
        mask = self._session_offset_from_month_end(node)
        dates = self._normalized_dates.tolist()
        rows: List[Dict[str, Any]] = []
        for index, triggered in enumerate(mask):
            if not triggered:
                continue
            target = pd.Timestamp(dates[index]).normalize()
            rows.append(
                {
                    "calendar_op": "calendar.session_offset_from_month_end",
                    "raw_event_date": None,
                    "target_date": self._date_string(target),
                    "resolved_session_date": self._date_string(target),
                    "triggered": True,
                    "skip_reason": None,
                    "weekday": WEEKDAY_NAMES.get(int(target.weekday()), str(target.weekday())),
                    "ordinal": None,
                    "months": [],
                    "period": str(target.to_period("M")),
                    "adjustment_policy": "session_offset_from_available_month_end",
                    "source_path": None,
                    "date_column": None,
                    "offset_sessions": offset,
                }
            )
        return rows

    @staticmethod
    def _session_offset_value(node: Dict[str, Any]) -> int:
        raw = node.get("offset_sessions", node.get("offset", 0))
        try:
            value = int(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "calendar.session_offset_from_month_end requires integer offset_sessions"
            ) from exc
        if value > 0:
            raise ValueError(
                "calendar.session_offset_from_month_end offset_sessions must be <= 0"
            )
        return value

    def _collect_audit_rows(
        self,
        node: Any,
        rows: List[Dict[str, Any]],
        *,
        combo: Dict[str, Any],
        backtest_id: Optional[str],
        strategy_id: Optional[str],
        event_role: str,
    ) -> None:
        if isinstance(node, list):
            for item in node:
                self._collect_audit_rows(
                    item,
                    rows,
                    combo=combo,
                    backtest_id=backtest_id,
                    strategy_id=strategy_id,
                    event_role=event_role,
                )
            return
        if not isinstance(node, dict):
            return
        op = str(node.get("op", "")).strip().lower()
        if op in {"and", "or"}:
            self._collect_audit_rows(
                node.get("nodes", []),
                rows,
                combo=combo,
                backtest_id=backtest_id,
                strategy_id=strategy_id,
                event_role=event_role,
            )
            return
        if op == "not":
            self._collect_audit_rows(
                node.get("node"),
                rows,
                combo=combo,
                backtest_id=backtest_id,
                strategy_id=strategy_id,
                event_role=event_role,
            )
            return
        if not self.is_calendar_op(op):
            return
        calendar_rows = self._calendar_node_rows(node)
        for index, row in enumerate(calendar_rows, start=1):
            rows.append(
                {
                    "schema_version": "1.0",
                    "audit_row_type": f"calendar_{self._safe_event_role(event_role)}",
                    "event_role": self._safe_event_role(event_role),
                    "audit_seq": index,
                    "backtest_id": backtest_id,
                    "strategy_id": strategy_id,
                    "combo": dict(combo),
                    **row,
                }
            )

    def _calendar_node_rows(self, node: Dict[str, Any]) -> List[Dict[str, Any]]:
        op = str((node or {}).get("op", "")).strip().lower()
        if op in {"calendar.last_weekday_of_month", "calendar.nth_weekday_of_month"}:
            return self._weekday_target_audit_rows(node)
        if op == "calendar.event_date":
            return self._event_date_audit_rows(node)
        if op == "calendar.session_offset_from_month_end":
            return self._session_offset_from_month_end_audit_rows(node)
        if op == "calendar.every_session":
            return self._calendar_filter_audit_rows(node)
        if op == "calendar.first_session":
            return self._first_session_audit_rows(node)
        if op in {
            "calendar.month_start",
            "calendar.month_end",
            "calendar.quarter_start",
            "calendar.quarter_end",
            "calendar.year_start",
            "calendar.year_end",
        }:
            return self._period_boundary_audit_rows(node)
        if self.is_calendar_op(op):
            return self._calendar_filter_audit_rows(node)
        return []

    def _every_session(self) -> np.ndarray:
        return self._normalized_dates.notna().to_numpy(dtype=np.bool_)

    def _first_session(self) -> np.ndarray:
        mask = np.zeros(len(self._dates), dtype=np.bool_)
        valid = self._normalized_dates.dropna()
        if valid.empty:
            return mask
        first = pd.Timestamp(valid.iloc[0]).normalize()
        return self._normalized_dates.eq(first).fillna(False).to_numpy(dtype=np.bool_)

    def _first_session_audit_rows(self, node: Dict[str, Any]) -> List[Dict[str, Any]]:
        valid = self._normalized_dates.dropna()
        if valid.empty:
            return []
        target = pd.Timestamp(valid.iloc[0]).normalize()
        return [
            {
                "calendar_op": str(node.get("op", "")).strip().lower(),
                "raw_event_date": None,
                "target_date": self._date_string(target),
                "resolved_session_date": self._date_string(target),
                "triggered": True,
                "skip_reason": None,
                "weekday": WEEKDAY_NAMES.get(int(target.weekday()), str(target.weekday())),
                "ordinal": None,
                "months": [],
                "period": str(target.to_period("M")),
                "adjustment_policy": "first_available_session",
                "source_path": None,
                "date_column": None,
            }
        ]

    def _period_boundary_audit_rows(self, node: Dict[str, Any]) -> List[Dict[str, Any]]:
        op = str(node.get("op", "")).strip().lower()
        if "year" in op:
            period_freq = "Y"
        elif "quarter" in op:
            period_freq = "Q"
        else:
            period_freq = "M"
        boundary = "end" if op.endswith("_end") else "start"
        targets = self._period_boundary_targets(
            node,
            period_freq=period_freq,
            boundary=boundary,
        )
        rows: List[Dict[str, Any]] = []
        for target in sorted(targets):
            rows.append(
                {
                    "calendar_op": op,
                    "raw_event_date": None,
                    "target_date": self._date_string(target),
                    "resolved_session_date": self._date_string(target),
                    "triggered": True,
                    "skip_reason": None,
                    "weekday": WEEKDAY_NAMES.get(int(target.weekday()), str(target.weekday())),
                    "ordinal": None,
                    "months": sorted(self._int_set(node.get("months"))),
                    "period": str(target.to_period(period_freq)),
                    "adjustment_policy": "available_session_boundary",
                    "source_path": None,
                    "date_column": None,
                }
            )
        return rows

    def _weekday_target_audit_rows(self, node: Dict[str, Any]) -> List[Dict[str, Any]]:
        op = str(node.get("op", "")).strip().lower()
        weekday = self._weekday_code(node.get("weekday"))
        ordinal = -1 if op == "calendar.last_weekday_of_month" else self._ordinal_value(node.get("ordinal"))
        if weekday is None or ordinal is None or ordinal == 0:
            return []
        months = self._int_set(node.get("months"))
        rows: List[Dict[str, Any]] = []
        for period in self._month_periods():
            if months and int(period.month) not in months:
                continue
            target = self._calendar_weekday_target_for_period(
                period=period,
                weekday=weekday,
                ordinal=ordinal,
            )
            if target.month != period.month:
                continue
            triggered = target in self._available_dates
            rows.append(
                {
                    "calendar_op": op,
                    "raw_event_date": None,
                    "target_date": self._date_string(target),
                    "resolved_session_date": self._date_string(target) if triggered else None,
                    "triggered": bool(triggered),
                    "skip_reason": None if triggered else "target_session_missing",
                    "weekday": WEEKDAY_NAMES.get(weekday, str(weekday)),
                    "ordinal": int(ordinal),
                    "months": sorted(months) if months else [],
                    "period": str(period),
                    "adjustment_policy": "skip",
                    "source_path": None,
                    "date_column": None,
                }
            )
        return rows

    def _calendar_filter_audit_rows(self, node: Dict[str, Any]) -> List[Dict[str, Any]]:
        mask = self.materialize(node)
        op = str(node.get("op", "")).strip().lower()
        rows: List[Dict[str, Any]] = []
        for idx, triggered in enumerate(mask):
            if not triggered:
                continue
            target = self._normalized_dates.iloc[idx]
            rows.append(
                {
                    "calendar_op": op,
                    "raw_event_date": None,
                    "target_date": self._date_string(target),
                    "resolved_session_date": self._date_string(target),
                    "triggered": True,
                    "skip_reason": None,
                    "weekday": WEEKDAY_NAMES.get(int(target.weekday()), str(target.weekday())),
                    "ordinal": None,
                    "months": sorted(self._int_set(node.get("months"))),
                    "period": str(target.to_period("M")),
                    "adjustment_policy": "skip",
                    "source_path": None,
                    "date_column": None,
                }
            )
        return rows

    def _event_date_audit_rows(self, node: Dict[str, Any]) -> List[Dict[str, Any]]:
        policy = str(node.get("adjustment_policy") or node.get("adjustment") or "skip").strip().lower()
        if policy in {"previous", "previous_trading_day", "prev"}:
            policy = "previous_trading_day"
        elif policy in {"next", "next_trading_day"}:
            policy = "next_trading_day"
        else:
            policy = "skip"
        source_path = node.get("path") if isinstance(node.get("path"), str) else None
        date_column = str(node.get("date_column") or "date")
        rows: List[Dict[str, Any]] = []
        for raw_date in sorted(self._event_dates(node)):
            resolved, skip_reason = self._resolve_event_session(raw_date, policy)
            rows.append(
                {
                    "calendar_op": "calendar.event_date",
                    "raw_event_date": self._date_string(raw_date),
                    "target_date": self._date_string(raw_date),
                    "resolved_session_date": self._date_string(resolved) if resolved is not None else None,
                    "triggered": resolved is not None,
                    "skip_reason": skip_reason,
                    "weekday": WEEKDAY_NAMES.get(int(raw_date.weekday()), str(raw_date.weekday())),
                    "ordinal": None,
                    "months": [int(raw_date.month)],
                    "period": str(raw_date.to_period("M")),
                    "adjustment_policy": policy,
                    "source_path": source_path,
                    "date_column": date_column,
                }
            )
        return rows

    def _resolve_event_session(
        self,
        raw_date: pd.Timestamp,
        policy: str,
    ) -> tuple[Optional[pd.Timestamp], Optional[str]]:
        raw_date = pd.Timestamp(raw_date).normalize()
        if raw_date in self._available_dates:
            return raw_date, None
        if policy == "previous_trading_day":
            candidates = [date for date in self._available_dates_sorted if date < raw_date]
            if candidates:
                return candidates[-1], "adjusted_previous_trading_day"
        elif policy == "next_trading_day":
            candidates = [date for date in self._available_dates_sorted if date > raw_date]
            if candidates:
                return candidates[0], "adjusted_next_trading_day"
        return None, "target_session_missing"

    def _month_periods(self) -> Iterable[pd.Period]:
        return self._dates.dropna().dt.to_period("M").unique()

    @staticmethod
    def _calendar_weekday_target_for_period(
        *,
        period: pd.Period,
        weekday: int,
        ordinal: int,
    ) -> pd.Timestamp:
        start = pd.Timestamp(period.start_time).normalize()
        end = pd.Timestamp(period.end_time).normalize()
        if ordinal > 0:
            day_offset = (weekday - start.weekday()) % 7
            return start + pd.Timedelta(days=day_offset + (ordinal - 1) * 7)
        day_offset = (end.weekday() - weekday) % 7
        return end - pd.Timedelta(days=day_offset + (abs(ordinal) - 1) * 7)

    @staticmethod
    def _date_string(value: Any) -> Optional[str]:
        if value is None or pd.isna(value):
            return None
        return pd.Timestamp(value).normalize().strftime("%Y-%m-%d")

    @staticmethod
    def _safe_event_role(value: Any) -> str:
        text = str(value or "signal").strip().lower().replace("-", "_").replace(" ", "_")
        return text if text.isidentifier() else "signal"

    def _event_dates(self, node: Dict[str, Any]) -> set[pd.Timestamp]:
        dates: set[pd.Timestamp] = set()
        for item in self._iterable(node.get("dates")):
            parsed = pd.to_datetime(item, errors="coerce")
            if not pd.isna(parsed):
                dates.add(pd.Timestamp(parsed).normalize())
        path_value = node.get("path")
        if isinstance(path_value, str) and path_value.strip():
            path = Path(path_value)
            if not path.is_absolute():
                path = self.repo_root / path
            if path.exists():
                column = str(node.get("date_column") or "date")
                frame = pd.read_csv(path)
                if column in frame.columns:
                    for item in frame[column].tolist():
                        parsed = pd.to_datetime(item, errors="coerce")
                        if not pd.isna(parsed):
                            dates.add(pd.Timestamp(parsed).normalize())
        return dates

    @staticmethod
    def _iterable(value: Any) -> Iterable[Any]:
        if isinstance(value, list):
            return value
        if value in (None, ""):
            return []
        return [value]

    @classmethod
    def _int_set(cls, value: Any) -> set[int]:
        out: set[int] = set()
        for item in cls._iterable(value):
            try:
                out.add(int(item))
            except (TypeError, ValueError):
                continue
        return out

    @staticmethod
    def _weekday_code(value: Any) -> Optional[int]:
        if isinstance(value, (int, float)) and 0 <= int(value) <= 6:
            return int(value)
        key = str(value or "").strip().lower()
        return WEEKDAY_ALIASES.get(key)

    @staticmethod
    def _ordinal_value(value: Any) -> Optional[int]:
        if isinstance(value, (int, float)):
            return int(value)
        key = str(value or "").strip().lower()
        if key == "last":
            return -1
        if key in {"first", "1st"}:
            return 1
        if key in {"second", "2nd"}:
            return 2
        if key in {"third", "3rd"}:
            return 3
        if key in {"fourth", "4th"}:
            return 4
        try:
            return int(key)
        except ValueError:
            return None


CalendarEventResolver = CalendarConditionMaterializer
