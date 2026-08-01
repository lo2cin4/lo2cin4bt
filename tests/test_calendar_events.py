import pandas as pd

from utils.calendar_events import CalendarEventResolver


def test_calendar_first_session_resolves_only_first_available_date() -> None:
    frame = pd.DataFrame(
        {
            "Time": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-05"]),
            "close": [100.0, 101.0, 102.0],
        }
    )
    resolver = CalendarEventResolver(frame)

    mask = resolver.materialize({"op": "calendar.first_session"})
    sessions = resolver.trigger_sessions({"op": "calendar.first_session"})

    assert mask.tolist() == [True, False, False]
    assert sessions == [pd.Timestamp("2024-01-02")]


def test_calendar_session_offset_from_month_end_uses_trading_sessions() -> None:
    frame = pd.DataFrame(
        {
            "Time": pd.to_datetime(
                [
                    "2024-01-25",
                    "2024-01-26",
                    "2024-01-29",
                    "2024-01-30",
                    "2024-01-31",
                    "2024-02-27",
                    "2024-02-28",
                    "2024-02-29",
                ]
            ),
            "close": list(range(8)),
        }
    )
    resolver = CalendarEventResolver(frame)

    node = {"op": "calendar.session_offset_from_month_end", "offset_sessions": -2}
    mask = resolver.materialize(node)

    assert mask.tolist() == [False, False, True, False, False, True, False, False]
    assert resolver.trigger_sessions(node) == [
        pd.Timestamp("2024-01-29"),
        pd.Timestamp("2024-02-27"),
    ]
