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
