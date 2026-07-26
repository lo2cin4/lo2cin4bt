"""verification template for a new Strategy Building Block.

Layer: verification
template_only: true
do_not_run: true

Use deterministic local data for oracle tests. Do not use live API data as
the source of exact expected trades because provider adjustments can change.
"""

import pandas as pd


def test_example_strategy_building_block_oracle():
    dates = pd.date_range("2024-01-01", periods=6, freq="D", tz="UTC")
    close = pd.Series([100.0, 90.0, 80.0, 95.0, 110.0, 120.0], index=dates, name="close")

    expected_entry_dates = [dates[2]]
    expected_exit_dates = [dates[4]]

    observed_entry_dates = [close.index[2]]
    observed_exit_dates = [close.index[4]]

    assert observed_entry_dates == expected_entry_dates
    assert observed_exit_dates == expected_exit_dates
