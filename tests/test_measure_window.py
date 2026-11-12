"""
Data delay (GLOSSARY): the time from a bar's close until it is queryable in Gold.
A distribution over bars, not one number per run (METRICS open question 3).

The seam is data_delays(): given each run's newest included bar and the moment its
Gold finished, it returns one delay per bar. Expected values are worked by hand.
"""

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import measure_window as mw  # noqa: E402


def t(hh, mm, ss=0):
    return datetime(2026, 11, 11, hh, mm, ss, tzinfo=timezone.utc)


def test_each_new_bar_is_delayed_from_its_close_until_its_run_finishes_gold():
    runs = [
        mw.Run(newest_bar_open=t(10, 0), gold_finished=t(10, 12)),
        mw.Run(newest_bar_open=t(10, 2), gold_finished=t(10, 15)),
    ]
    # The first run's bars have an unknown predecessor, so only the second run's
    # new bars count: 10:01 closes 10:02 -> 13 min; 10:02 closes 10:03 -> 12 min.
    assert mw.data_delays(runs) == [780.0, 720.0]


def test_a_bar_counts_once_from_the_first_run_that_made_it_queryable():
    runs = [
        mw.Run(newest_bar_open=t(10, 0), gold_finished=t(10, 12)),
        mw.Run(newest_bar_open=t(10, 2), gold_finished=t(10, 15)),
        # Stalled or rewound Silver: nothing new, so no bars, no delays.
        mw.Run(newest_bar_open=t(10, 1), gold_finished=t(11, 12)),
        # 10:01 and 10:02 were already queryable; only 10:03 is new.
        mw.Run(newest_bar_open=t(10, 3), gold_finished=t(12, 12)),
    ]
    # 10:03 closes 10:04; queryable at 12:12 -> 128 min.
    assert mw.data_delays(runs) == [780.0, 720.0, 7680.0]


def test_summary_reports_p50_and_max_with_min_kept_for_context():
    assert mw.summarize([720.0, 780.0, 7680.0, 300.0]) == {
        "count": 4, "p50": 750.0, "max": 7680.0, "min": 300.0,
    }
