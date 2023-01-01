"""This module provides functionality for timing using a context manager."""

import time
import math


def duration_as_string(seconds: float) -> str:
    """Return human-readable string representation of a duration in seconds."""
    if seconds <= 300.0:
        return "{:.3f} seconds".format(seconds)
    minutes = seconds / 60.0
    if minutes <= 120.0:
        return "{:.3f} minutes".format(minutes)
    hours = math.floor(minutes / 60.0)
    minutes_left = minutes - (60.0 * hours)
    return "{} hours and {:.3f} minutes".format(hours, minutes_left)


class TimerContextManager:
    """A context manager that allows timing of its execution time."""

    def __init__(self, total_work):
        self.total_work = total_work
        self.t_enter = None
        self.t_stop = None

    def __enter__(self):
        self.t_enter = time.monotonic()
        self.t_stop = None
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def stop(self) -> None:
        """Stop the timer if it hasn't previously been stopped."""
        if self.t_stop is None:
            self.t_stop = time.monotonic()

    def duration(self) -> float:
        """Stop timer and return duration, in seconds."""
        self.stop()
        return self.t_stop - self.t_enter

    def duration_string(self) -> str:
        """Stop timer and return duration as a human-readable string."""

        duration = self.duration()
        return duration_as_string(duration)

    def etc(self, work_completed = None, work_remaining = None) -> float:
        """Return estimated time to completion, in seconds."""

        t_current = time.monotonic()

        if (work_remaining is None) == (work_completed is None):
            raise RuntimeError("Either work_completed or work_remaining should be specified, but not both.")

        if work_completed is None:
            work_completed = self.total_work - work_remaining
        else:
            work_remaining = self.total_work - work_completed

        tempo = work_completed / (t_current - self.t_enter)

        etc = math.nan if tempo == 0.0 else work_remaining / tempo

        return etc

    def etc_string(self, work_completed = None, work_remaining = None) -> str:
        """Return estimated time to completion as a human-readable string."""

        etc = self.etc(work_completed, work_remaining)

        return duration_as_string(etc)


def start_timer(total_work = None) -> TimerContextManager:
    """Return a context timer that allows timing of its execution."""
    return TimerContextManager(total_work)
