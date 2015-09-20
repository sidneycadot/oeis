
import time
import math

def duration_as_string(seconds):
    if seconds <= 300.0:
        return "{:.3f} seconds".format(seconds)
    minutes = seconds / 60.0
    if minutes <= 120.0:
        return "{:.3f} minutes".format(minutes)
    hours = math.floor(minutes / 3600.0)
    minutes_left = minutes - (60.0 * hours)
    return "{} hours and {:.3f} minutes".format(hours, minutes_left)

class TimerContextManager:

    def __init__(self, total_work):
        self._total_work = total_work

    def __enter__(self):
        self.t_enter = time.time()
        self.t_stop = None
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def stop(self):
        if self.t_stop is None:
            self.t_stop = time.time()

    def duration(self):
        self.stop()
        return self.t_stop - self.t_enter

    def duration_string(self):
        seconds = self.duration()
        if seconds <= 300.0:
            return "{:.3f} seconds".format(seconds)
        minutes = seconds / 60.0
        if minutes <= 120.0:
            return "{:.3f} minutes".format(minutes)
        hours = math.floor(minutes / 3600.0)
        minutes_left = minutes - (60.0 * hours)
        return "{} hours and {:.3f} minutes".format(hours, minutes_left)

    def etc(self):

        t_current = time.time()

        assert (work_remaining is None) != (work_completed is None)

        if work_completed is None:
            work_completed = self._total_work - work_remaining
        else:
            work_remaining = self._total_work - work_completed

        tempo = work_completed / (t_current - self.t_enter)

        time_remaining = np.nan if tempo == 0.0 else work_remaining / tempo

        return time_remaining

    def etc_string(self, work_completed = None, work_remaining = None)

        time_remaining = self.etc()

        return duration_as_string(time_remaining)

def start_timer(total_work = None):
    return TimerContextManager(total_work)
