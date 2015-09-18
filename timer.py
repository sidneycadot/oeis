
import time
import math

class TimerContextManager:

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

def start_timer():
    return TimerContextManager()
