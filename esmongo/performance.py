import time
import functools
from typing import Sequence

from .models import DatabaseServer, Task


def record_runtime(func):
    """Functions wrapper, wraps any function and record its runtime"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        
        # Run functions
        result = func(*args, **kwargs)

        end_time = time.time()
        print(f">> {func.__name__:<20} elapsed for {end_time - start_time:>10.7f} seconds")
        return result

    return wrapper


class DbPerformance:
    def __init__(self, server: DatabaseServer, tasks: Sequence[Task]):
        self.server = server
        self.tasks = tasks

    def start(self):
        # Show Database Name
        cls_name = self.server.__class__.__name__
        print(f"{cls_name} Server")
        print("=" * 20)

        # Do all task here
        for task in self.tasks:
            record_runtime(task.operation)(**task.kwargs)

        # Drop all data on completions
        self.server.drop_collections()

def sleep_task():
    time.sleep(2)
    return


if __name__ == "__main__":
    record_runtime(sleep_task)()

