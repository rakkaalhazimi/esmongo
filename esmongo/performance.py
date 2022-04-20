import time
import functools
from typing import Callable, Mapping

from .models import DBClient, Task


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


class DBPerformanceTest:
    def __init__(self, client: DBClient):
        self.client = client
        self.tasks = []

    def register_task(self, operation: Callable, operation_kwargs: Mapping):
        self.tasks.append(Task(operation=operation, operation_kwargs=operation_kwargs))

    def start(self):
        # Show Database Name
        cls_name = self.client.__class__.__name__
        print(f"{cls_name} Server")
        print("=" * 20)

        # Do all task here
        for task in self.tasks:
            record_runtime(task.operation)(**task.operation_kwargs)

        # Drop all data on completions
        self.client.drop_collections()

def sleep_task():
    time.sleep(2)
    return


if __name__ == "__main__":
    record_runtime(sleep_task)()

