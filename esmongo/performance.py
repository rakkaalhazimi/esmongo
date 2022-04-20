import time
import functools
from typing import Sequence

from .models import DBClient, Task


def record_runtime(func):
    """Functions wrapper, wraps any function and record its runtime"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()

        # Run functions
        result = func(*args, **kwargs)

        end_time = time.time()
        print(
            f">> {func.__name__:<20} elapsed for {end_time - start_time:>10.7f} seconds")
        return result

    return wrapper

          
class CodeTimer:
    def __init__(self, name=None):
        self.name = f"'{name}'" if name else ''

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, exc_type, exc_value, traceback):
        self.took = (time.time() - self.start) * 1000.0
        print(f"Code block {self.name:30} took: {self.took:.4f} ms")


class DBPerformanceTest:
    def __init__(self, client: DBClient, tasks: Sequence[Task]):
        self.client = client
        self.tasks = tasks

    def run_task(self, task: Task):
        return task.operation(**task.operation_kwargs)

    def start(self):
        # Show Database Name
        cls_name = self.client.__class__.__name__
        print(f"{cls_name} Server")
        print("=" * 20)

        # Do all task here
        for task in self.tasks:
            self.run_task(task)

        # Drop all client data upon completions
        self.client.drop_collections()

        # Add new line
        print()


def sleep_task():
    time.sleep(2)
    return


if __name__ == "__main__":
    record_runtime(sleep_task)()
