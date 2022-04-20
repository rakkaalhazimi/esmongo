import time
import functools
from typing import Callable, Mapping, Sequence

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
    def __init__(self, client: DBClient, tasks: Sequence[Task]):
        self.client = client
        self.tasks = tasks
        

    def run_task(self, task: Task):
        return record_runtime(task.operation)(**task.operation_kwargs)

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

def sleep_task():
    time.sleep(2)
    return


if __name__ == "__main__":
    record_runtime(sleep_task)()

