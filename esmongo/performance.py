import time
import functools


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
        self.took = (time.time() - self.start)
        print(f"Operation {self.name:25} took: {self.took:04.3f} seconds")


def sleep_task():
    time.sleep(2)
    return


if __name__ == "__main__":
    record_runtime(sleep_task)()
