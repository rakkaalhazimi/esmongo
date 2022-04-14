import os
import pytest

def run_test():
    dirname = os.path.dirname(__file__)
    test_path = os.path.join(dirname, "tests")
    pytest.main([test_path])