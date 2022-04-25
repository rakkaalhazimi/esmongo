import argparse
from esmongo import app

arg = argparse.ArgumentParser(prog="Esmongo", description="MongoDB and ElasticSearch Performance Tester")
arg.add_argument("-d", "--data", default=None, help="CSV data path for testing")
options = arg.parse_args()


app.start_test(options)