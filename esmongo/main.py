import warnings
warnings.filterwarnings("ignore")

from . import constant as c
from .models import Task
from .db_server import MongoDB, ES
from .loader import load_from_csv
from .performance import DbPerformance

INDEX_NAME = "performance"

# Initiate db connections
mongo_server = MongoDB(host=c.HOST_MONGODB, database_name=INDEX_NAME, document_name=INDEX_NAME)
es_server = ES(host=c.HOST_ES, username=c.USER_ES, password=c.PWD_ES, index_name=INDEX_NAME)

# Load data
json_data = load_from_csv("esmongo/data/test_data.csv")

# List of MongoDB Tasks
mongo_task_list = [
    Task(operation=mongo_server.insert_data, kwargs=dict(data=json_data))
]
## Run Tasks
mongo_task_runner = DbPerformance(server=mongo_server, tasks=mongo_task_list)
mongo_task_runner.start()


# ElasticSearch Tasks