from . import constant as c
from .models import Task
from .db_client import MongoDB, ES
from .loader import load_from_csv
from .models import Task

INDEX_NAME = "performance"

# MongoDB Task
## Initiate connection
mongo_client = MongoDB(host=c.HOST_MONGODB, database_name=INDEX_NAME, document_name=INDEX_NAME)

## Data Source
json_data = load_from_csv("esmongo/data/test_data.csv")

## Make some list
mongodb_task = [
    Task(operation=mongo_client.insert_data, operation_kwargs=dict(data=json_data))
]


es_client = ES(host=c.HOST_ES, username=c.USER_ES, password=c.PWD_ES, index_name=INDEX_NAME)