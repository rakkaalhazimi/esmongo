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

## Make list of task
mongodb_tasks = [
    ### Insert task
    Task(operation=mongo_client.insert_data, operation_kwargs=dict(data=json_data)),

    ### Search task
    Task(operation=mongo_client.search_data, operation_kwargs=dict(filters={"name": "ame"})),

    ### Update task
    Task(operation=mongo_client.update_data, operation_kwargs=dict(filters={"name": "ame"}, update={"$set": {"name": "ami"}}, how="one")),

    ### Delete task
    Task(operation=mongo_client.delete_data, operation_kwargs=dict(filters={"name": "ami"}, how="one")),
]


# ElasticSearch Task
## Initiate connection
es_client = ES(host=c.HOST_ES, username=c.USER_ES, password=c.PWD_ES, index_name=INDEX_NAME)

## Data Source
json_data = load_from_csv("esmongo/data/test_data.csv")

## Make list of task
es_tasks = [
    ### Insert task
    Task(operation=es_client.insert_data, operation_kwargs=dict(data=json_data)),

    ### Search task
    Task(operation=es_client.search_data, operation_kwargs=dict(query={"match": {"name": "ame"}})),

    ### Update task
    Task(operation=es_client.update_data, operation_kwargs=dict(query={"match": {"name": "ame"}}, update={"name": "ami"}, how="one")),

    ### Delete task
    Task(operation=es_client.delete_data, operation_kwargs=dict(query={"match": {"name": "ame"}}, how="one")),
]