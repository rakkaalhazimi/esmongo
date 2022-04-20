import warnings
warnings.filterwarnings("ignore")

from . import constant as c
from .models import Task
from .db_client import MongoDB, ES
from .loader import load_from_csv
from .performance import DBPerformanceTest

INDEX_NAME = "performance"

# Initiate db connections
mongo_client = MongoDB(host=c.HOST_MONGODB, database_name=INDEX_NAME, document_name=INDEX_NAME)
es_client = ES(host=c.HOST_ES, username=c.USER_ES, password=c.PWD_ES, index_name=INDEX_NAME)

# Load data
json_data = load_from_csv("esmongo/data/test_data.csv")

# List DB Tasks
mongo_test = DBPerformanceTest(client=mongo_client)
mongo_test.register_task(operation=mongo_client.insert_data, operation_kwargs=dict(data=json_data))




# ElasticSearch Tasks