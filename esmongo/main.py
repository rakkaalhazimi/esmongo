import warnings
warnings.filterwarnings("ignore")

from .performance import DBPerformanceTest
from .tasks import mongo_client, mongodb_tasks, es_client, es_tasks


# MongoDB Performance Test
mongo_test = DBPerformanceTest(client=mongo_client, tasks=mongodb_tasks)
mongo_test.start()


# ElasticSearch Performance Test
es_test = DBPerformanceTest(client=es_client, tasks=es_tasks)
es_test.start()