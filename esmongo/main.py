import warnings
warnings.filterwarnings("ignore")

from .performance import DBPerformanceTest
from .tasks import mongo_client, mongodb_task


# MongoDB Performance Test
mongo_test = DBPerformanceTest(client=mongo_client, tasks=mongodb_task)
mongo_test.start()


# ElasticSearch Performance Test