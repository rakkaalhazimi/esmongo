import warnings
warnings.filterwarnings("ignore")

from esmongo import constant as c
from esmongo.db_client import MongoDB, ES
from esmongo.loader import load_from_csv




def start_mongodb_test(options):

    # Data source
    source = load_from_csv(options.data)

    # MongoDB Client
    print("Mongo DB")
    print("=" * 40)
    mongo_client = MongoDB(host=c.HOST_MONGODB,
                        db_name=c.INDEX_NAME, doc_name=c.INDEX_NAME)
    # Mongo CRUD
    mongo_client.insert_data(data=source)
    mongo_client.search_data(filters={"name": "ame"})
    mongo_client.update_data(filters={"name": "ame"}, update={"$set": {"name": "ami"}})
    mongo_client.delete_data(filters={"name": "ami"})
    mongo_client.drop_collections()
    print()


def start_es_test(options):

    # Data source
    source = load_from_csv(options.data)

    # ElasticSearch Client
    es_client = ES(host=c.HOST_ES, user=c.USER_ES,
                pwd=c.PWD_ES, index_name=c.INDEX_NAME)
    # ElasticSearch CRUD
    print("ElasticSearch")
    print("=" * 40)
    es_client.insert_data(data=source)
    es_client.search_data(query={"match": {"name": "ame"}})
    es_client.update_data(query={"match": {"name": "ame"}}, update={"name": "ami"})
    es_client.delete_data(query={"match": {"name": "ame"}})

    es_client.drop_collections()


def start_test(options):
    start_mongodb_test(options)
    start_es_test(options)