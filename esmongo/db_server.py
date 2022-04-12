from abc import ABC, abstractmethod
from typing import Sequence, Mapping, Any
from uuid import uuid4

from pymongo import MongoClient
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elastic_transport import ObjectApiResponse


Document = Filter = Mapping[str, Any]

class DatabaseServer(ABC):
    @abstractmethod
    def connect(self, *args, **kwargs):
        pass
    
    @abstractmethod
    def insert_data(self, data, **kwargs):
        pass
    
    @abstractmethod
    def search_data(self):
        pass

    @abstractmethod
    def update_data(self):
        pass

    @abstractmethod
    def delete_data(self):
        pass


class MongoDB(DatabaseServer):
    def __init__(self, host: str):
        self.host = host
        self.client = None

    def connect(self):
        self.client = MongoClient(self.host)
        return True

    def insert_data(self, database_name: str, document_name: str, data: Document or Sequence[Document]):
        db = self.client[database_name]
        collection = db[document_name]
        if not isinstance(data, Sequence):
            collection.insert_one(data)
        else:
            collection.insert_many(data)

    def search_data(self, database_name: str, document_name: str) -> Sequence[Document]:
        db = self.client[database_name]
        collection = db[document_name]
        items = collection.find()
        return [item for item in items]

    def update_data(self, database_name: str, document_name: str, filters: Filter, update: Document):
        db = self.client[database_name]
        collection = db[document_name]
        if not isinstance(update, Sequence):
            collection.update_one(filters, update)
        else:
            collection.update_many(filters, update)

    def delete_data(self):
        pass


class ES(DatabaseServer):
    def __init__(self, host: str):
        self.host = host
        self.es = None

    def connect(self, username: str, password: str):
        self.es = Elasticsearch(hosts=self.host, basic_auth=(username, password), verify_certs=False)
        return True

    def insert_data(self, index_name: str, data: Mapping[str, str] or Sequence[Document]):
        if not isinstance(data, Sequence):
            self.es.create(index=index_name, id=uuid4(), document=data)
        else:
            actions = ({"_op_type": "create", "_index": index_name, "_id": uuid4(), "_source": doc} for doc in data)
            for act in actions:
                print(act)
            bulk(client=self.es, actions=actions)

    def search_data(self, index_name: str) -> ObjectApiResponse:
        items = self.es.search(index=index_name)
        return items

    def update_data(self):
        pass

    def delete_data(self, index_name:str, id_: str):
        self.es.delete(index=index_name, id=id_)


if __name__ == "__main__":
    
    import constant as const

    mongo_server = MongoDB(host=const.HOST_MONGODB)
    mongo_server.connect()

    es_server = ES(host=const.HOST_ES)
    es_server.connect(username=const.USER_ES, password=const.PWD_ES)

    dummy_data = {"name": "rakka", "job": "entepreneur"}
    print(es_server.es)
