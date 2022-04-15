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
    def __init__(self, host: str, database_name: str, document_name: str):
        self.host = host
        self.client = MongoClient(self.host)
        self.db = self.client[database_name]
        self.collection = self.db[document_name]

    def count_documents(self, filters: Filter={}) -> int:
        return self.collection.count_documents(filters)

    def drop_documents(self):
        self.collection.drop()

    def insert_data(self, data: Document or Sequence[Document]):
        if not isinstance(data, Sequence):
            self.collection.insert_one(data)
        else:
            self.collection.insert_many(data)

    def search_data(self, filters: Filter={}) -> Sequence[Document]:
        items = self.collection.find(filters)
        return [item for item in items]

    def update_data(self, filters: Filter, update: Document, how: str="one"):
        how = how.lower()
        if how == "one":
            self.collection.update_one(filters, update)
        elif how == "many":
            self.collection.update_many(filters, update)
        else:
            raise ValueError(f"Update method is either 'single' or 'many' but you type {how}")

    def delete_data(self, filters: Filter, how: str="one"):
        if how == "one":
            self.collection.delete_one(filters)
        elif how == "many":
            self.collection.delete_many(filters)
        else:
            raise ValueError(f"Delete method is either 'single' or 'many' but you type {how}")



class ES(DatabaseServer):
    def __init__(self, host: str, username: str, password: str):
        self.host = host
        self.client = Elasticsearch(hosts=self.host, basic_auth=(username, password), verify_certs=False)

    def insert_data(self, index_name: str, data: Mapping[str, str] or Sequence[Document]):
        if not isinstance(data, Sequence):
            self.client.create(index=index_name, id=uuid4(), document=data)
        else:
            actions = ({"_op_type": "create", "_index": index_name, "_id": uuid4(), "_source": doc} for doc in data)
            for act in actions:
                print(act)
            bulk(client=self.client, actions=actions)

    def search_data(self, index_name: str) -> ObjectApiResponse:
        items = self.client.search(index=index_name)
        return items

    def update_data(self):
        pass

    def delete_data(self, index_name:str, id_: str):
        self.client.delete(index=index_name, id=id_)


if __name__ == "__main__":
    
    import constant as const

    mongo_server = MongoDB(host=const.HOST_MONGODB, database_name="rakka", document_name="rakka")

    es_server = ES(host=const.HOST_ES, username=const.USER_ES, password=const.PWD_ES)

    dummy_data = {"name": "rakka", "job": "entepreneur"}
    print(es_server.client)
