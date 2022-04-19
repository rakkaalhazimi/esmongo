from typing import Sequence, Mapping, Any
from uuid import uuid4

from pymongo import MongoClient
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elastic_transport import ObjectApiResponse

from models import DatabaseServer


Document = Filter = Query = Script = Mapping[str, Any]

class MongoDB(DatabaseServer):
    def __init__(self, host: str, database_name: str, document_name: str):
        self.host = host
        self.client = MongoClient(self.host)
        self.db = self.client[database_name]
        self.collection = self.db[document_name]

    def count_documents(self, filters: Filter = {}) -> int:
        return self.collection.count_documents(filters)

    def drop_collections(self):
        self.collection.drop()

    def insert_data(self, data: Document or Sequence[Document]):
        if not isinstance(data, Sequence):
            self.collection.insert_one(data)
        else:
            self.collection.insert_many(data)

    def search_data(self, filters: Filter = {}) -> Sequence[Document]:
        items = self.collection.find(filters)
        return [item for item in items]

    def update_data(self, filters: Filter, update: Document, how: str = "one"):
        how = how.lower()
        if how == "one":
            self.collection.update_one(filters, update)
        elif how == "many":
            self.collection.update_many(filters, update)
        else:
            raise ValueError(
                f"Update method is either 'one' or 'many' but you type {how}"
            )

    def delete_data(self, filters: Filter, how: str = "one"):
        if how == "one":
            self.collection.delete_one(filters)
        elif how == "many":
            self.collection.delete_many(filters)
        else:
            raise ValueError(
                f"Delete method is either 'one' or 'many' but you type {how}"
            )


class ES(DatabaseServer):
    def __init__(self, host: str, username: str, password: str):
        self.host = host
        self.client = Elasticsearch(
            hosts=self.host, basic_auth=(username, password), verify_certs=False
        )

    def quote_string(self, data):
        return f"'{data}'" if type(data) == str else data

    def count_documents(self, index_name: str, query: str = None) -> ObjectApiResponse:
        return self.client.count(index=index_name, query=query)

    def drop_collections(self, index_name: str) -> ObjectApiResponse:
        return self.client.indices.delete(index=index_name)

    def insert_data(
        self, index_name: str, data: Document or Sequence[Document]
    ) -> ObjectApiResponse:
        if not isinstance(data, Sequence):
            return self.client.create(
                index=index_name, id=uuid4(), document=data, refresh=True
            )
        else:
            actions = (
                {
                    "_op_type": "create",
                    "_index": index_name,
                    "_id": uuid4(),
                    "_source": doc,
                }
                for doc in data
            )
            return bulk(client=self.client, actions=actions, refresh=True)

    def search_data(self, index_name: str, query: Query = None) -> ObjectApiResponse:
        return self.client.search(index=index_name, query=query)

    def update_data(
        self, index_name: str, query: Query, update: Document, how: str = "one"
    ) -> ObjectApiResponse:
        script = {
            "source": ";".join(
                f"ctx._source['{key}']={self.quote_string(value)}" for key, value in update.items()
            ),
            "lang": "painless",
        }
        how = how.lower()
        if how == "one":
            return self.client.update_by_query(
                index=index_name, query=query, script=script, max_docs=1, refresh=True
            )
        elif how == "many":
            return self.client.update_by_query(
                index=index_name, query=query, script=script, refresh=True
            )
        else:
            raise ValueError(
                f"Update method is either 'one' or 'many' but you type {how}"
            )

    def delete_data(self, index_name: str, query: Query, how: str = "one") -> ObjectApiResponse:
        how = how.lower()
        if how == "one":
            return self.client.delete_by_query(
                index=index_name, query=query, max_docs=1, refresh=True
            )
        elif how == "many":
            return self.client.delete_by_query(
                index=index_name, query=query, refresh=True
            )
        else:
            raise ValueError(
                f"Delete method is either 'one' or 'many' but you type {how}"
            )


if __name__ == "__main__":

    import constant as const

    mongo_server = MongoDB(
        host=const.HOST_MONGODB, database_name="rakka", document_name="rakka"
    )

    es_server = ES(host=const.HOST_ES, username=const.USER_ES, password=const.PWD_ES)

    dummy_data = {"name": "rakka", "job": "entepreneur"}
    print(es_server.client)
