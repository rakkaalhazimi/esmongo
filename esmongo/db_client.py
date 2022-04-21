from typing import Sequence, Mapping, Any
from uuid import uuid4

from pymongo import MongoClient
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elastic_transport import ObjectApiResponse

from esmongo.models import DBClient
from esmongo.performance import CodeTimer


Document = Filter = Query = Script = Mapping[str, Any]


class MongoDB(DBClient):
    def __init__(self, host: str, db_name: str, doc_name: str):
        self.host = host
        self.client = MongoClient(self.host)
        self.db = self.client[db_name]
        self.collection = self.db[doc_name]

    def count_documents(self, filters: Filter = {}) -> int:
        return self.collection.count_documents(filters)

    def drop_collections(self):
        self.collection.drop()

    def insert_data(self, data: Document or Sequence[Document]):
        if not isinstance(data, Sequence):
            with CodeTimer("Insert Single Data"):
                self.collection.insert_one(data)
        else:
            with CodeTimer("Insert Multiple Data"):
                self.collection.insert_many(data)

    def search_data(self, filters: Filter = {}) -> Sequence[Document]:
        with CodeTimer("Search Data"):
            items = self.collection.find(filters)
        return [item for item in items]

    def update_data(self, filters: Filter, update: Document, how: str = "one"):
        how = how.lower()
        if how == "one":
            with CodeTimer("Update Single Data"):
                self.collection.update_one(filters, update)
        elif how == "many":
            with CodeTimer("Update Multiple Data"):
                self.collection.update_many(filters, update)
        else:
            raise ValueError(
                f"Update method is either 'one' or 'many' but you type {how}"
            )

    def delete_data(self, filters: Filter, how: str = "one"):
        if how == "one":
            with CodeTimer("Delete Single Data"):
                self.collection.delete_one(filters)
        elif how == "many":
            with CodeTimer("Delete Multiple Data"):
                self.collection.delete_many(filters)
        else:
            raise ValueError(
                f"Delete method is either 'one' or 'many' but you type {how}"
            )


class ES(DBClient):
    def __init__(self, host: str, user: str, pwd: str, index_name: str):
        self.host = host
        self.client = Elasticsearch(
            hosts=self.host, basic_auth=(user, pwd), verify_certs=False
        )
        self.index_name = index_name

    def quote_string(self, data):
        return f"'{data}'" if type(data) == str else data

    def create_actions_query(self, data: Sequence[Document]) -> Query:
        actions = [
            {
                "_op_type": "create",
                "_index": self.index_name,
                "_id": uuid4(),
                "_source": doc,
            }
            for doc in data
        ]
        return actions

    def create_script_query(self, data: Document) -> Script:
        script = {
            "source": ";".join(
                f"ctx._source['{key}']={self.quote_string(value)}"
                for key, value in data.items()
            ),
            "lang": "painless",
        }
        return script

    def count_documents(self, query: str = None) -> ObjectApiResponse:
        return self.client.count(index=self.index_name, query=query)

    def drop_collections(self) -> ObjectApiResponse:
        return self.client.indices.delete(index=self.index_name)

    def insert_data(self, data: Document or Sequence[Document]) -> ObjectApiResponse:
        if not isinstance(data, Sequence):
            with CodeTimer("Insert Single Data"):
                return self.client.create(
                    index=self.index_name, id=uuid4(), document=data, refresh=True
                )
        else:
            actions = self.create_actions_query(data=data)
            with CodeTimer("Insert Multiple Data"):
                return bulk(client=self.client, actions=actions, refresh=True)

    def search_data(self, query: Query = None) -> ObjectApiResponse:
        with CodeTimer("Search Data"):
            return self.client.search(index=self.index_name, query=query)

    def update_data(
        self, query: Query, update: Document, how: str = "one"
    ) -> ObjectApiResponse:
        script = self.create_script_query(data=update)
        how = how.lower()
        if how == "one":
            with CodeTimer("Update Single Data"):
                return self.client.update_by_query(
                    index=self.index_name,
                    query=query,
                    script=script,
                    max_docs=1,
                    refresh=True,
                )
        elif how == "many":
            with CodeTimer("Update Multiple Data"):
                return self.client.update_by_query(
                    index=self.index_name, query=query, script=script, refresh=True
                )
        else:
            raise ValueError(
                f"Update method is either 'one' or 'many' but you type {how}"
            )

    def delete_data(self, query: Query, how: str = "one") -> ObjectApiResponse:
        how = how.lower()
        if how == "one":
            with CodeTimer("Delete Single Data"):
                return self.client.delete_by_query(
                    index=self.index_name, query=query, max_docs=1, refresh=True
                )
        elif how == "many":
            with CodeTimer("Delete Multiple Data"):
                return self.client.delete_by_query(
                    index=self.index_name, query=query, refresh=True
                )
        else:
            raise ValueError(
                f"Delete method is either 'one' or 'many' but you type {how}"
            )
