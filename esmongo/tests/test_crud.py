from elasticsearch import NotFoundError
import esmongo.constant as const
from esmongo.db_server import MongoDB, ES
from esmongo.loader import load_dummy_data


class TestMongoDB:
    """Test """
    dummy = load_dummy_data()
    server = MongoDB(
        host=const.HOST_MONGODB,
        database_name=dummy["dummy_index"],
        document_name=dummy["dummy_index"],
    )

    # Drop collection before start
    server.drop_collections()

    def test_insert_single_data(self):
        self.server.insert_data(data=self.dummy["dummy_data_1"])
        assert self.server.count_documents(filters={}) == 1

    def test_insert_many_data(self):
        self.server.insert_data(
            data=[self.dummy["dummy_data_2"], self.dummy["dummy_data_3"]]
        )
        assert self.server.count_documents(filters={}) == 3

    def test_search_data(self):
        items = self.server.search_data(filters={})
        assert len(items) == 3

    def test_update_single_data(self):
        update = {"$set": self.dummy["dummy_update"]}
        self.server.update_data(
            filters=self.dummy["dummy_filter_update"], update=update
        )
        assert (
            self.server.count_documents(filters=self.dummy["dummy_filter_update"]) == 1
        )

    def test_update_many_data(self):
        update = {"$set": self.dummy["dummy_update"]}
        self.server.update_data(
            filters=self.dummy["dummy_filter_update"], update=update
        )
        assert (
            self.server.count_documents(filters=self.dummy["dummy_filter_update"]) == 0
        )

    def test_delete_single_data(self):
        self.server.delete_data(self.dummy["dummy_filter_delete_1"], how="one")
        assert (
            self.server.count_documents(filters=self.dummy["dummy_filter_delete_1"])
            == 0
        )

    def test_delete_many_data(self):
        self.server.delete_data(self.dummy["dummy_filter_delete_2"], how="many")
        print(self.server.search_data())
        assert (
            self.server.count_documents(filters=self.dummy["dummy_filter_delete_2"])
            == 0
        )


class TestES:
    dummy = load_dummy_data()
    server = ES(host=const.HOST_ES, username=const.USER_ES, password=const.PWD_ES)

    # Drop index before start, catch the error when no index is found
    try:
        server.drop_index(index_name=dummy["dummy_index"])
    except NotFoundError:
        print("No index found, continuing the test.")

    def test_insert_single_data(self):
        self.server.insert_data(
            index_name=self.dummy["dummy_index"], data=self.dummy["dummy_data_1"]
        )
        resp = self.server.count_documents(index_name=self.dummy["dummy_index"])
        self.server.search_data(self.dummy["dummy_index"])
        assert resp["count"] == 1

    def test_insert_many_data(self):
        self.server.insert_data(
            index_name=self.dummy["dummy_index"],
            data=[self.dummy["dummy_data_2"], self.dummy["dummy_data_3"]],
        )
        resp = self.server.count_documents(index_name=self.dummy["dummy_index"])
        assert resp["count"] == 3

    def test_search_data(self):
        resp = self.server.search_data(index_name=self.dummy["dummy_index"])
        assert len(resp["hits"]) == 3

    def test_update_single_data(self):
        query = {"match": self.dummy["dummy_filter_update"]}
        self.server.update_data(
            index_name=self.dummy["dummy_index"],
            query=query,
            update=self.dummy["dummy_update"],
            how="one",
        )
        resp = self.server.count_documents(
            index_name=self.dummy["dummy_index"], query=query
        )
        assert resp["count"] == 1

    def test_update_many_data(self):
        query = {"match": self.dummy["dummy_filter_update"]}
        self.server.update_data(
            index_name=self.dummy["dummy_index"],
            query=query,
            update=self.dummy["dummy_update"],
            how="many",
        )
        resp = self.server.count_documents(
            index_name=self.dummy["dummy_index"], query=query
        )
        assert resp["count"] == 0

    def test_delete_single_data(self):
        query = {"match": self.dummy["dummy_filter_delete_1"]}
        self.server.delete_data(
            index_name=self.dummy["dummy_index"],
            query=query,
            how="one",
        )
        resp = self.server.count_documents(
            index_name=self.dummy["dummy_index"]
        )
        assert resp["count"] == 2

    def test_delete_many_data(self):
        query = {"match": self.dummy["dummy_filter_delete_2"]}
        self.server.delete_data(
            index_name=self.dummy["dummy_index"],
            query=query,
            how="many",
        )
        resp = self.server.search_data(index_name=self.dummy["dummy_index"])
        print(resp)
        resp = self.server.count_documents(
            index_name=self.dummy["dummy_index"]
        )
        assert resp["count"] == 0

    