from elasticsearch import NotFoundError
import esmongo.constant as const
from esmongo.db_server import MongoDB, ES
from esmongo.loader import load_dummy_data


class TestMongoDB:
    dummy = load_dummy_data()
    server = MongoDB(
        host=const.HOST_MONGODB,
        database_name=dummy["dummy_index"],
        document_name=dummy["dummy_index"],
    )

    # Drop collection before start
    server.drop_documents()

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
        self.server.update_data(
            filters=self.dummy["dummy_filter_update"], update=self.dummy["dummy_update"]
        )
        assert (
            self.server.count_documents(filters=self.dummy["dummy_filter_update"]) == 1
        )

    def test_update_many_data(self):
        self.server.update_data(
            filters=self.dummy["dummy_filter_update"], update=self.dummy["dummy_update"]
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
        resp = self.server.count_index(index_name=self.dummy["dummy_index"])
        self.server.search_data(self.dummy["dummy_index"])
        assert resp["count"] == 1

    def test_insert_many_data(self):
        self.server.insert_data(
            index_name=self.dummy["dummy_index"],
            data=[self.dummy["dummy_data_2"], self.dummy["dummy_data_3"]],
        )
        resp = self.server.count_index(index_name=self.dummy["dummy_index"])
        assert resp["count"] == 3

    # def test_search_data(self):
    #     items = self.server.search_data(filters={})
    #     assert len(items) == 3

    # def test_update_single_data(self):
    #     self.server.update_data(filters=self.dummy["dummy_filter_update"], update=self.dummy["dummy_update"])
    #     assert self.server.count_documents(filters=self.dummy["dummy_filter_update"]) == 1

    # def test_update_many_data(self):
    #     self.server.update_data(filters=dummy["dummy_filter_update"], update=dummy["dummy_update"])
    #     assert self.server.count_documents(filters=dummy["dummy_filter_update"]) == 0

    # def test_delete_single_data(self):
    #     self.server.delete_data(dummy["dummy_filter_delete_1"], how="one")
    #     assert self.server.count_documents(filters=dummy["dummy_filter_delete_1"]) == 0

    # def test_delete_many_data(self):
    #     self.server.delete_data(dummy["dummy_filter_delete_2"], how="many")
    #     assert self.server.count_documents(filters=dummy["dummy_filter_delete_2"]) == 0
