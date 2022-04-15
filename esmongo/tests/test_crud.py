import esmongo.constant as const
from esmongo.db_server import MongoDB, ES

dummy_data_1 = {"name": "john", "age": 24}
dummy_data_2 = {"name": "reiner", "age": 25}
dummy_data_3 = {"name": "john", "age": 26}
dummy_filter_update = {"name": "john"}
dummy_update = {"$set": {"name": "paladin"}}
dummy_filter_delete_1 = {"name": "reiner"}
dummy_filter_delete_2 = {"name": "john"}
dummy_index = "pytest"

class TestMongoDB:
    # Initial connection
    server = MongoDB(host=const.HOST_MONGODB, database_name=dummy_index, document_name=dummy_index)

    # Drop collection before start
    server.drop_documents()

    def test_insert_single_data(self):
        self.server.insert_data(data=dummy_data_1)
        assert self.server.count_documents(filters={}) == 1

    def test_insert_many_data(self):
        self.server.insert_data(data=[dummy_data_2, dummy_data_3])
        assert self.server.count_documents(filters={}) == 3

    def test_search_data(self):
        items = self.server.search_data(filters={})
        assert len(items) == 3

    def test_update_single_data(self):
        self.server.update_data(filters=dummy_filter_update, update=dummy_update)
        assert self.server.count_documents(filters=dummy_filter_update) == 1

    def test_update_many_data(self):
        self.server.update_data(filters=dummy_filter_update, update=dummy_update)
        assert self.server.count_documents(filters=dummy_filter_update) == 0

    def test_delete_single_data(self):
        self.server.delete_data(dummy_filter_delete_1, how="one")
        assert self.server.count_documents(filters=dummy_filter_delete_1) == 0

    def test_delete_many_data(self):
        self.server.delete_data(dummy_filter_delete_2, how="many")
        assert self.server.count_documents(filters=dummy_filter_delete_2) == 0
    


class TestES:
    def test_init(self):
        server = ES(host=const.HOST_ES)
        server.connect(username=const.USER_ES, password=const.PWD_ES)

    def test_insert_data(self):
        pass