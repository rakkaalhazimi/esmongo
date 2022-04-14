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
    server = MongoDB(const.HOST_MONGODB)
    server.connect()

    # Choose db and collection
    db = server.client[dummy_index]
    collection = db[dummy_index]

    # Drop collection before start
    collection.drop()

    def test_insert_single_data(self):
        self.collection.insert_one(dummy_data_1)
        assert self.collection.count_documents({}) == 1

    def test_insert_many_data(self):
        self.collection.insert_many([dummy_data_2, dummy_data_3])
        assert self.collection.count_documents({}) == 3

    def test_update_single_data(self):
        self.collection.update_one(dummy_filter_update, dummy_update)
        assert self.collection.count_documents(dummy_filter_update) == 1

    def test_update_many_data(self):
        self.collection.update_many(dummy_filter_update, dummy_update)
        assert self.collection.count_documents(dummy_filter_update) == 0

    def test_delete_single_data(self):
        self.collection.delete_one(dummy_filter_delete_1)
        assert self.collection.count_documents(dummy_filter_delete_1) == 0

    def test_delete_many_data(self):
        self.collection.delete_many(dummy_filter_delete_2)
        assert self.collection.count_documents(dummy_filter_delete_2) == 0
    


class TestES:
    def test_init(self):
        server = ES(host=const.HOST_ES)
        server.connect(username=const.USER_ES, password=const.PWD_ES)

    def test_insert_data(self):
        pass