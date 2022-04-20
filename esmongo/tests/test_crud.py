from elasticsearch import NotFoundError
import esmongo.constant as const
from esmongo.db_client import MongoDB, ES
from esmongo.loader import load_dummy_data


class TestMongoDB:
    """
    Test MongoDB Create, Read, Update and Delete functionalities.
    The steps are:
    1. Initialize data and connection.
    2. Drop collection for us to have a fresh start.
    3. Perform CRUD test.

    We count the documents on every test to validate the results
    """

    # 1. Initialize data and connection.
    dummy = load_dummy_data()
    server = MongoDB(
        host=const.HOST_MONGODB,
        db_name=dummy["dummy_index"],
        doc_name=dummy["dummy_index"],
    )

    # 2. Drop collection before start.
    server.drop_collections()

    # 3. Test CRUD operations on database server.
    def test_insert_single_data(self):
        # insert_data() method can recognize single and multiple data
        self.server.insert_data(data=self.dummy["dummy_data_1"])
        assert self.server.count_documents(filters={}) == 1

    def test_insert_many_data(self):
        # insert_data() method can recognize single and multiple data
        self.server.insert_data(
            data=[self.dummy["dummy_data_2"], self.dummy["dummy_data_3"]]
        )
        assert self.server.count_documents(filters={}) == 3

    def test_search_data(self):
        items = self.server.search_data(filters={})
        assert len(items) == 3

    def test_update_single_data(self):
        # To update MongoDB documents, use {"$set": query} format.
        update = {"$set": self.dummy["dummy_update"]}

        # For single data, set "how" kwarg to "one"
        self.server.update_data(
            filters=self.dummy["dummy_filter_update"], update=update, how="one"
        )
        assert (
            self.server.count_documents(filters=self.dummy["dummy_filter_update"]) == 1
        )

    def test_update_many_data(self):
        # To update MongoDB documents, use {"$set": query} format.
        update = {"$set": self.dummy["dummy_update"]}

        # For multiple data, set "how" kwarg to "many"
        self.server.update_data(
            filters=self.dummy["dummy_filter_update"], update=update, how="many"
        )
        assert (
            self.server.count_documents(filters=self.dummy["dummy_filter_update"]) == 0
        )

    def test_delete_single_data(self):
        # For single data, set "how" kwarg to "one"
        self.server.delete_data(self.dummy["dummy_filter_delete_1"], how="one")
        assert (
            self.server.count_documents(filters=self.dummy["dummy_filter_delete_1"])
            == 0
        )

    def test_delete_many_data(self):
        # For multiple data, set "how" kwarg to "many"
        self.server.delete_data(self.dummy["dummy_filter_delete_2"], how="many")
        assert (
            self.server.count_documents(filters=self.dummy["dummy_filter_delete_2"])
            == 0
        )


class TestES:
    """
    Test ElasticSearch Create, Read, Update and Delete functionalities.
    The steps are:
    1. Initialize data and connection.
    2. Drop index for us to have a fresh start.
    3. Perform CRUD test.

    We count the documents on every test to validate the results
    """

    # 1. Initialize data and connection.
    dummy = load_dummy_data()
    server = ES(
        host=const.HOST_ES,
        user=const.USER_ES,
        pwd=const.PWD_ES,
        index_name=dummy["dummy_index"],
    )

    # 2. Drop index before start, catch the error when no index is found.
    try:
        server.drop_collections()
    except NotFoundError:
        print("No index found, continuing the test.")

    # 3. Test CRUD operations on database server.
    def test_insert_single_data(self):
        self.server.insert_data(data=self.dummy["dummy_data_1"])
        resp = self.server.count_documents()
        self.server.search_data()
        assert resp["count"] == 1

    def test_insert_many_data(self):
        self.server.insert_data(
            data=[self.dummy["dummy_data_2"], self.dummy["dummy_data_3"]],
        )
        resp = self.server.count_documents()
        assert resp["count"] == 3

    def test_search_data(self):
        # Search data on ElasticSearch returns dict-like data with many keys.
        # Get "hits" key to obtain the documents results
        resp = self.server.search_data()
        assert len(resp["hits"]) == 3

    def test_update_single_data(self):
        # To perform match in ElasticSearch, use {"match": query} format.
        query = {"match": self.dummy["dummy_filter_update"]}

        # For single data, set "how" kwarg to "one"
        self.server.update_data(
            query=query,
            update=self.dummy["dummy_update"],
            how="one",
        )
        resp = self.server.count_documents(query=query)
        assert resp["count"] == 1

    def test_update_many_data(self):
        # To perform match in ElasticSearch, use {"match": query} format.
        query = {"match": self.dummy["dummy_filter_update"]}

        # For multiple data, set "how" kwarg to "many"
        self.server.update_data(
            query=query,
            update=self.dummy["dummy_update"],
            how="many",
        )
        resp = self.server.count_documents(query=query)
        assert resp["count"] == 0

    def test_delete_single_data(self):
        # To perform match in ElasticSearch, use {"match": query} format.
        query = {"match": self.dummy["dummy_filter_delete_1"]}

        # For single data, set "how" kwarg to "one"
        self.server.delete_data(
            query=query,
            how="one",
        )
        resp = self.server.count_documents()
        assert resp["count"] == 2

    def test_delete_many_data(self):
        # To perform match in ElasticSearch, use {"match": query} format.
        query = {"match": self.dummy["dummy_filter_delete_2"]}

        # For multiple data, set "how" kwarg to "many"
        self.server.delete_data(
            query=query,
            how="many",
        )
        resp = self.server.search_data()
        resp = self.server.count_documents()
        assert resp["count"] == 0
