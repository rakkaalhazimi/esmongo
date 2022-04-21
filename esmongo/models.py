from abc import ABC, abstractmethod


class DBClient(ABC):
    @abstractmethod
    def insert_data(self):
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