from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Mapping


class DatabaseServer(ABC):
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


@dataclass
class Task:
    operation: Callable
    kwargs: Mapping
