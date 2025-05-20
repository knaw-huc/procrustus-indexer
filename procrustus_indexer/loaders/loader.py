from abc import ABC, abstractmethod

class Loader(ABC):
    """
    Base class for all loaders.
    """

    @abstractmethod
    def load(self, data: dict):
        """
        Load step of ETL, store data.
        :param data:
        :return:
        """


    @abstractmethod
    def load_bulk(self, data: list[dict]):
        """
        Load multiple items at once.
        :param data:
        :return:
        """


    @abstractmethod
    def setup(self) -> None:
        """
        Function to call before an import. Use this to e.g. initialize database tables if they
        don't exist.
        :return:
        """
