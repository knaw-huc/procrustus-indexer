from abc import ABC, abstractmethod

class Extractor(ABC):
    """
    Base class for all extracters.
    """
    @abstractmethod
    def extract_single(self, id):
        """
        Load a single item, by id.
        :param id:
        :return:
        """

    @abstractmethod
    def extract_batch(self, start: int, num: int) -> list:
        """
        Load a batch, from the start and num items.
        :param start:
        :param num:
        :return:
        """

    @abstractmethod
    def generate_batch(self, batch_size: int) -> list:
        """
        Generate batch.
        :return:
        """
