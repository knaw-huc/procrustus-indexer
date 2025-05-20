from abc import ABC, abstractmethod

class Transformer(ABC):
    """
    Base class for transformers.
    """
    @abstractmethod
    def transform(self, x) -> dict:
        """
        Transfom x into dict.
        :param x:
        :return:
        """

    def transform_batch(self, x: list) -> list:
        """
        Perform batch transformation.
        :param x:
        :return:
        """
        return [self.transform(i) for i in x]
