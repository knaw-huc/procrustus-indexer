"""
Base class for transformations.
"""
from abc import ABC, abstractmethod
from functools import lru_cache


class Transformation(ABC):
    """
    An abstract class for transformations.
    """
    @abstractmethod
    def __init__(self, **kwargs):
        """
        Constructor
        :param kwargs:
        """

    @classmethod
    @lru_cache
    def get(cls, **kwargs):
        """
        :return:
        """
        return cls(**kwargs)

    @abstractmethod
    @lru_cache
    def transform(self, value: str) -> str:
        """
        Apply the transformation to the given text.
        :param value:
        :return:
        """
