from abc import ABC
from typing import IO


class Parser(ABC):
    def parse_file(self, file: IO) -> dict:
        pass