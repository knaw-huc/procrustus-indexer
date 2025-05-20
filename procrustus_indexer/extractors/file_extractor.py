import glob
import os

from procrustus_indexer.extractors import Extractor

class FileExtractor(Extractor):
    """
    Loader for files on the filesystem.
    """
    folder_path: str
    glob_pattern: str

    def __init__(self, folder_path: str, glob_pattern: str = "*"):
        """
        Creates a new instance of the FileLoader class.
        :param folder_path:
        :param glob_pattern:
        """
        self.folder_path = folder_path
        self.glob_pattern = glob_pattern


    def _open_file(self, path) -> str:
        """
        Reads a file
        :param path:
        :return:
        """
        try:
            with open(path, "rb") as file:
                return file.read()
        except FileNotFoundError:
            return ""


    def extract_batch(self, start: int, num: int):
        """

        :param start:
        :param num:
        :return:
        """
        filenames = glob.glob(os.path.join(self.folder_path, self.glob_pattern), recursive=True)
        batch = filenames[start:start+num]

        tmp = []
        for filename in batch:
            tmp.append(self._open_file(filename))
        return tmp


    def extract_single(self, id):
        """

        :param id:
        :return:
        """
        # TODO: Check if id matches glob_pattern?
        filename = os.path.join(self.folder_path, id)
        return self._open_file(filename)


    def generate_batch(self, batch_size: int) -> list:
        """

        :return:
        """
        filenames = glob.glob(os.path.join(self.folder_path, self.glob_pattern), recursive=True)
        i = 0

        while i < len(filenames):
            batch = filenames[i:i+batch_size]
            tmp = []
            for filename in batch:
                tmp.append(self._open_file(filename))
            yield tmp
            i += batch_size
