from abc import ABC, abstractmethod
from pathlib import Path

from cstar.io import SourceData


class Retriever(ABC):
    @abstractmethod
    def read(self, source: SourceData) -> bytes:
        """Retrieve data to memory, if supported"""
        pass

    @abstractmethod
    def save(self, target_path: Path, source: SourceData) -> None:
        """Retrieve data to a local path"""


class RemoteBinaryFileRetriever(Retriever):
    pass


class LocalBinaryFileRetriever(Retriever):
    pass


class RemoteTextFileRetriever(Retriever):
    pass


class LocalTextFileRetriever(Retriever):
    pass


class RemoteRepositoryRetriever(Retriever):
    pass
