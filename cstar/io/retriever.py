import hashlib
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar

import requests

from cstar.base.gitutils import _checkout, _clone
from cstar.io.constants import SourceClassification

if TYPE_CHECKING:
    from cstar.io.source_data import SourceData

_registry: dict["SourceClassification", type["Retriever"]] = {}


def register_retriever(
    wrapped_cls: type["Retriever"],
) -> type["Retriever"]:
    """Register the decorated type as an available Retriever."""
    _registry[wrapped_cls._classification] = wrapped_cls

    return wrapped_cls


def get_retriever(source: "SourceData") -> "Retriever":
    """Get a retriever from the retriever registry.

    Returns
    -------
    Retriever
        The retriever matching the supplied name

    Raises
    ------
    ValueError
        if no registered retriever is associated with this classification
    """
    classification = source.classification
    if retriever := _registry.get(classification):
        return retriever(source=source)

    raise ValueError(f"No retriever for {classification}")


class Retriever(ABC):
    """Class to handle retrieval of data for access by C-Star

    Methods
    -------
    read:
        Reads data into memory, if supported
    save:
        Saves data to a target local path, if supported
    """

    _classification: ClassVar[SourceClassification]

    def __init__(self, source: "SourceData"):
        self.source = source

    @abstractmethod
    def read(self) -> bytes:
        """Retrieve data to memory, if supported"""
        pass

    def save(self, target_dir: Path) -> Path:
        """
        Save this object to the given directory.

        This method performs common setup ( ensuring the target directory exists)
        and then calls the subclass-defined `_save()` method for the actual save.
        """
        if target_dir.exists():
            if not target_dir.is_dir():
                raise ValueError(
                    f"Cannot save to target_dir={target_dir} (not a directory)"
                )
        else:
            target_dir.mkdir(parents=True)

        savepath = self._save(target_dir=target_dir)
        return savepath

    @abstractmethod
    def _save(self, target_dir: Path) -> Path:
        """Retrieve data to a local path"""
        pass


class RemoteFileRetriever(Retriever, ABC):
    """Retriever subclass for retrieving remote files."""

    def read(self) -> bytes:
        """Reads this remote file's contents into memory using `requests`"""
        response = requests.get(self.source.location, allow_redirects=True)
        response.raise_for_status()
        data = response.content

        return data

    @abstractmethod
    def _save(self, target_dir: Path) -> Path:
        pass


@register_retriever
class RemoteBinaryFileRetriever(RemoteFileRetriever):
    """Retriever subclass for retrieving remote binary files."""

    _classification = SourceClassification.REMOTE_BINARY_FILE

    def _save(self, target_dir: Path) -> Path:
        """Saves this remote file's contents to `target_dir`.

        If the file's SourceData specifies a checksum as its `identifier`,
        the downloaded file is validated using this checksum, and deleted
        if there is no match.

        Parameters
        ----------
        target_dir: pathlib.Path
            The directory in which to save the file

        Returns
        -------
        pathlib.Path:
            The path of the saved file

        Raises
        ------
        ValueError:
            if the checksum of the downloaded file does not match what is
            specified in its SourceData.
        """
        hash_obj = hashlib.sha256()
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / self.source.basename

        with requests.get(self.source.location, stream=True, allow_redirects=True) as r:
            r.raise_for_status()
            with open(target_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):  # Download in 8kB chunks
                    if chunk:
                        f.write(chunk)
                        hash_obj.update(chunk)

        # Hash verification if specified in "SourceData":
        if self.source.identifier:
            actual_hash = hash_obj.hexdigest()
            expected_hash = self.source.identifier.lower()

            if actual_hash != expected_hash:
                target_path.unlink(missing_ok=True)  # Remove downloaded file
                raise ValueError(
                    f"Hash mismatch for {self.source.location} (saved file):\n"
                    f"Expected: {expected_hash}\nActual:   {actual_hash}.\n"
                    f"File deleted for safety."
                )
        return target_path


@register_retriever
class RemoteTextFileRetriever(RemoteFileRetriever):
    """Retriever subclass for retrieving remote text files."""

    _classification = SourceClassification.REMOTE_TEXT_FILE

    def _save(self, target_dir: Path) -> Path:
        """Save this text file to `target_dir`.

        Parameters
        ----------
        target_dir, Path:
            The target directory in which to save the text file.

        Returns
        -------
        pathlib.Path:
            The path of the saved file
        """
        data = self.read()
        target_path = target_dir / self.source.basename
        with open(target_path, "wb") as f:
            f.write(data)
        return target_path


class LocalFileRetriever(Retriever):
    """Retriever subclass for retrieving local files."""

    def read(self) -> bytes:
        """Read this file's contents into memory

        Returns
        -------
        bytes:
           raw bytes from the read
        """
        with open(self.source.location, "rb") as f:
            return f.read()

    def _save(self, target_dir: Path) -> Path:
        """Save this file to `target_dir`

        Parameters
        ----------
        target_dir, pathlib.Path:
            The directory in which to save the file

        Returns
        -------
        pathlib.Path:
            The path of the saved file
        """
        target_path = target_dir / self.source.basename
        shutil.copy2(src=Path(self.source.location).resolve(), dst=target_path)
        return target_path


@register_retriever
class LocalBinaryFileRetriever(LocalFileRetriever):
    """Retriever subclass for retrieving local binary files."""

    _classification = SourceClassification.LOCAL_BINARY_FILE


@register_retriever
class LocalTextFileRetriever(LocalFileRetriever):
    """Retriever subclass for retrieving local text files."""

    _classification = SourceClassification.LOCAL_TEXT_FILE


@register_retriever
class RemoteRepositoryRetriever(Retriever):
    """Retriever subclass for retrieving remote repositories."""

    _classification = SourceClassification.REMOTE_REPOSITORY

    def read(self) -> bytes:
        """Unsupported method.

        Raises
        ------
        NotImplementedError
            if called
        """
        raise NotImplementedError("Cannot 'read' a remote repository to memory")

    def _save(self, target_dir: Path) -> Path:
        """Clone this repository to `target_dir`

        Parameters
        ----------
        target_dir, pathlib.Path:
            The directory in which to clone this repository. Must be empty.

        Returns
        -------
        pathlib.Path:
            The path to the local clone of the repository

        Raises
        ------
        ValueError:
            If `save` is called with a non-empty directory as `target_dir`
        """
        if any(target_dir.iterdir()):
            raise ValueError(f"cannot clone repository to {target_dir} - dir not empty")

        _clone(
            source_repo=self.source.location,
            local_path=target_dir,
        )
        if self.source.checkout_target:
            _checkout(
                source_repo=self.source.location,
                local_path=target_dir,
                checkout_target=self.source.checkout_target,
            )
        return target_dir
