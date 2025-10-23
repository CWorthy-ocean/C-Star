import os
import shutil
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import TYPE_CHECKING

from cstar.base.gitutils import _check_local_repo_changed_from_remote
from cstar.base.utils import _get_sha256_hash, _run_cmd

if TYPE_CHECKING:
    from pathlib import Path

    from cstar.io.source_data import SourceData


class StagedData(ABC):
    """Class to track locally staged data (files, repositories, etc.)

    Attributes
    ----------
    source (SourceData):
       The SourceData instance describing the source of the staged data.
    path (pathlib.Path):
       The local path to the location of the staged data
    changed_from_source (bool):
       True if the data have been modified since staging

    Methods
    -------
    unstage:
        Remove staged version of this data from `path`.
    reset:
        Revert to original staged state if changed_from_source.
    """

    def __init__(self, source: "SourceData", path: "Path"):
        """Initialize a StagedData instance.

        Parameters
        ----------
        source: SourceData
           The source from which the data originated
        path:
           The local path to the tracked version of the data
        """
        self._source = source
        self._path = path

    @property
    def source(self) -> "SourceData":
        """The SourceData describing the source of the staged data."""
        return self._source

    @property
    def path(self) -> "Path":
        """The local path to the staged data"""
        return self._path

    @property
    @abstractmethod
    def changed_from_source(self) -> bool:
        """True if the data have been modified since staging"""

    @abstractmethod
    def unstage(self) -> None:
        """Remove staged local filesystem version of this data"""

    @abstractmethod
    def reset(self) -> None:
        """Revert to original staged state if changed_from_source."""


class StagedFile(StagedData):
    """Class to track a locally staged file.

    Attributes
    ----------
    source (SourceData):
       The SourceData instance describing the source of the staged file
    path (pathlib.Path):
       The local path to the location of the staged file
    changed_from_source (bool):
       True if the file has been modified since staging

    Methods
    -------
    unstage:
        Remove staged version of this file from `path`.
    reset:
        Revert to original staged state if changed_from_source.
    """

    def __init__(
        self,
        source: "SourceData",
        path: "Path",
        sha256: str | None = None,
        stat: os.stat_result | None = None,
    ):
        """Initialize a StagedFile instance.

        StagedFile optionally tracks the file modification time and size,
        along with the SHA256 checksum, do determine if the file has
        changed since staging.

        Parameters
        ----------
        source: SourceData
           The source from which the file originated
        path: pathlib.Path
           The local path to the tracked version of the file
        sha256: str, optional, default None
           The checksum of the tracked file
        stat: os.stat_result, optional, default None
           The result of `os.stat` applied to the local file.
        """
        super().__init__(source, path)

        if sha256:
            self._sha256 = sha256
        else:
            self._sha256 = _get_sha256_hash(self.path)

        if stat:
            self._stat = stat
        else:
            self._stat = os.stat(self.path)

    @property
    def changed_from_source(self) -> bool:
        """True if the file has changed since staging.

        Checks cached checksum, filesize, and modification time against current
        values.
        """
        resolved_path = self._path.resolve()
        if not resolved_path.exists():
            return True
        if self._stat.st_mtime != os.stat(resolved_path.resolve()).st_mtime:
            return True
        if self._stat.st_size != os.stat(resolved_path.resolve()).st_size:
            return True
        if self._sha256 != _get_sha256_hash(resolved_path.resolve()):
            return True
        return False

    def _clear_cache(self):
        """Clear the `stat` and `sha256` attributes for tracking file modification"""
        self._stat = None
        self._sha256 = None

    def unstage(self) -> None:
        """Remove staged file from the local filesystem (source file is unaffected)"""
        self.path.unlink(missing_ok=True)
        self._clear_cache()

    def reset(self) -> None:
        """Revert to original staged state if changed_from_source."""
        if not self.changed_from_source:
            pass
        else:
            self.unstage()
            self._clear_cache()
            self.source.stage(target_dir=self.path)


class StagedRepository(StagedData):
    """Class to track a locally staged git repository

    Attributes
    ----------
    source (SourceData):
       The SourceData instance describing the source of the staged repository
    path (pathlib.Path):
       The local path to the location of the staged repository
    changed_from_source (bool):
       True if the repository has been modified since staging

    Methods
    -------
    unstage:
        Remove staged version of this repositoryfrom `path`.
    reset:
        Revert to original staged state if changed_from_source.
    """

    def __init__(self, source: "SourceData", path: "Path"):
        """Initialize a StagedRepository instance.

        Parameters
        ----------
        source: SourceData
           The source from which the repository originated
        path: pathlib.Path
           The local path to the tracked version of the repository
        """
        super().__init__(source, path)
        self._checkout_hash = _run_cmd(
            cmd="git rev-parse HEAD", cwd=self.path, raise_on_error=True
        )

    @property
    def changed_from_source(self) -> bool:
        """Check if the current repo is dirty or differs from a given commit hash."""
        # Check existence
        return _check_local_repo_changed_from_remote(
            remote_repo=self.source.location,
            local_repo=self.path,
            checkout_target=self._checkout_hash,
        )

    def unstage(self):
        """Remove the local clone of this repository from StagedRepository.path"""
        shutil.rmtree(self.path)

    def reset(self):
        """Hard reset back to original checkout target."""
        if not self.path.exists():
            self.source.stage(target_dir=self.path)
        else:
            _run_cmd(
                cmd=f"git reset --hard {self.source.checkout_target}",
                cwd=self.path,
                raise_on_error=True,
            )


class StagedDataCollection:
    """A class to hold a collection of related SourceData instances.

    Attributes
    ----------
    paths: list of pathlib.Path
        Flattened list of all paths across all staged entries.
    changed_from_source: bool
        True if any item's  StagedData.changed_from_source is True
    items: list of StagedData
        Flattened list of all StagedData entries

    Methods
    -------
    append:
        Add a new StagedData instance to this StagedDataCollection
    reset:
        Resets each StagedData item in the collection
    unstage:
        Unstages each StagedData item in the collection
    """

    def __init__(self, items: Iterable[StagedData]):
        self._items = list(items)
        self._validate()

    def _validate(self):
        for s in self._items:
            if not isinstance(s, StagedData):
                raise TypeError(
                    f"Invalid type: {type(s)} (must be StagedData subclass)"
                )

    def __len__(self) -> int:
        return len(self._items)

    def __getitem__(self, idx: int) -> StagedData:
        return self._items[idx]

    def __iter__(self) -> Iterator[StagedData]:
        return iter(self._items)

    def append(self, staged: StagedData):
        self._items.append(staged)
        self._validate()

    @property
    def paths(self) -> list["Path"]:
        """Flattened list of all paths across all staged entries."""
        return [s.path for s in self._items]

    @property
    def changed_from_source(self) -> bool:
        """True if any item's  StagedData.changed_from_source is True."""
        return any(s.changed_from_source for s in self._items)

    def reset(self) -> None:
        """Resets each StagedData item in the collection"""
        for s in self._items:
            s.reset()

    def unstage(self) -> None:
        """Unstages each StagedData item in the collection"""
        for s in self._items:
            s.unstage()

    @property
    def items(self) -> list[StagedData]:
        """Flattened list of all StagedData entries"""
        return list(self._items)

    @property
    def common_parent(self) -> Path:
        """Returns the nearest shared parent of SourceDataCollection.paths"""
        strpaths = [str(p) for p in self.paths]
        if len(strpaths) == 1:
            return self.paths[0].parent
        return Path(os.path.commonpath(strpaths))
