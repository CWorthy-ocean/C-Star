import os
import shutil
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING

from cstar.base.utils import _get_sha256_hash, _run_cmd

if TYPE_CHECKING:
    from pathlib import Path

    from cstar.io.source_data import SourceData


class StagedData(ABC):
    def __init__(self, source: "SourceData", path: Path):
        self._source = source
        self._path = path

    @property
    def source(self) -> "SourceData":
        """The SourceData describing the source of the staged data."""
        return self._source

    @property
    def path(self) -> Path:
        return self._path

    @property
    @abstractmethod
    def changed_from_source(self) -> bool:
        """Whether the data have been modified since staging."""

    @abstractmethod
    def unstage(self) -> None:
        """Remove staged local filesystem version of this data"""

    @abstractmethod
    def reset(self) -> None:
        """Revert to original staged state if changed_from_source."""


class StagedFile(StagedData):
    def __init__(
        self,
        source: "SourceData",
        path: Path,
        sha256: str | None = None,
        stat: os.stat_result | None = None,
    ):
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
        """Check cached checksum, filesize, and modification time against current
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
        self._stat = None
        self._sha256 = None

    def unstage(self) -> None:
        self.path.unlink(missing_ok=True)
        self._clear_cache()

    def reset(self) -> None:
        if not self.changed_from_source:
            pass
        else:
            self.unstage()
            self._clear_cache()
            self.source.stage(target_dir=self.path)


class StagedRepository(StagedData):
    def __init__(self, source: "SourceData", path: Path):
        super().__init__(source, path)

        self._checkout_hash = _run_cmd(
            cmd="git rev-parse HEAD", cwd=self.path, raise_on_error=True
        )

    @property
    def changed_from_source(self) -> bool:
        """Check if the current repo is dirty or differs from a given commit hash."""
        # Check existence
        if not self.path.exists():
            return True

        # Check if diverged from checkout target
        try:
            cached_hash = self._checkout_hash

            # 1. Check current HEAD commit hash
            head_hash = _run_cmd(
                cmd="git rev-parse HEAD", cwd=self.path, raise_on_error=True
            )

            if head_hash != cached_hash:
                return True  # HEAD is not at the expected hash

            # if HEAD is at expected hash, check if dirty:
            status_output = _run_cmd(
                cmd="git status --porcelain", cwd=self.path, raise_on_error=True
            )

            return bool(status_output.strip())  # True if any changes

        except RuntimeError as e:
            print(f"Git error: {e}")
            return True

        return False

    def unstage(self):
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
    def paths(self) -> list[Path]:
        """Flattened list of all paths across all staged entries."""
        return [s.path for s in self._items]

    def changed_from_source(self) -> bool:
        return any(s.changed_from_source for s in self._items)

    def reset(self):
        for s in self._items:
            s.reset()

    @property
    def items(self) -> list[StagedData]:
        return list(self._items)
