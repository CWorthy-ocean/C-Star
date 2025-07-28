import os
import shutil
from abc import ABC, abstractmethod
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

        self._path = path
        if sha256:
            self._sha256 = sha256
        else:
            self._sha256 = _get_sha256_hash(self.path)

        if stat:
            self._stat = stat
        else:
            self._stat = os.stat(self.path)

    # Abstract
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
            self.source.get(target_dir=self.path)


class StagedRepository(StagedData):
    def __init__(self, source: "SourceData", path: Path):
        super().__init__(source, path)
        self._source = source
        self._path = path

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
        _run_cmd(
            cmd=f"git reset --hard {self.source.checkout_target}",
            cwd=self.path,
            raise_on_error=True,
        )


class StagedDataCollection:
    pass


# class StagedFileSet(StagedData):
#     """Collection of related StagedFile instances."""

#     def __init__(self, source: "SourceData", files: Sequence[StagedFile]):
#         super().__init__(source)
#         self._files = list(files)

#     @property
#     def paths(self) -> list[Path]:
#         return [f.path for f in self._files]

#     @property
#     def changed_from_source(self) -> bool:
#         return any(f.changed_from_source for f in self._files)

#     def unstage(self):
#         pass

#     def reset(self):
#         for f in self._files:
#             f.reset()
