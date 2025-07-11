import os
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from cstar.base.log import LoggingMixin
from cstar.base.utils import _get_sha256_hash


class FileInfo(LoggingMixin):
    """Holds information about an individual file on the local system."""

    def __init__(
        self,
        path: Path | str,
        stat: os.stat_result | None = None,
        sha256: str | None = None,
    ):
        self.path = Path(path).absolute()
        self._stat = stat
        self._sha256 = sha256

    @property
    def stat(self) -> os.stat_result:
        """File status from os.stat()"""

        if self._stat is None:
            self._stat = self.path.stat()
        return self._stat

    @property
    def sha256(self) -> str:
        """The sha256 hash of this file."""

        if self._sha256 is None:
            self._sha256 = _get_sha256_hash(self.path)
        return self._sha256

    def validate(self):
        """Confirms that the cached path, stat, and sha256 values for this file are
        correct.

        Raises
        ------
        FileNotFoundError:
           If the path does not exist on the local filesystem
        ValueError:
           If the filesize, modification time, or sha256 sum do not match cached values
        """

        if not self.path.exists():
            raise FileNotFoundError(f"File {self.path} does not exist locally")

        current_stat = self.path.stat()

        if not self._stat:
            self.log.debug(f"File {self.path} has no cached stat")
        else:
            if (
                current_stat.st_size != self.stat.st_size
                or current_stat.st_mtime != self.stat.st_mtime
            ):
                raise ValueError(
                    f"File statistics for {self.path} do not match those in cache"
                )

        current_hash = _get_sha256_hash(self.path)
        if not self._sha256:
            self.log.debug(f"File {self.path} has no cached hash")
        else:
            if current_hash != self.sha256:
                raise ValueError(
                    f"Computed SHA256 hash for {self.path} ({current_hash}) does not "
                    f"match value in cache ({self.sha256})"
                )


class LocalFileStatistics(LoggingMixin):
    """Tracks file metadata (stat and SHA-256 hash) for a list of local files."""

    def __init__(self, files: Sequence[Path | FileInfo]):
        """Initialize the LocalFileStatistics object.

        Constructs a LocalFileStatistics instance from a list of local file paths
        and optionally precomputed metadata such as file stat results and SHA-256 hashes.
        All paths must be located within the same parent directory.
        Metadata is lazily computed if not provided.

        Parameters
        ----------
        files: list[Path | FileInfo]
            List of files to track. If the sha256 hash or status of the file are already known,
            provide a FileInfo containing these attributes.
            Otherwise, these are lazily computed from a list of Paths.

        Raises
        ------
        ValueError
            If the provided files are not all in the same directory.

        Examples
        --------
        >>> files = [Path("data/a.txt"), Path("data/b.txt")]
        >>> tracker = LocalFileStatistics(files)
        >>> tracker.stats[files[0]]
        os.stat_result(...)
        >>> tracker.hashes[files[0]]
        'e3b0c44298fc1c149afbf4c8996fb924...'
        """

        # Check files is not empty
        if not files:
            raise ValueError("At least one file must be provided.")

        # Normalize files to all be FileInfo instances:
        def to_fileinfo(item: Path | FileInfo) -> FileInfo:
            if isinstance(item, FileInfo):
                return item
            return FileInfo(path=item)

        file_infos = [to_fileinfo(f) for f in files]
        self.files: dict[Path, FileInfo] = {fi.path: fi for fi in file_infos}

        # For checking all files are colocated with the first:
        first_parent = self.paths[0].parent

        for f in self.paths:
            if f.parent != first_parent:
                raise ValueError("All files must be in the same directory")

        self.parent_dir: Path = first_parent

    @property
    def paths(self) -> list[Path]:
        """The list of tracked file paths."""
        return [f.path for f in self.files.values()]

    @property
    def stats(self) -> dict[Path, os.stat_result]:
        """Mapping of paths to their `os.stat()` results.

        Evaluated lazily.
        """
        return {f.path: f.stat for f in self.files.values()}

    @property
    def hashes(self) -> dict[Path, str]:
        """Mapping of paths to their SHA-256 hash strings.

        Evaluated lazily.
        """
        return {f.path: f.sha256 for f in self.files.values()}

    def __getitem__(self, key: str | Path) -> FileInfo:
        """Allows retrieval of FileInfo for a given path using
        my_local_file_statistics[path]"""
        path = Path(key).absolute()

        if path not in self.paths:
            raise KeyError(f"{path} not found.")

        return self.files[path]

    def validate(self) -> None:
        """Validate that all tracked files exist and match cached metadata.

        Calls file.validate() for each file tracked by this LocalFileStatistics
        instance.
        """

        [f.validate() for f in self.files.values()]

    def __str__(self) -> str:
        base_str = self.__class__.__name__ + "\n"
        base_str += "-" * (len(base_str) - 1) + "\n"
        base_str += "Parent directory: "
        base_str += f"{self.parent_dir}\n"
        base_str += "Files: \n\n"
        base_str += self.as_table()
        return base_str

    def __repr__(self) -> str:
        repr_str = self.__class__.__name__ + "("
        repr_str += "paths="
        repr_str += f"{(',\n'+' '*len(repr_str)).join([str(p) for p in self.paths])}"
        repr_str += ")"
        return repr_str

    def as_table(self, max_rows: int = 20) -> str:
        """Return a tabular string view of tracked files with basic metadata.

        Parameters
        ----------
        max_rows : int
            Maximum number of rows to show (truncate with ... if more)

        Returns
        -------
        str
            Formatted table of file metadata.
        """

        header = f"{'Name':<40} {'Hash':<12} {'Size (bytes)':<12} {'Modified'}"
        rows = []

        for p in self.paths[:max_rows]:
            fi = self.files[p]
            abspath = fi.path
            stat = fi.stat
            hash_ = fi.sha256

            name = str(abspath.name)
            size = stat.st_size
            mtime = datetime.fromtimestamp(stat.st_mtime).isoformat(
                sep=" ", timespec="seconds"
            )

            short_hash = hash_[:10] + "…"

            rows.append(f"{name:<40} {short_hash:<12} {size:<12} {mtime}")

        if len(self.paths) > max_rows:
            rows.append(f"... ({len(self.paths) - max_rows} more files)")

        return "\n".join([header] + rows)
