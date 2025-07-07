import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from cstar.base.utils import _get_sha256_hash


@dataclass
class FileInfo:
    path: Path
    stat: Optional[os.stat_result] = None
    sha256: Optional[str] = None


class LocalFileStatistics:
    """Tracks file metadata (stat and SHA-256 hash) for a list of local files.

    Parameters
    ----------
    paths : list of Path
        List of file paths to track. All paths must be in the same parent directory.
    stats : dict of Path to os.stat_result, optional
        Optional precomputed stat results. If not provided, they will be computed lazily.
    hashes : dict of Path to str, optional
        Optional precomputed SHA-256 hashes. If not provided, they will be computed lazily.

    Attributes
    ----------
    paths : list of Path
        The list of tracked file paths.
    parent_dir : Path
        The common parent directory of all paths.
    stats : dict of Path to os.stat_result
        Mapping of paths to their `os.stat()` results. Evaluated lazily.
    hashes : dict of Path to str
        Mapping of paths to their SHA-256 hash strings. Evaluated lazily.

    Raises
    ------
    ValueError
        If the provided paths are not all in the same directory
        or if the keys of the `stats` or `hashes` dictionaries do not exactly
        match the resolved set of `paths`.

    Examples
    --------
    >>> files = [Path("data/a.txt"), Path("data/b.txt")]
    >>> tracker = LocalFileStatistics(files)
    >>> tracker.stats[files[0]]
    os.stat_result(...)
    >>> tracker.hashes[files[0]]
    'e3b0c44298fc1c149afbf4c8996fb924...'
    """

    def __init__(self, files: Sequence[Path | FileInfo]):
        """Initialize the LocalFileStatistics object.

        Stores a list of local file paths and optionally precomputed metadata
        such as file stat results and SHA-256 hashes. All paths must be located
        within the same parent directory. Metadata is lazily computed if not
        provided.

        Parameters
        ----------
        files : list of FileInfo
            List of files to track with optional stat and hash metadata.
            All files must reside in the same directory

        Raises
        ------
        ValueError
            If the provided paths are not all in the same directory.
        """

        # Check files is not empty
        if not files:
            raise ValueError("At least one file must be provided.")

        # Initialize attributes
        self._stat_cache: dict[Path, os.stat_result] = {}
        self._hash_cache: dict[Path, str] = {}
        self.paths: list[Path] = []

        # Normalize files to all be FileInfo instances:
        def to_fileinfo(item: Path | FileInfo) -> FileInfo:
            if isinstance(item, FileInfo):
                return item
            return FileInfo(path=item)

        normalized_files = [to_fileinfo(f) for f in files]

        # For checking all files are colocated with the first:
        first_parent = normalized_files[0].path.absolute().parent

        for f in normalized_files:
            abs_path = f.path.absolute()

            if abs_path.parent != first_parent:
                raise ValueError("All files must be in the same directory")

            self.paths.append(abs_path)

            if f.stat is not None:
                self._stat_cache[abs_path] = f.stat
            if f.sha256 is not None:
                self._hash_cache[abs_path] = f.sha256

        self.parent_dir: Path = first_parent

    def __getitem__(self, key: str | Path) -> FileInfo:
        path = Path(key).absolute()

        if path not in self.paths:
            raise KeyError(f"{path} not found.")

        return FileInfo(
            path=path,
            stat=self.stats[path],
            sha256=self.hashes[path],
        )

    @property
    def stats(self) -> dict[Path, os.stat_result]:
        """File stat metadata for each tracked file.

        Lazily computes and caches the output of `os.stat()` for each tracked
        file path if not already provided. Returned as a dictionary mapping
        absolute file paths to their `os.stat_result` values.

        Returns
        -------
        dict of Path to os.stat_result
            Mapping from file paths to their stat metadata.
        """
        if not self._stat_cache:
            self._stat_cache = {path: path.stat() for path in self.paths}
        return self._stat_cache

    @property
    def hashes(self) -> dict[Path, str]:
        """SHA-256 hashes for each tracked file.

        Lazily computes and caches the SHA-256 hash for each file using
        `_get_sha256_hash()` if not already provided. Returned as a dictionary
        mapping absolute file paths to their hash strings.

        Returns
        -------
        dict of Path to str
            Mapping from file paths to their SHA-256 hash values.
        """

        if not self._hash_cache:
            self._hash_cache = {
                path: _get_sha256_hash(path.resolve()) for path in self.paths
            }
        return self._hash_cache

    def validate(self) -> None:
        """Validate that all tracked files exist and match cached metadata.

        Verifies that each tracked file still exists on disk, and that its
        current size, modification time, and SHA-256 hash match the cached
        values. Raises an error if any discrepancies are found.

        Raises
        ------
        FileNotFoundError
            If any tracked file no longer exists at the expected path.
        ValueError
            If the current size, modification time, or SHA-256 hash of a file
            differs from the cached value.
        """

        for f in self.paths:
            if not f.exists():
                raise FileNotFoundError(f"File {f} does not exist locally")

            cached_stats = self.stats[f]
            current_stats = f.stat()

            if (
                current_stats.st_size != cached_stats.st_size
                or current_stats.st_mtime != cached_stats.st_mtime
            ):
                raise ValueError(f"File statistics for {f} do not match those in cache")

            current_hash = _get_sha256_hash(f)
            if current_hash != self.hashes[f]:
                raise ValueError(
                    f"Computed SHA256 hash for {f} ({current_hash}) does not "
                    f"match value in cache ({self.hashes[f]})"
                )

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

        for path in self.paths[:max_rows]:
            abspath = path.absolute()
            stat = self.stats.get(abspath)
            hash_ = self.hashes.get(abspath)

            name = str(path.name)
            size = stat.st_size if stat else "UNKNOWN"
            mtime = (
                datetime.fromtimestamp(stat.st_mtime).isoformat(
                    sep=" ", timespec="seconds"
                )
                if stat
                else "UNKNOWN"
            )
            short_hash = hash_[:10] + "…" if hash_ else "UNKNOWN"

            rows.append(f"{name:<40} {short_hash:<12} {size:<12} {mtime}")

        if len(self.paths) > max_rows:
            rows.append(f"... ({len(self.paths) - max_rows} more files)")

        return "\n".join([header] + rows)
