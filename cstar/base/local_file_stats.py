import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from cstar.base.utils import _get_sha256_hash


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

    def __init__(
        self,
        paths: list[Path],
        stats: Optional[dict[Path, os.stat_result]] = None,
        hashes: Optional[dict[Path, str]] = None,
    ):
        self.paths = [p.resolve() for p in paths]

        if not all([d.parent == self.paths[0].parent for d in self.paths]):
            raise ValueError(
                "LocalFileStatistics requires paths to exist in a common directory"
            )

        self.parent_dir = paths[0].parent

        if stats is not None:
            if set(k.resolve() for k in stats.keys()) != self.paths:
                raise ValueError("Provided 'stats' keys must match provided 'paths'")
            self._stat_cache = stats
        else:
            self._stat_cache = dict[Path, os.stat_result]()

        if hashes is not None:
            if set(k.resolve() for k in hashes.keys()) != self.paths:
                raise ValueError("Provided 'hashes' keys must provided 'paths'")
            self._hash_cache = hashes
        else:
            self._hash_cache = dict[Path, str]()

    @property
    def stats(self):
        if not self._stat_cache:
            self._stat_cache = {path: path.stat() for path in self.paths}
        return self._stat_cache

    @property
    def hashes(self):
        if not self._hash_cache:
            self._hash_cache = {
                path: _get_sha256_hash(path.resolve()) for path in self.paths
            }
        return self._hash_cache

    def validate(self) -> None:
        """TOOD docstring."""

        for f in self.paths:
            if not f.exists():
                raise FileNotFoundError(f"File {f} does not exist locally")

            current_stats = f.stat()
            if (f.stat().st_size != current_stats.st_size) or (
                f.stat().st_mtime != current_stats.st_mtime
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
            resolved = path.resolve()
            stat = self.stats.get(resolved)
            hash_ = self.hashes.get(resolved)

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
