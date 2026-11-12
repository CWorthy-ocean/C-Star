"""Detect the types of data files a run holds, from their headers.

C-Star does not hard-code what a model writes: output streams change with a
configuration and as models evolve. `FileTypeDetector` instead reads the header
of every data file (`HeaderReader`; netCDF today, zarr stores later), and
groups the files into `FileType`s by filename template and variable contents.
A `FileTypeMatch` describes a type portably (a filename glob plus a few required
variables), so a saved description re-matches the files of another run.
"""

import fnmatch
import re
import typing as t
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from pydantic import Field

from cstar.base.netcdf_compress import (
    ChunkSpec,
    default_chunks,
    spatial_dims,
    time_dimension,
)
from cstar.orchestration.models import ConfiguredBaseModel

_LONG_NUMBER: t.Final = re.compile(r"\d{6,}")
"""A run of digits long enough to be a date or timestamp (YYYYMM and longer)."""
_PARTITION: t.Final = re.compile(r"\.\d+(?=\.nc$)")
"""The rank index of a partitioned file, `<name>.<timestamp>.<rank>.nc`."""
_MATCH_VARIABLES: t.Final[int] = 8
"""The number of spatial variables a `FileTypeMatch` requires."""


@dataclass(frozen=True)
class VariableInfo:
    """The header of one variable."""

    dims: tuple[str, ...]
    shape: tuple[int, ...]
    dtype: str
    long_name: str = ""


@dataclass(frozen=True)
class FileHeader:
    """What a `HeaderReader` reports about one data file: no data are read."""

    format: str
    dims: dict[str, int]
    time_dim: str | None
    variables: dict[str, VariableInfo]
    attrs: dict[str, str] = field(default_factory=dict)
    deflated: bool = False
    """Whether any variable is already stored deflated (a compressed file)."""

    def spatial_variables(self) -> dict[str, VariableInfo]:
        """The variables with two or more non-time dimensions: what compression is for."""
        return {
            n: v
            for n, v in self.variables.items()
            if len(spatial_dims(v.dims, self.time_dim)) >= 2
        }


class HeaderReader(t.Protocol):
    """Reads the header of one kind of data file (`ncdump -h`, or a zarr store's metadata)."""

    def can_read(self, path: Path) -> bool:
        """Whether this reader understands the file or store at `path`."""
        ...

    def read(self, path: Path) -> FileHeader:
        """Read the header; no data."""
        ...


class NetcdfHeaderReader:
    """Reads netCDF headers (classic, CDF-5 and netCDF-4) with netCDF4-python."""

    def can_read(self, path: Path) -> bool:
        """Whether `path` is a netCDF file, by its magic bytes."""
        from cstar.base.utils import NetCDFFormat, netcdf_format

        try:
            return (
                path.is_file() and netcdf_format(path) is not NetCDFFormat.UNRECOGNIZED
            )
        except (OSError, ValueError):
            return False

    def read(self, path: Path) -> FileHeader:
        """Read the dimensions, variables and global attributes of a netCDF file."""
        import netCDF4

        with netCDF4.Dataset(path) as ds:
            variables = {
                name: VariableInfo(
                    dims=tuple(var.dimensions),
                    shape=tuple(var.shape),
                    dtype=str(var.dtype),
                    long_name=str(getattr(var, "long_name", "")),
                )
                for name, var in ds.variables.items()
            }
            return FileHeader(
                format=ds.data_model,
                dims={name: len(dim) for name, dim in ds.dimensions.items()},
                time_dim=time_dimension(ds),
                variables=variables,
                attrs={k: str(ds.getncattr(k)) for k in ds.ncattrs()},
                deflated=ds.data_model == "NETCDF4"
                and any((v.filters() or {}).get("zlib") for v in ds.variables.values()),
            )


DEFAULT_READERS: t.Final[tuple[HeaderReader, ...]] = (NetcdfHeaderReader(),)
"""The readers tried, in order. A `ZarrHeaderReader` slots in here."""


class FileTypeMatch(ConfiguredBaseModel):
    """A portable description of a file type: it ignores dimension sizes, so it
    matches the same output of another domain.
    """

    filename: str
    """A glob for the file's path relative to its step root, e.g. `output/output_cdr.*.nc`."""
    requires_variables: list[str] = Field(default_factory=list)
    """Variables every file of the type holds."""

    def matches(self, relative_path: str, header: FileHeader | None) -> bool:
        """Whether a file belongs to this type.

        Parameters
        ----------
        relative_path : str
            The file's path relative to its step root.
        header : FileHeader | None
            The file's header; `None` to match on the name only.
        """
        if not fnmatch.fnmatchcase(relative_path, self.filename):
            return False
        return header is None or all(
            v in header.variables for v in self.requires_variables
        )


@dataclass
class DataFile:
    """One data file of a run."""

    step: str
    """The name of the step that produced it."""
    application: str
    """The application of that step."""
    step_root: Path
    relative_path: str
    """Its path relative to the step root, as its step record names it."""
    size_bytes: int
    header: FileHeader

    @property
    def path(self) -> Path:
        """The file's absolute path."""
        return self.step_root / self.relative_path


@dataclass
class FileType:
    """Data files with the same filename template and variable contents."""

    name: str
    """A short label, e.g. `output_cdr` (with a suffix when a template splits)."""
    application: str
    """The application of the steps that wrote these files."""
    match: FileTypeMatch
    files: list[DataFile]

    @property
    def total_bytes(self) -> int:
        """The total size of the files."""
        return sum(f.size_bytes for f in self.files)

    @property
    def sample(self) -> FileHeader:
        """The header of the first file."""
        return self.files[0].header

    def counts(self) -> tuple[int, int]:
        """The numbers of 3-D and 2-D spatial variables."""
        spatial = self.sample.spatial_variables()
        n3 = sum(
            1
            for v in spatial.values()
            if len(spatial_dims(v.dims, self.sample.time_dim)) >= 3
        )
        return n3, len(spatial) - n3

    def default_chunks(self, itemsize: int = 4) -> ChunkSpec:
        """The derived chunk shape of the largest spatial variable, as (x, y, z)."""
        spatial = self.sample.spatial_variables()
        if not spatial:
            return ChunkSpec()
        largest = max(spatial.values(), key=lambda v: len(v.dims))
        shape = [
            s for d, s in zip(largest.dims, largest.shape) if d != self.sample.time_dim
        ]
        chunks = default_chunks(shape, itemsize)
        return ChunkSpec(
            x=chunks[-1], y=chunks[-2], z=chunks[-3] if len(chunks) >= 3 else None
        )


def filename_template(relative_path: str) -> str:
    """The path with its timestamps and rank index replaced by `*`.

    `output/output_cdr.20140301000000.nc` becomes `output/output_cdr.*.nc`.
    """
    return _LONG_NUMBER.sub("*", _PARTITION.sub(".*", relative_path))


def _type_label(template: str) -> str:
    """A short label for a template: its file name, up to the first `.` or `*`."""
    name = Path(template).name
    return re.split(r"[.*]", name, maxsplit=1)[0].rstrip("_") or name


class FileTypeDetector:
    """Groups the data files of a run into `FileType`s.

    Parameters
    ----------
    readers : Sequence[HeaderReader]
        The header readers to try for each file, in order.
    """

    def __init__(self, readers: Sequence[HeaderReader] = DEFAULT_READERS) -> None:
        self.readers = tuple(readers)

    def read_header(self, path: Path) -> FileHeader | None:
        """The header of a file, or `None` when no reader understands it."""
        for reader in self.readers:
            if reader.can_read(path):
                return reader.read(path)
        return None

    def data_files(
        self, candidates: Iterable[tuple[str, str, Path, str, int]]
    ) -> list[DataFile]:
        """Read the headers of candidate files and keep those worth compressing.

        Parameters
        ----------
        candidates : Iterable[tuple[str, str, Path, str, int]]
            `(step, application, step_root, relative_path, size_bytes)` for each file.

        Returns
        -------
        list[DataFile]
            The files a reader understands that hold at least one variable with
            two or more non-time dimensions.
        """
        files = []
        for step, application, step_root, relative, size in candidates:
            header = self.read_header(step_root / relative)
            if header is not None and header.spatial_variables():
                files.append(
                    DataFile(step, application, step_root, relative, size, header)
                )
        return files

    def detect(self, files: Sequence[DataFile]) -> list[FileType]:
        """Group files by application, filename template and variable contents.

        A template whose files hold different variables is split into types
        labelled `<name>-a`, `<name>-b`, ... Dimension sizes are ignored.
        """
        groups: dict[
            tuple[str, str, frozenset[tuple[str, tuple[str, ...]]]], list[DataFile]
        ]
        groups = defaultdict(list)
        for f in files:
            signature = frozenset((n, v.dims) for n, v in f.header.variables.items())
            groups[f.application, filename_template(f.relative_path), signature].append(
                f
            )

        by_template: dict[tuple[str, str], list[list[DataFile]]] = defaultdict(list)
        for (application, template, _), members in groups.items():
            by_template[application, template].append(members)

        types = []
        for (application, template), variants in sorted(by_template.items()):
            variants.sort(key=lambda m: -len(m))
            for i, members in enumerate(variants):
                label = _type_label(template)
                if len(variants) > 1:
                    label = f"{label}-{chr(ord('a') + i)}"
                spatial = sorted(members[0].header.spatial_variables())
                match = FileTypeMatch(
                    filename=template, requires_variables=spatial[:_MATCH_VARIABLES]
                )
                types.append(FileType(label, application, match, members))
        return types
