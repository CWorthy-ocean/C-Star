"""The names and models of the files a ROMS-MARBL step writes.

How ROMS names its output streams, which streams have an artifact kind of
their own, and the models that parse restart, boundary and carbonate
sensitivity file names.
"""

import re
import typing as t
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from pydantic import (
    BaseModel,
    PrivateAttr,
    ValidationInfo,
    field_validator,
    model_validator,
)

from cstar.applications.core import ArtifactKind

if t.TYPE_CHECKING:
    from cstar.base.log import TraceLogger

OUTPUT_ROOT_NAME: t.Final[str] = "output"
"""The root ROMS prefixes its output files with: `output_<stream>.<timestamp>.nc`."""

EXTRACT_ROOT_NAME: t.Final[str] = "child"
"""The root ROMS prefixes a boundary extraction with: `child_bry.<timestamp>.nc`."""

EXTRACT_ROLE: t.Final[str] = "child"
"""The role a boundary extraction's products are recorded under: they force the child grid."""

UPSCALING_SUFFIX: t.Final[str] = "_uscl"
"""The suffix of the ROMS stream holding boundary tracer fluxes, read by the upscaler."""

CDR_OUTPUT_SUFFIX: t.Final[str] = "_cdr"
"""The suffix of the ROMS stream holding CDR diagnostics."""


class RestartFile(BaseModel):
    """Reference to a path that contains restart checkpoints."""

    path: Path
    """The path to a restart file."""
    _base: str = PrivateAttr()
    """The base name of the file."""
    _segment: str | None = PrivateAttr(default=None)
    """The segment identifier of the file."""
    _ts: datetime = PrivateAttr()
    """The timestamp parsed from the file name."""

    EXT: t.ClassVar[t.Literal["nc"]] = "nc"
    """The expected file extension for a restart file."""
    FMT_TS: t.ClassVar[t.Literal["%Y%m%d%H%M%S"]] = "%Y%m%d%H%M%S"
    """The expected timestamp format in the restart file name"""
    TS_GLOB: t.ClassVar[str] = "[0-9]" * 14
    """Glob fragment matching the fixed-width (14-digit) timestamp (see `FMT_TS`)."""
    PATTERN_RST: t.ClassVar[t.Literal[r"^(.*?)_rst\.(\d{14})(?:\.(\d{1,9}))?\.nc$"]] = (
        r"^(.*?)_rst\.(\d{14})(?:\.(\d{1,9}))?\.nc$"
    )
    """A regex identifying full restart or partitioned files."""
    SUFFIX: t.ClassVar[t.Literal["_rst"]] = "_rst"
    """A unique suffix found in the name of restart files"""
    LABEL: t.ClassVar[str] = "restart"
    """A human-readable name for restart files, used in messages."""

    @classmethod
    def candidates(cls, search_path: Path, *, partitioned: bool) -> list["RestartFile"]:
        """Enumerate the restart files under a directory.

        Parameters
        ----------
        search_path : Path
            The directory to search recursively.
        partitioned : bool
            If True, match partition pieces (`*_rst.<ts>.<part>.nc`); otherwise
            match whole files (`*_rst.<ts>.nc`).

        Returns
        -------
        list[RestartFile]
            Every matching file, in filesystem order.
        """
        parted_clause = ".*" if partitioned else ""
        glob_pattern = f"*{cls.SUFFIX}.{cls.TS_GLOB}{parted_clause}.{cls.EXT}"
        return [
            RestartFile(path=match)
            for match in search_path.rglob(glob_pattern)
            if re.fullmatch(cls.PATTERN_RST, match.name, flags=re.ASCII)
        ]

    @classmethod
    def _first_piece_at(
        cls, rst_files: Sequence["RestartFile"], timestamp: datetime
    ) -> "RestartFile | None":
        """Select the restart file dated exactly `timestamp`.

        Parameters
        ----------
        rst_files : Sequence[RestartFile]
            The restart files to choose from.
        timestamp : datetime
            The timestamp the file name must carry.

        Returns
        -------
        RestartFile | None
            The match with the lowest partition (a whole file counts as the
            0th), or `None` if no file is dated `timestamp`.
        """
        return min(
            (rst for rst in rst_files if rst.timestamp == timestamp),
            key=lambda rst: rst.partition or 0,
            default=None,
        )

    @classmethod
    def in_directory(cls, search_path: Path) -> list["RestartFile"]:
        """Enumerate every restart file under a directory, partition pieces first.

        Parameters
        ----------
        search_path : Path
            The directory to search recursively.

        Returns
        -------
        list[RestartFile]
            The partition pieces, then the whole files; empty if there are none.
        """
        # one walk matches both `*_rst.<ts>.nc` and `*_rst.<ts>.<part>.nc`
        glob_pattern = f"*{cls.SUFFIX}.{cls.TS_GLOB}*.{cls.EXT}"
        found = [
            RestartFile(path=match)
            for match in search_path.rglob(glob_pattern)
            if re.fullmatch(cls.PATTERN_RST, match.name, flags=re.ASCII)
        ]
        return sorted(found, key=lambda rst: not rst.is_partitioned)

    @classmethod
    def select(
        cls,
        rst_files: Sequence["RestartFile"],
        timestamp: datetime | None = None,
        *,
        source: Path,
    ) -> "RestartFile":
        """Select the restart file to continue from.

        Partition pieces are preferred over whole files, as in `find`.

        Parameters
        ----------
        rst_files : Sequence[RestartFile]
            The restart files to choose from.
        timestamp : datetime | None
            The timestamp the file name must carry; matched exactly. The
            latest timestamp is selected if `None`.
        source : Path
            Where `rst_files` came from, for the error message.

        Returns
        -------
        RestartFile
            The file with the lowest partition (a whole file counts as the
            0th) among those with the selected timestamp.

        Raises
        ------
        FileNotFoundError
            If `rst_files` is empty and no `timestamp` was given, or no file is
            dated `timestamp`; the message lists the timestamps that are present.
        """
        groups = [
            [rst for rst in rst_files if rst.is_partitioned],
            [rst for rst in rst_files if not rst.is_partitioned],
        ]
        for group in groups:
            if not group:
                continue
            at = max(rst.timestamp for rst in group) if timestamp is None else timestamp
            if match := cls._first_piece_at(group, at):
                return match

        if timestamp is None:
            msg = f"No restart files located. Unable to continue from {source!r}"
            raise FileNotFoundError(msg)

        found = sorted({rst.timestamp for rst in rst_files})
        listing = ", ".join(str(ts) for ts in found) or "none"
        msg = (
            f"No restart file dated {timestamp} located in {source!r}; "
            f"restart timestamps found there: {listing}"
        )
        raise FileNotFoundError(msg)

    @classmethod
    def find(cls, search_path: Path, notfound_ok: bool = True) -> "RestartFile | None":
        """Search for a restart file in the specified location.

        If `search_path` identifies a directory, the item matching the _latest_
        timestamp and the 0th partition piece (if partitioned) is returned.

        If `search_path` identifies a file, that `RestartFile` will be returned.


        Parameters
        ----------
        search_path : Path
            The path to search
        notfound_ok : bool
            If False, raise an exception if no restart files are found.

        Returns
        -------
        ResetFile

        Raises
        ------
        ValueError
            If no directory or file exists at the search path.
        FileNotFoundError
            If no recognizable restart files are found.
        """
        search_path = search_path.expanduser().resolve()

        if search_path.is_file():
            return RestartFile(path=search_path)

        if not search_path.exists():
            msg = f"No directory or file found at path: {search_path!r}"
            raise ValueError(msg)

        rst_files = cls.in_directory(search_path)
        if not rst_files and notfound_ok:
            return None

        return cls.select(rst_files, source=search_path)

    @classmethod
    def find_at(cls, search_path: Path, timestamp: datetime) -> "RestartFile":
        """Search for the restart file dated exactly `timestamp`.

        If `search_path` identifies a directory, the item whose name carries
        `timestamp` and the 0th partition piece (if partitioned) is returned;
        as in `find`, partitioned files are preferred over whole files.

        If `search_path` identifies a file, that `RestartFile` will be returned
        provided its name carries `timestamp`.

        Parameters
        ----------
        search_path : Path
            The path to search
        timestamp : datetime
            The timestamp the restart file name must carry; matched exactly.

        Returns
        -------
        RestartFile

        Raises
        ------
        ValueError
            If no directory or file exists at the search path.
        FileNotFoundError
            If no restart file is dated `timestamp`; the message lists the
            timestamps that are present.
        """
        search_path = search_path.expanduser().resolve()

        if search_path.is_file():
            rst_files = [RestartFile(path=search_path)]
        elif search_path.exists():
            rst_files = cls.in_directory(search_path)
        else:
            msg = f"No directory or file found at path: {search_path!r}"
            raise ValueError(msg)

        return cls.select(rst_files, timestamp, source=search_path)

    @classmethod
    def from_parts(
        cls,
        base: str,
        timestamp: datetime,
        segment: str | None = None,
        directory: Path | None = None,
    ) -> "RestartFile":
        """Create a ResetFile from components.

        Parameters
        ----------
        base : str
            The base name for the restart file.
        timestamp : datetime
            The timestamp for the restart file.
        segment : str | None
            The 0-padded segment number if partitioned, otherwise `None`.
        directory : Path | None
            The directory to contain the file. If not specified, defaults to cwd.

        Returns
        -------
        ResetFile

        Raises
        ------
        ValueError
            If the search path does not exist or contains no recognizable restart files.
        """
        ts = timestamp.strftime(cls.FMT_TS)
        parted_clause = f".{segment}" if segment is not None else ""
        filename = f"{base}{cls.SUFFIX}.{ts}{parted_clause}.{cls.EXT}"

        if directory:
            return RestartFile(path=directory / filename)

        return RestartFile(path=Path(filename))

    @field_validator("path")
    @classmethod
    def _validate_path(cls, value: Path, _info: "ValidationInfo") -> Path:
        """Verify the supplied path meets the restart file naming convention.

        Parameters
        ----------
        value : str
            The value of the checkpoint frequency property
        _info : ValidationInfo
            Metadata for the current validation context
        """
        if value.suffix != f".{RestartFile.EXT}":
            msg = f"File extension does not match expected naming convention: {value.suffix}"
            raise ValueError(msg)

        if re.fullmatch(RestartFile.PATTERN_RST, value.name, flags=re.ASCII):
            return value.expanduser().resolve()

        msg = f"File name does not match expected naming convention: {value}"
        raise ValueError(msg)

    @model_validator(mode="after")
    def _model_validate(self) -> "RestartFile":
        """Perform post-processing on the restart file path.

        Returns
        -------
        ResetFile
        """
        matches = re.fullmatch(
            RestartFile.PATTERN_RST, self.path.as_posix(), flags=re.ASCII
        )
        if not matches:
            msg = f"File name does not match expected naming convention: {self.path}"
            raise ValueError(msg)

        self._base = matches.group(1)
        self._ts = datetime.strptime(matches.group(2), RestartFile.FMT_TS)
        # look for segment for partition number, e.g. <base>.<ts>.000.nc vs. <base>.<ts>.nc
        self._segment = matches.group(3)
        return self

    @property
    def timestamp(self) -> datetime:
        """Return a datetime derived from the timestamp in the restart file name.

        Returns
        -------
        datetime
        """
        return self._ts

    @property
    def is_partitioned(self) -> bool:
        """Return `True` if the restart file belongs to a partitioned dataset.

        Returns
        -------
        datetime
        """
        return self._segment is not None

    @property
    def partition(self) -> int | None:
        if self._segment:
            return int(self._segment)
        return None

    @property
    def formatted_timestamp(self) -> str:
        return self._ts.strftime(self.FMT_TS)


def restart_timestamp(location: str | Path) -> datetime | None:
    """Return the timestamp encoded in a restart-style file name, or `None`.

    Only the file name is inspected (via `RestartFile`'s validators); the file
    need not exist.

    Parameters
    ----------
    location : str | Path
        A path or file name to inspect.

    Returns
    -------
    datetime | None
        The timestamp encoded in the file name, or `None` if `location` does
        not match the restart file naming convention.
    """
    try:
        return RestartFile(path=Path(location)).timestamp
    except ValueError:
        return None


def warn_on_restart_start_date_mismatch(
    location: str | Path, start_date: datetime, *, log: "TraceLogger"
) -> None:
    """Warn when a restart-style initial-conditions file is dated differently
    from `start_date`.

    ROMS takes its clock from the restart file while `ntimes` is computed from
    `start_date`, so a mismatch means the run will not cover the intended
    period.

    Parameters
    ----------
    location : str | Path
        The path or file name of the initial-conditions file.
    start_date : datetime
        The blueprint's configured start date.
    log : TraceLogger
        The logger to emit the warning to.
    """
    ts = restart_timestamp(location)
    if ts is not None and ts != start_date:
        log.warning(
            "Restart file %s is dated %s but start_date is %s. ROMS starts "
            "from the restart's time; the previous segment may not have "
            "reached its end date, or the restart directory / start_date is "
            "wrong.",
            Path(location).name,
            ts,
            start_date,
        )


class TimestampedOutputFile(BaseModel):
    """Reference to a ROMS output file named
    `<base><SUFFIX>.<timestamp>[.<segment>].nc`.

    Subclasses set `SUFFIX` (and `LABEL`) and inherit the naming validation,
    the timestamp and partition accessors, and the directory search. A
    segment in the name marks one piece of a partitioned file.
    """

    path: Path
    """The path to an output file."""
    _base: str = PrivateAttr()
    """The base name of the file."""
    _segment: str | None = PrivateAttr(default=None)
    """The segment identifier of the file."""
    _ts: datetime = PrivateAttr()
    """The timestamp parsed from the file name."""

    EXT: t.ClassVar[t.Literal["nc"]] = "nc"
    """The expected file extension for an output file."""
    FMT_TS: t.ClassVar[t.Literal["%Y%m%d%H%M%S"]] = "%Y%m%d%H%M%S"
    """The expected timestamp format in the output file name"""
    SUFFIX: t.ClassVar[str]
    """A unique suffix found in the name of this kind of output file."""
    LABEL: t.ClassVar[str]
    """A human-readable name for this kind of output file, used in messages."""
    PATTERN: t.ClassVar[str]
    """A regex identifying full or partitioned files; derived from `SUFFIX`."""

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: t.Any) -> None:
        """Derive the file name pattern from the subclass's `SUFFIX`."""
        super().__pydantic_init_subclass__(**kwargs)
        cls.PATTERN = (
            rf"^(.*?){re.escape(cls.SUFFIX)}\.(\d{{14}})(?:\.(\d{{1,9}}))?\.{cls.EXT}$"
        )

    @classmethod
    def in_directory(cls, search_path: Path) -> list[t.Self]:
        """Enumerate every output file of this kind under a directory.

        Parameters
        ----------
        search_path : Path
            The directory to search recursively.

        Returns
        -------
        list[Self]
            The files, sorted by path; empty if there are none.

        Raises
        ------
        ValueError
            If a file's name matches the kind's suffix but does not follow its
            naming convention.
        """
        return [
            cls(path=match)
            for match in sorted(search_path.rglob(f"*{cls.SUFFIX}*.{cls.EXT}"))
        ]

    @classmethod
    def find(
        cls, search_path: Path, notfound_ok: bool = True
    ) -> Sequence[t.Self] | None:
        """Search for output files of this kind in the specified location.

        If `search_path` identifies a file, that file alone is returned.

        Parameters
        ----------
        search_path : Path
            The directory (searched recursively) or file to search
        notfound_ok : bool
            If False, raise an exception if no files are found.

        Returns
        -------
        Sequence[Self] | None

        Raises
        ------
        ValueError
            If the search path does not exist, or names a file that does not
            follow this kind of file's naming convention.
        FileNotFoundError
            If no recognizable files are found in the search path
        """
        search_path = search_path.expanduser().resolve()

        if search_path.is_file():
            return (cls(path=search_path),)

        if not search_path.exists():
            msg = f"No directory or file found at path: {search_path!r}"
            raise ValueError(msg)

        if matches := cls.in_directory(search_path):
            return tuple(matches)

        if not notfound_ok:
            msg = (
                f"No {cls.LABEL} files located. Unable to continue from {search_path!r}"
            )
            raise FileNotFoundError(msg)

        return None

    @classmethod
    def from_parts(
        cls,
        base: str,
        timestamp: datetime,
        segment: str | None = None,
        directory: Path | None = None,
    ) -> t.Self:
        """Create an output file reference from components.

        Parameters
        ----------
        base : str
            The base name for the file.
        timestamp : datetime
            The timestamp for the file.
        segment : str | None
            The 0-padded segment number if partitioned, otherwise `None`.
        directory : Path | None
            The directory to contain the file. If not specified, defaults to cwd.

        Returns
        -------
        Self
        """
        ts = timestamp.strftime(cls.FMT_TS)
        parted_clause = f".{segment}" if segment is not None else ""
        filename = f"{base}{cls.SUFFIX}.{ts}{parted_clause}.{cls.EXT}"

        path = directory / filename if directory else Path(filename)
        return cls(path=path)

    @field_validator("path")
    @classmethod
    def _validate_path(cls, value: Path, _info: "ValidationInfo") -> Path:
        """Verify the supplied path meets this kind of file's naming convention.

        Parameters
        ----------
        value : str
            The value of the checkpoint frequency property
        _info : ValidationInfo
            Metadata for the current validation context
        """
        if value.suffix != f".{cls.EXT}":
            msg = f"File extension does not match expected naming convention: {value.suffix}"
            raise ValueError(msg)

        if re.fullmatch(cls.PATTERN, value.name, flags=re.ASCII):
            return value

        msg = f"File name does not match expected naming convention: {value}"
        raise ValueError(msg)

    @model_validator(mode="after")
    def _model_validate(self) -> t.Self:
        """Perform post-processing on the output file path.

        Returns
        -------
        Self
        """
        matches = re.fullmatch(type(self).PATTERN, self.path.as_posix(), flags=re.ASCII)
        if not matches:
            msg = f"File name does not match expected naming convention: {self.path}"
            raise ValueError(msg)

        self._base = matches.group(1)
        self._ts = datetime.strptime(matches.group(2), self.FMT_TS)
        # look for segment for partition number, e.g. <base>.<ts>.000.nc vs. <base>.<ts>.nc
        self._segment = matches.group(3)
        return self

    @property
    def timestamp(self) -> datetime:
        """Return a datetime derived from the timestamp in the file name.

        Returns
        -------
        datetime
        """
        return self._ts

    @property
    def is_partitioned(self) -> bool:
        """Return `True` if the file belongs to a partitioned dataset.

        Returns
        -------
        bool
        """
        return self._segment is not None

    @property
    def partition(self) -> int | None:
        if self._segment:
            return int(self._segment)
        return None


class BoundaryFile(TimestampedOutputFile):
    """Reference to a path that contains boundary forcing."""

    SUFFIX: t.ClassVar[str] = "_bry"
    """A unique suffix found in the name of boundary files"""
    LABEL: t.ClassVar[str] = "boundary"
    PATTERN_BRY: t.ClassVar[str]
    """A regex identifying full boundary or partitioned files (alias of `PATTERN`)."""


BoundaryFile.PATTERN_BRY = BoundaryFile.PATTERN


class CarbonateSensitivityFile(TimestampedOutputFile):
    """Reference to a path that contains carbonate sensitivities
    (`ddic_dco2`, `ddic_dalk`), as written by a ROMS-MARBL run that sets
    `cdr_gas_exch_output.do_cdr_gas_exch_output`.
    """

    SUFFIX: t.ClassVar[str] = "_cdrgas"
    """A unique suffix found in the name of carbonate sensitivity files"""
    LABEL: t.ClassVar[str] = "carbonate sensitivity"


OUTPUT_STREAMS: t.Final[tuple[tuple[str, ArtifactKind, dict[str, str]], ...]] = (
    (f"{OUTPUT_ROOT_NAME}{RestartFile.SUFFIX}", ArtifactKind.restart, {}),
    (f"{OUTPUT_ROOT_NAME}_his", ArtifactKind.history, {}),
    (f"{OUTPUT_ROOT_NAME}_avg", ArtifactKind.average, {}),
    (
        f"{EXTRACT_ROOT_NAME}{BoundaryFile.SUFFIX}",
        ArtifactKind.boundary_forcing,
        {"role": EXTRACT_ROLE},
    ),
    (
        f"{OUTPUT_ROOT_NAME}{CarbonateSensitivityFile.SUFFIX}",
        ArtifactKind.gas_exchange,
        {},
    ),
    (f"{OUTPUT_ROOT_NAME}{UPSCALING_SUFFIX}", ArtifactKind.upscaling, {}),
    (f"{OUTPUT_ROOT_NAME}{CDR_OUTPUT_SUFFIX}", ArtifactKind.cdr_output, {}),
)
"""The ROMS output files that have a kind of their own, by name stem
(`<root>_<stream>`), with the metadata they start with. A stream another step or
directive consumes is a kind, so the consumer can find it by kind; every other
stream is `output`. The stems are spelled from the consumers' own suffixes."""

OUTPUT_FILE_PATTERN: t.Final[re.Pattern[str]] = re.compile(
    rf"^(?P<stem>(?:{OUTPUT_ROOT_NAME}|{EXTRACT_ROOT_NAME})_(?P<stream>.+))"
    r"\.(?P<timestamp>\d{14})\.nc$"
)
"""The name ROMS gives a joined output file, `<root>_<stream>.<14-digit timestamp>.nc`:
the root is `output`, or `child` for a boundary extraction."""
