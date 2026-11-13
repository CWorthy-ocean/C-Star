"""The ``compress`` blueprint: which files of a run to compress, and how.

A `CompressBlueprint` holds one `FileTypeSettings` per file type of the run,
each with a portable `FileTypeMatch` (a filename glob plus a few required
variables, no dimension sizes), so the same blueprint applies to another run.
"""

import fnmatch
import typing as t
from pathlib import Path

from pydantic import Field, model_validator

from cstar.base.netcdf_compress import DEFAULT_DEFLATE_LEVEL, ChunkSpec, Encoding
from cstar.orchestration.models import Blueprint, ConfiguredBaseModel

if t.TYPE_CHECKING:
    from cstar.applications.compress.detect import FileHeader

APP_NAME: t.Final[str] = "compress"
"""The unique identifier for the compress application type."""


class FileTypeMatch(ConfiguredBaseModel):
    """A portable description of a file type: it ignores dimension sizes, so it
    matches the same output of another domain.
    """

    filename: str
    """A glob for the file's path relative to its step root, e.g. `output/output_cdr.*.nc`."""
    requires_variables: list[str] = Field(default_factory=list)
    """Variables every file of the type holds. When a filename template splits
    into several types, the variables that tell them apart come first, so the
    most specific match wins (`settings_for`)."""

    def matches(self, relative_path: str, header: "FileHeader | None") -> bool:
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


class ChunkSettings(ConfiguredBaseModel):
    """A chunk-shape override along x, y and z; an unset axis is derived."""

    x: int | None = Field(default=None, gt=0)
    y: int | None = Field(default=None, gt=0)
    z: int | None = Field(default=None, gt=0)


class FileTypeSettings(ConfiguredBaseModel):
    """How to compress one type of data file."""

    name: str
    """A label for the type, e.g. `output_cdr`."""
    application: str
    """The application of the steps whose files are this type."""
    match: FileTypeMatch
    """Which files are this type."""
    compress: bool = Field(default=False)
    """Rewrite the files as chunked, deflated netCDF-4 (lossless on its own)."""
    float32: bool = Field(default=False)
    """Cast float64 variables that are not protected to float32."""
    significant_digits: int | None = Field(default=None, gt=0)
    """With `float32`, also quantize those variables (BitGroom) to this many
    significant decimal digits."""
    chunks: ChunkSettings = Field(default_factory=ChunkSettings)
    """Chunk-shape overrides; unset axes are derived from each variable."""
    protected: list[str] = Field(default_factory=list)
    """Variables (names or globs) kept exact; filled from the application's
    `protected_variables` and editable."""

    @model_validator(mode="after")
    def _digits_need_float32(self) -> "FileTypeSettings":
        """Quantization applies to the float32-cast variables only."""
        if self.significant_digits is not None and not self.float32:
            msg = f"file type {self.name!r}: significant_digits requires float32"
            raise ValueError(msg)
        return self

    def encoding(self, deflate_level: int) -> Encoding:
        """The engine encoding these settings describe."""
        return Encoding(
            float32=self.float32,
            significant_digits=self.significant_digits,
            deflate_level=deflate_level,
            chunks=ChunkSpec(x=self.chunks.x, y=self.chunks.y, z=self.chunks.z),
            protected=tuple(self.protected),
        )


class CompressBlueprint(Blueprint):
    """A blueprint compressing the data files of a finished run in place."""

    application: str = Field(default=APP_NAME)
    """The application identifier."""
    schema_version: str = Field(default="1.1.1")
    """The current schema version for this blueprint."""
    run: Path
    """The root directory of the run to compress."""
    file_types: list[FileTypeSettings] = Field(min_length=1)
    """The settings of each file type; a file takes the most specific match
    (`settings_for`), and a file matching none is left as it is."""
    originals: t.Literal["delete", "keep"] = Field(default="keep")
    """What happens to an original once its compressed copy is verified: deleted,
    or moved to `<run>/uncompressed_originals/`."""
    deflate_level: int = Field(default=DEFAULT_DEFLATE_LEVEL, ge=0, le=9)
    """The zlib deflate level."""
    workers: int = Field(default=1, ge=1)
    """Files compressed at once, each in its own process."""
