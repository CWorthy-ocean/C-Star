"""Rewrite a netCDF file as chunked, deflated netCDF-4, optionally float32 and quantized.

This module is the single owner of netCDF compression, as `convert_to_cdf5` is of
`nccopy`. It reads any format netCDF4-python can read (classic, CDF-5, netCDF-4)
and writes netCDF-4 in one pass:

- every variable is chunked (one time record per chunk) and deflated with shuffle;
- with `Encoding.float32`, float64 variables that are not protected are cast to
  float32, and with `Encoding.significant_digits` also quantized (BitGroom) by
  netCDF-C, so they deflate far better;
- protected variables, coordinates and 1-D time variables keep their values
  exactly: they are only chunked and deflated.

Data are copied one time record of one variable at a time, so memory stays
bounded by the largest single record. Values are copied raw (no masking or
scaling), so an exact variable is bit-identical to its source.
"""

import fnmatch
import math
import typing as t
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

if t.TYPE_CHECKING:
    import netCDF4

TARGET_CHUNK_BYTES: t.Final[int] = 1_048_576
"""Chunks stay at or below HDF5's default 1 MiB chunk cache, so readers using the
default cache never re-decompress a chunk."""
DEFAULT_DEFLATE_LEVEL: t.Final[int] = 5
QUANTIZE_MODE: t.Final[str] = "BitGroom"
_CF_COORDINATE_NAMES: t.Final[frozenset[str]] = frozenset(
    {
        "latitude",
        "longitude",
        "time",
        "projection_x_coordinate",
        "projection_y_coordinate",
    }
)


@dataclass(frozen=True)
class ChunkSpec:
    """A chunk shape for spatial variables, by axis; `None` derives that axis.

    `z` is the third-to-last dimension of a variable with three or more non-time
    dimensions, `y` and `x` its last two.
    """

    x: int | None = None
    y: int | None = None
    z: int | None = None


@dataclass(frozen=True)
class Encoding:
    """How to encode one file type."""

    float32: bool = False
    """Cast float64 variables that are not protected to float32."""
    significant_digits: int | None = None
    """Quantize the float32-cast variables to this many significant decimal
    digits (BitGroom) before deflating; `None` for no quantization."""
    deflate_level: int = DEFAULT_DEFLATE_LEVEL
    chunks: ChunkSpec = field(default_factory=ChunkSpec)
    protected: tuple[str, ...] = ()
    """Names or glob patterns of variables that keep their exact values."""


@dataclass(frozen=True)
class CompressResult:
    """What `compress_netcdf` did to the variables of one file."""

    reduced: tuple[str, ...]
    """Variables cast to float32 (and quantized, when the encoding says so)."""
    exact: tuple[str, ...]
    """Float variables kept at their source precision."""


def matches_any(name: str, patterns: Iterable[str]) -> bool:
    """Whether a variable name matches any of the names or glob patterns."""
    return any(fnmatch.fnmatchcase(name, pattern) for pattern in patterns)


def time_dimension(ds: "netCDF4.Dataset") -> str | None:
    """The name of the record (unlimited) dimension, or of `time` when none is unlimited."""
    for name, dim in ds.dimensions.items():
        if dim.isunlimited():
            return name
    return "time" if "time" in ds.dimensions else None


def spatial_dims(dims: Sequence[str], time_dim: str | None) -> tuple[str, ...]:
    """The dimensions of a variable other than its record dimension."""
    return tuple(d for d in dims if d != time_dim)


def coordinate_names(ds: "netCDF4.Dataset") -> set[str]:
    """Variables that are coordinates and so always keep their exact values.

    Dimension coordinates, variables named in a `coordinates` or `bounds`
    attribute, and variables with an `axis` or a CF coordinate `standard_name`.
    """
    names = {n for n in ds.variables if n in ds.dimensions}
    for var in ds.variables.values():
        for attr in ("coordinates", "bounds"):
            if attr in var.ncattrs():
                names.update(str(var.getncattr(attr)).split())
    for name, var in ds.variables.items():
        attrs = var.ncattrs()
        if "axis" in attrs or (
            "standard_name" in attrs
            and str(var.getncattr("standard_name")) in _CF_COORDINATE_NAMES
        ):
            names.add(name)
    return names & set(ds.variables)


def _even_chunk(n: int, side: int) -> int:
    """A chunk length no larger than `side` that splits `n` into equal-ish pieces."""
    pieces = max(1, -(-n // max(1, side)))
    return -(-n // pieces)


def _nearest_divisor(n: int, target: float) -> int:
    """The divisor of `n` nearest `target` in ratio, else an even split."""
    best = min(
        (d for d in range(1, n + 1) if n % d == 0),
        key=lambda d: abs(math.log(d / target)),
    )
    return (
        best if 0.5 <= best / target <= 2.0 else _even_chunk(n, max(1, round(target)))
    )


def default_chunks(
    shape: Sequence[int], itemsize: int, target_bytes: int = TARGET_CHUNK_BYTES
) -> tuple[int, ...]:
    """A chunk shape for the non-time dimensions of a variable.

    With `N` elements per chunk, a 3-D chunk `(cz, cy, cx)` balances reading a
    horizontal map (touches about `ny * nx * cz / N` chunks) against reading a
    water column (`nz / cz` chunks): `cz = sqrt(nz * N / (ny * nx))`, snapped to
    a divisor of `nz`; the rest of the budget is a near-square horizontal tile.
    Leading dimensions beyond three get chunk length 1.

    Parameters
    ----------
    shape : Sequence[int]
        The variable's non-time dimension sizes.
    itemsize : int
        Bytes per value as written.
    target_bytes : int
        The chunk size budget.

    Returns
    -------
    tuple[int, ...]
        One chunk length per dimension of `shape`.
    """
    n_elems = max(1, target_bytes // itemsize)
    shape = [max(1, s) for s in shape]
    if len(shape) == 1:
        return (min(shape[0], n_elems),)
    if len(shape) == 2:
        side = max(1, int(n_elems**0.5))
        return (_even_chunk(shape[0], side), _even_chunk(shape[1], side))
    *lead, nz, ny, nx = shape
    cz = _nearest_divisor(nz, max(1.0, math.sqrt(nz * n_elems / (ny * nx))))
    side = max(1, int((n_elems / cz) ** 0.5))
    return (*[1] * len(lead), cz, _even_chunk(ny, side), _even_chunk(nx, side))


def chunk_shape(
    dims: Sequence[str],
    shape: Sequence[int],
    time_dim: str | None,
    itemsize: int,
    spec: ChunkSpec,
) -> tuple[int, ...] | None:
    """The chunk shape of one variable: time 1, `spec` where given, derived elsewhere.

    Returns `None` for a scalar variable, which is stored contiguously.
    """
    if not shape:
        return None
    rest = [s for d, s in zip(dims, shape) if d != time_dim]
    chunks = list(default_chunks(rest, itemsize)) if rest else []
    overrides = [spec.x, spec.y, spec.z] if len(rest) >= 2 else [spec.x]
    for i, value in enumerate(overrides[: len(rest)]):
        if value is not None:
            chunks[-1 - i] = max(1, min(value, rest[-1 - i]))
    out = iter(chunks)
    return tuple(1 if d == time_dim else next(out) for d in dims)


def exact_variables(ds: "netCDF4.Dataset", protected: Iterable[str]) -> set[str]:
    """Variables `compress_netcdf` keeps exact: protected, coordinates and 1-D time."""
    time_dim = time_dimension(ds)
    patterns = tuple(protected)
    return (
        {n for n in ds.variables if matches_any(n, patterns)}
        | coordinate_names(ds)
        | {n for n, v in ds.variables.items() if v.dimensions == (time_dim,)}
    )


def compress_netcdf(src: Path, dst: Path, encoding: Encoding) -> CompressResult:
    """Write a compressed copy of a netCDF file.

    The caller chooses `dst` (usually a `.partial` name it renames after
    `verify_compressed`); an existing `dst` is overwritten.

    Parameters
    ----------
    src : Path
        The file to compress.
    dst : Path
        Where to write the netCDF-4 copy.
    encoding : Encoding
        How to encode it.

    Returns
    -------
    CompressResult
    """
    import netCDF4
    import numpy as np

    reduced: list[str] = []
    exact_floats: list[str] = []
    with netCDF4.Dataset(src) as s, netCDF4.Dataset(dst, "w", format="NETCDF4") as d:
        s.set_auto_maskandscale(False)
        d.set_auto_maskandscale(False)
        time_dim = time_dimension(s)
        keep = exact_variables(s, encoding.protected)

        d.setncatts({k: s.getncattr(k) for k in s.ncattrs()})
        for name, dim in s.dimensions.items():
            d.createDimension(name, None if dim.isunlimited() else len(dim))

        for name, var in s.variables.items():
            is_float = var.dtype.kind == "f"
            reduce = (
                encoding.float32
                and var.dtype == np.float64
                and name not in keep
                and len(spatial_dims(var.dimensions, time_dim)) >= 2
            )
            out_dtype = np.dtype(np.float32) if reduce else var.dtype
            kwargs: dict[str, t.Any] = {}
            if var.dtype.kind in "fiub" and var.shape:
                kwargs.update(
                    zlib=encoding.deflate_level > 0,
                    complevel=encoding.deflate_level,
                    shuffle=True,
                    chunksizes=chunk_shape(
                        var.dimensions,
                        var.shape,
                        time_dim,
                        out_dtype.itemsize,
                        encoding.chunks,
                    ),
                )
            if reduce and encoding.significant_digits:
                kwargs.update(
                    significant_digits=encoding.significant_digits,
                    quantize_mode=QUANTIZE_MODE,
                )
            if "_FillValue" in var.ncattrs():
                kwargs["fill_value"] = np.array(var.getncattr("_FillValue")).astype(
                    out_dtype
                )
            out = d.createVariable(name, out_dtype, var.dimensions, **kwargs)
            out.setncatts(
                {k: var.getncattr(k) for k in var.ncattrs() if k != "_FillValue"}
            )

            if reduce:
                reduced.append(name)
            elif is_float:
                exact_floats.append(name)

            if not var.shape:
                out.assignValue(var.getValue())
            elif time_dim in var.dimensions and var.dimensions[0] == time_dim:
                for rec in range(var.shape[0]):
                    out[rec, ...] = var[rec, ...].astype(out_dtype, copy=False)
            else:
                out[...] = var[...].astype(out_dtype, copy=False)

    return CompressResult(reduced=tuple(reduced), exact=tuple(exact_floats))


VERIFIED_RECORDS: t.Final[str] = "the first and last record of each variable"
"""What `verify_compressed` compares, for help text and reports: a lossless
rewrite through netCDF-C either succeeds or raises, so the check samples rather
than re-reads every record."""


def _sample_records(n: int, limit: int = 2) -> list[int]:
    """The record indices `verify_compressed` compares: the first and the last."""
    return sorted({0, n - 1})[:limit] if n else []


def verify_compressed(src: Path, dst: Path, encoding: Encoding) -> list[str]:
    """Check a compressed copy against its source.

    The variables, dimensions and record count must match. On the first and the
    last record of each variable (`VERIFIED_RECORDS`; every value of a variable
    without a record dimension), exact variables must be bit-identical and
    reduced variables within float32 rounding, or within the quantization
    tolerance of `significant_digits`.

    Parameters
    ----------
    src : Path
        The source file.
    dst : Path
        The compressed copy.
    encoding : Encoding
        The encoding it was written with.

    Returns
    -------
    list[str]
        The problems found; empty when the copy is good.
    """
    import netCDF4
    import numpy as np

    problems: list[str] = []
    digits = encoding.significant_digits
    rtol = 10.0 ** (1 - digits) if digits else 1e-6
    with netCDF4.Dataset(src) as s, netCDF4.Dataset(dst) as d:
        s.set_auto_maskandscale(False)
        d.set_auto_maskandscale(False)
        if set(s.variables) != set(d.variables):
            return [f"variables differ: {sorted(set(s.variables) ^ set(d.variables))}"]
        time_dim = time_dimension(s)
        keep = exact_variables(s, encoding.protected)
        for name, var in s.variables.items():
            out = d.variables[name]
            if var.shape != out.shape:
                problems.append(f"{name}: shape {out.shape} != {var.shape}")
                continue
            if not var.shape:
                same = np.array_equal(
                    var.getValue(), out.getValue(), equal_nan=var.dtype.kind == "f"
                )
                if not same:
                    problems.append(f"{name}: scalar value differs")
                continue
            records: list[int | None] = (
                list(_sample_records(var.shape[0]))
                if var.dimensions[0] == time_dim
                else [None]
            )
            for rec in records:
                a = var[rec, ...] if rec is not None else var[...]
                b = out[rec, ...] if rec is not None else out[...]
                if var.dtype.kind != "f" or name in keep or out.dtype == var.dtype:
                    ok = np.array_equal(a, b, equal_nan=var.dtype.kind == "f")
                else:
                    ok = bool(np.allclose(b, a, rtol=rtol, atol=0, equal_nan=True))
                if not ok:
                    problems.append(f"{name}: values differ at record {rec}")
                    break
    return problems
