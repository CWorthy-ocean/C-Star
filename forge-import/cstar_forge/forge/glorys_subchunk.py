"""Just-in-time subchunking for GLORYS input data (interim, experimental).

Motivation and status
----------------------
Reading GLORYS from many per-day NetCDF files is slow: roms-tools opens them with
``xr.open_mfdataset`` and each file's on-disk chunking splits depth across many small
reads. The roms-tools docs describe a "subchunking" technique
(https://roms-tools.readthedocs.io/en/latest/datasets_read.html) that uses kerchunk to
build a combined reference (collapsing depth to a single chunk) which xarray can then
open lazily.

That build code is **copy-paste in the roms-tools docs, not part of the roms-tools
package** (the docs say as much), so this module vendors it. No monkeypatch is
needed to read the result: the ``kerchunk`` package registers its own xarray backend
(``kerchunk.xarray_backend:KerchunkBackend``) whose ``guess_can_open()`` recognizes
``.parquet``/``.json`` kerchunk references by extension. roms-tools' loader
(``roms_tools.utils.load_data`` / ``_load_data_dask``) never passes an explicit
``engine=`` for GLORYS, so when handed a ``.parquet`` reference path it transparently
opens via the kerchunk backend instead of guessing netcdf -- confirmed against
``GLORYSDataset``, ``rt.InitialConditions``, and ``rt.BoundaryForcing`` directly, with
both ``use_dask=True`` and ``use_dask=False``. So forge's job is only to build the
reference and hand its path through the existing ``source["path"]`` plumbing
(``RomsMarblInputData._resolve_source_block``) -- nothing in roms-tools is patched.

This remains an *interim, experimental* module (not something roms-tools documents or
guarantees) rather than a permanent feature: gated behind ``--subchunk``
(``python -m cstar_forge.run``, default off) pending review from the roms-tools
maintainers.

Note that reading a reference now requires ``kerchunk`` to be *importable at read
time* too (for xarray's backend auto-detection to find it), not just at build time.

Caveats
-------
- kerchunk only *warns* (never raises) when a multi-file time-concat degenerates (e.g.
  mismatched per-file time encodings collapsing several files onto one timestep).
  ``build_ref_for_files``/``build_subchunk_refs`` add an explicit post-build check to
  fail loudly instead of silently handing roms-tools a corrupted time axis.
"""

from __future__ import annotations

import glob
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import xarray as xr

logger = logging.getLogger(__name__)

#: 4D physics variables GLORYS ships that benefit from depth/lat subchunking.
#: ``zos`` (sea surface height) is 2D and passes through the combine unchanged.
DEFAULT_DATA_VARS_4D = ("thetao", "so", "uo", "vo")


def _require_subchunk_deps() -> None:
    """Raise a clear error if the interim subchunking deps aren't installed."""
    try:
        import fastparquet  # noqa: F401
        import kerchunk  # noqa: F401
        import nest_asyncio  # noqa: F401
        import ujson  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "GLORYS subchunking (--subchunk) requires the 'kerchunk', 'nest_asyncio', "
            "'ujson', and 'fastparquet' packages. These are listed in environment.yml; "
            "install them into the cstar-forge-v0 environment to use this feature."
        ) from e


def _dict_to_parquet(
    refs_dict: dict[str, Any], output_path: str, record_size: int = 100_000
) -> None:
    """Write a kerchunk refs dict to parquet.

    Copied from the roms-tools docs (datasets_read.html). Uses a two-pass write to
    work around a ``LazyReferenceMapper`` bug where ``.zarray`` metadata written in
    the same ``translate()`` call isn't visible via zmetadata until flushed to disk.
    """
    from fsspec.implementations.reference import LazyReferenceMapper

    refs = refs_dict.get("refs", {})
    out = LazyReferenceMapper.create(output_path, record_size=record_size)

    for k, v in refs.items():  # pass 1: .zarray / .zattrs / top-level
        if ".z" in k or "/" not in k:
            out[k] = v
    out.flush()  # flush so zmetadata can find .zarray

    for k, v in refs.items():  # pass 2: chunk references
        if ".z" not in k and "/" in k:
            out[k] = v
    out.flush()


def _subchunk_var(
    refs: dict[str, Any], var: str, depth_factor: int | None, lat_factor: int | None
) -> dict[str, Any]:
    from kerchunk.utils import subchunk

    if depth_factor and depth_factor > 1:
        refs = subchunk(
            refs, var, depth_factor
        )  # splits outermost non-unit axis = depth
    if lat_factor and lat_factor > 1:
        refs = subchunk(refs, var, lat_factor)  # depth now == 1, so this hits latitude
    return refs


def _build_one(
    path: str,
    data_vars_4d: list[str],
    depth_factor: int | None,
    lat_factor: int | None,
    inline_threshold: int,
) -> dict[str, Any]:
    from kerchunk.hdf import SingleHdf5ToZarr

    refs = SingleHdf5ToZarr(path, inline_threshold=inline_threshold).translate()
    for v in data_vars_4d:
        refs = _subchunk_var(refs, v, depth_factor, lat_factor)
    return refs


def _run(
    files: list[str],
    out: str,
    output_format: str = ".parquet",
    data_vars_4d: list[str] | None = None,
    concat_dim: str = "time",
    identical_dims: list[str] | None = None,
    inline_threshold: int = 5000,
    depth_factor: int | None = None,
    lat_factor: int | None = None,
) -> dict[str, Any]:
    """Build combined kerchunk references for a set of NetCDF4 files."""
    from kerchunk.combine import MultiZarrToZarr

    data_vars_4d = (
        data_vars_4d if data_vars_4d is not None else list(DEFAULT_DATA_VARS_4D)
    )
    identical_dims = (
        identical_dims
        if identical_dims is not None
        else ["latitude", "longitude", "depth"]
    )

    # 1) per-file refs
    single_refs = [
        _build_one(p, data_vars_4d, depth_factor, lat_factor, inline_threshold)
        for p in files
    ]

    # 2) combine across files
    combined = MultiZarrToZarr(
        single_refs,
        remote_protocol="file",
        concat_dims=[concat_dim],
        identical_dims=list(identical_dims),
    ).translate()

    # 3) emit
    if output_format == ".json":
        import ujson

        with open(out, "w") as f:
            ujson.dump(combined, f)
    elif output_format == ".parquet":
        _dict_to_parquet(combined, out)

    return combined


def open_subchunk_ref(ref_path: str | Path) -> xr.Dataset:
    """Open a kerchunk parquet reference as a lazy, dask-backed xarray Dataset.

    Used internally to validate a freshly-built reference (see
    ``build_subchunk_refs``). roms-tools itself never calls this -- it opens the
    same reference path via its own loader, which auto-detects the kerchunk xarray
    backend from the ``.parquet`` extension (see module docstring).
    """
    return xr.open_dataset(str(ref_path), engine="kerchunk", chunks={})


def build_subchunk_refs(
    input_files: str | list[str],
    out: str,
    output_format: str = ".parquet",
    data_vars_4d: tuple[str, ...] = DEFAULT_DATA_VARS_4D,
    concat_dim: str = "time",
    identical_dims: tuple[str, ...] = ("latitude", "longitude", "depth"),
    inline_threshold: int = 5000,
    depth_factor: int | None = None,
    lat_factor: int = 1,
    overwrite: bool = False,
) -> str | dict[str, Any]:
    """Build (and write) subchunked kerchunk references for a set of GLORYS files.

    Copied (with minor renames) from the roms-tools docs at
    https://roms-tools.readthedocs.io/en/latest/datasets_read.html -- that page notes
    this code "isn't in the ROMS-Tools codebase because it requires several additional
    libraries and is not required to be used." See the module docstring for why forge
    vendors it as an interim hack.

    Parameters
    ----------
    input_files : str or sequence of str
        A glob pattern or an explicit, already-sorted list of NetCDF4 file paths.
    out : str
        Output path *without* extension. The extension is appended from
        ``output_format`` (ignored for ``"in-memory"``).
    output_format : {".parquet", ".json", "in-memory"}
        How to emit the combined references.
    data_vars_4d : sequence of str
        4D data variables to subchunk.
    concat_dim : str
        Dimension along which per-file references are concatenated (time).
    identical_dims : sequence of str
        Dimensions identical across files (not concatenated).
    inline_threshold : int
        Bytes below which chunks are inlined rather than referenced.
    depth_factor : int or None
        subchunk factor along depth. ``None`` (default) auto-detects the depth size
        from the first file, collapsing depth to 1 chunk.
    lat_factor : int
        subchunk factor along latitude (1 = no latitude subchunking).
    overwrite : bool
        If False (default) and the output already exists, skip and return its path
        without rebuilding.

    Returns
    -------
    str or dict
        The output path for ``".parquet"``/``".json"``, or the combined references
        dict for ``"in-memory"``.

    Raises
    ------
    ImportError
        If the interim subchunking dependencies (kerchunk, ujson, nest_asyncio) are
        not installed.
    FileNotFoundError
        If ``input_files`` matches no files.
    RuntimeError
        If the combined reference's concat dimension doesn't have one entry per input
        file -- kerchunk only warns (never raises) on this, usually caused by
        per-file time encodings with mismatched reference dates.
    """
    _require_subchunk_deps()
    import nest_asyncio

    nest_asyncio.apply()

    if isinstance(input_files, str):
        files = sorted(glob.glob(input_files))
    else:
        files = list(input_files)
    if not files:
        raise FileNotFoundError(f"No input files matched: {input_files!r}")

    if output_format != "in-memory":
        out = out + output_format
        if Path(out).exists() and not overwrite:
            logger.info(
                "%s already exists, skipping (pass overwrite=True to rebuild)", out
            )
            return out

    if depth_factor is None:
        with xr.open_dataset(files[0]) as ds:
            depth_factor = ds.sizes["depth"]

    result = _run(
        files=files,
        out=out,
        output_format=output_format,
        data_vars_4d=list(data_vars_4d),
        concat_dim=concat_dim,
        identical_dims=list(identical_dims),
        inline_threshold=inline_threshold,
        depth_factor=depth_factor,
        lat_factor=lat_factor,
    )

    if output_format != "in-memory" and len(files) > 1:
        with open_subchunk_ref(out) as check_ds:
            actual = check_ds.sizes.get(concat_dim)
            if actual is not None and actual != len(files):
                raise RuntimeError(
                    f"Subchunk build for {out} produced {concat_dim!r} size {actual}, "
                    f"expected {len(files)} (one per input file). kerchunk only warns "
                    "on this rather than raising -- it usually means the input files' "
                    "time encoding reference dates differ. Refusing to hand roms-tools "
                    "a corrupted reference."
                )

    return out if output_format != "in-memory" else result


def build_ref_for_files(
    files: list[Path],
    out_dir: Path,
    key: str,
    start: datetime,
    end: datetime,
    overwrite: bool = False,
) -> Path:
    """Build (or reuse) a subchunked kerchunk reference for a list of GLORYS files.

    Wraps :func:`build_subchunk_refs` with forge's naming/caching convention:
    ``<out_dir>/subchunk/<key>_<startYYYYMMDD>_<endYYYYMMDD>.parquet``.
    """
    subchunk_dir = Path(out_dir) / "subchunk"
    subchunk_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{key}_{start:%Y%m%d}_{end:%Y%m%d}"
    out_path = build_subchunk_refs(
        input_files=sorted(str(f) for f in files),
        out=str(subchunk_dir / stem),
        output_format=".parquet",
        data_vars_4d=DEFAULT_DATA_VARS_4D,
        overwrite=overwrite,
    )
    return Path(out_path)
