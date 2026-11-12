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
(``RomsInputGenerator._resolve_source_block``) -- nothing in roms-tools is patched.

This remains an *interim, experimental* module (not something roms-tools documents or
guarantees) rather than a permanent feature, pending review from the roms-tools
maintainers. It is nonetheless *on by default* (``cstar forge run``;
disable with ``--no-subchunk``), so its dependencies are ordinary runtime
dependencies of forge, not optional extras.

Note that reading a reference now requires ``kerchunk`` to be *importable at read
time* too (for xarray's backend auto-detection to find it), not just at build time.

Caveats
-------
- kerchunk only *warns* (never raises) when a multi-file time-concat degenerates (e.g.
  mismatched per-file time encodings collapsing several files onto one timestep).
  ``build_subchunk_refs`` checks the built reference before publishing it and raises
  :class:`SubchunkUnsupportedError` instead of handing roms-tools a corrupted time
  axis. Reading the files directly decodes each file's time on its own, so it copes.
- Subchunking splits each stored chunk's raw bytes into depth slices, so it only works
  on files stored uncompressed, one (depth, latitude, longitude) block per time step:
  how Copernicus Marine downloads are written. kerchunk's own guard misses compressed
  HDF5 (it records compression as ``filters`` and only checks ``compressor``), so the
  build checks every file first and raises :class:`SubchunkUnsupportedError` for any
  other layout. Forge then reads the files directly, as ``--no-subchunk`` does.
- A reference holds byte offsets into the files it was built from, so it is valid only
  for those exact files. ``build_ref_for_files`` names it after a fingerprint of their
  paths, sizes and modification times: domains sharing the source-data cache, or
  files replaced in place, never pick up a reference built for other bytes.
"""

from __future__ import annotations

import concurrent.futures
import glob
import hashlib
import logging
import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any

import xarray as xr

if TYPE_CHECKING:
    from datetime import datetime

logger = logging.getLogger(__name__)

#: 4D physics variables GLORYS ships that benefit from depth/lat subchunking.
#: ``zos`` (sea surface height) is 2D and passes through the combine unchanged.
DEFAULT_DATA_VARS_4D = ("thetao", "so", "uo", "vo")


class SubchunkUnsupportedError(ValueError):
    """Subchunking can't index these input files.

    Raised before any reference is published. The files are still readable as they
    are; callers read them directly instead (what ``--no-subchunk`` does).
    """


def _codec_ids(meta: dict[str, Any]) -> list[str]:
    """Ids of the codecs (compressor and filters) in a zarr ``.zarray`` dict."""
    codecs = [meta.get("compressor"), *(meta.get("filters") or [])]
    return [c["id"] for c in codecs if c]


def _subchunk_blocker(meta: dict[str, Any], var: str) -> str:
    """Why ``var``'s chunks (its ``.zarray`` dict) can't be split by depth, or ``""``.

    Splitting a chunk's byte range into equal depth slices is valid only for raw
    bytes holding a whole (depth, latitude, longitude) block of one time step.
    """
    if codecs := _codec_ids(meta):
        return f"{var!r} is stored compressed or filtered ({', '.join(codecs)})"
    shape, chunks = meta["shape"], meta["chunks"]
    if len(shape) != 4 or chunks[0] != 1 or chunks[1] != shape[1]:
        return (
            f"{var!r} is not stored as one depth-spanning block per time step "
            f"(shape {shape}, chunks {chunks})"
        )
    return ""


def _require_subchunk_deps() -> None:
    """Raise a clear error if the interim subchunking deps aren't installed."""
    try:
        import fastparquet  # noqa: F401
        import kerchunk  # noqa: F401
        import ujson  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "GLORYS subchunking (on by default) requires the 'kerchunk', 'ujson', "
            "and 'fastparquet' packages. They are declared as forge dependencies, "
            "so an environment missing them predates that change: install them "
            "(e.g. `conda install -c conda-forge kerchunk ujson fastparquet`), or "
            "rerun with --no-subchunk."
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
    import h5py
    import ujson
    from kerchunk.hdf import SingleHdf5ToZarr

    if not h5py.is_hdf5(path):
        raise SubchunkUnsupportedError(f"{path} is not a NetCDF4 (HDF5) file")
    refs = SingleHdf5ToZarr(path, inline_threshold=inline_threshold).translate()
    # Checked on the translated refs, before any split: kerchunk's subchunk returns a
    # flat store without the "refs" wrapper.
    for v in data_vars_4d:
        zarray = refs["refs"].get(f"{v}/.zarray")
        if zarray is not None and (reason := _subchunk_blocker(ujson.loads(zarray), v)):
            raise SubchunkUnsupportedError(f"{path}: {reason}")
    for v in data_vars_4d:
        refs = _subchunk_var(refs, v, depth_factor, lat_factor)
    return refs


def _input_fingerprint(files: list[str]) -> str:
    """Short digest of the files' paths, sizes and modification times.

    From ``stat`` alone, never the contents (tens of GB per run window).
    """
    digest = hashlib.sha256()
    for f in files:
        st = os.stat(f)
        digest.update(f"{f}\0{st.st_size}\0{st.st_mtime_ns}\n".encode())
    return digest.hexdigest()[:12]


def _remove_reference(path: str) -> None:
    """Remove a reference: a JSON file, or a parquet directory."""
    if Path(path).is_dir():
        shutil.rmtree(path)
    else:
        Path(path).unlink(missing_ok=True)


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
        If the interim subchunking dependencies (kerchunk, ujson, fastparquet) are
        not installed.
    FileNotFoundError
        If ``input_files`` matches no files.
    SubchunkUnsupportedError
        If any input file isn't NetCDF4, or stores a ``data_vars_4d`` variable
        compressed or in chunks other than one depth-spanning block per time step;
        or if the combined reference's concat dimension doesn't have one entry per
        input file (kerchunk only warns on this, usually caused by per-file time
        encodings with mismatched reference dates). Nothing is published: the
        reference is built under a temporary name and moved to ``out`` only after
        that check, so an existing ``out`` is always one that passed it.
    """
    _require_subchunk_deps()

    if isinstance(input_files, str):
        files = sorted(glob.glob(input_files))
    else:
        files = list(input_files)
    if not files:
        raise FileNotFoundError(f"No input files matched: {input_files!r}")

    # Built under a per-process temporary name and moved into place after the check
    # below, so a failed check or an interrupted build never leaves a reference at
    # `out` for a later run to reuse, and concurrent builds don't interleave writes.
    partial = out
    if output_format != "in-memory":
        partial = f"{out}.partial-{os.getpid()}{output_format}"
        out = out + output_format
        if Path(out).exists() and not overwrite:
            logger.info(
                "%s already exists, skipping (pass overwrite=True to rebuild)", out
            )
            return out

    if depth_factor is None:
        with xr.open_dataset(files[0]) as ds:
            depth_factor = ds.sizes["depth"]

    # kerchunk's MultiZarrToZarr calls asyncio.run() itself, which raises inside a
    # running event loop -- and a forge step runs inline on `cstar blueprint run`'s
    # loop (see ForgeRunner.run). A fresh worker thread has no running loop, so the
    # build runs there rather than patching asyncio process-wide with nest_asyncio
    # (which breaks asyncio's current-task tracking on Python 3.14). An interrupt
    # during the build waits for it to finish.
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            result = pool.submit(
                _run,
                files=files,
                out=partial,
                output_format=output_format,
                data_vars_4d=list(data_vars_4d),
                concat_dim=concat_dim,
                identical_dims=list(identical_dims),
                inline_threshold=inline_threshold,
                depth_factor=depth_factor,
                lat_factor=lat_factor,
            ).result()
        if output_format == "in-memory":
            return result

        if len(files) > 1:
            with open_subchunk_ref(partial) as check_ds:
                actual = check_ds.sizes.get(concat_dim)
            if actual is not None and actual != len(files):
                raise SubchunkUnsupportedError(
                    f"the {len(files)} files combine into {actual} {concat_dim!r} "
                    "entries; their time encodings likely use different reference "
                    "dates"
                )
        if Path(out).is_dir():  # overwrite=True over a parquet directory
            shutil.rmtree(out)
        os.replace(partial, out)
        return out
    finally:
        if output_format != "in-memory":
            _remove_reference(partial)  # a no-op once it has moved to `out`


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
    ``<out_dir>/subchunk/<key>_<startYYYYMMDD>_<endYYYYMMDD>_<fingerprint>.json``,
    where the fingerprint covers the files' paths, sizes and modification times. A
    reference holds byte offsets into those files, so it is reused only for the same
    files unchanged -- not for another domain's regional files with the same run
    window (``out_dir`` is the shared source-data cache), nor after a file is
    replaced in place. The reference built for the old files stays behind unused
    (about 30 kB per input file); nothing prunes it, since a sibling with the same
    window may belong to another domain. ``subchunk/`` can be cleared while no Forge
    step is running; Forge rebuilds what it needs.

    Uses the JSON reference format rather than parquet: GLORYS regularly has
    chunks that are legitimately missing (fill-value-only, never written to the
    source file -- ordinary zarr/kerchunk semantics). fsspec's parquet-backed
    ``LazyReferenceMapper`` represents those as placeholder rows that round-trip
    through pandas as ``pd.NA`` rather than ``None``, which crashes
    ``ReferenceFileSystem``'s remote-protocol auto-detection with ``TypeError:
    boolean value of NA is ambiguous`` (reproduced independently of this
    codebase). Plain JSON refs are a flat dict with no preallocated padding, so
    missing chunks are simply absent instead of malformed placeholder entries.
    """
    subchunk_dir = Path(out_dir) / "subchunk"
    subchunk_dir.mkdir(parents=True, exist_ok=True)
    input_files = sorted(str(f) for f in files)
    stem = f"{key}_{start:%Y%m%d}_{end:%Y%m%d}_{_input_fingerprint(input_files)}"
    out_path = build_subchunk_refs(
        input_files=input_files,
        out=str(subchunk_dir / stem),
        output_format=".json",
        data_vars_4d=DEFAULT_DATA_VARS_4D,
        overwrite=overwrite,
    )
    # build_subchunk_refs only returns a dict for output_format="in-memory"; with
    # ".json" (hardcoded above) it always returns the output path as a str.
    assert isinstance(out_path, str)
    return Path(out_path)
