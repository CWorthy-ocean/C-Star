"""
Tests for ``cstar.base.digest``: the bounded-read netCDF content digest and the
whole-file sha256 digest.
"""

import hashlib
import re
import shutil
import subprocess
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from cstar.base.digest import (
    HASH_SCHEME,
    SAMPLE_BUDGET,
    SHA256_SCHEME,
    digest_scheme,
    hash_netcdf_contents,
    sha256_digest,
    short_digest,
)


def _numeric_dataset() -> xr.Dataset:
    """A small, re-encoding-safe dataset: float32/float64 only, no NaNs, no
    non-dimension coords -- avoids format-dependent ``_FillValue``/``coordinates``
    attrs that would make cross-format digests legitimately differ for reasons
    unrelated to the hash algorithm itself.
    """
    rng = np.random.default_rng(0)
    temp = rng.random((3, 4), dtype=np.float64).astype(np.float32)
    return xr.Dataset(
        {"temp": (["y", "x"], temp)},
        attrs={"title": "test dataset", "version": 1},
    )


def _string_coord_dataset() -> xr.Dataset:
    """A dataset with a string/object variable, like roms-tools' ``river_name``.

    Named on a dimension it doesn't share its name with (``river``, not
    ``river_name``) so xarray doesn't promote it to a pandas ``Index`` -- pandas'
    newer string-backed ``Index`` dtype isn't netCDF-encodable and would make this
    a test of a pandas quirk rather than the hash algorithm.
    """
    names = np.array(["Amazon", "Congo", "Nile"])
    return xr.Dataset(
        {
            "flow": (["river"], np.array([1.0, 2.0, 3.0])),
            "river_name": (["river"], names),
        }
    )


_BIG_SHAPE = (8, 400, 500)


def _big_dataset(seed: int = 0) -> xr.Dataset:
    """A float32 variable of ~6.4 MB (above ``FULL_HASH_LIMIT``, so it is hashed by
    sampling its two end slabs) plus a small coordinate that is hashed in full.
    """
    rng = np.random.default_rng(seed)
    data = rng.random(_BIG_SHAPE, dtype=np.float64).astype(np.float32)
    return xr.Dataset(
        {"tracer": (["t", "y", "x"], data), "t": ("t", np.arange(8.0))},
    )


class _ReadSpy:
    """Counts ``netCDF4.Variable.__getitem__`` calls and elements read, per variable."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import netCDF4

        self.calls: dict[str, int] = {}
        self.elements: dict[str, int] = {}
        self.sizes: dict[str, list[int]] = {}
        self.keys: dict[str, list[tuple]] = {}
        original = netCDF4.Variable.__getitem__

        def spy(var, key):
            result = original(var, key)
            size = int(np.asarray(result).size)
            self.calls[var.name] = self.calls.get(var.name, 0) + 1
            self.elements[var.name] = self.elements.get(var.name, 0) + size
            self.sizes.setdefault(var.name, []).append(size)
            self.keys.setdefault(var.name, []).append(
                key if isinstance(key, tuple) else (key,)
            )
            return result

        monkeypatch.setattr(netCDF4.Variable, "__getitem__", spy)


def _assert_contiguous_slab(key: tuple, shape: tuple[int, ...]) -> None:
    """``key`` is ints, then one unit-step slice, then full slices: one C-contiguous
    block, which the netCDF4 backend reads with a single unstrided request.
    """
    kinds = ["int" if isinstance(k, int) else "slice" for k in key]
    first = kinds.index("slice")
    assert kinds[:first] == ["int"] * first and kinds[first:] == ["slice"] * (
        len(key) - first
    )
    assert key[first].step in (None, 1)
    for axis, k in enumerate(key[first + 1 :], start=first + 1):
        assert (k.start in (None, 0)) and (k.stop in (None, shape[axis]))
        assert k.step in (None, 1)


def _write_big(path: Path, ds: xr.Dataset, fmt: str = "NETCDF4") -> Path:
    ds.to_netcdf(path, format=fmt, engine="netcdf4")
    return path


class TestHashNetcdfContents:
    def test_digest_carries_scheme_prefix(self, tmp_path):
        p = tmp_path / "a.nc"
        _numeric_dataset().to_netcdf(p)
        assert re.fullmatch(rf"{HASH_SCHEME}:[0-9a-f]{{64}}", hash_netcdf_contents(p))

    def test_deterministic_same_dataset_two_files(self, tmp_path):
        ds = _numeric_dataset()
        p1 = tmp_path / "a.nc"
        p2 = tmp_path / "b.nc"
        ds.to_netcdf(p1)
        ds.to_netcdf(p2)
        assert hash_netcdf_contents(p1) == hash_netcdf_contents(p2)

    def test_changing_data_value_changes_digest(self, tmp_path):
        ds = _numeric_dataset()
        p1 = tmp_path / "a.nc"
        ds.to_netcdf(p1)
        h1 = hash_netcdf_contents(p1)

        ds2 = ds.copy(deep=True)
        ds2["temp"].values[0, 0] += 1.0
        p2 = tmp_path / "b.nc"
        ds2.to_netcdf(p2)
        assert hash_netcdf_contents(p2) != h1

    def test_changing_variable_attr_changes_digest(self, tmp_path):
        ds = _numeric_dataset()
        p1 = tmp_path / "a.nc"
        ds.to_netcdf(p1)
        h1 = hash_netcdf_contents(p1)

        ds2 = ds.copy(deep=True)
        ds2["temp"].attrs["units"] = "celsius"
        p2 = tmp_path / "b.nc"
        ds2.to_netcdf(p2)
        assert hash_netcdf_contents(p2) != h1

    def test_survives_netcdf3_reencoding(self, tmp_path):
        ds = _numeric_dataset()
        p_nc4 = tmp_path / "a_nc4.nc"
        p_nc3 = tmp_path / "a_nc3.nc"
        ds.to_netcdf(p_nc4, format="NETCDF4")
        ds.to_netcdf(p_nc3, format="NETCDF3_64BIT")
        assert hash_netcdf_contents(p_nc4) == hash_netcdf_contents(p_nc3)

    def test_survives_nccopy_cdf5(self, tmp_path):
        if shutil.which("nccopy") is None:
            pytest.skip("nccopy not on PATH")
        ds = _numeric_dataset()
        p_nc4 = tmp_path / "a_nc4.nc"
        p_cdf5 = tmp_path / "a_cdf5.nc"
        ds.to_netcdf(p_nc4, format="NETCDF4")
        h_before = hash_netcdf_contents(p_nc4)

        subprocess.run(["nccopy", "-k", "cdf5", str(p_nc4), str(p_cdf5)], check=True)
        assert hash_netcdf_contents(p_cdf5) == h_before

    def test_string_coord_dataset_hashes_deterministically(self, tmp_path):
        ds = _string_coord_dataset()
        p1 = tmp_path / "a.nc"
        p2 = tmp_path / "b.nc"
        ds.to_netcdf(p1)
        ds.to_netcdf(p2)
        assert hash_netcdf_contents(p1) == hash_netcdf_contents(p2)


class TestHashLargeVariables:
    def test_deterministic_across_two_writes(self, tmp_path):
        ds = _big_dataset()
        p1 = _write_big(tmp_path / "a.nc", ds)
        p2 = _write_big(tmp_path / "b.nc", ds)
        assert hash_netcdf_contents(p1) == hash_netcdf_contents(p2)

    @pytest.mark.parametrize(
        "index",
        [(0, 0, 0), (-1, -1, -1)],
        ids=["first-element", "last-element"],
    )
    def test_changing_edge_element_changes_digest(self, tmp_path, index):
        ds = _big_dataset()
        h1 = hash_netcdf_contents(_write_big(tmp_path / "a.nc", ds))

        ds2 = ds.copy(deep=True)
        ds2["tracer"].values[index] += 1.0
        assert hash_netcdf_contents(_write_big(tmp_path / "b.nc", ds2)) != h1

    def test_interior_change_is_not_sampled(self, tmp_path):
        """Only the two end slabs are read, so a middle-record change is invisible."""
        ds = _big_dataset()
        h1 = hash_netcdf_contents(_write_big(tmp_path / "a.nc", ds))

        ds2 = ds.copy(deep=True)
        ds2["tracer"].values[4, 200, 250] += 1.0
        assert hash_netcdf_contents(_write_big(tmp_path / "b.nc", ds2)) == h1

    def test_slab_is_contiguous_within_first_record(self, tmp_path, monkeypatch):
        """With ``(8, 400, 500)`` float32, ``m = 262144`` elements fit per end, so
        ``j = 1`` and ``r = 400``: the whole first and whole last record are read.
        """
        ds = _big_dataset()
        p = _write_big(tmp_path / "a.nc", ds)
        spy = _ReadSpy(monkeypatch)
        h1 = hash_netcdf_contents(p)
        monkeypatch.undo()

        assert spy.calls["tracer"] == 2
        assert spy.sizes["tracer"] == [400 * 500, 400 * 500]
        for key in spy.keys["tracer"]:
            _assert_contiguous_slab(key, _BIG_SHAPE)

        for index in [(0, 399, 499), (7, 0, 0)]:
            ds2 = ds.copy(deep=True)
            ds2["tracer"].values[index] += 1.0
            assert hash_netcdf_contents(_write_big(tmp_path / "b.nc", ds2)) != h1

    def test_single_record_reads_partial_slabs(self, tmp_path, monkeypatch):
        """``(1, 1500, 1500)`` float32: a record exceeds the budget, so each slab is
        ``r = 174`` whole rows at one end of the one record, read contiguously.
        """
        shape = (1, 1500, 1500)
        ds = xr.Dataset(
            {
                "field": (
                    ["t", "y", "x"],
                    np.random.default_rng(1).random(shape, dtype=np.float32),
                )
            }
        )
        p = _write_big(tmp_path / "a.nc", ds)
        spy = _ReadSpy(monkeypatch)
        h1 = hash_netcdf_contents(p)
        monkeypatch.undo()

        rows = SAMPLE_BUDGET // (2 * 4) // shape[2]
        assert spy.sizes["field"] == [rows * shape[2]] * 2
        assert spy.keys["field"] == [
            (0, slice(0, rows, 1), slice(None)),
            (0, slice(shape[1] - rows, shape[1], 1), slice(None)),
        ]
        for index in [
            (0, 0, 0),
            (0, rows - 1, 1499),
            (0, 1500 - rows, 0),
            (0, 1499, 1499),
        ]:
            ds2 = ds.copy(deep=True)
            ds2["field"].values[index] += 1.0
            assert hash_netcdf_contents(_write_big(tmp_path / "b.nc", ds2)) != h1

    def test_element_larger_than_budget_still_hashes(self, tmp_path):
        """A fixed-width string element wider than the budget (itemsize > 1 MiB)
        reads one element per end instead of failing to pick a slab.
        """
        ds = xr.Dataset({"s": ("n", np.array(["x" * 600_000, "y" * 600_000]))})
        p = _write_big(tmp_path / "a.nc", ds, "NETCDF3_64BIT")
        h1 = hash_netcdf_contents(p)
        assert h1.startswith(f"{HASH_SCHEME}:")
        ds2 = xr.Dataset({"s": ("n", np.array(["x" * 600_000, "z" * 600_000]))})
        assert (
            hash_netcdf_contents(_write_big(tmp_path / "b.nc", ds2, "NETCDF3_64BIT"))
            != h1
        )

    def test_survives_netcdf3_reencoding(self, tmp_path):
        ds = _big_dataset()
        p_nc4 = _write_big(tmp_path / "a_nc4.nc", ds, "NETCDF4")
        p_nc3 = _write_big(tmp_path / "a_nc3.nc", ds, "NETCDF3_64BIT")
        assert hash_netcdf_contents(p_nc4) == hash_netcdf_contents(p_nc3)

    def test_survives_nccopy_cdf5(self, tmp_path):
        if shutil.which("nccopy") is None:
            pytest.skip("nccopy not on PATH")
        p_nc4 = _write_big(tmp_path / "a_nc4.nc", _big_dataset())
        p_cdf5 = tmp_path / "a_cdf5.nc"
        h_before = hash_netcdf_contents(p_nc4)

        subprocess.run(["nccopy", "-k", "cdf5", str(p_nc4), str(p_cdf5)], check=True)
        assert hash_netcdf_contents(p_cdf5) == h_before

    def test_large_variable_is_read_bounded(self, tmp_path, monkeypatch):
        p = _write_big(tmp_path / "a.nc", _big_dataset())
        spy = _ReadSpy(monkeypatch)
        hash_netcdf_contents(p)

        assert 0 < spy.elements["tracer"] <= SAMPLE_BUDGET // 4
        assert spy.calls["tracer"] == 2
        assert spy.elements["t"] == _BIG_SHAPE[0]
        assert spy.calls["t"] == 1

    def test_long_rows_keep_first_and_last_record(self, tmp_path, monkeypatch):
        """Rows longer than the budget still sample the last record, within budget."""
        shape = (3, 300_000)  # 2.4 MB float64 rows: two rows would exceed the budget
        data = np.random.default_rng(0).random(shape)
        ds = xr.Dataset({"field": (["t", "x"], data)})
        h1 = hash_netcdf_contents(_write_big(tmp_path / "a.nc", ds))

        ds2 = ds.copy(deep=True)
        ds2["field"].values[-1, -1] += 1.0
        assert hash_netcdf_contents(_write_big(tmp_path / "b.nc", ds2)) != h1

        spy = _ReadSpy(monkeypatch)
        hash_netcdf_contents(tmp_path / "a.nc")
        assert spy.calls["field"] == 2
        assert 0 < spy.elements["field"] <= SAMPLE_BUDGET // 8

    def test_every_variable_is_read_bounded(self, tmp_path, monkeypatch):
        """A large dimension coordinate and a single-record 3-D variable are sampled,
        not read whole at open (xarray would index the coordinate by default).
        """
        ds = xr.Dataset(
            {
                "field": (["t", "y", "x"], np.zeros((1, 1000, 1000), dtype=np.float32)),
                "x": ("x", np.arange(1000.0)),
                "big_coord": ("n", np.arange(2_000_000.0)),
            }
        )
        p = _write_big(tmp_path / "a.nc", ds)
        spy = _ReadSpy(monkeypatch)
        hash_netcdf_contents(p)

        assert 0 < spy.elements["big_coord"] <= SAMPLE_BUDGET // 8
        assert 0 < spy.elements["field"] <= SAMPLE_BUDGET // 4
        assert spy.elements["x"] == 1000
        assert spy.calls["big_coord"] == spy.calls["field"] == 2
        assert spy.calls["x"] == 1

    def test_long_row_is_sampled_at_both_ends(self, tmp_path):
        data = np.random.default_rng(0).random(1_000_000)
        ds = xr.Dataset({"series": ("n", data)})
        h1 = hash_netcdf_contents(_write_big(tmp_path / "a.nc", ds))
        assert h1 == hash_netcdf_contents(_write_big(tmp_path / "b.nc", ds))

        for index in (0, -1):
            ds2 = ds.copy(deep=True)
            ds2["series"].values[index] += 1.0
            assert hash_netcdf_contents(_write_big(tmp_path / "c.nc", ds2)) != h1


def test_short_digest_strips_scheme_tag():
    assert short_digest(f"{HASH_SCHEME}:" + "0123456789abcdef" * 4) == "0123456789ab"
    assert short_digest("deadbeef" * 8) == "deadbeefdead"


@pytest.mark.parametrize(
    ("value", "scheme"),
    [
        pytest.param("bp-v2:abc", "bp-v2", id="tagged"),
        pytest.param(f"{SHA256_SCHEME}:x", SHA256_SCHEME, id="another-scheme"),
        pytest.param("a:b:c", "a", id="first-colon"),
        pytest.param("0123456789abcdef" * 4, "", id="untagged"),
        pytest.param("", "", id="empty"),
    ],
)
def test_digest_scheme_is_the_tag_before_the_first_colon(
    value: str, scheme: str
) -> None:
    """Verify a tagged digest names its scheme, and an untagged or empty one has none."""
    assert digest_scheme(value) == scheme


def test_sha256_digest_is_the_scheme_tagged_hash_of_the_bytes(tmp_path):
    p = tmp_path / "log.txt"
    p.write_bytes(b"some bytes")

    assert (
        sha256_digest(p)
        == f"{SHA256_SCHEME}:{hashlib.sha256(b'some bytes').hexdigest()}"
    )
    assert sha256_digest(str(p)) == sha256_digest(p)
