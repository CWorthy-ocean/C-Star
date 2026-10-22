"""
Tests for ``cstar.applications.forge.user_files``: the verification/staging
contract for user-supplied pre-made netCDF files (grid / river / CDR forcing).
"""

import logging
import shutil
import subprocess
from pathlib import Path
from unittest import mock

import numpy as np
import pytest
import xarray as xr

from cstar.applications.forge.blueprint import UserProvidedFile
from cstar.applications.forge.user_files import stage_user_netcdf, verify_user_file
from cstar.base.digest import HASH_SCHEME, hash_netcdf_contents
from cstar.base.utils import NetCDFFormat, netcdf_format


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


class TestVerifyUserFile:
    def test_missing_file_raises_with_label(self, tmp_path):
        f = UserProvidedFile(location=str(tmp_path / "missing.nc"), content_hash="x")
        with pytest.raises(FileNotFoundError, match="my-grid"):
            verify_user_file(f, label="my-grid")

    def test_matching_hash_returns_path_no_warning(self, tmp_path, recwarn):
        ds = _numeric_dataset()
        p = tmp_path / "grid.nc"
        ds.to_netcdf(p)
        h = hash_netcdf_contents(p)
        f = UserProvidedFile(location=str(p), content_hash=h)

        result = verify_user_file(f, label="my-grid")
        assert result == p.resolve()
        assert len(recwarn) == 0

    def test_mismatched_hash_warns_and_returns_path(self, tmp_path):
        ds = _numeric_dataset()
        p = tmp_path / "grid.nc"
        ds.to_netcdf(p)
        f = UserProvidedFile(
            location=str(p), content_hash=f"{HASH_SCHEME}:" + "deadbeef" * 8
        )

        with pytest.warns(UserWarning, match="my-grid") as record:
            result = verify_user_file(f, label="my-grid")
        assert result == p.resolve()
        # the short digests name the hex, not the scheme tag
        assert "recorded deadbeefdead…" in str(record[0].message)
        assert f"{HASH_SCHEME}…" not in str(record[0].message)

    def test_unknown_scheme_is_not_verified(self, tmp_path, recwarn, caplog):
        p = tmp_path / "grid.nc"
        _numeric_dataset().to_netcdf(p)
        f = UserProvidedFile(location=str(p), content_hash="nc-sample-v9:" + "a" * 64)

        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            assert verify_user_file(f, label="my-grid") == p.resolve()

        assert not [w for w in recwarn if issubclass(w.category, UserWarning)]
        messages = [r.getMessage() for r in caplog.records if r.name == _LOGGER]
        assert len(messages) == 1
        assert "'nc-sample-v9'" in messages[0] and "not verified" in messages[0]

    def test_legacy_untagged_hash_is_not_verified(self, tmp_path, recwarn, caplog):
        p = tmp_path / "grid.nc"
        _numeric_dataset().to_netcdf(p)
        f = UserProvidedFile(location=str(p), content_hash="deadbeef" * 8)

        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            result = verify_user_file(f, label="my-grid")

        assert result == p.resolve()
        assert not [w for w in recwarn if issubclass(w.category, UserWarning)]
        records = [r for r in caplog.records if r.name == _LOGGER]
        assert len(records) == 1
        assert records[0].levelno == logging.WARNING
        assert "not verified" in records[0].getMessage()


_RUN = "cstar.base.utils.subprocess.run"
_LOGGER = "cstar.applications.forge.user_files"


def _write(path, fmt):
    # netcdf4 is the only engine that writes CDF-5; xarray>=2026.9 refuses to pick one
    _numeric_dataset().to_netcdf(path, format=fmt, engine="netcdf4")
    return path


def _touch_dest(cmd, check):
    """Stand-in for ``nccopy``: writes the destination (the last argument)."""
    Path(cmd[-1]).write_bytes(b"CDF\x05")


class TestStageUserNetcdf:
    def test_copies_without_pio(self, tmp_path):
        ds = _numeric_dataset()
        src = tmp_path / "src.nc"
        ds.to_netcdf(src)
        dest = tmp_path / "nested" / "dest.nc"

        result = stage_user_netcdf(src, dest, use_pio=False, label="test file")
        assert result == dest
        assert dest.exists()
        with xr.open_dataset(dest) as back:
            assert bool((back["temp"].values == ds["temp"].values).all())

    def test_no_pio_never_converts_nc4(self, tmp_path):
        src = _write(tmp_path / "src.nc", "NETCDF4")
        dest = tmp_path / "dest.nc"

        with mock.patch(_RUN) as run:
            stage_user_netcdf(src, dest, use_pio=False, label="test file")

        run.assert_not_called()
        assert dest.read_bytes() == src.read_bytes()

    def test_pio_copies_classic_without_converting(self, tmp_path, caplog):
        src = _write(tmp_path / "src.nc", "NETCDF3_64BIT_DATA")
        dest = tmp_path / "nested" / "dest.nc"

        with (
            mock.patch(_RUN) as run,
            caplog.at_level(logging.WARNING, logger=_LOGGER),
        ):
            result = stage_user_netcdf(src, dest, use_pio=True, label="test file")

        assert result == dest
        run.assert_not_called()
        assert dest.read_bytes() == src.read_bytes()
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]

    def test_pio_converts_nc4_and_warns(self, tmp_path, caplog):
        src = _write(tmp_path / "src.nc", "NETCDF4")
        dest = tmp_path / "nested" / "dest.nc"

        with (
            mock.patch(_RUN, side_effect=_touch_dest) as run,
            caplog.at_level(logging.WARNING, logger=_LOGGER),
        ):
            result = stage_user_netcdf(src, dest, use_pio=True, label="test file")

        assert result == dest
        run.assert_called_once_with(
            ["nccopy", "-k", "cdf5", str(src), str(dest)], check=True
        )
        warnings_ = [
            r.getMessage() for r in caplog.records if r.levelno == logging.WARNING
        ]
        assert len(warnings_) == 1
        assert all(x in warnings_[0] for x in ("test file", str(src), str(dest)))
        assert src.exists()

    def test_conversion_failure_removes_partial_dest(self, tmp_path):
        src = _write(tmp_path / "src.nc", "NETCDF4")
        dest = tmp_path / "dest.nc"

        def _partial_then_fail(cmd, check):
            Path(cmd[-1]).write_bytes(b"partial")
            raise subprocess.CalledProcessError(1, "nccopy")

        with (
            mock.patch(_RUN, side_effect=_partial_then_fail),
            pytest.raises(RuntimeError, match="test file") as excinfo,
        ):
            stage_user_netcdf(src, dest, use_pio=True, label="test file")

        assert isinstance(excinfo.value.__cause__, subprocess.CalledProcessError)
        assert not dest.exists()
        assert src.exists()

    def test_interrupt_during_conversion_removes_partial_dest(self, tmp_path):
        src = _write(tmp_path / "src.nc", "NETCDF4")
        dest = tmp_path / "dest.nc"

        def _partial_then_interrupt(cmd, check):
            Path(cmd[-1]).write_bytes(b"partial")
            raise KeyboardInterrupt

        with (
            mock.patch(_RUN, side_effect=_partial_then_interrupt),
            pytest.raises(KeyboardInterrupt),
        ):
            stage_user_netcdf(src, dest, use_pio=True, label="test file")

        assert not dest.exists()
        assert src.exists()

    def test_missing_nccopy_raises_runtime_error(self, tmp_path):
        src = _write(tmp_path / "src.nc", "NETCDF4")
        dest = tmp_path / "dest.nc"

        with (
            mock.patch(_RUN, side_effect=FileNotFoundError("nccopy")),
            pytest.raises(RuntimeError, match=r"(?s)nccopy.*PATH"),
        ):
            stage_user_netcdf(src, dest, use_pio=True, label="test file")

        assert not dest.exists()

    @pytest.mark.parametrize(
        ("use_pio", "fmt"),
        [(False, "NETCDF4"), (True, "NETCDF3_64BIT_DATA")],
    )
    def test_same_path_is_left_alone(self, tmp_path, use_pio, fmt):
        src = _write(tmp_path / "in_place.nc", fmt)
        before = src.read_bytes()

        with mock.patch(_RUN) as run:
            result = stage_user_netcdf(src, src, use_pio=use_pio, label="test file")

        assert result == src
        run.assert_not_called()
        assert src.read_bytes() == before

    def test_same_path_nc4_with_pio_raises(self, tmp_path):
        src = _write(tmp_path / "in_place.nc", "NETCDF4")

        with (
            mock.patch(_RUN) as run,
            pytest.raises(RuntimeError, match="already at its destination"),
        ):
            stage_user_netcdf(src, src, use_pio=True, label="test file")

        run.assert_not_called()
        assert src.exists()

    def test_pio_runs_nccopy(self, tmp_path):
        if shutil.which("nccopy") is None:
            pytest.skip("nccopy not on PATH")
        ds = _numeric_dataset()
        src = tmp_path / "src.nc"
        ds.to_netcdf(src, format="NETCDF4")
        dest = tmp_path / "nested" / "dest.nc"

        result = stage_user_netcdf(src, dest, use_pio=True, label="test file")
        assert result == dest
        assert dest.exists()
        assert netcdf_format(dest) == NetCDFFormat.CDF5
        with xr.open_dataset(dest) as back:
            assert bool((back["temp"].values == ds["temp"].values).all())
