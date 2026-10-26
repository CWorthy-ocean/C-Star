"""Tests for the namelist schema `ROMSSimulation.build()` records beside the
executable and `attach()` reads back, so a resumed run writes the namelist its
executable was built for even after C-Star gains a newer schema.
"""

import json
import logging
import tempfile
from collections.abc import Callable, Iterator
from pathlib import Path
from unittest import mock

import pytest
from pydantic import Field

from cstar.base.exceptions import CstarExpectationFailed
from cstar.io.source_data import SourceData
from cstar.marbl.external_codebase import MARBLExternalCodeBase
from cstar.roms.external_codebase import ROMSExternalCodeBase
from cstar.roms.file_system import BUILD_RECORD_NAME, ROMS_EXECUTABLE_NAME
from cstar.roms.input_dataset import ROMSInputDataset, ROMSPartitioning
from cstar.roms.namelist import (
    NAMELIST_SCHEMA_REGISTRY,
    UCLA_ROMS_0_12_0,
    CdrLiteSettingsV0_12_0,
    RomsNamelistV0_11_0,
    RomsNamelistV0_12_0,
    namelist_schema_for_ref,
)
from cstar.roms.simulation import ROMSSimulation

FIXTURES = Path(__file__).parent / "fixtures"

UCLA_ROMS_NEXT = (0, 13, 0)


class _CdrLiteSettingsNext(CdrLiteSettingsV0_12_0):
    cdr_next_release_switch: bool = Field(default=True)


class _RomsNamelistNext(RomsNamelistV0_12_0):
    """A schema for a ucla-roms release after the newest one C-Star has, adding
    one always-written key, as a C-Star upgrade would.
    """

    cdr_lite_settings: _CdrLiteSettingsNext = Field(
        default_factory=_CdrLiteSettingsNext
    )


REGISTRY_WITH_NEXT_TIER = (
    *NAMELIST_SCHEMA_REGISTRY[:-1],
    (UCLA_ROMS_0_12_0, UCLA_ROMS_NEXT, RomsNamelistV0_12_0),
    (UCLA_ROMS_NEXT, None, _RomsNamelistNext),
)


@pytest.fixture
def registry_with_next_tier() -> Iterator[None]:
    """This C-Star after an upgrade that added a namelist schema."""
    with mock.patch(
        "cstar.roms.namelist.NAMELIST_SCHEMA_REGISTRY", REGISTRY_WITH_NEXT_TIER
    ):
        yield


def _pin(checkout_target: str) -> mock._patch:
    """Pin every repository source (ucla-roms included) to `checkout_target`."""
    return mock.patch.object(
        SourceData,
        "checkout_target",
        new_callable=mock.PropertyMock,
        return_value=checkout_target,
    )


def _write_record(sim: ROMSSimulation, content: str) -> None:
    (sim.fs_manager.compile_time_code_dir / BUILD_RECORD_NAME).write_text(content)


@pytest.fixture
def staged_sim(stub_romssimulation: ROMSSimulation) -> Iterator[ROMSSimulation]:
    """`stub_romssimulation` with a working directory a previous attempt staged,
    built and partitioned, its runtime namelist a real 0.12.0 one, and the
    codebase and linkage checks of `attach()` patched out.
    """
    sim = stub_romssimulation
    assert sim.compile_time_code is not None
    np_xi, np_eta = sim.discretization.n_procs_x, sim.discretization.n_procs_y
    assert np_xi is not None and np_eta is not None

    for code, target in (
        (sim.compile_time_code, sim.fs_manager.compile_time_code_dir),
        (sim.runtime_code, sim.fs_manager.runtime_code_dir),
    ):
        target.mkdir(parents=True, exist_ok=True)
        for f in code.source:
            (target / f.basename).write_text("x")
    (sim.fs_manager.runtime_code_dir / sim._namelist_file).write_text(
        (FIXTURES / "example_namelist_v0_12_0.nml").read_text()
    )
    (sim.fs_manager.compile_time_code_dir / ROMS_EXECUTABLE_NAME).write_text("binary")

    inputs_dir = sim.fs_manager.input_datasets_dir
    inputs_dir.mkdir(parents=True, exist_ok=True)
    for inp in sim.input_datasets:
        path = inputs_dir / inp.source.basename
        path.write_text("x")
        if inp.partitionable:
            n = np_xi * np_eta
            for i in range(n):
                stem = path.with_suffix("")
                Path(f"{stem}{ROMSPartitioning.suffix(i, n)}").write_text("x")

    with (
        mock.patch("cstar.roms.simulation.verify_roms_linkage"),
        mock.patch.object(ROMSExternalCodeBase, "attach"),
        mock.patch.object(MARBLExternalCodeBase, "attach"),
        mock.patch.object(ROMSInputDataset, "get"),
        mock.patch.object(ROMSInputDataset, "partition"),
    ):
        yield sim


@pytest.fixture
def build(
    staged_sim: ROMSSimulation, stageddatacollection_remote_files
) -> Iterator[Callable[[], None]]:
    """Call `staged_sim.build()` with compilation and linkage checks patched out,
    the ucla-roms clone at commit ``abc123``.
    """
    sim = staged_sim
    assert sim.compile_time_code is not None
    build_dir = sim.fs_manager.compile_time_code_dir
    sim.compile_time_code._working_copy = stageddatacollection_remote_files(
        paths=[build_dir / f.basename for f in sim.compile_time_code.source]
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        (Path(tmpdir) / "Work").mkdir()
        (Path(tmpdir) / "Work" / "Makefile").touch()
        working_copy = mock.MagicMock(path=Path(tmpdir), checkout_hash="abc123")
        with (
            mock.patch("cstar.roms.simulation.rpath_link_flags", return_value=None),
            mock.patch("cstar.roms.simulation.assert_single_toolchain_stack"),
            mock.patch("cstar.roms.simulation.explicit_mpi_wrapper", return_value=None),
            mock.patch(
                "cstar.roms.simulation._get_sha256_hash", return_value="dummy_hash"
            ),
            mock.patch(
                "subprocess.run",
                return_value=mock.MagicMock(returncode=0, stderr=""),
            ),
            mock.patch.object(
                ROMSExternalCodeBase,
                "working_copy",
                new_callable=mock.PropertyMock,
                return_value=working_copy,
            ),
        ):

            def _build() -> None:
                with pytest.warns(UserWarning, match="not a release tag"):
                    sim.build(rebuild=True)

            yield _build


def test_branch_pinned_build_records_newest_schema(
    staged_sim: ROMSSimulation, build: Callable[[], None]
):
    """A build records the schema it selected, with the pin and the commit it built."""
    sim = staged_sim
    checkout_target = sim.codebase.source.checkout_target
    assert checkout_target == "roms_branch"

    build()

    record_path = sim.fs_manager.compile_time_code_dir / BUILD_RECORD_NAME
    assert json.loads(record_path.read_text()) == {
        "namelist_schema": NAMELIST_SCHEMA_REGISTRY[-1][2].__name__,
        "ucla_roms_ref": checkout_target,
        "ucla_roms_commit": "abc123",
    }
    assert sim._namelist_schema is NAMELIST_SCHEMA_REGISTRY[-1][2]


def test_a_failed_record_write_warns_and_leaves_no_stale_record(
    staged_sim: ROMSSimulation,
    build: Callable[[], None],
    caplog: pytest.LogCaptureFixture,
):
    """A rebuild removes the previous build's record before compiling; a write
    that then fails is warned about, never a reason to stop the run.
    """
    sim = staged_sim
    _write_record(sim, json.dumps({"namelist_schema": "RomsNamelistV0_11_0"}))
    caplog.set_level(logging.WARNING, logger=sim.log.name)
    real_write_text = Path.write_text

    def _write_text(path: Path, *args, **kwargs):
        if path.name == BUILD_RECORD_NAME:
            raise OSError("disk full")
        return real_write_text(path, *args, **kwargs)

    with mock.patch.object(Path, "write_text", _write_text):
        build()

    assert not (sim.fs_manager.compile_time_code_dir / BUILD_RECORD_NAME).exists()
    assert sim._namelist_schema is NAMELIST_SCHEMA_REGISTRY[-1][2]
    assert any("disk full" in r.message for r in caplog.records)


def test_resume_writes_the_recorded_schema_after_a_newer_one_is_added(
    staged_sim: ROMSSimulation, registry_with_next_tier, tmp_path: Path
):
    """A branch pin built under 0.12.0's schema resumes with it, though this
    C-Star would now select a newer one, and the written namelist lacks the
    newer schema's key.
    """
    sim = staged_sim
    _write_record(sim, json.dumps({"namelist_schema": "RomsNamelistV0_12_0"}))
    with pytest.warns(UserWarning, match="not a release tag"):
        assert namelist_schema_for_ref(sim.codebase.source.checkout_target) is (
            _RomsNamelistNext
        )

    sim.attach()
    nml = sim.roms_runtime_settings

    assert type(nml) is RomsNamelistV0_12_0
    written = tmp_path / "roms.nml"
    nml.write(written)
    assert "cdr_carbonate_sensitivity_time_interpolation" in written.read_text()
    assert "cdr_next_release_switch" not in written.read_text()


def test_tag_pinned_resume_is_unchanged(
    staged_sim: ROMSSimulation, registry_with_next_tier
):
    """A release tag selects the schema it was built with, before and after an upgrade."""
    sim = staged_sim
    (sim.fs_manager.runtime_code_dir / sim._namelist_file).write_text(
        (FIXTURES / "example_namelist_v0_11_0.nml").read_text()
    )
    _write_record(sim, json.dumps({"namelist_schema": "RomsNamelistV0_11_0"}))

    with _pin("v0.11.0"):
        sim.attach()
        _, schema, _ = sim._read_raw_namelist()
        assert schema is namelist_schema_for_ref("v0.11.0") is RomsNamelistV0_11_0
    assert sim._namelist_schema is RomsNamelistV0_11_0


def test_resume_without_a_record_selects_from_the_ref(
    staged_sim: ROMSSimulation,
    registry_with_next_tier,
    caplog: pytest.LogCaptureFixture,
):
    """A build by an earlier C-Star has no record: the schema is selected from the pin, as before."""
    sim = staged_sim
    caplog.set_level(logging.DEBUG, logger=sim.log.name)

    sim.attach()
    with pytest.warns(UserWarning, match="not a release tag"):
        _, schema, chosen_by = sim._read_raw_namelist()

    assert schema is _RomsNamelistNext
    assert "roms_branch" in chosen_by
    assert sum("No build record" in r.message for r in caplog.records) == 1


@pytest.mark.parametrize("content", ["{not json", "[]", '{"other": 1}'])
def test_resume_with_an_unreadable_record_warns_and_selects_from_the_ref(
    staged_sim: ROMSSimulation, caplog: pytest.LogCaptureFixture, content: str
):
    """A record that cannot be read is warned about, never a reason to stop the run."""
    sim = staged_sim
    _write_record(sim, content)
    caplog.set_level(logging.WARNING, logger=sim.log.name)

    sim.attach()

    assert sim._namelist_schema is None
    assert any(
        "Could not read the namelist schema" in r.message for r in caplog.records
    )


def test_resume_refuses_a_schema_this_cstar_does_not_have(
    staged_sim: ROMSSimulation,
):
    """A recorded schema this C-Star lacks is refused, naming it and the way to
    rebuild, and drops the schema an earlier attach adopted.
    """
    sim = staged_sim
    _write_record(sim, json.dumps({"namelist_schema": "RomsNamelistV0_12_0"}))
    sim.attach()
    assert sim._namelist_schema is RomsNamelistV0_12_0
    _write_record(sim, json.dumps({"namelist_schema": "RomsNamelistV0_99_0"}))

    with pytest.raises(CstarExpectationFailed) as exc_info:
        sim.attach()

    message = str(exc_info.value)
    assert "'RomsNamelistV0_99_0'" in message
    assert "upgrade cstar-ocean" in message
    assert "--clobber" in message
    assert sim._namelist_schema is None
