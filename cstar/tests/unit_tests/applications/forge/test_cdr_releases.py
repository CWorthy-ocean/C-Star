"""CDR releases against the run and the grid: ``check_cdr_releases`` and its three
call sites, the resolver's ``cdr_frc.time_interpolation``,
``check_cdr_release_positions`` and its preflight, and the reference date
``_generate_cdr_forcing`` hands roms-tools.
"""

import copy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
import roms_tools as rt
import xarray as xr
from test_forge_blueprint import _CATALOG, _build, _FakeBuilder
from test_input_generator import _LITE_TRACERS, _hand_built_cdr_input_data
from test_write_namelist import _base_settings, _write_and_read

import cstar.applications.ucla_roms.models as cstar_models
from cstar.applications.forge.engine import (
    process_forge_blueprint,
    split_model_settings,
)
from cstar.applications.forge.executor import ForgeExecutor
from cstar.applications.forge.host import HostPaths
from cstar.applications.forge.input_generator import (
    CDR_FORCING_NETCDF_STEM,
    INPUT_REGISTRY,
)
from cstar.applications.forge.namelist_model import (
    cdr_releases_time_interpolation,
    check_cdr_releases,
)
from cstar.applications.forge.util import check_cdr_release_positions

RUN_START = datetime(2012, 1, 1)
RUN_END = datetime(2012, 1, 2)
DAY = timedelta(days=1)


def _iso(when: datetime) -> str:
    return when.isoformat()


def _release(
    name: str = "oae",
    times: tuple[datetime, ...] = (RUN_START, RUN_END),
    **extra,
) -> dict:
    """A tracer-perturbation release at the ``_build`` grid's centre."""
    return {
        "name": name,
        "lat": 55.0,
        "lon": 0.0,
        "depth": 10.0,
        "hsc": 10.0,
        "vsc": 10.0,
        "times": [_iso(t) for t in times],
        "release_type": "tracer_perturbation",
        "tracer_fluxes": {"ALK": [2.0e6] * len(times)},
        **extra,
    }


def _forcing(
    *releases: dict, start: datetime = RUN_START, end: datetime = RUN_END
) -> dict:
    """roms-tools ``CDRForcing`` kwargs over ``[start, end]``."""
    return {
        "start_time": _iso(start),
        "end_time": _iso(end),
        "releases": list(releases) or [_release()],
    }


def _simple(forcing: dict) -> dict:
    return {"mode": "simple", "cdr_forcing": forcing}


def _check(forcing: dict | None, **kwargs) -> None:
    check_cdr_releases(forcing, start_date=RUN_START, end_date=RUN_END, **kwargs)


class TestCheckCdrReleases:
    """The rule itself, on roms-tools ``CDRForcing`` kwargs dicts."""

    def test_a_window_and_release_on_the_run_pass(self):
        _check(_forcing())

    def test_a_wider_window_passes(self):
        _check(_forcing(start=RUN_START - 30 * DAY, end=RUN_END + 30 * DAY))

    def test_no_cdr_forcing_checks_nothing(self):
        _check(None)

    def test_a_window_starting_after_the_run_start_is_refused(self):
        forcing = _forcing(
            _release(times=(RUN_START + DAY / 2, RUN_END)), start=RUN_START + DAY / 2
        )
        with pytest.raises(ValueError, match=r"CDR window starts .* after the run"):
            _check(forcing)

    def test_a_window_ending_before_the_run_end_is_refused(self):
        forcing = _forcing(
            _release(times=(RUN_START, RUN_END - DAY / 2)), end=RUN_END - DAY / 2
        )
        with pytest.raises(ValueError, match=r"CDR window ends .* before the run"):
            _check(forcing)

    def test_a_window_in_another_year_names_the_window_and_the_release(self):
        start = datetime(2030, 1, 1)
        forcing = _forcing(
            _release("y2030", times=(start, start + DAY)), start=start, end=start + DAY
        )
        with pytest.raises(ValueError) as excinfo:
            _check(forcing)
        message = str(excinfo.value)
        assert "CDR window starts at 2030-01-01" in message
        assert "release 'y2030'" in message

    @pytest.mark.parametrize(
        "forcing",
        [
            {"releases": []},
            {"start_time": None, "end_time": None, "releases": [_release()]},
            {"releases": [_release()]},
        ],
    )
    def test_unset_window_bounds_are_not_checked(self, forcing):
        _check(forcing)

    def test_a_release_after_the_run_end_is_refused(self):
        forcing = _forcing(
            _release("late", times=(RUN_END + 3 * DAY, RUN_END + 4 * DAY)),
            end=RUN_END + 10 * DAY,
        )
        with pytest.raises(ValueError, match=r"release 'late'.*after the run ends"):
            _check(forcing)

    def test_a_release_starting_at_the_run_end_is_refused(self):
        """It delivers nothing: the run ends as it starts."""
        forcing = _forcing(
            _release("edge", times=(RUN_END, RUN_END + DAY)), end=RUN_END + DAY
        )
        with pytest.raises(ValueError, match=r"release 'edge'"):
            _check(forcing)

    def test_a_release_before_the_run_start_is_refused(self):
        forcing = _forcing(
            _release("early", times=(RUN_START - 3 * DAY, RUN_START - 2 * DAY)),
            start=RUN_START - 10 * DAY,
        )
        with pytest.raises(ValueError, match=r"release 'early'.*before the run starts"):
            _check(forcing)

    def test_a_release_ending_at_the_run_start_passes(self):
        """Its value at the run start applies in the run."""
        forcing = _forcing(
            _release(times=(RUN_START - DAY, RUN_START)), start=RUN_START - DAY
        )
        _check(forcing)

    def test_a_release_without_times_passes(self):
        _check(_forcing(_release(times=())))

    def test_every_offending_release_is_named_in_one_error(self):
        forcing = _forcing(
            _release("ok"),
            _release("late", times=(RUN_END + DAY, RUN_END + 2 * DAY)),
            _release("early", times=(RUN_START - 2 * DAY, RUN_START - DAY)),
            start=RUN_START - 10 * DAY,
            end=RUN_END + 10 * DAY,
        )
        with pytest.raises(ValueError) as excinfo:
            _check(forcing)
        message = str(excinfo.value)
        assert "release 'late'" in message
        assert "release 'early'" in message
        assert "release 'ok'" not in message

    def test_yaml_native_and_timezone_aware_times_compare_with_the_run(self):
        forcing = {
            "start_time": RUN_START,
            "end_time": "2012-01-02T00:00:00Z",
            "releases": [
                {
                    **_release(),
                    "times": [
                        datetime(2012, 1, 1, tzinfo=UTC),
                        "2012-01-02T00:00:00+00:00",
                    ],
                }
            ],
        }
        _check(forcing)

    def test_an_unparseable_time_is_reported(self):
        forcing = _forcing()
        forcing["start_time"] = "not a date"
        with pytest.raises(ValueError, match=r"cdr_forcing\.start_time 'not a date'"):
            _check(forcing)

    def test_times_that_are_not_a_list_are_reported(self):
        forcing = _forcing({**_release("one"), "times": _iso(RUN_START)})
        with pytest.raises(ValueError, match=r"release 'one': times .* is not a list"):
            _check(forcing)

    def test_releases_given_as_a_tuple_are_checked(self):
        forcing = _forcing(end=RUN_END + 10 * DAY)
        forcing["releases"] = (
            _release("late", times=(RUN_END + DAY, RUN_END + 2 * DAY)),
        )
        with pytest.raises(ValueError, match=r"release 'late'"):
            _check(forcing)

    def test_a_time_interpolation_that_is_not_a_boolean_is_reported(self):
        forcing = _forcing(_release("odd", time_interpolation="maybe"))
        assert cdr_releases_time_interpolation(forcing) is None
        with pytest.raises(
            ValueError, match=r"release 'odd': time_interpolation 'maybe' is not"
        ):
            _check(forcing, time_interpolation=False)

    def test_releases_disagreeing_on_time_interpolation_are_refused(self):
        forcing = _forcing(_release("ramp", time_interpolation=True), _release("step"))
        with pytest.raises(ValueError, match=r"disagree on time_interpolation"):
            _check(forcing)

    @pytest.mark.parametrize("releases_interp", [True, False])
    def test_a_flag_contradicting_the_releases_is_refused(self, releases_interp):
        forcing = _forcing(_release(time_interpolation=releases_interp))
        with pytest.raises(
            ValueError,
            match=rf"cdr_frc\.time_interpolation is {str(not releases_interp).lower()}",
        ):
            _check(forcing, time_interpolation=not releases_interp)

    def test_a_matching_flag_passes(self):
        _check(_forcing(_release(time_interpolation=True)), time_interpolation=True)

    def test_a_release_without_the_key_is_a_step_release(self):
        forcing = _forcing()
        assert cdr_releases_time_interpolation(forcing) is False
        with pytest.raises(ValueError, match=r"time_interpolation is true"):
            _check(forcing, time_interpolation=True)

    def test_the_flag_is_not_checked_when_not_given(self):
        _check(_forcing(_release(time_interpolation=True)))


class TestResolverCdrReleases:
    """``build_forge_blueprint`` derives ``cdr_frc.time_interpolation`` from the
    releases unless the user set it, and refuses what does not fit the run.
    """

    @pytest.mark.parametrize("interp", [True, False])
    def test_the_flag_follows_the_releases(self, interp):
        cfg = _build(cdr=_simple(_forcing(_release(time_interpolation=interp))))
        assert cfg.model_settings["cdr_frc"]["time_interpolation"] is interp

    def test_a_matching_override_is_kept(self):
        cfg = _build(
            cdr=_simple(_forcing(_release(time_interpolation=True))),
            run_time_overrides={"cdr_frc": {"time_interpolation": True}},
        )
        assert cfg.model_settings["cdr_frc"]["time_interpolation"] is True

    def test_a_contradicting_override_is_refused(self):
        with pytest.raises(ValueError, match=r"cdr_frc\.time_interpolation is false"):
            _build(
                cdr=_simple(_forcing(_release(time_interpolation=True))),
                run_time_overrides={"cdr_frc": {"time_interpolation": False}},
            )

    def test_a_contradicting_forcing_spec_cdr_frc_is_refused(self):
        forcing_inputs = copy.deepcopy(_CATALOG.forcing_data("glorys-era5-unified"))
        forcing_inputs["cdr_frc"] = {"time_interpolation": True}
        with pytest.raises(ValueError, match=r"cdr_frc\.time_interpolation is true"):
            _build(cdr=_simple(_forcing()), forcing_inputs=forcing_inputs)

    def test_a_time_interpolation_that_is_not_a_boolean_is_named(self):
        with pytest.raises(ValueError, match=r"(?s)does not fit the run.*'maybe'"):
            _build(cdr=_simple(_forcing(_release(time_interpolation="maybe"))))

    def test_without_cdr_forcing_the_flag_is_left_alone(self):
        cfg = _build(run_time_overrides={"cdr_frc": {"time_interpolation": True}})
        assert cfg.model_settings["cdr_frc"]["time_interpolation"] is True

    def test_a_release_after_the_run_end_is_refused(self):
        forcing = _forcing(
            _release("late", times=(RUN_END + 3 * DAY, RUN_END + 4 * DAY)),
            end=RUN_END + 10 * DAY,
        )
        with pytest.raises(ValueError, match=r"release 'late'"):
            _build(cdr=_simple(forcing))

    def test_a_cdr_window_in_another_year_is_refused(self):
        start = datetime(2030, 1, 1)
        forcing = _forcing(
            _release(times=(start, start + DAY)), start=start, end=start + DAY
        )
        with pytest.raises(ValueError, match=r"CDR window starts at 2030-01-01"):
            _build(cdr=_simple(forcing))


class TestCdrTimeInterpolationNamelist:
    """The resolved flag is what the namelist writes as ``cdr_time_interpolation``."""

    def test_interpolated_releases_write_true(self, tmp_path):
        cfg = _build(cdr=_simple(_forcing(_release(time_interpolation=True))))
        settings = _base_settings()
        settings["cdr_frc"] = copy.deepcopy(cfg.model_settings["cdr_frc"])

        nml = _write_and_read(tmp_path, settings)

        assert nml["cdr_frc_settings"]["cdr_time_interpolation"] is True


def _run_configure_build(cfg, tmp_path: Path) -> ForgeExecutor:
    """``configure_build`` on ``cfg`` as stored, without generating inputs."""
    host = HostPaths(working_dir=tmp_path, source_data_cache=tmp_path, system="test")
    builder = ForgeExecutor.from_forge_blueprint(cfg, host=host)
    run_ov, compile_ov = split_model_settings(cfg)
    with (
        patch("cstar.applications.forge.executor.render_roms_settings") as render,
        patch("cstar.applications.forge.executor.write_roms_namelist"),
    ):
        render.return_value = {
            "location": str(builder.compile_time_code_dir),
            "filter": {"files": ["test.opt"]},
            "branch": "main",
        }
        builder.configure_build(
            compile_time_settings=compile_ov, run_time_settings=run_ov
        )
    return builder


class TestStoredBlueprintCdrNets:
    """A stored blueprint reaches ``process_forge_blueprint`` and ``configure_build``
    without re-resolving: a wizard override applied after the resolver, a hand edit,
    or a blueprint written before the flag followed the releases.
    """

    @staticmethod
    def _process(cfg) -> None:
        process_forge_blueprint(
            cfg,
            validate=True,
            ensure_data=False,
            generate=False,
            configure=False,
            executor_factory=_FakeBuilder,
        )

    def test_process_refuses_a_stored_flag_contradicting_the_releases(self):
        cfg = _build(cdr=_simple(_forcing(_release(time_interpolation=True))))
        cfg.model_settings["cdr_frc"]["time_interpolation"] = False

        with pytest.raises(
            ValueError,
            match=r"(?s)invalid values.*cdr_frc\.time_interpolation is false",
        ):
            self._process(cfg)

    def test_process_refuses_a_run_window_moved_off_the_releases(self):
        cfg = _build(cdr=_simple(_forcing()))
        cfg.run = cfg.run.model_copy(
            update={"start_date": RUN_START + DAY, "end_date": RUN_END + DAY}
        )

        with pytest.raises(ValueError, match=r"(?s)invalid values.*CDR window ends"):
            self._process(cfg)

    def test_process_accepts_a_matching_blueprint(self):
        self._process(_build(cdr=_simple(_forcing(_release(time_interpolation=True)))))

    def test_configure_build_refuses_a_flag_set_after_the_resolver(self, tmp_path):
        cfg = _build(cdr=_simple(_forcing()))
        cfg.model_settings["cdr_frc"]["time_interpolation"] = True

        with pytest.raises(ValueError, match=r"cdr_frc\.time_interpolation is true"):
            _run_configure_build(cfg, tmp_path)

    def test_configure_build_keeps_the_resolved_flag(self, tmp_path):
        cfg = _build(cdr=_simple(_forcing(_release(time_interpolation=True))))

        builder = _run_configure_build(cfg, tmp_path)

        assert builder._settings_run_time["cdr_frc"]["time_interpolation"] is True


@pytest.fixture(scope="module")
def real_grid() -> rt.Grid:
    """A real 20x20 grid at 55N 0E (the ``sample_grid`` of the input generator
    tests): North Sea, with land on its edges.
    """
    return rt.Grid(
        nx=20,
        ny=20,
        size_x=500,
        size_y=1000,
        center_lon=0,
        center_lat=55,
        rot=10,
        N=3,
        theta_s=5.0,
        theta_b=2.0,
        hc=250.0,
    )


def _at(grid: rt.Grid, eta: int, xi: int, **extra) -> dict:
    """A release at rho point ``(eta, xi)`` of ``grid``."""
    return _release(
        lat=float(grid.ds["lat_rho"][eta, xi]),
        lon=float(grid.ds["lon_rho"][eta, xi]),
        **extra,
    )


def _interior_land(grid: rt.Grid) -> tuple[int, int]:
    """An interior land cell at least two cells from the edge."""
    mask = grid.ds["mask_rho"].values
    eta, xi = np.argwhere(mask[2:-2, 2:-2] == 0)[0] + 2
    return int(eta), int(xi)


def _wet_edge(grid: rt.Grid) -> tuple[int, int]:
    """A boundary rho point whose interior neighbour is water over 20 m deep."""
    mask = grid.ds["mask_rho"].values
    h = grid.ds["h"].values
    n_eta, n_xi = mask.shape
    for edge, inner in (
        *(((0, xi), (1, xi)) for xi in range(2, n_xi - 2)),
        *(((n_eta - 1, xi), (n_eta - 2, xi)) for xi in range(2, n_xi - 2)),
        *(((eta, 0), (eta, 1)) for eta in range(2, n_eta - 2)),
        *(((eta, n_xi - 1), (eta, n_xi - 2)) for eta in range(2, n_eta - 2)),
    ):
        if mask[inner] > 0 and h[inner] > 20:
            return edge
    raise AssertionError("the grid has no wet edge")


CENTRE = (11, 11)
"""A wet interior cell of ``real_grid`` (about 45 m deep)."""


def _positions(grid: rt.Grid, *releases: dict, relocate: bool = True) -> None:
    check_cdr_release_positions(grid, list(releases), relocate_to_wet_pts=relocate)


class TestCheckCdrReleasePositions:
    """What Forge refuses and warns about before ROMS moves a release."""

    def test_a_release_on_a_wet_interior_cell_passes_silently(self, real_grid, recwarn):
        _positions(real_grid, _at(real_grid, *CENTRE))
        assert not [w for w in recwarn if "CDR releases" in str(w.message)]

    def test_a_release_outside_the_domain_is_refused(self, real_grid):
        with pytest.raises(ValueError, match=r"release 'far'.* outside the domain"):
            _positions(real_grid, _release("far", lat=0.0, lon=100.0))

    def test_a_release_on_land_warns_when_roms_relocates_it(self, real_grid):
        release = _at(real_grid, *_interior_land(real_grid), name="coast")
        with pytest.warns(UserWarning, match=r"release 'coast'.* on land; ROMS moves"):
            _positions(real_grid, release)

    def test_a_release_on_land_is_refused_without_relocation(self, real_grid):
        release = _at(real_grid, *_interior_land(real_grid), name="coast")
        with pytest.raises(
            ValueError, match=r"release 'coast'.*relocate_to_wet_pts is off"
        ):
            _positions(real_grid, release, relocate=False)

    def test_a_release_on_the_edge_warns(self, real_grid):
        with pytest.warns(UserWarning, match=r"release 'rim'.* on the edge"):
            _positions(real_grid, _at(real_grid, *_wet_edge(real_grid), name="rim"))

    def test_an_edge_release_whose_cell_is_land_gets_only_the_land_warning(
        self, real_grid
    ):
        mask = real_grid.ds["mask_rho"].values
        xi = int(np.argwhere(mask[1, 2:-2] == 0)[0][0]) + 2
        with pytest.warns(UserWarning) as record:
            _positions(real_grid, _at(real_grid, 0, xi, name="rim"))
        (message,) = [
            str(w.message) for w in record if "CDR releases" in str(w.message)
        ]
        assert "on land" in message
        assert "on the edge" not in message

    def test_a_non_numeric_scale_is_left_to_roms_tools(self, real_grid, recwarn):
        _positions(real_grid, _at(real_grid, *CENTRE, hsc="wide"))
        assert not [w for w in recwarn if "CDR releases" in str(w.message)]

    def test_a_release_just_below_the_seafloor_warns(self, real_grid):
        h = float(real_grid.ds["h"][CENTRE])
        release = _at(real_grid, *CENTRE, name="deep", depth=h + 5.0)
        with pytest.warns(UserWarning, match=r"release 'deep'.* below the seafloor"):
            _positions(real_grid, release)

    def test_a_release_whose_profile_vanishes_is_refused(self, real_grid):
        release = _at(real_grid, *CENTRE, name="abyss", depth=9000.0)
        with pytest.raises(ValueError, match=r"release 'abyss'.* zero at every level"):
            _positions(real_grid, release)

    def test_a_point_release_far_below_the_seafloor_warns(self, real_grid):
        """With no vertical scale ROMS puts it in the bottom level."""
        release = _at(real_grid, *CENTRE, name="abyss", depth=9000.0, vsc=0.0)
        with pytest.warns(UserWarning, match=r"release 'abyss'.* below the seafloor"):
            _positions(real_grid, release)

    def test_every_refused_release_is_named_in_one_error(self, real_grid):
        with pytest.raises(ValueError) as excinfo:
            _positions(
                real_grid,
                _at(real_grid, *CENTRE, name="ok"),
                _release("far", lat=0.0, lon=100.0),
                _at(real_grid, *CENTRE, name="abyss", depth=9000.0),
            )
        message = str(excinfo.value)
        assert "release 'far'" in message
        assert "release 'abyss'" in message
        assert "release 'ok'" not in message


class TestPreflightCdrReleases:
    """``generate_all`` checks the release positions before any input is built."""

    @staticmethod
    def _steps(forcing: dict) -> list:
        return [(INPUT_REGISTRY["cdr_forcing"], {"cdr_kwargs": forcing})]

    def test_a_release_outside_the_domain_is_refused_before_generation(
        self, tmp_path, real_grid
    ):
        data = _hand_built_cdr_input_data(tmp_path, grid=real_grid)
        forcing = _forcing(_release("far", lat=0.0, lon=100.0))
        with pytest.raises(ValueError, match=r"release 'far'"):
            data._preflight_cdr_releases(self._steps(forcing), None)

    def test_an_only_inputs_selection_without_cdr_skips_it(self, tmp_path, real_grid):
        data = _hand_built_cdr_input_data(tmp_path, grid=real_grid)
        forcing = _forcing(_release("far", lat=0.0, lon=100.0))
        data._preflight_cdr_releases(self._steps(forcing), {"grid"})

    @pytest.mark.parametrize("relocate", [True, False])
    def test_relocation_is_read_from_the_namelist_settings(
        self, tmp_path, real_grid, relocate
    ):
        data = _hand_built_cdr_input_data(tmp_path, grid=real_grid)
        data._settings_run_time["cdr_frc"] = {"relocate_to_wet_pts": relocate}
        forcing = _forcing(_at(real_grid, *_interior_land(real_grid), name="coast"))
        if relocate:
            with pytest.warns(UserWarning, match=r"on land; ROMS moves"):
                data._preflight_cdr_releases(self._steps(forcing), None)
        else:
            with pytest.raises(ValueError, match=r"relocate_to_wet_pts"):
                data._preflight_cdr_releases(self._steps(forcing), None)


class TestGenerateCdrForcingCall:
    """What ``_generate_cdr_forcing`` hands ``rt.CDRForcing``."""

    @patch("cstar.applications.forge.input_generator.rt.CDRForcing")
    def test_the_runs_reference_date_reaches_roms_tools_without_the_grid(
        self, mock_cdr_class, tmp_path
    ):
        data = _hand_built_cdr_input_data(
            tmp_path, model_reference_date=datetime(2010, 1, 1)
        )
        data._settings_compile_time["cppdefs"] = {"marbl": False, "cdr_lite": True}
        data._settings_run_time["param"] = {"nt_passive": 0, "ntrc_bio": 0}
        data.ucla_roms_blueprint_elements.cdr_forcing = cstar_models.Dataset(data=[])
        built = MagicMock()
        built.save.return_value = data._forcing_filename(CDR_FORCING_NETCDF_STEM)
        built.ds = xr.Dataset(
            {"cdr_trcflx": (("ntracers",), np.zeros(len(_LITE_TRACERS)))},
            coords={"tracer_name": ("ntracers", _LITE_TRACERS)},
        )
        mock_cdr_class.return_value = built
        # a roms-tools YAML dump carries its own reference date
        cdr_kwargs = {**_forcing(), "model_reference_date": "2000-01-01T00:00:00"}

        data._generate_cdr_forcing(key="cdr_forcing", cdr_kwargs=cdr_kwargs)

        kwargs = mock_cdr_class.call_args.kwargs
        # with a grid roms-tools would refuse the releases ROMS moves
        assert "grid" not in kwargs
        assert kwargs["model_reference_date"] == datetime(2010, 1, 1)
        assert kwargs["releases"] == cdr_kwargs["releases"]
