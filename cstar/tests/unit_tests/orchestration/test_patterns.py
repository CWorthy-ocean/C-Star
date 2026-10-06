"""Tests for the workplan shape generators and normalizers."""

import os
import typing as t
from datetime import datetime, timedelta
from pathlib import Path

import pytest
import yaml

from cstar.applications.core import EmittedBlueprint
from cstar.orchestration.models import (
    DeferredBlueprintRef,
    InlineBlueprintRef,
    RunRef,
    Step,
    Workplan,
)
from cstar.orchestration.patterns import (
    TIMESTAMP_FMT,
    Generated,
    StepSource,
    available_restarts,
    chunk_steps,
    equal_windows,
    fixed_windows,
    forge_then_run,
    month_windows,
    normalize_legacy,
    predicted_restarts,
    spinup_ramp,
    upscale_chain,
)
from cstar.orchestration.serialization import serialize

FIXTURES = Path(__file__).parent / "fixtures" / "patterns"
"""Directory holding the golden workplans."""

BASE = Step(name="run", application="roms_marbl", blueprint="/path/B.yaml")
"""A base step whose blueprint file does not exist."""


def dt(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day)


def dig(store: t.Any, *keys: str | int) -> t.Any:
    """Return the value at a nested key path of an override store."""
    for key in keys:
        store = store[key]
    return store


def assert_golden(name: str, generated: Generated) -> None:
    """Compare a generated workplan with its golden file.

    Regenerate deliberately with `UPDATE_GOLDEN=1`; a diff is a behaviour change.
    """
    wp = Workplan(
        name=name,
        description=f"golden for {name}",
        runs=generated.runs,
        steps=generated.steps,
    )
    path = FIXTURES / f"{name}.yaml"
    out = path.parent / f".{name}.actual.yaml"
    serialize(out, wp)
    try:
        if os.environ.get("UPDATE_GOLDEN") == "1":
            path.write_text(out.read_text())
        assert out.read_text() == path.read_text()
    finally:
        out.unlink(missing_ok=True)


class TestWindows:
    def test_month_partial_ends(self) -> None:
        windows = month_windows(dt(2023, 1, 15), dt(2023, 4, 10))
        assert windows == [
            (dt(2023, 1, 15), dt(2023, 2, 1)),
            (dt(2023, 2, 1), dt(2023, 3, 1)),
            (dt(2023, 3, 1), dt(2023, 4, 1)),
            (dt(2023, 4, 1), dt(2023, 4, 10)),
        ]

    def test_month_end_on_boundary_has_no_empty_window(self) -> None:
        windows = month_windows(dt(2023, 1, 1), dt(2023, 3, 1))
        assert windows == [
            (dt(2023, 1, 1), dt(2023, 2, 1)),
            (dt(2023, 2, 1), dt(2023, 3, 1)),
        ]

    def test_month_single_window_and_year_wrap(self) -> None:
        assert month_windows(dt(2023, 5, 3), dt(2023, 5, 20)) == [
            (dt(2023, 5, 3), dt(2023, 5, 20))
        ]
        assert month_windows(dt(2022, 12, 20), dt(2023, 1, 5)) == [
            (dt(2022, 12, 20), dt(2023, 1, 1)),
            (dt(2023, 1, 1), dt(2023, 1, 5)),
        ]

    @pytest.mark.parametrize("fn", [month_windows, lambda a, b: equal_windows(a, b, 2)])
    def test_empty_span_rejected(self, fn: t.Callable[..., t.Any]) -> None:
        with pytest.raises(ValueError, match="must be after start"):
            fn(dt(2023, 1, 1), dt(2023, 1, 1))

    def test_fixed_last_partial(self) -> None:
        windows = fixed_windows(dt(2023, 1, 1), dt(2023, 1, 11), timedelta(days=4))
        assert [w[1] for w in windows] == [
            dt(2023, 1, 5),
            dt(2023, 1, 9),
            dt(2023, 1, 11),
        ]

    def test_fixed_longer_than_span(self) -> None:
        assert fixed_windows(dt(2023, 1, 1), dt(2023, 1, 3), timedelta(days=30)) == [
            (dt(2023, 1, 1), dt(2023, 1, 3))
        ]

    def test_fixed_rejects_nonpositive(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            fixed_windows(dt(2023, 1, 1), dt(2023, 1, 3), timedelta(0))

    def test_equal(self) -> None:
        windows = equal_windows(dt(2023, 1, 1), dt(2023, 1, 9), 4)
        assert windows[0][0] == dt(2023, 1, 1)
        assert windows[-1][1] == dt(2023, 1, 9)
        assert {b - a for a, b in windows} == {timedelta(days=2)}

    def test_equal_rejects_bad_counts(self) -> None:
        with pytest.raises(ValueError, match="at least 1"):
            equal_windows(dt(2023, 1, 1), dt(2023, 1, 9), 0)
        with pytest.raises(ValueError, match="non-empty"):
            equal_windows(dt(2023, 1, 1), dt(2023, 1, 1) + timedelta(microseconds=2), 5)


class TestChunkSteps:
    def test_golden_calendar_months_with_external_source(self) -> None:
        windows = month_windows(dt(2023, 1, 15), dt(2023, 4, 20))
        generated = chunk_steps(
            BASE,
            windows,
            prefix="run",
            first_source=StepSource(
                step="seg8@spinup",
                run_id="iceland0_ini",
                timestamp=dt(2023, 1, 15),
            ),
        )

        assert generated.runs == {"spinup": RunRef(run_id="iceland0_ini")}
        steps = generated.steps
        assert [s.name for s in steps] == [f"run-0{i}" for i in range(1, 5)]
        assert steps[0].depends_on == ["seg8@spinup"]
        assert [s.depends_on for s in steps[1:]] == [["run-01"], ["run-02"], ["run-03"]]
        assert steps[0].directives == {
            "continue-from": {"step": "seg8@spinup", "timestamp": "2023-01-15 00:00:00"}
        }
        assert steps[2].directives == {"continue-from": {"step": "run-02"}}
        for step, (_, end) in zip(steps, windows, strict=True):
            assert step.blueprint_overrides == {"runtime_params": {"end_date": end}}
            assert step.blueprint_path == BASE.blueprint_path

        assert_golden("chunks_monthly_external", generated)

    def test_first_source_path_and_cadence_and_walltime(self) -> None:
        windows = fixed_windows(dt(2023, 1, 1), dt(2023, 1, 3), timedelta(days=1))
        base = Step(
            name="run",
            application="roms_marbl",
            blueprint="/path/B.yaml",
            depends_on=["prep"],
            blueprint_overrides={"runtime_params": {"start_date": "x"}, "a": 1},
            compute_overrides={"slurm": {"num_cpus": 8}},
            directives={"continue-from": {"path": "/old"}, "nest-from": {"step": "p"}},
        )
        # `prep` and `p` need not exist: only the generated steps are inspected
        generated = chunk_steps(
            base,
            windows,
            prefix="seg",
            first_source=StepSource(path="/restarts"),
            walltime=lambda a, b: f"{(b - a).days * 12}:00:00",
            restart_cadence={"output_period_rst": 86400.0},
        )
        first, second = generated.steps
        assert generated.runs == {}
        assert first.depends_on == ["prep"]
        assert first.directives["continue-from"] == {"path": "/restarts"}
        assert first.directives["nest-from"] == {"step": "p"}
        assert second.directives["continue-from"] == {"step": "seg-01"}
        assert first.compute_overrides == {
            "slurm": {"num_cpus": 8, "max_walltime": "12:00:00"}
        }
        assert first.blueprint_overrides["a"] == 1
        assert dig(first.blueprint_overrides, "runtime_params", "start_date") == "x"
        assert first.blueprint_overrides["namelist_overrides"] == {
            "basic_output_settings": {"output_period_rst": 86400.0}
        }
        assert base.directives["continue-from"] == {"path": "/old"}
        assert dict(base.blueprint_overrides) == {
            "runtime_params": {"start_date": "x"},
            "a": 1,
        }
        assert base.compute_overrides == {"slurm": {"num_cpus": 8}}

    def test_local_first_source_is_a_dependency(self) -> None:
        generated = chunk_steps(
            BASE,
            [(dt(2023, 1, 1), dt(2023, 1, 2))],
            prefix="x",
            first_source=StepSource(step="spin"),
        )
        assert generated.steps[0].depends_on == ["spin"]

    def test_no_directive_chain(self) -> None:
        base = Step(name="h", application="hello_world", blueprint="/path/h.yaml")
        generated = chunk_steps(
            base,
            equal_windows(dt(2023, 1, 1), dt(2023, 1, 4), 3),
            prefix="h",
            chain_directive=False,
        )
        assert [s.depends_on for s in generated.steps] == [[], ["h-01"], ["h-02"]]
        assert all(not s.directives for s in generated.steps)

    def test_name_padding_widens(self) -> None:
        windows = fixed_windows(dt(2023, 1, 1), dt(2023, 4, 11), timedelta(days=1))
        names = [s.name for s in chunk_steps(BASE, windows, prefix="d").steps]
        assert names[0] == "d-001" and names[-1] == "d-100"

    def test_invalid_inputs(self) -> None:
        window = [(dt(2023, 1, 1), dt(2023, 1, 2))]
        with pytest.raises(ValueError, match="at least one window"):
            chunk_steps(BASE, [], prefix="x")
        with pytest.raises(ValueError, match="prefix"):
            chunk_steps(BASE, window, prefix=" ")
        with pytest.raises(ValueError, match="chain_directive"):
            chunk_steps(
                BASE,
                window,
                prefix="x",
                first_source=StepSource(path="/p"),
                chain_directive=False,
            )

    def test_bad_walltime_is_loud(self) -> None:
        window = [(dt(2023, 1, 1), dt(2023, 1, 2))]
        with pytest.raises(ValueError, match=r"'3:00:00'.*2023-01-01"):
            chunk_steps(BASE, window, prefix="x", walltime=lambda a, b: "3:00:00")
        with pytest.raises(ValueError, match="'soon'"):
            chunk_steps(BASE, window, prefix="x", walltime="soon")

    def test_source_validation(self) -> None:
        with pytest.raises(ValueError, match="exactly one"):
            StepSource()
        with pytest.raises(ValueError, match="exactly one"):
            StepSource(step="a", path="/p")
        with pytest.raises(ValueError, match="run_id"):
            StepSource(step="a", run_id="r")


class TestSpinupRamp:
    def test_pac_shape(self) -> None:
        start = dt(2012, 1, 1)
        ramp = [
            (timedelta(weeks=1), 100.0),
            (timedelta(weeks=1), 200.0),
            (timedelta(weeks=2), 450.0),
        ]
        spin = spinup_ramp(
            BASE,
            ramp,
            start=start,
            prefix="spin",
            first_source=StepSource(path="/ini"),
        )
        main_start = start + timedelta(weeks=4)
        windows = month_windows(main_start, dt(2012, 4, 1))
        main_base = Step(
            name="run",
            application="roms_marbl",
            blueprint="/path/B.yaml",
            blueprint_overrides={
                "namelist_overrides": {"time_stepping": {"dt": 900.0}}
            },
        )
        main = chunk_steps(
            main_base,
            windows,
            prefix="main",
            first_source=StepSource(step=spin.steps[-1].name),
            walltime=lambda a, b: f"{(b - a).days:02d}:00:00",
        )
        generated = Generated(steps=[*spin.steps, *main.steps], runs={})

        assert [s.name for s in spin.steps] == ["spin-01", "spin-02", "spin-03"]
        assert [
            dig(s.blueprint_overrides, "namelist_overrides", "time_stepping", "dt")
            for s in spin.steps
        ] == [100.0, 200.0, 450.0]
        assert [
            dig(s.blueprint_overrides, "runtime_params", "end_date") for s in spin.steps
        ] == [dt(2012, 1, 8), dt(2012, 1, 15), dt(2012, 1, 29)]
        for step in generated.steps:
            assert set(dig(step.blueprint_overrides, "runtime_params")) == {"end_date"}
        assert {
            dig(s.blueprint_overrides, "namelist_overrides", "time_stepping", "dt")
            for s in main.steps
        } == {900.0}
        assert spin.steps[0].directives == {"continue-from": {"path": "/ini"}}
        assert main.steps[0].depends_on == ["spin-03"]
        assert main.steps[0].directives == {"continue-from": {"step": "spin-03"}}
        assert [
            dig(s.compute_overrides, "slurm", "max_walltime") for s in main.steps
        ] == [f"{(b - a).days:02d}:00:00" for a, b in windows]
        assert_golden("pac_ramp_then_months", generated)

    def test_rejects_nonpositive_duration(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            spinup_ramp(BASE, [(timedelta(0), 1.0)], start=dt(2012, 1, 1), prefix="s")


def test_forge_then_run_matches_wizard_export() -> None:
    emitted = EmittedBlueprint(
        filename="B_x.yaml",
        application="roms_marbl",
        cpus_needed=32,
        start_date=dt(2012, 1, 1),
        end_date=dt(2012, 2, 1),
        use_pio=True,
        grid_filename="g.nc",
    )
    generated = forge_then_run(Path("/data/forge.yaml"), emitted)
    forge, run = generated.steps

    assert generated.runs == {}
    assert (forge.name, forge.application) == ("forge", "forge")
    assert forge.blueprint_path == "/data/forge.yaml"
    assert (run.name, run.application, run.depends_on) == (
        "roms_marbl",
        "roms_marbl",
        ["forge"],
    )
    assert run.blueprint_path == DeferredBlueprintRef(
        from_step="forge", filename="B_x.yaml"
    )
    assert run.compute_overrides == {"slurm": {"num_cpus": 32}}

    renamed = forge_then_run(Path("/data/forge.yaml"), emitted, names=("f", "r"))
    assert [s.name for s in renamed.steps] == ["f", "r"]
    assert renamed.steps[1].depends_on == ["f"]


class TestUpscaleChain:
    @staticmethod
    def level(name: str, **kwargs: t.Any) -> Step:
        return Step(
            name=name,
            application="roms_marbl",
            blueprint=f"/path/{name}.yaml",
            **kwargs,
        )

    def test_three_levels(self) -> None:
        inner = self.level("inner", directives={"nest-from": {"step": "middle"}})
        middle = self.level(
            "middle", depends_on=["outer"], workflow_overrides={"clobber": True}
        )
        outer = self.level("outer")
        generated = upscale_chain(
            [inner, middle, outer], use_pio_of=lambda s: s.name == "outer"
        )

        names = [s.name for s in generated.steps]
        assert names == [
            "upscale_inner_middle",
            "middle_up",
            "upscale_middle_outer",
            "outer_up",
        ]
        up1, mid_up, up2, out_up = generated.steps
        assert up1.application == up2.application == "upscaler"
        assert isinstance(up1.blueprint_path, InlineBlueprintRef)
        assert up1.depends_on == ["inner"]
        assert up1.blueprint_overrides == {
            "uscl_file_location": "{{output_dir: inner}}",
            "pio": False,
        }
        # the second upscale reads the re-run parent, not the original
        assert up2.depends_on == ["middle_up"]
        assert up2.blueprint_overrides == {
            "uscl_file_location": "{{output_dir: middle_up}}",
            "pio": True,
        }
        assert mid_up.depends_on == ["outer", "upscale_inner_middle"]
        assert mid_up.workflow_overrides == {"clobber": True}
        assert mid_up.blueprint_path == middle.blueprint_path
        assert dig(mid_up.blueprint_overrides, "cdr_forcing", "data", 0) == {
            "location": "{{output_dir: upscale_inner_middle}}/upscaled_cdr.nc"
        }
        assert out_up.depends_on == ["upscale_middle_outer"]
        assert dig(
            out_up.blueprint_overrides, "cdr_forcing", "data", 0, "location"
        ) == ("{{output_dir: upscale_middle_outer}}/upscaled_cdr.nc")
        assert "cdr_forcing" not in middle.blueprint_overrides
        Workplan(
            name="u",
            description="u",
            steps=[inner, middle, outer, *generated.steps],
        )

    def test_refuses_chunked_level(self) -> None:
        first = self.level("a-01")
        second = Step(
            name="a-02",
            application="roms_marbl",
            blueprint=first.blueprint_path,
            depends_on=["a-01"],
            directives={"continue-from": {"step": "a-01"}},
        )
        with pytest.raises(ValueError, match="not yet supported"):
            upscale_chain(
                [first, second, self.level("outer")], use_pio_of=lambda _: True
            )

    def test_refuses_parent_with_cdr_forcing(self) -> None:
        parent = self.level("outer", blueprint_overrides={"cdr_forcing": {"data": []}})
        with pytest.raises(ValueError, match="not yet supported"):
            upscale_chain([self.level("inner"), parent], use_pio_of=lambda _: True)

    def test_refuses_too_few_levels_and_other_applications(self) -> None:
        with pytest.raises(ValueError, match="at least two"):
            upscale_chain([self.level("only")], use_pio_of=lambda _: True)
        other = Step(name="o", application="nest_ic", blueprint="inline")
        with pytest.raises(ValueError, match="not yet supported"):
            upscale_chain([self.level("inner"), other], use_pio_of=lambda _: True)

    def test_name_collision_is_loud(self) -> None:
        with pytest.raises(ValueError, match="collide"):
            upscale_chain(
                [self.level("inner"), self.level("outer"), self.level("outer_up")],
                use_pio_of=lambda _: True,
            )


class TestNormalizeLegacy:
    @pytest.fixture
    def workplan(self, tmp_path: Path) -> tuple[Workplan, Path]:
        run_root = tmp_path / "run"
        blueprint = tmp_path / "nest.yaml"
        blueprint.write_text(
            yaml.safe_dump(
                {
                    "name": "nest",
                    "application": "nest_ic",
                    "parent_grid": "/g.nc",
                    "parent_rst": "/r.nc",
                    "child_grid": "/c.nc",
                    "pio": True,
                }
            )
        )
        nest_conversion = Step(
            name="nest_conversion_Iceland0_Iceland1",
            application="nest_ic",
            blueprint=str(blueprint),
            blueprint_overrides={
                "parent_grid": "/g2.nc",
                "parent_rst": "/legacy/joined_output/output_rst.20230101000000.nc",
                "child_grid": "/c.nc",
                "pio": False,
            },
        )
        spin_up = Step(
            name="spin-up",
            application="roms_marbl",
            blueprint="/path/B.yaml",
            directives={
                "nest-from": {
                    "rst_path": str(
                        run_root / "tasks" / "nest-conversion-iceland0-iceland1"
                    ),
                    "bry_path": "/elsewhere/segment1/joined_output/",
                }
            },
        )
        segment1 = Step(
            name="segment1",
            application="roms_marbl",
            blueprint="/path/B.yaml",
            directives={
                "nest-from": {
                    "rst_path": "{{root_dir: spin-up}}/output",
                    "bry_path": "/elsewhere/joined_output/",
                },
                "continue-from": {
                    "path": str(
                        run_root
                        / "tasks"
                        / "spin-up"
                        / "output"
                        / "output_rst.20230215000000.nc"
                    )
                },
            },
        )
        wp = Workplan(
            name="legacy",
            description="legacy",
            steps=[nest_conversion, spin_up, segment1],
        )
        return wp, run_root

    def test_rewrites(self, workplan: tuple[Workplan, Path]) -> None:
        wp, run_root = workplan
        before = wp.model_dump()

        normalized, changes = normalize_legacy(wp, run_root=run_root)

        assert wp.model_dump() == before
        assert normalized is not wp
        nest_conv, spin, seg = normalized.steps

        assert isinstance(nest_conv.blueprint_path, InlineBlueprintRef)
        assert nest_conv.blueprint_overrides["parent_rst"] == (
            "/legacy/output/output_rst.20230101000000.nc"
        )

        assert spin.directives == {
            "nest-from": {"path": "/elsewhere/segment1/output/"},
            "continue-from": {"step": "nest_conversion_Iceland0_Iceland1"},
        }
        assert spin.depends_on == ["nest_conversion_Iceland0_Iceland1"]

        # rst_path conflicts with the existing continue-from: left in place
        assert seg.directives["nest-from"] == {
            "rst_path": "{{root_dir: spin-up}}/output",
            "path": "/elsewhere/output/",
        }
        assert seg.directives["continue-from"] == {
            "step": "spin-up",
            "timestamp": dt(2023, 2, 15).strftime(TIMESTAMP_FMT),
        }
        assert seg.depends_on == ["spin-up"]

        by_step: dict[str, list[tuple[str, str]]] = {}
        for c in changes:
            by_step.setdefault(c.step, []).append((c.field, c.reason))
        fields = {k: [f for f, _ in v] for k, v in by_step.items()}
        assert fields["nest_conversion_Iceland0_Iceland1"] == [
            "blueprint_overrides.parent_rst",
            "blueprint",
        ]
        assert fields["spin-up"] == [
            "directives.nest-from.rst_path",
            "directives.nest-from.bry_path",
            "directives.nest-from.path",
            "directives.continue-from.path",
            "depends_on",
        ]
        assert fields["segment1"] == [
            "directives.nest-from.rst_path",
            "directives.nest-from.bry_path",
            "directives.nest-from.path",
            "directives.continue-from.path",
            "depends_on",
        ]
        conflict = by_step["segment1"][0]
        assert conflict[1].startswith("conflict: continue-from already present")
        assert (
            Workplan.model_validate(normalized.model_dump(by_alias=True)) == normalized
        )

    def test_without_run_root_keeps_paths(
        self, workplan: tuple[Workplan, Path]
    ) -> None:
        wp, _ = workplan
        normalized, changes = normalize_legacy(wp)
        spin = normalized.steps[1]
        assert "path" in dig(spin.directives, "continue-from")
        assert not any(c.field == "depends_on" for c in changes)

    def test_nest_path_tokens_merge_into_steps(self, tmp_path: Path) -> None:
        run_root = tmp_path / "r"
        a = Step(name="a", application="roms_marbl", blueprint="/b.yaml")
        b = Step(name="B b", application="roms_marbl", blueprint="/b.yaml")
        c = Step(
            name="c",
            application="roms_marbl",
            blueprint="/b.yaml",
            directives={
                "nest-from": {
                    "path": f"{run_root}/tasks/a/output; {run_root}/tasks/b-b/output"
                }
            },
        )
        wp = Workplan(name="w", description="w", steps=[a, b, c])
        normalized, _ = normalize_legacy(wp, run_root=run_root)
        assert normalized.steps[2].directives == {"nest-from": {"step": "a;B b"}}
        assert normalized.steps[2].depends_on == ["a", "B b"]

        mixed = c.model_copy(
            update={"directives": {"nest-from": {"path": f"{run_root}/tasks/a;/x"}}}
        )
        wp = Workplan(name="w", description="w", steps=[a, b, mixed])
        normalized, changes = normalize_legacy(wp, run_root=run_root)
        assert normalized.steps[2].directives == mixed.directives
        assert changes == []

    def test_missing_blueprint_file_and_partial_overrides_not_inlined(
        self, tmp_path: Path
    ) -> None:
        file = tmp_path / "n.yaml"
        file.write_text(
            yaml.safe_dump({"application": "nest_ic", "parent_rst": "/r", "pio": True})
        )
        partial = Step(
            name="partial",
            application="nest_ic",
            blueprint=str(file),
            blueprint_overrides={"parent_rst": "/r"},
        )
        missing = Step(
            name="missing",
            application="nest_ic",
            blueprint=str(tmp_path / "gone.yaml"),
            blueprint_overrides={"parent_rst": "/r"},
        )
        wp = Workplan(name="w", description="w", steps=[partial, missing])
        normalized, changes = normalize_legacy(wp)
        assert changes == []
        assert not any(s.is_inline for s in normalized.steps)


class TestRestarts:
    def test_available_restarts(self, tmp_path: Path) -> None:
        out = tmp_path / "output"
        out.mkdir()
        (out / "output_rst.20230101000000.nc").touch()
        (out / "output_rst.20230102000000.000.nc").touch()
        (out / "output_rst.20230102000000.001.nc").touch()
        (out / "output_rst.20230101000000.001.nc").touch()
        (out / "output_his.20230103000000.nc").touch()
        assert available_restarts(out) == [dt(2023, 1, 1), dt(2023, 1, 2)]

    def test_available_restarts_missing_dir(self, tmp_path: Path) -> None:
        assert available_restarts(tmp_path / "nope") == []

    def test_predicted_restarts(self) -> None:
        windows = month_windows(dt(2023, 1, 1), dt(2023, 3, 1))
        assert predicted_restarts(windows, None) == [dt(2023, 2, 1), dt(2023, 3, 1)]

        weekly = predicted_restarts(
            [(dt(2023, 1, 1), dt(2023, 1, 20))],
            {"output_period_rst": 7 * 86400.0, "monthly_restarts": False},
        )
        assert weekly == [dt(2023, 1, 8), dt(2023, 1, 15), dt(2023, 1, 20)]

        monthly = predicted_restarts(
            [(dt(2023, 1, 1), dt(2023, 1, 20))],
            {"output_period_rst": 7 * 86400.0, "monthly_restarts": True},
        )
        assert monthly == [dt(2023, 1, 20)]
