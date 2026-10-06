"""Tests for the wizard's Workplan page (cstar.wizard.workplan_builder)."""

from __future__ import annotations

import pytest

pytest.importorskip("ipywidgets")

import shutil
from datetime import datetime
from pathlib import Path

import yaml

from cstar.orchestration.models import Step, Workplan
from cstar.orchestration.serialization import deserialize, serialize
from cstar.wizard import workplan_builder as wb
from cstar.wizard.ui import labels
from cstar.wizard.wizard import ForgeBlueprintWizardApp
from cstar.wizard.workplan_builder import WorkplanBuilderPage, _StepPane

_BP_TEMPLATE = (
    Path(wb.__file__).parents[1]
    / "additional_files"
    / "templates"
    / "bp"
    / "roms_marbl"
    / "blueprint.3.0.0.yaml"
)
_FORGE_BP = "wio-toy-simple"


@pytest.fixture(scope="module")
def bp_app(tmp_path_factory: pytest.TempPathFactory) -> ForgeBlueprintWizardApp:
    """A Blueprint page over a throwaway writable catalog (construction takes seconds)."""
    return ForgeBlueprintWizardApp(str(tmp_path_factory.mktemp("catalog")))


@pytest.fixture(autouse=True)
def _no_live_system(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the page independent of the machine the tests run on."""
    monkeypatch.setattr(wb, "live_system_name", lambda: "")


@pytest.fixture
def page(bp_app: ForgeBlueprintWizardApp) -> WorkplanBuilderPage:
    """A fresh Workplan page per test, so no state leaks between tests."""
    return WorkplanBuilderPage(bp_app)


@pytest.fixture
def roms_bp(tmp_path: Path) -> Path:
    """A readable roms_marbl blueprint (128 CPUs, 2020-01-01 start, ucla-roms ``main``)."""
    path = tmp_path / "roms.yaml"
    shutil.copy(_BP_TEMPLATE, path)
    return path


def _find_card(root, key):
    if getattr(root, "forge_key", None) == key:
        return root
    for c in getattr(root, "children", []):
        found = _find_card(c, key)
        if found is not None:
            return found
    return None


def _forge_keys(root) -> set[str]:
    keys: set[str] = set()
    if (key := getattr(root, "forge_key", None)) is not None:
        keys.add(key)
    for c in getattr(root, "children", []):
        keys |= _forge_keys(c)
    return keys


def _values(widget) -> list:
    """The option values of a selection widget."""
    return [o[1] if isinstance(o, tuple) else o for o in widget.options]


def _texts(widget) -> str:
    """The HTML values of ``widget`` and everything under it, joined."""
    parts = [widget.value] if isinstance(getattr(widget, "value", None), str) else []
    parts += [_texts(c) for c in getattr(widget, "children", ())]
    return " ".join(parts)


def _name_page(page: WorkplanBuilderPage, name: str = "demo") -> None:
    page.name.value = name
    page.description.value = f"{name} description"


def _path_step(pane: _StepPane, name: str, path: Path) -> None:
    pane.name.value = name
    pane.source.value = wb.SOURCE_PATH
    pane.path.value = str(path)


# ---------------------------------------------------------------------------
# structure and labels
# ---------------------------------------------------------------------------
def test_page_builds_with_every_card(page):
    root = page.widget
    assert {"forge-app", "forge-workplan"} <= set(root._dom_classes)
    for key in ("start", "workplan", "compute", "steps", "recipes", "review"):
        assert _find_card(root, key) is not None, key
    assert page.blueprint_app is not None


def test_glossary_loads_and_covers_the_tree(page):
    glossary = labels.glossary(wb.PAGE)
    assert glossary["sections"] and glossary["fields"]
    keys = _forge_keys(page.widget)
    cards = {k for k in keys if k in labels.known_sections(wb.PAGE)}
    assert {"start", "workplan", "compute", "steps", "recipes", "review"} <= cards
    unknown = keys - labels.known_keys(wb.PAGE) - labels.known_sections(wb.PAGE)
    assert not unknown, f"field rows without a glossary entry: {sorted(unknown)}"


def test_every_bare_field_key_is_a_page_attribute(page):
    bare = [k for k in labels.known_keys(wb.PAGE) if "." not in k]
    assert bare
    for key in bare:
        assert hasattr(page, key), f"WorkplanBuilderPage has no attribute {key!r}"


def test_every_step_field_key_is_a_pane_attribute(page):
    keys = [
        k.split(".", 1)[1] for k in labels.known_keys(wb.PAGE) if k.startswith("step.")
    ]
    assert keys
    pane = page.panes[0]
    for key in keys:
        assert hasattr(pane, key), f"_StepPane has no attribute {key!r}"


def test_button_captions_come_from_the_glossary(page):
    assert page.add_step_btn.description == "Add step"
    assert "buttons.add_step" in labels.known_keys(wb.PAGE)


# ---------------------------------------------------------------------------
# gathering
# ---------------------------------------------------------------------------
def test_two_steps_gather_and_round_trip(page, roms_bp, tmp_path):
    _name_page(page)
    _path_step(page.panes[0], "first", roms_bp)
    second = page.add_step("second")
    _path_step(second, "second", roms_bp)
    second.depends_on.value = ("first",)
    second.end_date.value = "2020-02-01"

    assert page.problems == []
    draft = page.draft
    assert draft is not None
    assert [s.name for s in draft.steps] == ["first", "second"]
    assert draft.steps[1].depends_on == ["first"]
    assert draft.steps[1].blueprint_overrides["runtime_params"]["end_date"].month == 2

    out = tmp_path / "out.yaml"
    serialize(out, draft)
    assert deserialize(out, Workplan).model_dump() == draft.model_dump()
    assert wb.yaml_text(draft).startswith(
        f"# yaml-language-server: $schema={wb.WORKPLAN_SCHEMA_REF}\n"
    )


def test_pane_prefills_cpus_and_shows_the_blueprint_window(page, roms_bp):
    pane = page.panes[0]
    _path_step(pane, "run", roms_bp)
    assert pane.num_cpus.value == 128
    assert pane.application.value == "roms_marbl"
    assert pane.application.disabled  # derived from the file
    assert "2020-01-01" in pane.start_note.value
    assert "start_date" not in str(pane.gather().blueprint_overrides)


def test_invalid_draft_lists_problems(page):
    assert page.draft is None
    assert any("description" in p for p in page.problems)
    pane = page.panes[0]
    pane.name.value = "a"
    pane.source.value = wb.SOURCE_PATH
    pane.path.value = ""
    assert any("choose a blueprint" in p for p in page.problems)
    assert "problem" in pane.status.value


def test_namelist_override_row_is_typed_and_lossless(page, roms_bp):
    _name_page(page)
    pane = page.panes[0]
    _path_step(pane, "run", roms_bp)
    row = pane._add_row()
    row.section.value = "time_stepping"
    row.field.value = "dt"
    row.holder.children[0].value = 90.0
    assert page.draft is not None
    assert page.draft.steps[0].blueprint_overrides["namelist_overrides"] == {
        "time_stepping": {"dt": 90.0}
    }
    # required namelist fields (no default) build widgets without error
    row.field.value = "ntimes"
    assert row.value() == ("time_stepping", "ntimes", 0)


def test_raw_overrides_merge_last_and_must_be_a_mapping(page, roms_bp):
    _name_page(page)
    pane = page.panes[0]
    _path_step(pane, "run", roms_bp)
    pane.raw_overrides.value = "partitioning:\n  n_cores: 4\n"
    assert page.draft.steps[0].blueprint_overrides == {"partitioning": {"n_cores": 4}}
    pane.raw_overrides.value = "- not a mapping"
    assert page.draft is None
    assert any("YAML mapping" in p for p in page.problems)


def test_directives_continue_from_step_path_and_nest_from(page, roms_bp, tmp_path):
    _name_page(page)
    _path_step(page.panes[0], "a", roms_bp)
    b = page.add_step("b")
    _path_step(b, "b", roms_bp)
    b.cont_kind.value = "step"
    b.cont_step.value = "a"
    b.cont_timestamp.value = "2020-01-15"
    b.nest_kind.value = "path"
    b._add_nest_row("path", "/data/one")
    b._add_nest_row("path", "/data/two")
    step = page.draft.steps[1]
    assert step.directives["continue-from"] == {
        "step": "a",
        "timestamp": "2020-01-15 00:00:00",
    }
    assert step.directives["nest-from"] == {"path": "/data/one;/data/two"}
    assert step.depends_on == ["a"]  # implied by the directive, added for you
    assert "a" in b.locked_deps.value


def test_restart_picker_lists_the_restarts_at_a_path(page, roms_bp, tmp_path):
    out = tmp_path / "out"
    out.mkdir()
    (out / "ocean_rst.20200115000000.nc").write_bytes(b"")
    (out / "ocean_rst.20200201000000.nc").write_bytes(b"")
    _name_page(page)
    pane = page.panes[0]
    _path_step(pane, "run", roms_bp)
    pane.cont_kind.value = "path"
    pane.cont_path.value = str(out)
    assert _values(pane.cont_pick) == [
        "",
        "2020-01-15 00:00:00",
        "2020-02-01 00:00:00",
    ]
    assert pane.cont_pick.layout.display != "none"
    pane.cont_pick.value = "2020-02-01 00:00:00"
    cont = page.draft.steps[0].directives["continue-from"]
    assert cont == {"path": str(out), "timestamp": "2020-02-01 00:00:00"}


def test_renaming_a_step_follows_references(page, roms_bp):
    _name_page(page)
    _path_step(page.panes[0], "a", roms_bp)
    b = page.add_step("b")
    _path_step(b, "b", roms_bp)
    b.depends_on.value = ("a",)
    page.panes[0].name.value = "alpha"
    assert page.draft.steps[1].depends_on == ["alpha"]


def test_step_buttons_reorder_duplicate_and_delete(page, roms_bp):
    _name_page(page)
    _path_step(page.panes[0], "a", roms_bp)
    b = page.add_step("b")
    _path_step(b, "b", roms_bp)
    page._move(b, -1)
    assert [s.name for s in page.draft.steps] == ["b", "a"]
    page._duplicate(b)
    assert [p.name.value for p in page.panes] == ["b", "b-copy", "a"]
    page._delete(page.panes[1])
    assert [p.name.value for p in page.panes] == ["b", "a"]


# ---------------------------------------------------------------------------
# blueprint sources
# ---------------------------------------------------------------------------
def test_deferred_step_is_prefilled_from_the_forge_hook(page):
    _name_page(page)
    producer = page.panes[0]
    producer.name.value = "forge"
    producer.source.value = wb.SOURCE_CATALOG_FORGE
    producer.catalog_forge.value = _FORGE_BP
    assert producer.application.value == "forge"

    run = page.add_step("run")
    run.source.value = wb.SOURCE_DEFERRED
    run.producer.value = "forge"
    assert run.filename.value == f"B_{_FORGE_BP}.yaml"
    assert run.num_cpus.value == 10
    assert run.application.value == "roms_marbl"
    step = page.draft.steps[1]
    assert step.is_deferred
    assert step.blueprint_path.filename == f"B_{_FORGE_BP}.yaml"
    assert step.depends_on == ["forge"]
    assert step.compute_overrides == {"slurm": {"num_cpus": 10}}


def test_deferred_step_without_cpus_is_flagged(page):
    _name_page(page)
    run = page.panes[0]
    run.name.value = "run"
    run.source.value = wb.SOURCE_DEFERRED
    other = page.add_step("other")
    other.name.value = "other"
    run.producer.value = "other"
    assert any("num_cpus" in p for p in page.problems)


def test_inline_nest_ic_form_marks_required_fields(page):
    pane = page.panes[0]
    pane.source.value = wb.SOURCE_INLINE
    assert _values(pane.application) == ["nest_ic", "upscaler", "hello_world"]
    pane.application.value = "nest_ic"
    names = {w.description for w, _b, _p in pane._form.values()}
    assert {"parent_rst *", "parent_grid *", "child_grid *"} <= names
    assert "pio" in names  # optional, no star
    assert sorted(pane.required_form_fields) == [
        "child_grid",
        "parent_grid",
        "parent_rst",
    ]
    assert any("parent_rst" in p for p in pane.problems())


def test_inline_form_writes_touched_and_required_fields_with_step_placeholders(page):
    _name_page(page)
    producer = page.panes[0]
    producer.name.value = "outer"
    producer.source.value = wb.SOURCE_INLINE
    producer.application.value = "hello_world"
    producer._form["target"][0].value = "world"
    nest = page.add_step("nest")
    nest.source.value = wb.SOURCE_INLINE
    nest.application.value = "nest_ic"
    widget, _base, picker = nest._form["parent_grid"]
    assert "output_dir: outer" in [label for label, _v in picker.options[1:]]
    picker.value = "{{output_dir: outer}}"
    assert widget.value == "{{output_dir: outer}}"
    nest._form["parent_rst"][0].value = "/data/rst.nc"
    nest._form["child_grid"][0].value = "/data/child.nc"
    steps = {s.name: s for s in page.draft.steps}
    overrides = steps["nest"].blueprint_overrides
    assert overrides["parent_grid"] == "{{output_dir: outer}}"
    assert "pio" not in overrides  # untouched optional field keeps its default
    assert steps["nest"].depends_on == ["outer"]  # implied by the placeholder
    assert steps["outer"].blueprint_overrides == {"target": "world"}
    assert steps["nest"].is_inline


def test_forge_step_offers_no_override_form(page):
    pane = page.panes[0]
    pane.source.value = wb.SOURCE_CATALOG_FORGE
    pane.catalog_forge.value = _FORGE_BP
    assert pane.form_box.layout.display == "none"
    assert pane.directive_note.value == ""
    assert pane._directives_section.layout.display == "none"


def test_current_blueprint_page_config_saves_and_uses_that_file(
    page, bp_app, monkeypatch
):
    monkeypatch.setattr(bp_app.inner, "_ensure_boundaries_derived", lambda: True)
    pane = page.panes[0]
    assert bp_app.inner.config is not None  # the default blueprint is valid
    pane.source.value = wb.SOURCE_CURRENT
    path = Path(pane._current_path)
    assert path == Path(bp_app.inner.save_path.value)
    assert path.exists()
    assert pane.application.value == "forge"


def test_upload_source_stages_the_file(page, roms_bp):
    pane = page.panes[0]
    pane.source.value = wb.SOURCE_UPLOAD
    pane.upload.value = (
        {
            "name": "up.yaml",
            "type": "",
            "size": 1,
            "content": roms_bp.read_bytes(),
            "last_modified": datetime(2026, 1, 1),
        },
    )
    assert Path(pane._uploaded_path).name == "up.yaml"
    assert Path(pane._uploaded_path).read_bytes() == roms_bp.read_bytes()
    assert pane.application.value == "roms_marbl"


# ---------------------------------------------------------------------------
# compute target
# ---------------------------------------------------------------------------
def test_compute_target_writes_the_block_and_omits_it_when_unspecified(page, roms_bp):
    _name_page(page)
    _path_step(page.panes[0], "run", roms_bp)
    assert page.compute_target.value == wb.TARGET_NONE
    assert page.draft.compute_environment == {}

    page.compute_target.value = wb.TARGET_SLURM
    page.machine.value = "anvil"
    assert _values(page.queue)[1:] == ["wholenode", "shared", "debug"]
    page.queue.value = "wholenode"
    page.account.value = "x-abc"
    page.walltime.value = "04:00:00"
    assert page.draft.compute_environment == {
        "launcher": "slurm",
        "system": "anvil",
        "slurm": {
            "max_walltime": "04:00:00",
            "queue_name": "wholenode",
            "account_name": "x-abc",
        },
    }

    page.compute_target.value = wb.TARGET_LOCAL
    assert page.draft.compute_environment == {"launcher": "local"}
    page.compute_target.value = wb.TARGET_NONE
    assert page.draft.compute_environment == {}


def test_bad_walltime_is_a_problem(page, roms_bp):
    _name_page(page)
    _path_step(page.panes[0], "run", roms_bp)
    page.compute_target.value = wb.TARGET_SLURM
    page.machine.value = "anvil"
    page.walltime.value = "soon"
    assert any(p.startswith("compute:") for p in page.problems)


def test_custom_machine_takes_free_text_and_cpus_per_node(page, roms_bp):
    _name_page(page)
    _path_step(page.panes[0], "run", roms_bp)
    page.compute_target.value = wb.TARGET_SLURM
    page.machine.value = wb.MACHINE_CUSTOM
    assert page.queue_text.layout.display != "none"
    assert page.queue.layout.display == "none"
    page.queue_text.value = "batch"
    page.cpus_per_node.value = 64
    assert page.draft.compute_environment == {
        "launcher": "slurm",
        "slurm": {"queue_name": "batch", "cpus_per_node": 64},
    }


def test_pbs_machines_are_not_offered(page):
    assert "derecho" not in _values(page.machine)
    assert "derecho" in page.machine_note.value


def test_live_system_is_preselected(bp_app, monkeypatch):
    monkeypatch.setattr(wb, "live_system_name", lambda: "anvil")
    page = WorkplanBuilderPage(bp_app)
    assert page.compute_target.value == wb.TARGET_SLURM
    assert page.machine.value == "anvil"


# ---------------------------------------------------------------------------
# runs table
# ---------------------------------------------------------------------------
def test_runs_table_declares_aliases_and_offers_their_steps(page, roms_bp):
    _name_page(page)
    _path_step(page.panes[0], "run", roms_bp)
    page._add_run()
    row = page.run_rows[0]
    row.alias.value = "ini"
    row.run_id.value = "{{ini_run}}"
    row.steps.value = "create-ic"
    assert page.draft.runs["ini"].run_id == "{{ini_run}}"
    assert _values(page.panes[0].depends_on)[-1] == "create-ic@ini"
    page.panes[0].depends_on.value = ("create-ic@ini",)
    assert page.draft.steps[0].depends_on == ["create-ic@ini"]
    row.alias.value = "spin"  # renaming follows the references
    assert page.draft.steps[0].depends_on == ["create-ic@spin"]
    page._remove_run(row)
    assert page.draft is None  # the dependency now names an undeclared alias


def test_refresh_runs_lists_records_newest_first_and_loads_steps(
    page, roms_bp, tmp_path, monkeypatch
):
    monkeypatch.setenv("CSTAR_DATA_HOME", str(tmp_path / "data"))
    recorded = tmp_path / "recorded.yaml"
    recorded.write_text(
        yaml.safe_dump(
            {
                "name": "old",
                "description": "an earlier run",
                "steps": [
                    {
                        "name": "create-ic",
                        "application": "roms_marbl",
                        "blueprint": str(roms_bp),
                    }
                ],
            }
        )
    )

    class _Run:
        def __init__(self, run_id: str, start_at: datetime) -> None:
            self.run_id, self.start_at = run_id, start_at
            self.trx_workplan_path = recorded

    runs = [_Run("old", datetime(2026, 1, 1)), _Run("new", datetime(2026, 6, 1))]

    async def _list(self, run_id_filter: str = ""):
        return runs

    from cstar.orchestration.tracking import TrackingRepository

    monkeypatch.setattr(TrackingRepository, "list_latest_runs", _list)

    _name_page(page)
    _path_step(page.panes[0], "run", roms_bp)
    page._add_run()
    row = page.run_rows[0]
    row.alias.value = "ini"
    page.refresh_runs()
    assert _values(row.pick) == ["", "new", "old"]

    row.pick.value = "old"
    assert row.run_id.value == "old"
    assert page.run_steps["ini"] == ["create-ic"]
    assert "not finished" in row.status.value  # no sentinel: not known to be Done
    assert page.draft.runs["ini"].run_id == "old"


# ---------------------------------------------------------------------------
# loading and saving
# ---------------------------------------------------------------------------
@pytest.fixture
def legacy_file(roms_bp, tmp_path) -> Path:
    """A workplan written with the deprecated spellings the normalizer rewrites."""
    path = tmp_path / "legacy.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "name": "legacy",
                "description": "legacy plan",
                "steps": [
                    {
                        "name": "parent",
                        "application": "roms_marbl",
                        "blueprint": str(roms_bp),
                        "blueprint_overrides": {
                            "runtime_params": {"end_date": datetime(2020, 2, 1)},
                            "namelist_overrides": {"time_stepping": {"dt": 100.0}},
                            "partitioning": {"n_cores": 4},
                        },
                    },
                    {
                        "name": "child",
                        "application": "roms_marbl",
                        "blueprint": str(roms_bp),
                        "depends_on": ["parent"],
                        "directives": {
                            "nest-from": {
                                "rst_path": "/data/run/output_rst.20200101000000.nc",
                                "bry_path": "/data/run/joined_output",
                            }
                        },
                        "compute_overrides": {"slurm": {"num_cpus": 8, "num_nodes": 1}},
                        "workflow_overrides": {"clobber": True},
                    },
                ],
            }
        )
    )
    return path


def test_loading_a_legacy_file_populates_panes_and_reports_changes(page, legacy_file):
    before = legacy_file.read_bytes()
    page._load_from_path(str(legacy_file))

    assert [p.name.value for p in page.panes] == ["parent", "child"]
    assert page.name.value == "legacy"
    fields = {c.field for c in page.changes}
    assert any("rst_path" in f for f in fields)
    assert any("bry_path" in f for f in fields)
    rows = _texts(page.changes_box)
    assert "rst_path" in rows and "dropped" in rows  # the clobber key is reported

    child = page.panes[1]
    assert child.cont_kind.value == "path"
    assert child.cont_path.value == "/data/run/output_rst.20200101000000.nc"
    assert child.nest_kind.value == "path"
    assert (
        child.nest_rows[0].widget.value == "/data/run/output"
    )  # joined_output -> output
    assert child.num_cpus.value == 8
    assert page.panes[0].end_date.value == "2020-02-01 00:00:00"
    assert page.panes[0]._rows[0].value() == ("time_stepping", "dt", 100.0)

    # populate -> gather is lossless (modulo the run-entry keys the page never exposes)
    from cstar.orchestration.patterns import normalize_legacy

    normalized, _ = normalize_legacy(deserialize(legacy_file, Workplan), run_root=None)
    expected = normalized.model_dump()
    for step in expected["steps"]:
        step["workflow_overrides"] = {}
    assert page.problems == []
    assert page.draft.model_dump() == expected

    assert legacy_file.read_bytes() == before  # loading never writes the source


def test_save_writes_a_new_file_and_leaves_the_source_alone(
    page, legacy_file, tmp_path
):
    page._load_from_path(str(legacy_file))
    before = legacy_file.read_bytes()
    default = Path(page.save_path.value)
    assert default != legacy_file
    assert default.parent == Path(page.catalog.workplans_dir)

    page.save_path.value = str(tmp_path / "copy.yaml")
    page._on_save()
    assert (tmp_path / "copy.yaml").exists()
    assert deserialize(tmp_path / "copy.yaml", Workplan).name == "legacy"
    assert (tmp_path / "copy.yaml").read_text() == wb.yaml_text(page.draft)
    assert legacy_file.read_bytes() == before
    assert page.save_btn.description == "Save"


def test_saving_onto_the_loaded_file_needs_a_confirm_click(page, legacy_file):
    page._load_from_path(str(legacy_file))
    before = legacy_file.read_bytes()
    page.save_path.value = str(legacy_file)

    page._on_save()  # first click arms
    assert page.save_btn.description == "Confirm overwrite"
    assert page.save_btn.button_style == "danger"
    assert legacy_file.read_bytes() == before

    page._on_save()  # second click writes
    assert legacy_file.read_bytes() != before
    assert page.save_btn.description == "Save"
    assert legacy_file.read_text().startswith("# yaml-language-server")


def test_editing_the_save_path_disarms_the_confirm(page, legacy_file, tmp_path):
    page._load_from_path(str(legacy_file))
    page.save_path.value = str(legacy_file)
    page._on_save()
    page.save_path.value = str(tmp_path / "other.yaml")
    assert page.save_btn.description == "Save"


def test_saved_workplans_are_listed_and_loadable(page, roms_bp):
    _name_page(page, "listed")
    _path_step(page.panes[0], "run", roms_bp)
    target = Path(page.save_path.value)
    assert target.name == "listed.yaml"
    page._on_save()
    assert str(target) in _values(page.load_dd)
    page._new()
    assert page.name.value == ""
    page.load_dd.value = str(target)
    page._load_saved()
    assert page.name.value == "listed"
    assert page.panes[0].name.value == "run"


def test_load_errors_go_to_the_status_line(page, tmp_path):
    page._load_from_path(str(tmp_path / "missing.yaml"))
    assert "forge-msg-err" in page.load_status.value
    bad = tmp_path / "bad.yaml"
    bad.write_text("name: x\n")
    page._load_from_path(str(bad))
    assert "forge-msg-err" in page.load_status.value


def test_upload_loads_a_workplan(page, legacy_file):
    page.upload.value = (
        {
            "name": "legacy.yaml",
            "type": "",
            "size": 1,
            "content": legacy_file.read_bytes(),
            "last_modified": datetime(2026, 1, 1),
        },
    )
    page.upload_btn.click()
    assert page.name.value == "legacy"
    assert page.loaded_from is None
    # an upload has no source path, so any save path is a new file
    page.save_path.value = str(legacy_file)
    page._on_save()
    assert page.save_btn.description == "Save"


def test_unmodelled_keys_survive_populate_and_gather(page, roms_bp, tmp_path):
    path = tmp_path / "plan.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "name": "keep",
                "description": "keeps what it cannot show",
                "compute_environment": {
                    "launcher": "slurm",
                    "system": "anvil",
                    "slurm": {"account_name": "x", "num_nodes": 2},
                },
                "runs": {"r": {"run_id": "run-a"}},
                "steps": [
                    {
                        "name": "s",
                        "application": "roms_marbl",
                        "blueprint": str(roms_bp),
                        "blueprint_overrides": {
                            "runtime_params": {"start_date": "2020-01-02 00:00:00"},
                            "namelist_overrides": {"nonsense": {"x": 1}},
                        },
                        "compute_overrides": {
                            "slurm": {"single_node": True},
                            "local": {"force_kill_timeout": 5},
                        },
                        "directives": {"continue-from": {"step": "x@r", "extra": 1}},
                        "depends_on": ["x@r"],
                    }
                ],
            }
        )
    )
    original = deserialize(path, Workplan)
    page.load_workplan(original, path)
    assert page.problems == []
    assert page.draft.model_dump() == original.model_dump()


# ---------------------------------------------------------------------------
# catalog edge cases (stub catalogs; the page reads the catalog on demand)
# ---------------------------------------------------------------------------
class _StubCatalog:
    def __init__(self, root: Path, *, read_only: bool = False) -> None:
        self.root = root
        self.read_only = read_only
        self.workplans_dir = root / "workplans"
        self.roms_marbl_blueprint_names = ["one", "many"]
        self.forge_blueprint_names: list[str] = []

    def roms_marbl_blueprint_path(self, name: str) -> Path:
        return self.root / name


def _stub_page(catalog: _StubCatalog) -> WorkplanBuilderPage:
    from types import SimpleNamespace

    app = SimpleNamespace(inner=SimpleNamespace(catalog=catalog, config=None))
    return WorkplanBuilderPage(app)


def test_catalog_blueprint_directory_resolves_to_its_roms_marbl_file(tmp_path, roms_bp):
    (tmp_path / "one").mkdir()
    shutil.copy(roms_bp, tmp_path / "one" / "bp.yaml")
    (tmp_path / "one" / "notes.yaml").write_text("application: forge\n")
    (tmp_path / "many").mkdir()
    shutil.copy(roms_bp, tmp_path / "many" / "a.yaml")
    shutil.copy(roms_bp, tmp_path / "many" / "b.yaml")

    page = _stub_page(_StubCatalog(tmp_path))
    pane = page.panes[0]
    assert pane.source.value == wb.SOURCE_CATALOG_ROMS  # the catalog has some
    pane.catalog_roms.value = "one"
    assert pane.resolve() == (str(tmp_path / "one" / "bp.yaml"), [])
    assert pane.num_cpus.value == 128
    pane.catalog_roms.value = "many"
    path, problems = pane.resolve()
    assert path == "" and "2 roms_marbl blueprint files" in problems[0]


def test_catalog_flat_blueprint_file_resolves_to_itself(tmp_path, roms_bp):
    """A ``blueprints/B_<name>.yaml`` entry maps to the file, not a directory."""
    flat = tmp_path / "B_flat.yaml"
    shutil.copy(roms_bp, flat)

    class _FlatCatalog(_StubCatalog):
        def __init__(self, root: Path) -> None:
            super().__init__(root)
            self.roms_marbl_blueprint_names = ["flat"]

        def roms_marbl_blueprint_path(self, name: str) -> Path:
            return flat

    page = _stub_page(_FlatCatalog(tmp_path))
    pane = page.panes[0]
    pane.catalog_roms.value = "flat"
    assert pane.resolve() == (str(flat), [])
    assert page.roms_blueprint_file("flat") == (str(flat), [])


def test_read_only_catalog_saves_next_to_the_loaded_file(tmp_path, legacy_file):
    page = _stub_page(_StubCatalog(tmp_path, read_only=True))
    page._load_from_path(str(legacy_file))
    assert Path(page.save_path.value).parent == legacy_file.parent
    assert Path(page.save_path.value).name == "legacy.yaml"
    assert page.saved_workplans() == {}


def test_page_survives_a_blueprint_page_without_a_catalog():
    from types import SimpleNamespace

    page = WorkplanBuilderPage(SimpleNamespace(inner=None))
    assert page.catalog is None
    assert page.panes[0].source.value == wb.SOURCE_PATH
    assert page.saved_workplans() == {}


# ---------------------------------------------------------------------------
# recipes
# ---------------------------------------------------------------------------
def _roms_page(page: WorkplanBuilderPage, roms_bp: Path, name: str = "base") -> None:
    _name_page(page)
    _path_step(page.panes[0], name, roms_bp)


def _round_trips(page: WorkplanBuilderPage, tmp_path: Path) -> Workplan:
    """The draft as the YAML text Save writes, read back."""
    assert page.problems == [], page.problems
    draft = page.draft
    assert draft is not None
    out = tmp_path / "preview.yaml"
    out.write_text(wb.yaml_text(draft))
    assert deserialize(out, Workplan).model_dump() == draft.model_dump()
    return draft


def test_chunk_recipe_adds_chained_panes(page, roms_bp, tmp_path):
    _roms_page(page, roms_bp)
    page.chunk_base.step.value = "base"
    assert page.chunk_start.value == "2020-01-01 00:00:00"  # from the blueprint
    assert page.chunk_prefix.value == "base"
    page.chunk_prefix.value = "run"
    page.chunk_end.value = "2020-04-01"
    page.chunk_walltime.value = "06:00:00"
    page.chunk_cadence.value = True
    page.chunk_btn.click()

    assert "Added 3 step(s)" in page.chunk_status.value
    assert [p.name.value for p in page.panes] == ["base", "run-01", "run-02", "run-03"]
    draft = _round_trips(page, tmp_path)
    chunks = {s.name: s for s in draft.steps}
    assert chunks["run-02"].directives["continue-from"] == {"step": "run-01"}
    assert chunks["run-02"].depends_on == ["run-01"]
    overrides = chunks["run-02"].blueprint_overrides
    assert overrides["runtime_params"]["end_date"].month == 3
    assert "start_date" not in overrides["runtime_params"]
    assert overrides["namelist_overrides"]["basic_output_settings"] == (
        wb.FREQUENT_RESTARTS
    )
    assert chunks["run-02"].compute_overrides["slurm"]["max_walltime"] == "06:00:00"
    # the generated panes are ordinary, editable panes
    assert page.panes[2].end_date.value == "2020-03-01 00:00:00"
    assert page.panes[2].cont_kind.value == "step"


def test_chunk_recipe_walltime_scales_with_the_window(page, roms_bp):
    _roms_page(page, roms_bp)
    page.chunk_base.step.value = "base"
    page.chunk_end.value = "2020-03-01"
    page.chunk_hours_per_day.value = 0.5
    page.chunk_btn.click()
    walltimes = [
        s.compute_overrides["slurm"]["max_walltime"] for s in page.draft.steps[1:]
    ]
    assert walltimes == ["15:30:00", "14:30:00"]  # 31 and 29 days at 30 min/day
    page.chunk_walltime.value = "01:00:00"
    page.chunk_prefix.value = "other"
    page.chunk_btn.click()
    assert "not both" in page.chunk_status.value


def test_chunk_recipe_first_source_declares_the_external_run(page, roms_bp, tmp_path):
    _roms_page(page, roms_bp)
    page._add_run()
    row = page.run_rows[0]
    row.alias.value, row.run_id.value, row.steps.value = "ini", "ini-run", "create-ic"
    page.chunk_base.step.value = "base"
    page.chunk_end.value = "2020-02-01"
    page.chunk_first.kind.value = "step"
    page.chunk_first.step.value = "create-ic@ini"
    page.chunk_first.timestamp.value = "2019-12-31"
    page.chunk_prefix.value = "go"
    page.chunk_btn.click()
    first = {s.name: s for s in page.draft.steps}["go-01"]
    assert first.directives["continue-from"] == {
        "step": "create-ic@ini",
        "timestamp": "2019-12-31 00:00:00",
    }
    assert "create-ic@ini" in first.depends_on
    assert list(page.draft.runs) == ["ini"]
    _round_trips(page, tmp_path)


def test_time_recipes_offer_only_roms_marbl_steps_as_bases(page):
    pane = page.panes[0]
    pane.name.value = "hello"
    pane.source.value = wb.SOURCE_INLINE
    pane.application.value = "hello_world"
    assert page.roms_step_names() == []
    assert _values(page.chunk_base.step) == [""]
    assert _values(page.ramp_base.step) == [""]
    assert "upscaling needs roms_marbl steps" in page.recipe_hint.value
    assert page.upscale_btn.disabled
    # chunk and ramp can still start from a blueprint, so they stay enabled
    assert not page.chunk_btn.disabled and not page.ramp_btn.disabled
    page.chunk_btn.click()
    assert "choose the base step" in page.chunk_status.value
    assert [p.name.value for p in page.panes] == ["hello"]


def test_chunk_recipe_refuses_a_name_clash(page, roms_bp):
    _roms_page(page, roms_bp)
    page.chunk_base.step.value = "base"
    page.chunk_end.value = "2020-02-01"
    page.chunk_prefix.value = "x"
    page.chunk_btn.click()
    page.chunk_btn.click()
    assert "already in use" in page.chunk_status.value
    assert len(page.panes) == 2


def test_ramp_recipe_adds_steps_with_each_dt(page, roms_bp, tmp_path):
    _roms_page(page, roms_bp)
    page.ramp_base.step.value = "base"
    page._add_ramp_row(14.0, 200.0)
    page.ramp_rows[0][0].value, page.ramp_rows[0][1].value = 7.0, 100.0
    page.ramp_btn.click()
    assert [p.name.value for p in page.panes] == ["base", "spinup-01", "spinup-02"]
    draft = _round_trips(page, tmp_path)
    dts = [
        s.blueprint_overrides["namelist_overrides"]["time_stepping"]["dt"]
        for s in draft.steps[1:]
    ]
    assert dts == [100.0, 200.0]
    ends = [
        s.blueprint_overrides["runtime_params"]["end_date"] for s in draft.steps[1:]
    ]
    assert (ends[0].day, ends[1].day) == (8, 22)  # 7 then 14 days from 2020-01-01
    assert draft.steps[2].directives["continue-from"] == {"step": "spinup-01"}


def test_forge_then_run_recipe_adds_forge_and_deferred_steps(page, tmp_path):
    page._delete(page.panes[0])  # start from an empty plan
    _name_page(page)
    page.forge_source.value = _FORGE_BP
    page.forge_btn.click()
    assert [p.name.value for p in page.panes] == ["forge", "roms_marbl"]
    draft = _round_trips(page, tmp_path)
    forge, run = draft.steps
    assert forge.application == "forge"
    assert run.is_deferred and run.blueprint_path.filename == f"B_{_FORGE_BP}.yaml"
    assert run.depends_on == ["forge"]
    assert run.compute_overrides == {"slurm": {"num_cpus": 10}}
    # a second addition picks fresh names
    page.forge_btn.click()
    assert [p.name.value for p in page.panes][2:] == ["forge-2", "roms_marbl-2"]
    page.forge_source.value = ""
    page.forge_path.value = str(tmp_path / "missing.yaml")
    page.forge_btn.click()
    assert "cannot read a forge blueprint" in page.forge_status.value


def test_upscale_recipe_reads_use_pio_from_the_blueprints(page, roms_bp, tmp_path):
    _roms_page(page, roms_bp, "inner")
    outer = page.add_step("outer")
    _path_step(outer, "outer", roms_bp)
    assert page.upscale_levels.allowed_tags == ["inner", "outer"]
    page.upscale_levels.value = ["inner", "outer"]
    page.upscale_btn.click()
    names = [p.name.value for p in page.panes]
    assert names == ["inner", "outer", "upscale_inner_outer", "outer_up"]
    draft = _round_trips(page, tmp_path)
    upscale = {s.name: s for s in draft.steps}["upscale_inner_outer"]
    assert upscale.is_inline and upscale.blueprint_overrides["pio"] is True
    assert page.panes[2].application.value == "upscaler"
    page.upscale_levels.value = ["inner"]
    page.upscale_btn.click()
    assert "at least two levels" in page.upscale_status.value


# ---------------------------------------------------------------------------
# diagnostics
# ---------------------------------------------------------------------------
@pytest.fixture
def hello_bp(tmp_path: Path) -> Path:
    path = tmp_path / "hello.yaml"
    shutil.copy(_BP_TEMPLATE.parents[1] / "hello_world" / "blueprint.1.0.0.yaml", path)
    return path


def test_readiness_lists_what_a_pre_run_prepares_and_skips(page, roms_bp):
    page._delete(page.panes[0])
    _name_page(page)
    page.forge_source.value = _FORGE_BP
    page.forge_btn.click()
    assert "<code>forge</code>" in page.readiness.value
    assert "skipped" in page.readiness.value
    assert page.readiness_kept == 0
    extra = page.add_step("solo")
    _path_step(extra, "solo", roms_bp)
    assert page.readiness_kept == 1
    assert "will be prepared:</b> <code>solo</code>" in page.readiness.value


def test_deep_check_passes_a_hello_world_draft(page, hello_bp, tmp_path, monkeypatch):
    monkeypatch.setenv("CSTAR_DATA_HOME", str(tmp_path / "data"))
    _name_page(page)
    pane = page.panes[0]
    _path_step(pane, "hello", hello_bp)
    assert pane.application.value == "hello_world"
    assert page.problems == []
    page.deep_btn.click()
    assert "Deep check passed" in page.diagnostics.value
    assert page.deep_btn.disabled is False


def test_deep_check_reports_unreadable_blueprints_as_not_verifiable(
    page, tmp_path, monkeypatch
):
    monkeypatch.setenv("CSTAR_DATA_HOME", str(tmp_path / "data"))
    _name_page(page)
    _path_step(page.panes[0], "far", tmp_path / "elsewhere" / "bp.yaml")
    assert page.draft is not None
    page.deep_check()
    assert "Not verifiable here" in page.diagnostics.value
    assert "forge-banner err" not in page.diagnostics.value


def test_deep_check_needs_a_valid_draft(page):
    page.deep_check()
    assert "Fix the problems" in page.diagnostics.value


def test_deep_check_flags_a_stale_deferred_cpu_count(page):
    page._delete(page.panes[0])
    _name_page(page)
    page.forge_source.value = _FORGE_BP
    page.forge_btn.click()
    page.panes[1].num_cpus.value = 4  # the forge blueprint predicts 10
    page.deep_check()
    assert "predicted to emit a blueprint needing 10" in page.diagnostics.value


def test_format_walltime():
    assert wb.format_walltime(0.5) == "00:30:00"
    assert wb.format_walltime(15.5) == "15:30:00"
    assert wb.format_walltime(30) == "1-06:00:00"
    assert wb.format_walltime(0.0001) == "00:01:00"


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------
class _FakeStdout:
    def __init__(self, data: bytes) -> None:
        self._data, self._pos = data, 0

    async def read(self, n: int = -1) -> bytes:
        chunk = self._data[self._pos : self._pos + 7]  # line-unaligned reads
        self._pos += len(chunk)
        return chunk


class _FakeProcess:
    def __init__(self, data: bytes, returncode: int = 0) -> None:
        self.stdout = _FakeStdout(data)
        self._returncode = returncode

    async def wait(self) -> int:
        return self._returncode


@pytest.fixture
def commands(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, ...]]:
    """Capture the commands the page launches, answering with canned output."""
    import asyncio

    seen: list[tuple[str, ...]] = []

    async def _fake(*args, **kwargs):
        seen.append(args)
        assert kwargs["stderr"] == asyncio.subprocess.STDOUT
        return _FakeProcess(b"scheduled step one\nstep two\n")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake)
    return seen


def _slurm_run_page(page, roms_bp, tmp_path):
    _roms_page(page, roms_bp, "base")
    page.compute_target.value = wb.TARGET_SLURM
    page.machine.value = "anvil"
    page.save_path.value = str(tmp_path / "plans" / "demo.yaml")


def test_run_saves_streams_and_runs_pre_run_first_on_slurm(
    page, roms_bp, tmp_path, commands
):
    _slurm_run_page(page, roms_bp, tmp_path)
    assert page.run_id.value == "demo"  # slug of the workplan name
    assert page.pre_run_first.layout.display != "none"
    assert page.pre_run_first.value is True  # on by default for SLURM with a kept step

    page.run_btn.click()

    assert (tmp_path / "plans" / "demo.yaml").exists()  # saved first
    cmd = commands[0]
    assert cmd[-6:] == (
        "workplan",
        "run",
        str(tmp_path / "plans" / "demo.yaml"),
        "--run-id",
        "demo",
        "--pre-run",
    )
    text = "".join(o["text"] for o in page.run_output.outputs)
    assert "scheduled step one" in text and "step two" in text
    assert "prepared (pre-run)" in page.run_status.value
    # the follow-up is the same command without the flag, and status is named
    assert "demo.yaml --run-id demo</code>" in page.run_status.value
    assert "cstar workplan status demo" in page.run_status.value
    assert page.run_btn.disabled is False


def test_run_without_pre_run_points_at_status(page, roms_bp, tmp_path, commands):
    _slurm_run_page(page, roms_bp, tmp_path)
    page.pre_run_first.value = False
    page.run_vars.value = "a=1, b=two"
    page.run_btn.click()
    assert "--pre-run" not in commands[0]
    assert commands[0].count("--var") == 2 and "a=1" in commands[0]
    assert "scheduled" in page.run_status.value
    assert "cstar workplan status demo" in page.run_status.value


def test_pre_run_first_is_hidden_unless_slurm_has_a_preparable_step(
    page, roms_bp, tmp_path
):
    _roms_page(page, roms_bp)
    assert page.compute_target.value == wb.TARGET_NONE
    assert page.pre_run_first.layout.display == "none"
    page.compute_target.value = wb.TARGET_SLURM
    assert page.pre_run_first.layout.display != "none"
    page.compute_target.value = wb.TARGET_LOCAL
    assert page.pre_run_first.layout.display == "none"
    assert page.pre_run_first.value is False


def test_check_streams_the_check_command(page, roms_bp, tmp_path, commands):
    _slurm_run_page(page, roms_bp, tmp_path)
    page.check_btn.click()
    assert commands[0][-3:] == (
        "workplan",
        "check",
        str(tmp_path / "plans" / "demo.yaml"),
    )
    assert "--run-id" not in commands[0] and "--pre-run" not in commands[0]
    assert "check finished" in page.run_status.value


def test_failed_run_reports_the_exit_code(page, roms_bp, tmp_path, monkeypatch):
    import asyncio

    async def _fake(*args, **kwargs):
        return _FakeProcess(b"boom\n", returncode=2)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake)
    _slurm_run_page(page, roms_bp, tmp_path)
    page.run_btn.click()
    assert "exited with code 2" in page.run_status.value
    assert page.run_btn.disabled is False


def test_run_refuses_an_invalid_draft_and_the_loaded_file(page, legacy_file, commands):
    page.run_btn.click()
    assert "invalid" in page.run_status.value and commands == []

    page._load_from_path(str(legacy_file))
    page.save_path.value = str(legacy_file)  # differs from the draft: needs a confirm
    before = legacy_file.read_bytes()
    page.run_btn.click()
    assert "loaded file" in page.run_status.value
    assert commands == [] and legacy_file.read_bytes() == before


def test_run_command_uses_the_cstar_script_beside_python(page, tmp_path, monkeypatch):
    bindir = tmp_path / "bin"
    bindir.mkdir()
    (bindir / "cstar").write_text("")
    monkeypatch.setattr("sys.executable", str(bindir / "python"))
    assert page.run_command(wb.CHECK, tmp_path / "x.yaml")[:3] == [
        str(bindir / "cstar"),
        "workplan",
        "check",
    ]
    monkeypatch.setattr("sys.executable", str(tmp_path / "python"))
    assert page.run_command(wb.CHECK, tmp_path / "x.yaml")[1:4] == [
        "-m",
        "cstar.cli.cli",
        "workplan",
    ]


# ---------------------------------------------------------------------------
# normalizer report
# ---------------------------------------------------------------------------
def test_revert_restores_a_step_as_loaded(page, legacy_file):
    page._load_from_path(str(legacy_file))
    child = page.panes[1]
    assert child.cont_kind.value == "path"  # rst_path moved to continue-from
    before = legacy_file.read_bytes()

    page._revert("child")

    original = deserialize(legacy_file, Workplan).steps[1]
    assert page.panes[1] is child
    assert child.cont_kind.value == "none"  # back to the deprecated spelling
    assert page.draft.steps[1].directives == original.directives
    assert page.draft.steps[1].blueprint_overrides == original.blueprint_overrides
    assert "child" in page._reverted
    buttons = [
        c
        for row in page.changes_box.children
        if hasattr(row, "children")
        for c in row.children
        if hasattr(c, "click")
    ]
    assert buttons and all(b.disabled for b in buttons)
    assert legacy_file.read_bytes() == before


def test_revert_button_is_wired_to_the_step(page, legacy_file):
    page._load_from_path(str(legacy_file))
    buttons = [
        c
        for row in page.changes_box.children
        if hasattr(row, "children")
        for c in row.children
        if hasattr(c, "click")
    ]
    assert buttons
    buttons[0].click()
    assert page.panes[1].cont_kind.value == "none"


def test_pinned_start_at_is_never_authored(page, roms_bp, tmp_path):
    path = tmp_path / "pinned.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "name": "pinned",
                "description": "a transformed plan",
                "runs": {"r": {"run_id": "old", "start_at": datetime(2026, 1, 1)}},
                "steps": [
                    {
                        "name": "s",
                        "application": "roms_marbl",
                        "blueprint": str(roms_bp),
                        "depends_on": ["x@r"],
                    }
                ],
            }
        )
    )
    page._load_from_path(str(path))
    assert page.draft.runs["r"].start_at is None
    assert "start_at" in _texts(page.changes_box)
    assert "start_at:" not in wb.yaml_text(page.draft)


def test_sticky_bar_links_every_card(page):
    import re

    anchors = re.findall(r"#forge-sec-(\w+)'", page.sticky_bar.value)
    assert anchors == ["start", "workplan", "compute", "recipes", "steps", "review"]
    nums = {
        c.forge_key: c.forge_header.value
        for c in page.widget.children
        if hasattr(c, "forge_key")
    }
    assert ">4</span>" in nums["recipes"] and ">5</span>" in nums["steps"]


# ---------------------------------------------------------------------------
# review follow-ups
# ---------------------------------------------------------------------------
def test_current_config_is_not_saved_when_boundaries_cannot_be_derived(
    page, bp_app, monkeypatch
):
    saved = Path(bp_app.inner.save_path.value)
    saved.unlink(missing_ok=True)
    monkeypatch.setattr(bp_app.inner, "_ensure_boundaries_derived", lambda: False)
    pane = page.panes[0]
    pane.source.value = wb.SOURCE_CURRENT
    assert pane._current_path == ""
    assert "boundaries could not be derived" in pane.current_note.value
    assert "forge-banner err" in pane.current_note.value
    assert not saved.exists()
    assert any("choose a blueprint" in p for p in page.problems)


def test_upscale_use_pio_defaults_to_off_when_unknown(page, tmp_path):
    for name in ("inner", "outer"):
        pane = page.panes[0] if name == "inner" else page.add_step(name)
        pane.name.value = name
        pane.source.value = wb.SOURCE_PATH
        pane.path.value = str(tmp_path / "unreadable.yaml")
        pane.application.value = "roms_marbl"
    page.upscale_levels.value = ["inner", "outer"]
    page.upscale_btn.click()
    upscale = {p.name.value: p for p in page.panes}["upscale_inner_outer"]
    assert upscale._form["pio"][0].value is False


def test_upscale_use_pio_of_a_deferred_level_comes_from_the_hook(page):
    page._delete(page.panes[0])
    _name_page(page)
    page.forge_source.value = _FORGE_BP
    page.forge_btn.click()
    inner = page.add_step("inner")
    inner.source.value = wb.SOURCE_DEFERRED
    inner.producer.value = "forge"  # wio-toy-simple predicts use_pio=True
    outer = page.add_step("outer")
    outer.source.value = wb.SOURCE_DEFERRED
    outer.producer.value = "forge"
    page.upscale_levels.value = ["inner", "outer"]
    page.upscale_btn.click()
    upscale = {p.name.value: p for p in page.panes}["upscale_inner_outer"]
    assert upscale._form["pio"][0].value is True


@pytest.mark.parametrize(
    "block",
    [
        {"launcher": "slurm", "system": "derecho", "slurm": {"cpus_per_node": 128}},
        {"launcher": "local", "slurm": {"max_walltime": "01:00:00"}},
        {
            "launcher": "slurm",
            "system": "anvil",
            "slurm": {"cpus_per_node": 64, "queue_name": "shared", "num_nodes": 2},
        },
        {"launcher": "slurm", "slurm": {"account_name": "x"}},
    ],
)
def test_compute_environment_round_trips(page, roms_bp, tmp_path, block):
    path = tmp_path / "plan.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "name": "rt",
                "description": "round trip",
                "compute_environment": block,
                "steps": [
                    {
                        "name": "s",
                        "application": "roms_marbl",
                        "blueprint": str(roms_bp),
                    }
                ],
            }
        )
    )
    original = deserialize(path, Workplan)
    page.load_workplan(original, path)
    assert page.problems == []
    assert page.draft.compute_environment == original.compute_environment == block


def test_loaded_unknown_system_is_kept_and_shown(page, roms_bp, tmp_path):
    path = tmp_path / "plan.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "name": "rt",
                "description": "d",
                "compute_environment": {"launcher": "slurm", "system": "derecho"},
                "steps": [
                    {
                        "name": "s",
                        "application": "roms_marbl",
                        "blueprint": str(roms_bp),
                    }
                ],
            }
        )
    )
    page._load_from_path(str(path))
    assert page.machine.value == wb.MACHINE_CUSTOM
    assert "derecho" in page.compute_note.value
    page.machine.value = "anvil"  # choosing a real machine replaces it
    assert page.draft.compute_environment["system"] == "anvil"


def test_renaming_a_step_remaps_placeholders_everywhere(page):
    _name_page(page)
    a = page.panes[0]
    a.name.value = "a"
    a.source.value = wb.SOURCE_INLINE
    a.application.value = "hello_world"
    a._form["target"][0].value = "world"
    b = page.add_step("user")
    b.source.value = wb.SOURCE_INLINE
    b.application.value = "nest_ic"
    b._form["parent_grid"][0].value = "{{output_dir: a}}/grid.nc"
    b._form["parent_rst"][0].value = "/x/rst.nc"
    b._form["child_grid"][0].value = "{{ input_dir : a }}/child.nc"
    b._passthrough_directives = {"other": {"path": "{{output_dir: a}}"}}
    b._passthrough_compute = {"slurm": {"queue_name": "{{output_dir: a}}"}}
    b.raw_overrides.value = "extra: '{{output_dir: a}}'"
    assert {s.name: s for s in page.draft.steps}["user"].depends_on == ["a"]

    a.name.value = "b2"

    step = {s.name: s for s in page.draft.steps}["user"]
    overrides = step.blueprint_overrides
    assert overrides["parent_grid"] == "{{output_dir: b2}}/grid.nc"
    assert overrides["child_grid"] == "{{input_dir: b2}}/child.nc"
    assert overrides["extra"] == "{{output_dir: b2}}"
    assert step.directives == {"other": {"path": "{{output_dir: b2}}"}}
    assert step.compute_overrides == {"slurm": {"queue_name": "{{output_dir: b2}}"}}
    assert step.depends_on == ["b2"]


def test_renaming_a_step_remaps_cdr_and_path_references(page, roms_bp):
    _name_page(page)
    a = page.panes[0]
    _path_step(a, "a", roms_bp)
    b = page.add_step("b")
    _path_step(b, "b", roms_bp)
    b.cdr_location.value = "{{output_dir: a}}/cdr.nc"
    b.cont_kind.value = "path"
    b.cont_path.value = "{{output_dir: a}}"
    a.name.value = "z"
    step = {s.name: s for s in page.draft.steps}["b"]
    assert step.blueprint_overrides["cdr_forcing"] == {
        "data": [{"location": "{{output_dir: z}}/cdr.nc"}]
    }
    assert step.directives["continue-from"] == {"path": "{{output_dir: z}}"}
    assert step.depends_on == ["z"]


def test_chunk_window_is_prefilled_from_a_deferred_producer(page):
    page._delete(page.panes[0])
    _name_page(page)
    page.forge_source.value = _FORGE_BP
    page.forge_btn.click()
    assert page.roms_step_names() == ["roms_marbl"]
    page.chunk_base.step.value = "roms_marbl"
    assert page.chunk_start.value == "2012-01-01 00:00:00"
    assert page.chunk_end.value == "2012-01-02 00:00:00"
    assert page.chunk_prefix.value == "roms_marbl"


# ---------------------------------------------------------------------------
# user-testing follow-ups
# ---------------------------------------------------------------------------
def test_download_links_get_their_gap_from_css():
    from cstar.wizard.ui import components

    assert ".forge-dl-btn code {" in components.WIZARD_CSS
    assert "margin-left: 0.5em" in components.WIZARD_CSS
    assert ".forge-code" in components.WIZARD_CSS


def test_recipe_card_precedes_steps_and_is_an_accordion(page):
    kids = [getattr(c, "forge_key", None) for c in page.widget.children]
    assert kids.index("recipes") < kids.index("steps")
    acc = page.recipes_accordion
    assert "forge-open-acc" in acc._dom_classes
    assert [acc.get_title(i) for i in range(4)] == [
        labels.section_for(f"recipes.{k}", page=wb.PAGE).title
        for k in ("chunk", "ramp", "forge", "upscale")
    ]
    assert all(inner.selected_index is None for inner in acc.panes)  # collapsed


def test_runs_section_title():
    assert (
        labels.section_for("workplan.runs", page=wb.PAGE).title
        == "Add aliases to previous run-ids"
    )


def test_chunk_recipe_from_a_catalog_roms_blueprint(tmp_path, roms_bp):
    (tmp_path / "one").mkdir()
    shutil.copy(roms_bp, tmp_path / "one" / "my.bp.yaml")
    page = _stub_page(_StubCatalog(tmp_path))
    page._delete(page.panes[0])
    _name_page(page)
    chooser = page.chunk_base
    chooser.kind.value = wb.SOURCE_CATALOG_ROMS
    chooser.catalog_roms.value = "one"
    assert page.chunk_start.value == "2020-01-01 00:00:00"  # from the blueprint
    assert page.chunk_prefix.value == "my.bp"
    page.chunk_end.value = "2020-03-01"
    page.chunk_btn.click()
    names = [p.name.value for p in page.panes]
    assert names == ["my.bp-01", "my.bp-02"]  # the base is synthesized, not added
    first, second = page.draft.steps
    assert first.blueprint_path == str(tmp_path / "one" / "my.bp.yaml")
    assert first.compute_overrides == {"slurm": {"num_cpus": 128}}
    assert second.directives["continue-from"] == {"step": "my.bp-01"}


def test_chunk_recipe_from_a_blueprint_path_and_ramp_from_a_blueprint(
    page, roms_bp, tmp_path
):
    page._delete(page.panes[0])
    _name_page(page)
    page.chunk_base.kind.value = wb.SOURCE_PATH
    page.chunk_base.path.value = str(roms_bp)
    assert page.chunk_start.value == "2020-01-01 00:00:00"
    page.chunk_end.value = "2020-02-15"
    page.chunk_btn.click()
    assert [p.name.value for p in page.panes] == ["roms-01", "roms-02"]
    page.ramp_base.kind.value = wb.SOURCE_PATH
    page.ramp_base.path.value = str(roms_bp)
    page.ramp_btn.click()
    assert [p.name.value for p in page.panes][2:] == ["spinup-01"]
    assert page.problems == []
    _round_trips(page, tmp_path)


def test_chunk_recipe_from_a_forge_blueprint_adds_forge_then_deferred_chunks(
    page, tmp_path
):
    page._delete(page.panes[0])
    _name_page(page)
    chooser = page.chunk_base
    chooser.kind.value = wb.SOURCE_CATALOG_FORGE
    chooser.catalog_forge.value = _FORGE_BP
    assert page.chunk_start.value == "2012-01-01 00:00:00"
    assert page.chunk_end.value == "2012-01-02 00:00:00"
    assert page.chunk_prefix.value == _FORGE_BP
    page.chunk_end.value = "2012-01-04"
    page.chunk_mode.value = "days"
    page.chunk_btn.click()
    assert [p.name.value for p in page.panes] == [
        "forge",
        f"{_FORGE_BP}-01",
        f"{_FORGE_BP}-02",
        f"{_FORGE_BP}-03",
    ]
    draft = _round_trips(page, tmp_path)
    steps = {s.name: s for s in draft.steps}
    for name in (f"{_FORGE_BP}-01", f"{_FORGE_BP}-02", f"{_FORGE_BP}-03"):
        assert steps[name].is_deferred
        assert "forge" in steps[name].depends_on  # every chunk waits for the producer
        assert steps[name].compute_overrides == {"slurm": {"num_cpus": 10}}


def test_chunk_recipe_rejects_a_non_roms_blueprint(page, hello_bp):
    page.chunk_base.kind.value = wb.SOURCE_PATH
    page.chunk_base.path.value = str(hello_bp)
    page.chunk_btn.click()
    assert "hello_world" in page.chunk_status.value


def test_preview_is_an_editable_code_box(page, roms_bp):
    _roms_page(page, roms_bp)
    assert "forge-code" in page.preview._dom_classes
    assert page.preview.value.startswith("# yaml-language-server")
    assert not page._preview_dirty and page.preview_chip.value == ""


def test_apply_edits_repopulates_the_page_and_keeps_the_save_path(
    page, roms_bp, tmp_path
):
    _roms_page(page, roms_bp)
    page.save_path.value = str(tmp_path / "chosen.yaml")
    loaded = tmp_path / "loaded.yaml"
    loaded.write_text("untouched")
    page.loaded_from = loaded.resolve()
    edited = yaml.safe_load(page.preview.value)
    edited["name"] = "edited name"
    edited["steps"][0]["name"] = "renamed"
    edited["steps"].append({**edited["steps"][0], "name": "second"})
    page.preview.value = yaml.safe_dump(edited, sort_keys=False)
    assert page._preview_dirty and "unapplied edits" in page.preview_chip.value
    assert page.draft.name == "demo"  # not applied yet

    page.apply_btn.click()

    assert page.draft.name == "edited name"
    assert [s.name for s in page.draft.steps] == ["renamed", "second"]
    assert [p.name.value for p in page.panes] == ["renamed", "second"]
    assert page.name.value == "edited name"
    assert not page._preview_dirty and page.preview_chip.value == ""
    assert "edited name" in page.preview.value
    assert page.save_path.value == str(tmp_path / "chosen.yaml")
    assert page.loaded_from == loaded.resolve()
    assert loaded.read_text() == "untouched"


def test_invalid_edits_show_a_banner_and_leave_the_draft(page, roms_bp):
    _roms_page(page, roms_bp)
    before = page.draft.model_dump()
    text = page.preview.value
    for bad in (
        "name: [unclosed",
        "- just\n- a list\n",
        "name: x\ndescription: y\nsteps: []\n",
    ):
        page.preview.value = bad
        page.apply_edits()
        assert "was not applied" in page.validation.value
        assert page.draft.model_dump() == before
        assert page._preview_dirty and page.preview.value == bad  # the edit is kept
    # a rebuild does not overwrite unapplied edits...
    page.runtime_vars.value = ""
    page._rebuild()
    assert page.preview.value == bad
    # ...until they are discarded
    page.discard_btn.click()
    assert page.preview.value == text
    assert not page._preview_dirty


def test_save_and_download_use_the_applied_draft_not_the_textarea(
    page, roms_bp, tmp_path
):
    _roms_page(page, roms_bp)
    page.preview.value = "name: not applied\n"
    page.save_path.value = str(tmp_path / "out.yaml")
    page._on_save()
    assert deserialize(tmp_path / "out.yaml", Workplan).name == "demo"
    assert "not applied" not in page.download_link.value


def test_blank_description_defaults_to_the_workplan_name(page, roms_bp):
    pane = page.panes[0]
    _path_step(pane, "run", roms_bp)
    page.name.value = "My plan"
    assert page.description.value == ""
    assert page.problems == []
    assert page.draft.description == "My plan"
    assert page.description.placeholder == "My plan"
    page.description.value = "explicit"
    assert page.draft.description == "explicit"


def test_base_chooser_shows_only_the_chosen_source_row(page):
    """Each chooser widget owns its Layout, so hiding one does not hide the rest."""
    chooser = page.chunk_base
    widgets = (chooser.step, chooser.catalog_roms, chooser.catalog_forge, chooser.path)
    assert len({id(w.layout) for w in widgets}) == len(widgets)
    chooser.kind.value = "catalog_roms"
    shown = [w for w in widgets if w.layout.display != "none"]
    assert shown == [chooser.catalog_roms]
    chooser.kind.value = "path"
    shown = [w for w in widgets if w.layout.display != "none"]
    assert shown == [chooser.path]


def test_sticky_bar_is_the_styled_box_with_a_download_link(page):
    """The status bar sits in a `forge-sticky` box like the blueprint page's."""
    sticky = next(
        child for child in page.widget.children if "forge-sticky" in child._dom_classes
    )
    assert list(sticky.children) == [page.sticky_bar, page.sticky_download]
    page.name.value = "bar test"
    page.add_step().populate(
        Step(name="s", application="hello_world", blueprint="/b.yaml")
    )
    page._rebuild()
    assert 'class="forge-dl-btn"' in page.sticky_download.value
    assert "<code>" not in page.sticky_download.value
