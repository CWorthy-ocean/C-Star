"""Tests for the wizard's Save and Next steps cards: the blueprint's name,
description and Save path, Save and the download links, and what to do with the
saved blueprint (the workplan row and Run Forge here).
"""

from datetime import date
from pathlib import Path

import yaml

from cstar.tests.unit_tests.wizard.helpers import (
    _downloaded_yaml,
    _FakeProcess,
    _new_wizard,
    _run_wiz_with_output,
    _save,
    _saving_wizard,
)
from cstar.wizard.app import ForgeBlueprintWizardApp
from cstar.wizard.wizard import (
    WIZARD_TOOL,
    ForgeBlueprintWizard,
    WorkplanHooks,
)


def test_run_warning_names_the_selected_system(monkeypatch):
    """The run note names the system C-Star uses, saying `CSTAR_SYSTEM` chose it."""
    monkeypatch.setenv("CSTAR_SYSTEM", "linux_x86_64")
    wiz = ForgeBlueprintWizard()
    assert (
        "System: <code>linux_x86_64 (set by CSTAR_SYSTEM)</code>"
        in wiz.run_warning.value
    )


def test_build_run_command_uses_cstar_blueprint_run_from_this_env():
    """The Run button invokes `cstar blueprint run <path>` -- the one command the docs
    give for both pipeline steps -- via the `cstar` script installed next to the
    interpreter already running the wizard's kernel, not a bare `cstar` from PATH or a
    `conda run` invocation (avoids conda/micromamba env-discovery issues). Not
    `cstar forge run`: the button exposes no per-run flags, so that passthrough's only
    advantage does not apply here.
    """
    import sys

    wiz = ForgeBlueprintWizard()
    cmd = wiz._build_run_command("/tmp/some_blueprint.yaml")
    cstar_exe = Path(sys.executable).with_name("cstar")
    if cstar_exe.exists():
        assert cmd == [str(cstar_exe), "blueprint", "run", "/tmp/some_blueprint.yaml"]
    else:
        assert cmd == [
            sys.executable,
            "-m",
            "cstar.cli.cli",
            "forge",
            "run",
            "/tmp/some_blueprint.yaml",
        ]


def test_on_run_guards_on_invalid_config(monkeypatch):
    """Clicking Run with no resolved config shows an error and spawns nothing."""
    import asyncio

    wiz = ForgeBlueprintWizard()
    wiz._boundaries_touched = True  # not exercising boundary derivation here
    wiz.config = None

    def _boom(*a, **kw):
        raise AssertionError("must not spawn a subprocess for an invalid config")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _boom)
    wiz._on_run(None)
    assert "invalid" in wiz.run_status.value
    assert wiz.run_output.outputs == ()


def test_on_run_streams_subprocess_output_and_reports_success(monkeypatch, tmp_path):
    """Run auto-saves the current blueprint, launches the built command with
    stderr merged into stdout, and streams each line into run_output. There's no
    running event loop in a plain test function, so _schedule_coroutine's
    asyncio.run(...) fallback runs the whole thing to completion synchronously --
    no pytest.mark.asyncio needed.
    """
    import asyncio

    wiz = ForgeBlueprintWizard()
    wiz.run_card.start.value = date(2012, 1, 1)
    wiz.run_card.end.value = date(2012, 1, 2)
    wiz._rebuild()
    wiz.save_path.value = str(tmp_path / "bp.yaml")
    wiz._boundaries_touched = True  # not exercising boundary derivation here

    captured_cmd = {}

    async def _fake_create_subprocess_exec(*args, **kwargs):
        captured_cmd["args"] = args
        captured_cmd["kwargs"] = kwargs
        return _FakeProcess([b"line one\n", b"line two\n"], returncode=0)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_create_subprocess_exec)

    wiz._on_run(None)

    assert (tmp_path / "bp.yaml").exists()  # auto-saved before running
    assert captured_cmd["kwargs"]["stderr"] == asyncio.subprocess.STDOUT
    text = "".join(o["text"] for o in wiz.run_output.outputs)
    assert "line one" in text
    assert "line two" in text
    assert "✓ finished" in wiz.run_status.value
    # ...and the success message hands the user their next command, since the
    # app-framework path prints no "run it with" trailer of its own
    assert "cstar blueprint run" in wiz.run_status.value
    assert wiz.run_btn.disabled is False


def test_on_run_reports_nonzero_exit_code(monkeypatch, tmp_path):
    """A failing subprocess is reported as an error status, not a silent success."""
    import asyncio

    wiz = ForgeBlueprintWizard()
    wiz.run_card.start.value = date(2012, 1, 1)
    wiz.run_card.end.value = date(2012, 1, 2)
    wiz._rebuild()
    wiz.save_path.value = str(tmp_path / "bp.yaml")
    wiz._boundaries_touched = True  # not exercising boundary derivation here

    async def _fake_create_subprocess_exec(*args, **kwargs):
        return _FakeProcess([b"uh oh\n"], returncode=1)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_create_subprocess_exec)

    wiz._on_run(None)

    assert "exited with code 1" in wiz.run_status.value
    assert wiz.run_btn.disabled is False


def test_on_run_survives_line_longer_than_stream_limit(monkeypatch, tmp_path):
    r"""A child that emits far more than 64 KiB with no newline (the classic
    \r-less/progress-less long line) must NOT raise the asyncio StreamReader
    "Separator is not found, and chunk exceed the limit" ValueError that the old
    ``async for line in proc.stdout`` loop did -- the content still lands, split
    across multiple appends by the memory-bounding flush.
    """
    from cstar.wizard.wizard import _STREAM_MAX_LINE

    giant = b"x" * (_STREAM_MAX_LINE * 3 + 17) + b"\ndone\n"
    # Read in chunks well below the flush threshold and unaligned to it, so the
    # buffer must *accumulate across many reads* before each flush -- the real
    # production path (asyncio read() returns whatever is buffered, usually far less
    # than the threshold), not one oversized read that drains immediately.
    wiz = _run_wiz_with_output(monkeypatch, tmp_path, giant, chunk_size=3000)

    assert "✓ finished" in wiz.run_status.value  # no exception surfaced
    text = "".join(o["text"] for o in wiz.run_output.outputs)
    assert text.count("x") == _STREAM_MAX_LINE * 3 + 17  # every byte preserved
    assert "done\n" in text
    # bounded memory => the giant run was flushed as several appends, not held whole
    assert len(wiz.run_output.outputs) > 3


def test_on_run_splits_carriage_return_progress_into_lines(monkeypatch, tmp_path):
    r"""\r-redrawn progress (git clone / tqdm) surfaces as successive log lines
    instead of one accumulating line.
    """
    wiz = _run_wiz_with_output(monkeypatch, tmp_path, b"10%\r20%\r30%\n")

    texts = [o["text"] for o in wiz.run_output.outputs]
    assert texts == ["10%\n", "20%\n", "30%\n"]


def test_on_run_error_status_names_the_command(monkeypatch, tmp_path):
    """An exception during the run is reported WITH the command that was running,
    not as a context-free error string.
    """
    import asyncio

    wiz = ForgeBlueprintWizard()
    wiz.run_card.start.value = date(2012, 1, 1)
    wiz.run_card.end.value = date(2012, 1, 2)
    wiz._rebuild()
    wiz.save_path.value = str(tmp_path / "bp.yaml")
    wiz._boundaries_touched = True

    async def _boom(*args, **kwargs):
        raise OSError("no such executable")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _boom)
    wiz._on_run(None)

    assert "OSError: no such executable" in wiz.run_status.value
    assert "while running:" in wiz.run_status.value
    assert "bp.yaml" in wiz.run_status.value  # the built command is named
    assert wiz.run_btn.disabled is False


def test_saving_a_never_stamped_blueprint_unchanged_records_the_wizard(tmp_path):
    """A blueprint written without a stamp (a script's plain ``to_yaml``, a catalog
    entry) records a hash that already matches its content: saved unchanged, it
    must still name the wizard as its producer.
    """
    writer = _new_wizard()
    writer._rebuild()
    assert writer.config is not None, writer.derived.value
    writer.config.to_yaml(tmp_path / "unstamped.yaml")  # the hash, and nothing else

    wiz = _new_wizard()
    wiz.load_path.value = str(tmp_path / "unstamped.yaml")
    wiz._on_load_path(None)
    assert wiz.config is not None, wiz.derived.value
    # no producer and a matching hash: only the missing stamp can trigger a stamp
    assert wiz.config.provenance.generated_by is None
    assert wiz.config.provenance.content_hash == wiz.config.content_hash()
    wiz.save_path.value = str(tmp_path / "bp.yaml")
    wiz._boundaries_touched = True

    saved = _save(wiz)

    assert saved.generated_by is not None
    assert saved.generated_by.tool == WIZARD_TOOL
    assert saved.content_hash == wiz.config.content_hash()


def test_editing_a_description_does_not_restamp(tmp_path):
    """The description is excluded from the content hash, so it is not content."""
    wiz = _saving_wizard(tmp_path / "bp.yaml")
    first = _save(wiz)

    wiz.description.value = "a different description"
    second = _save(wiz)

    assert second.generated_by == first.generated_by


def test_a_download_does_not_count_as_a_save(tmp_path):
    """Saving after a rebuild stamps afresh: the download's stamp was never carried."""
    wiz = _saving_wizard(tmp_path / "bp.yaml")
    downloaded = _downloaded_yaml(wiz.download_link.value)["provenance"]["generated_by"]

    saved = _save(wiz)

    assert saved.generated_by.id != downloaded["id"]


def test_download_links_hand_out_the_file_just_saved(tmp_path):
    """Saving and then downloading with no edit between must not mint a second
    stamp: both links carry the very file that was written.
    """
    wiz = _saving_wizard(tmp_path / "bp.yaml")

    saved = _save(wiz)

    written = yaml.safe_load((tmp_path / "bp.yaml").read_text())
    for link in (wiz.download_link, wiz.download_link_review):
        downloaded = _downloaded_yaml(link.value)
        assert downloaded["provenance"]["generated_by"]["id"] == saved.generated_by.id
        assert downloaded == written


# ===========================================================================
# Card 8: Next steps (saved-blueprint gate, workplan save, run pane)
# ===========================================================================


def _saved_wizard(tmp_path, *, hooks=None):
    """A valid wizard over a throwaway catalog, saved to ``tmp_path/bp.yaml``."""
    app = ForgeBlueprintWizardApp(str(tmp_path / "catalog"))
    wiz = app.inner
    assert wiz is not None
    assert Path(wiz.catalog.workplans_dir).is_relative_to(tmp_path)
    if hooks is not None:
        app.set_workplan_hooks(hooks)
    wiz.run_card.start.value = date(2012, 1, 1)
    wiz.run_card.end.value = date(2012, 1, 2)
    wiz._rebuild()
    wiz.save_path.value = str(tmp_path / "bp.yaml")
    wiz._boundaries_touched = True
    return wiz


def _next_buttons(wiz):
    return (wiz.save_workplan_btn, wiz.open_workplan_btn, wiz.run_btn)


def test_saved_blueprint_survives_a_rebuild_and_not_an_edit(tmp_path):
    wiz = _saved_wizard(tmp_path)
    assert wiz._saved_path() is None
    wiz._on_save(None)
    assert wiz._saved_path() == (tmp_path / "bp.yaml").resolve()
    wiz._rebuild()
    assert wiz._saved_path() == (tmp_path / "bp.yaml").resolve()
    wiz.run_card.end.value = date(2012, 1, 3)
    assert wiz._saved_path() is None


def test_saved_blueprint_accepts_a_save_path_written_unnormalized(tmp_path):
    """A Save path that ``Path`` rewrites (a doubled slash) still counts as saved."""
    wiz = _saved_wizard(tmp_path)
    wiz.save_path.value = f"{tmp_path}//bp.yaml"
    wiz._on_save(None)
    assert wiz._saved_path() == (tmp_path / "bp.yaml").resolve()
    assert not wiz.save_workplan_btn.disabled


def test_next_steps_follow_the_saved_state(tmp_path):
    wiz = _saved_wizard(tmp_path)
    assert all(b.disabled for b in _next_buttons(wiz))
    assert "Save the blueprint above first" in wiz.next_status.value
    wiz._on_save(None)
    assert not any(b.disabled for b in _next_buttons(wiz))
    assert str((tmp_path / "bp.yaml").resolve()) in wiz.next_status.value
    wiz._rebuild()
    assert not any(b.disabled for b in _next_buttons(wiz))
    wiz.run_card.end.value = date(2012, 1, 3)
    assert all(b.disabled for b in _next_buttons(wiz))
    assert "has changed" in wiz.next_status.value
    wiz.run_card.end.value = date(2012, 1, 2)
    assert not any(b.disabled for b in _next_buttons(wiz))
    wiz.save_path.value = str(tmp_path / "other.yaml")
    assert all(b.disabled for b in _next_buttons(wiz))
    wiz.save_path.value = str(tmp_path / "bp.yaml")
    wiz.run_card.start.value = None
    wiz._rebuild()
    assert wiz.config is None
    assert all(b.disabled for b in _next_buttons(wiz))


def test_a_run_in_progress_keeps_run_disabled_through_a_rebuild(tmp_path):
    wiz = _saved_wizard(tmp_path)
    wiz._on_save(None)
    wiz._run_in_progress = True
    wiz._rebuild()
    assert wiz.run_btn.disabled
    wiz._run_in_progress = False
    wiz._rebuild()
    assert not wiz.run_btn.disabled


def _load_workplan(path):
    from cstar.orchestration.models import Workplan

    raw = yaml.safe_load(path.read_text())
    raw.pop("$schema", None)
    return Workplan.model_validate(raw)


def test_save_workplan_writes_the_forge_then_run_workplan(tmp_path):
    from cstar.base.utils import slugify
    from cstar.orchestration.models import DeferredBlueprintRef

    calls = []
    wiz = _saved_wizard(
        tmp_path,
        hooks=WorkplanHooks(
            open_steps=lambda p, n: None, saved=lambda: calls.append(1)
        ),
    )
    wiz._on_save(None)
    wiz._on_save_workplan(None)
    assert wiz.config is not None
    slug = slugify(wiz.config.name)
    target = Path(wiz.catalog.workplans_dir) / f"{slug}.yaml"
    assert target.exists(), wiz.workplan_status.value
    plan = _load_workplan(target)
    assert plan.name == slug
    forge, run = plan.steps
    assert [forge.name, run.name] == ["forge", "ucla_roms"]
    assert forge.application == "forge"
    assert Path(str(forge.blueprint_path)) == (tmp_path / "bp.yaml").resolve()
    assert not forge.compute_overrides
    assert run.depends_on == ["forge"]
    assert isinstance(run.blueprint_path, DeferredBlueprintRef)
    assert run.blueprint_path.from_step == "forge"
    assert run.compute_overrides["slurm"]["num_cpus"] == wiz.config.n_procs
    assert "download=" in wiz.workplan_download.value
    assert calls == [1]
    assert "cstar workplan submit" in wiz.workplan_status.value


def test_save_workplan_asks_before_replacing_a_different_file(tmp_path):
    wiz = _saved_wizard(tmp_path)
    wiz._on_save(None)
    wiz._on_save_workplan(None)
    target = next(Path(wiz.catalog.workplans_dir).glob("*.yaml"))
    good = target.read_text()
    target.write_text("different: true\n")
    wiz._on_save_workplan(None)
    assert wiz.save_workplan_btn.description == "Confirm overwrite"
    assert wiz.save_workplan_btn.button_style == "danger"
    assert target.read_text() == "different: true\n"
    wiz._on_save_workplan(None)
    assert target.read_text() == good
    assert wiz.save_workplan_btn.description == "Save workplan"
    assert wiz.save_workplan_btn.button_style == "primary"
    # identical text: no confirm
    wiz._on_save_workplan(None)
    assert wiz.save_workplan_btn.description == "Save workplan"
    # an edit disarms
    target.write_text("different: true\n")
    wiz._on_save_workplan(None)
    assert wiz._workplan_confirm == target
    wiz.run_card.end.value = date(2012, 1, 3)
    assert wiz._workplan_confirm is None
    assert wiz.save_workplan_btn.description == "Save workplan"


def test_save_workplan_without_a_current_save_is_refused(tmp_path):
    wiz = _saved_wizard(tmp_path)
    wiz._on_save_workplan(None)
    assert "Save the blueprint above first" in wiz.workplan_status.value
    assert not list(Path(wiz.catalog.workplans_dir).glob("*.yaml"))
    wiz._on_save(None)
    (tmp_path / "bp.yaml").unlink()
    wiz._on_save_workplan(None)
    assert "gone" in wiz.workplan_status.value


def test_open_in_builder_calls_the_hook(tmp_path):
    seen = []

    def open_steps(path, name):
        seen.append((path, name))
        if len(seen) > 1:
            raise ValueError("no good")

    wiz = _saved_wizard(
        tmp_path, hooks=WorkplanHooks(open_steps=open_steps, saved=lambda: None)
    )
    wiz._on_save(None)
    wiz._on_open_workplan(None)
    assert seen == [((tmp_path / "bp.yaml").resolve(), wiz.config.name)]
    assert "Opened the Workplan page" in wiz.workplan_status.value
    wiz._on_open_workplan(None)
    assert "no good" in wiz.workplan_status.value


def test_run_log_lands_while_the_pane_is_collapsed(monkeypatch, tmp_path):
    import asyncio

    wiz = _saved_wizard(tmp_path)
    _ = wiz.widget
    wiz.run_forge_pane.selected_index = None

    async def _fake(*args, **kwargs):
        return _FakeProcess([b"line one\n"], returncode=0)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake)
    wiz._on_run(None)
    assert "line one" in "".join(o["text"] for o in wiz.run_output.outputs)
