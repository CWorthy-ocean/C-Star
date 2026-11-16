"""Tests for `cstar system list`, `show` and `check`."""

import os
import time
import typing as t
from pathlib import Path

import pytest
import typer
from click.testing import Result
from typer.testing import CliRunner

from cstar.base.env import ENV_CSTAR_CATALOG
from cstar.base.exceptions import CstarError
from cstar.cli.cli import attach_subcommands
from cstar.cli.system import NAME, app
from cstar.cli.system import check as check_module
from cstar.cli.system.check import (
    STATUS_FAIL,
    STATUS_NOT_CHECKED,
    STATUS_PASS,
    CheckResult,
    check_scheduler,
    check_system,
    check_toolchain,
)
from cstar.system.environment import CStarEnvironment
from cstar.system.manager import SystemSelection, SystemSource, registered_context
from cstar.system.spec import (
    BUNDLED_SYSTEMS_DIR,
    DRAFT_LAYER,
    SystemSpec,
    read_system_file,
)

BUNDLED_ANVIL = BUNDLED_SYSTEMS_DIR / "anvil.yaml"
"""The bundled Anvil system file."""

MINIMAL = """\
schema_version: 1.0.0
name: mycluster
toolchains:
  cpu:
    compiler: gnu
    mpi_prefix: mpirun
"""
"""A system file with no modules, scheduler or scratch rule."""


def write_system(path: Path, text: str = MINIMAL) -> Path:
    """Write a system file readable by everyone but writable only by its owner."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    path.chmod(0o644)
    return path


def run(*args: str) -> Result:
    """Invoke `cstar system` with arguments."""
    return CliRunner().invoke(app, list(args), color=False)


def by_step(results: list[CheckResult]) -> dict[str, CheckResult]:
    """Index results by step name."""
    return {r.step: r for r in results}


def spec_of(text: str, tmp_path: Path) -> SystemSpec:
    """Read a system file's content as a spec."""
    return read_system_file(
        write_system(tmp_path / "x.yaml", text), DRAFT_LAYER, check_name=False
    ).spec


@pytest.fixture
def mine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, system_file_layers: None
) -> Path:
    """A user catalog root whose `systems/` directory the tests fill."""
    root = tmp_path / "mine"
    (root / "systems").mkdir(parents=True)
    monkeypatch.setenv(ENV_CSTAR_CATALOG, str(root))
    return root / "systems"


def test_system_group_is_registered_on_the_root_app() -> None:
    """`system` is a root command group, reserved against plugins."""
    root = typer.Typer()
    attach_subcommands(root)
    assert NAME in {g.name for g in root.registered_groups}


# list


def test_list_shows_the_bundled_systems_and_where_they_are_searched() -> None:
    """Each bundled system has a line with its layer, and the search path is named."""
    result = run("list")

    assert result.exit_code == 0
    lines = {line.split()[0]: line for line in result.stdout.splitlines() if line}
    assert " bundled " in lines["anvil"]
    assert "perlmutter" in lines
    assert result.stdout.rstrip().splitlines()[-1].startswith("Searched: ")


def test_list_marks_the_system_in_use(monkeypatch: pytest.MonkeyPatch) -> None:
    """The selected system's line says so, and no other does."""
    selection = SystemSelection(registered_context("anvil"), SystemSource.SETTING)
    monkeypatch.setattr("cstar.cli.system.ls.select_system", lambda: selection)

    result = run("list")

    marked = [ln for ln in result.stdout.splitlines() if ln.endswith("in use")]
    assert len(marked) == 1
    assert marked[0].startswith("anvil")


def test_list_says_once_why_no_system_is_in_use(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed selection marks no line and prints the reason at the end."""

    def refuse() -> SystemSelection:
        raise CstarError("no such system")

    monkeypatch.setattr("cstar.cli.system.ls.select_system", refuse)

    result = run("list")

    assert result.exit_code == 0
    assert "in use" not in result.stdout.replace("The system in use", "")
    assert result.stdout.count("no such system") == 1
    assert result.stdout.rstrip().endswith("no such system")


def test_list_shows_a_user_file_over_the_bundled_one(mine: Path) -> None:
    """A user file and the bundled file it shadows both appear, the latter noted."""
    write_system(mine / "anvil.yaml", BUNDLED_ANVIL.read_text())

    result = run("list")

    anvil = [ln for ln in result.stdout.splitlines() if ln.startswith("anvil")]
    assert len(anvil) == 2
    assert " user " in anvil[0]
    assert "shadowed" not in anvil[0]
    assert " bundled " in anvil[1]
    assert "shadowed by" in anvil[1]
    assert str(mine) in anvil[1].replace("~", str(Path.home()))


def test_list_shows_a_refused_file_with_its_reason(mine: Path) -> None:
    """A file C-Star cannot use is listed with why."""
    (mine / "broken.yaml").write_text("name: [")
    (mine / "broken.yaml").chmod(0o644)

    result = run("list")

    refused = [ln for ln in result.stdout.splitlines() if ln.startswith("refused:")]
    assert len(refused) == 1
    assert "broken.yaml" in refused[0]
    assert len(refused[0]) > len("refused: broken.yaml: ")


# show


def test_show_prints_the_file_and_nothing_else() -> None:
    """Standard output is exactly the file, so it can be redirected and edited."""
    result = run("show", "anvil")

    assert result.exit_code == 0
    assert result.stdout == BUNDLED_ANVIL.read_text()


def test_show_prints_the_file_in_effect(mine: Path) -> None:
    """A user file that shadows a bundled one is the one shown."""
    text = BUNDLED_ANVIL.read_text() + "# mine\n"
    write_system(mine / "anvil.yaml", text)

    result = run("show", "anvil")

    assert result.stdout == text


def test_show_of_an_unknown_name_exits_1_on_stderr() -> None:
    """The registry's message, which lists the registered systems, goes to stderr."""
    result = run("show", "nope")

    assert result.exit_code == 1
    assert result.stdout == ""
    assert "'nope' is not a registered system" in result.stderr


def test_show_of_a_context_without_a_file_exits_1(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A system registered in code has no file to print."""

    class CodeContext:
        name = "incode"

    monkeypatch.setattr(
        "cstar.cli.system.show.registered_context", lambda _name: CodeContext
    )

    result = run("show", "incode")

    assert result.exit_code == 1
    assert result.stdout == ""
    assert "defined in code" in result.stderr


# check: the file


def test_check_of_a_draft_with_another_file_name_says_how_it_installs(
    tmp_path: Path,
) -> None:
    """A draft named differently installs under its system's name."""
    draft = write_system(tmp_path / "draft.yaml")

    result = run("check", str(draft))

    assert "installs as: mycluster.yaml" in result.stdout
    assert "this file is named draft.yaml" in result.stdout
    assert result.exit_code == 0


def test_check_of_an_unparseable_file_fails_with_the_reason(tmp_path: Path) -> None:
    """A file that does not validate fails the schema step and stops the check."""
    bad = write_system(tmp_path / "bad.yaml", "name: [")

    result = run("check", str(bad))

    assert result.exit_code == 1
    lines = result.stdout.splitlines()
    assert lines[0].startswith("FAIL")
    assert "schema" in lines[0]
    assert "modules" not in result.stdout
    assert "1 passed" not in result.stdout


def test_check_of_a_missing_path_fails(tmp_path: Path) -> None:
    """A path that does not exist is a failed schema step, not a traceback."""
    result = run("check", str(tmp_path / "absent.yaml"))

    assert result.exit_code == 1
    assert result.exception is None or isinstance(result.exception, SystemExit)
    assert "FAIL" in result.stdout


def test_check_of_a_world_writable_draft_fails(tmp_path: Path) -> None:
    """C-Star refuses such a file once installed, so the check says so."""
    draft = write_system(tmp_path / "mycluster.yaml")
    draft.chmod(0o666)

    result = run("check", str(draft))

    assert result.exit_code == 1
    assert "FAIL" in result.stdout
    assert "refuses a world-writable system file" in result.stdout


def test_check_says_when_a_draft_shadows_a_bundled_system(tmp_path: Path) -> None:
    """Shadowing is information, not a failure."""
    draft = write_system(tmp_path / "anvil.yaml", BUNDLED_ANVIL.read_text())

    results = by_step(check_system(str(draft)))

    assert results["shadowing"].status == STATUS_PASS
    assert "bundled" in results["shadowing"].detail


def test_check_by_name_reads_the_file_in_effect() -> None:
    """A name is looked up as `show` does; an unknown one fails."""
    assert check_system("anvil")[0].status == STATUS_PASS
    result = run("check", "nope")
    assert result.exit_code == 1
    assert "not a registered system" in result.stdout


# check: modules and prefixes


def lmod_toolchain(tmp_path: Path) -> tuple[SystemSpec, Path]:
    """A system whose cpu toolchain lists a module."""
    text = MINIMAL + "    modules: [gcc/11.2.0]\n"
    spec = spec_of(text, tmp_path)
    return spec, tmp_path / "x.yaml"


def test_modules_are_not_checked_without_lmod(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With modules listed and no Lmod, the step and the prefixes are not checked."""
    monkeypatch.delenv("LMOD_CMD", raising=False)
    spec, path = lmod_toolchain(tmp_path)

    results = by_step(check_toolchain("cpu", spec.toolchains["cpu"], spec, path))

    assert results["modules (cpu)"].status == STATUS_NOT_CHECKED
    assert results["modules (cpu)"].detail == "Lmod is not available here"
    assert results["prefixes (cpu)"].status == STATUS_NOT_CHECKED


def test_a_toolchain_without_modules_passes_the_modules_step(tmp_path: Path) -> None:
    """There is nothing to load."""
    spec = spec_of(MINIMAL, tmp_path)

    results = by_step(
        check_toolchain("cpu", spec.toolchains["cpu"], spec, tmp_path / "x.yaml")
    )

    assert results["modules (cpu)"] == CheckResult(
        "modules (cpu)", STATUS_PASS, "no modules to load"
    )


def fake_lmod(monkeypatch: pytest.MonkeyPatch, fail: str = "") -> list[str]:
    """Pretend Lmod exists; fail the call that contains `fail`."""
    calls: list[str] = []

    def call(self: CStarEnvironment, *args: str) -> None:
        calls.append(" ".join(args))
        if fail and fail in " ".join(args):
            raise RuntimeError(f"Lmod could not run {' '.join(args)}")

    monkeypatch.setattr(CStarEnvironment, "uses_lmod", property(lambda self: True))
    monkeypatch.setattr(CStarEnvironment, "_call_lmod", call)
    return calls


def test_modules_load_from_a_reset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With Lmod, the modules load after `module reset` and the step passes."""
    calls = fake_lmod(monkeypatch)
    spec, path = lmod_toolchain(tmp_path)

    results = by_step(check_toolchain("cpu", spec.toolchains["cpu"], spec, path))

    assert calls == ["reset", "load gcc/11.2.0"]
    assert results["modules (cpu)"].status == STATUS_PASS


def test_a_module_that_does_not_load_fails_with_the_lmod_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The failure is the step's result, not a traceback."""
    fake_lmod(monkeypatch, fail="load gcc")
    spec, path = lmod_toolchain(tmp_path)

    results = check_toolchain("cpu", spec.toolchains["cpu"], spec, path)

    assert [r.step for r in results] == ["modules (cpu)"]
    assert results[0].status == STATUS_FAIL
    assert "Lmod could not run load gcc/11.2.0" in results[0].detail


def test_prefixes_must_be_non_empty_directories_with_lib(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Empty prefixes and ones without `lib/` fail; the wrapper and nf-config are checked."""
    with_lib = tmp_path / "prefix"
    (with_lib / "lib").mkdir(parents=True)
    monkeypatch.setenv("NOT_SET_HERE", "")
    text = (
        MINIMAL
        + "    env:\n"
        + f"      MPIHOME: {with_lib}\n"
        + f"      NETCDFHOME: {tmp_path}\n"
        + "      NETCDFFHOME: ${NOT_SET_HERE}\n"
    )
    spec = spec_of(text, tmp_path)
    monkeypatch.setattr(check_module.shutil, "which", lambda _cmd: None)

    results = by_step(
        check_toolchain("cpu", spec.toolchains["cpu"], spec, tmp_path / "x.yaml")
    )

    prefixes = results["prefixes (cpu)"]
    assert prefixes.status == STATUS_FAIL
    assert "NETCDFFHOME is empty" in prefixes.detail
    assert f"NETCDFHOME={tmp_path} has no lib/ directory" in prefixes.detail
    assert "MPIHOME" not in prefixes.detail
    assert results["mpi wrapper (cpu)"].status == STATUS_FAIL
    assert results["nf-config (cpu)"] == CheckResult(
        "nf-config (cpu)", STATUS_FAIL, "not on PATH"
    )


def test_prefixes_pass_with_the_wrapper_and_nf_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A complete prefix passes every prefix step."""
    prefix = tmp_path / "prefix"
    (prefix / "lib").mkdir(parents=True)
    (prefix / "bin").mkdir()
    (prefix / "bin" / "mpifort").write_text("")
    spec = spec_of(MINIMAL + f"    env:\n      MPIHOME: {prefix}\n", tmp_path)
    monkeypatch.setattr(check_module.shutil, "which", lambda cmd: f"/bin/{cmd}")

    results = check_toolchain("cpu", spec.toolchains["cpu"], spec, tmp_path / "x.yaml")

    assert {r.status for r in results} == {STATUS_PASS}
    assert {r.step for r in results} == {
        "modules (cpu)",
        "prefixes (cpu)",
        "mpi wrapper (cpu)",
        "nf-config (cpu)",
    }


def test_a_toolchain_is_unloaded_after_its_checks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The variables a toolchain sets do not leak into the next one."""
    monkeypatch.delenv("CHECK_LEAK", raising=False)
    spec = spec_of(MINIMAL + "    env:\n      CHECK_LEAK: here\n", tmp_path)

    check_toolchain("cpu", spec.toolchains["cpu"], spec, tmp_path / "x.yaml")

    assert "CHECK_LEAK" not in os.environ


def test_an_unexpected_error_in_a_step_is_a_failure_not_a_traceback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Whatever a step raises is reported against that step."""
    draft = write_system(tmp_path / "mycluster.yaml")

    def explode(_spec: SystemSpec) -> bool:
        raise ValueError("boom")

    monkeypatch.setattr(check_module, "detection_would_select", explode)

    results = by_step(check_system(str(draft)))

    assert results["detection"] == CheckResult(
        "detection", STATUS_FAIL, "ValueError: boom"
    )


# check: scheduler

SLURM = """\
schema_version: 1.0.0
name: mycluster
toolchains:
  cpu: {compiler: gnu, mpi_prefix: srun}
scheduler:
  family: slurm
  primary_queue: main
  queues:
    - {name: main, kind: partition}
    - {name: fixed, kind: qos, max_walltime: "02:00:00"}
"""


def which_only(*present: str) -> t.Callable[[str], str | None]:
    """A `shutil.which` that finds only the commands named."""
    return lambda cmd: f"/usr/bin/{cmd}" if cmd in present else None


def test_a_missing_submit_command_fails_and_a_missing_query_command_is_not_checked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The login node needs `sbatch`; the walltime needs `sinfo` (`sacctmgr` here)."""
    monkeypatch.setattr(check_module.shutil, "which", which_only())
    spec = spec_of(SLURM, tmp_path)

    results = by_step(check_scheduler(spec))

    assert results["scheduler"].status == STATUS_FAIL
    assert "sbatch is not on PATH: run this on the cluster's login node" in (
        results["scheduler"].detail
    )
    assert results["queue main"].status == STATUS_NOT_CHECKED
    assert "sinfo" in results["queue main"].detail
    assert results["queue fixed"] == CheckResult(
        "queue fixed", STATUS_PASS, "maximum walltime 02:00:00"
    )


def test_a_queue_walltime_query_passes_with_its_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The queue's own query runs and its answer is reported."""
    monkeypatch.setattr(check_module.shutil, "which", which_only("sbatch", "sinfo"))
    monkeypatch.setattr(
        "cstar.system.scheduler.SlurmPartition._default_max_walltime_method",
        property(lambda self: lambda name: "12:00:00"),
    )
    spec = spec_of(SLURM, tmp_path)

    results = by_step(check_scheduler(spec))

    assert results["scheduler"].status == STATUS_PASS
    assert results["queue main"] == CheckResult(
        "queue main", STATUS_PASS, "maximum walltime 12:00:00"
    )


@pytest.mark.parametrize(
    ("answer", "expected"),
    [
        (None, "reported no walltime limit"),
        (RuntimeError("sinfo broke"), "sinfo broke"),
    ],
)
def test_a_walltime_query_that_fails_or_answers_nothing_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    answer: object,
    expected: str,
) -> None:
    """A query that raises, or that finds no limit, is a failed step."""

    def query(_name: str) -> str | None:
        if isinstance(answer, Exception):
            raise answer
        return None

    monkeypatch.setattr(check_module.shutil, "which", which_only("sbatch", "sinfo"))
    monkeypatch.setattr(check_module, "_walltime", lambda queue: query(queue.name))
    spec = spec_of(SLURM, tmp_path)

    results = by_step(check_scheduler(spec))

    assert results["queue main"].status == STATUS_FAIL
    assert expected in results["queue main"].detail


def test_a_walltime_query_that_never_answers_times_out(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A hung `sinfo` fails the step after the timeout instead of hanging the check."""
    monkeypatch.setattr(check_module.shutil, "which", which_only("sbatch", "sinfo"))
    monkeypatch.setattr(check_module, "QUERY_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(
        "cstar.system.scheduler.SlurmPartition._default_max_walltime_method",
        property(lambda self: lambda name: time.sleep(2)),
    )
    spec = spec_of(SLURM, tmp_path)

    start = time.monotonic()
    results = by_step(check_scheduler(spec))

    assert time.monotonic() - start < 1.5
    assert results["queue main"].status == STATUS_FAIL
    assert "no answer after 0.05 seconds" in results["queue main"].detail


def test_a_pbs_queue_passes_with_its_fixed_walltime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PBS walltimes are fixed in the file; the submit command is `qsub`."""
    text = """\
schema_version: 1.0.0
name: mycluster
toolchains:
  cpu: {compiler: gnu, mpi_prefix: mpiexec}
scheduler:
  family: pbs
  primary_queue: main
  queues:
    - {name: main, max_walltime: "12:00:00"}
"""
    monkeypatch.setattr(check_module.shutil, "which", which_only("qsub"))
    spec = spec_of(text, tmp_path)

    results = by_step(check_scheduler(spec))

    assert results["scheduler"].status == STATUS_PASS
    assert results["scheduler"].detail == "/usr/bin/qsub"
    assert results["queue main"].detail == "maximum walltime 12:00:00"


def test_a_system_without_a_scheduler_has_no_scheduler_steps(tmp_path: Path) -> None:
    """A workstation file reports none."""
    assert check_scheduler(spec_of(MINIMAL, tmp_path)) == []


# check: detection, scratch and what is left


def test_detection_passes_or_says_how_to_set_the_system(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Not being detected is information with the command to fix it."""
    draft = write_system(tmp_path / "mycluster.yaml")

    monkeypatch.setattr(check_module, "detection_would_select", lambda _spec: True)
    selects = by_step(check_system(str(draft)))["detection"]
    monkeypatch.setattr(check_module, "detection_would_select", lambda _spec: False)
    declines = by_step(check_system(str(draft)))["detection"]

    assert selects == CheckResult(
        "detection", STATUS_PASS, "detection selects mycluster here"
    )
    assert declines.status == STATUS_NOT_CHECKED
    assert "`cstar env set CSTAR_SYSTEM mycluster`" in declines.detail


def test_scratch_passes_with_the_path_or_fails(tmp_path: Path) -> None:
    """The rule must match a directory here."""
    found = tmp_path / "scratch"
    found.mkdir()
    rule = "scratch:\n  match: {path}\n  subdir: sub\n"

    passing = by_step(
        check_system(
            str(write_system(tmp_path / "a.yaml", MINIMAL + rule.format(path=found)))
        )
    )
    failing = by_step(
        check_system(
            str(
                write_system(
                    tmp_path / "b.yaml",
                    MINIMAL + rule.format(path=tmp_path / "absent"),
                )
            )
        )
    )

    assert passing["scratch"] == CheckResult("scratch", STATUS_PASS, str(found / "sub"))
    assert failing["scratch"].status == STATUS_FAIL


def test_check_ends_with_what_it_could_not_check(tmp_path: Path) -> None:
    """The last lines name the detection rule in a job, and a build and a job."""
    draft = write_system(tmp_path / "mycluster.yaml")

    result = run("check", str(draft))

    lines = result.stdout.splitlines()
    assert "detection rule holds inside a job" in lines[-4]
    assert "a build and a job" in lines[-3]
    assert lines[-1].endswith("not checked")


def test_check_without_an_argument_checks_the_system_in_use(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No argument means the selected system, with no configuration file needed."""
    selection = SystemSelection(registered_context("anvil"), SystemSource.SETTING)
    monkeypatch.setattr(check_module, "select_system", lambda: selection)
    monkeypatch.setattr(check_module.shutil, "which", which_only())

    result = run("check")

    assert "schema: " in result.stdout
    assert str(BUNDLED_ANVIL) in result.stdout
    assert "modules (cpu)" in result.stdout


def test_check_reports_a_failed_selection_as_a_failed_schema_step(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the system in use cannot be determined there is nothing to check."""

    def refuse() -> SystemSelection:
        raise CstarError("no system")

    monkeypatch.setattr(check_module, "select_system", refuse)

    result = run("check")

    assert result.exit_code == 1
    assert "FAIL         schema: no system" in result.stdout


def test_a_name_with_a_separator_or_suffix_is_a_path(tmp_path: Path) -> None:
    """Only a bare word is looked up as a name."""
    assert check_module.looks_like_path("anvil.yaml")
    assert check_module.looks_like_path("dir/anvil")
    assert check_module.looks_like_path(str(write_system(tmp_path / "anvil")))
    assert not check_module.looks_like_path("anvil")
