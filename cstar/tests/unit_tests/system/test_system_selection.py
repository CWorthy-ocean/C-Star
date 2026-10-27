import logging
import os
import platform
import stat
from collections.abc import Generator
from pathlib import Path
from unittest import mock

import pytest

from cstar.base.env import ENV_CSTAR_SYSTEM
from cstar.base.exceptions import CstarError
from cstar.system import manager
from cstar.system.environment import CStarEnvironment
from cstar.system.manager import (
    CTX_REGISTRY,
    AnvilSystemContext,
    BouchetSystemContext,
    CStarSystemManager,
    HostNameEvaluator,
    LinuxSystemContext,
    PerlmutterSystemContext,
    SystemContext,
    SystemSelection,
    SystemSource,
    detect_system,
    get_system_context,
    registered_context,
    select_system,
    system_label,
    warn_fallback,
    warn_once,
)
from cstar.system.scheduler import VISIBLE_SCHEDULERS, visible_scheduler

GENERIC_CONTEXT_NAMES = frozenset({"linux_x86_64", "linux_aarch64", "darwin_arm64"})
"""The platform contexts, which C-Star ships with no Lmod module list."""

BUNDLED_CONTEXTS = sorted(
    (ctx for ctx in CTX_REGISTRY.values() if ctx.__module__ == manager.__name__),
    key=lambda ctx: ctx.name,
)
"""The contexts C-Star ships, excluding any a test registered."""

DETECTION_VARIABLES = ("RCAC_CLUSTER", "CLUSTER", "SLURM_CLUSTER_NAME", "HOSTNAME")
"""Variables the shipped contexts read to recognise their machine."""


@pytest.fixture(autouse=True)
def _unrecognisable_machine(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[None]:
    """Strip every hint of the host's identity from the environment, and restore the
    environment (which building a system manager also writes to) afterwards.
    """
    hints = [name for name in os.environ if name.startswith("LMOD_")]
    with mock.patch.dict(os.environ):
        for name in (*hints, *DETECTION_VARIABLES):
            monkeypatch.delenv(name, raising=False)
        yield


@pytest.fixture(autouse=True)
def _linux_x86_64(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin the platform to x86-64 Linux, whatever machine runs the test.

    `cstar.system.manager` and `cstar.system.environment` both ask the one
    `platform` module, so this decides the generic context and `uses_lmod` alike.
    """
    monkeypatch.setattr(platform, "system", lambda: "Linux")
    monkeypatch.setattr(platform, "machine", lambda: "x86_64")


def fallback_messages(caplog: pytest.LogCaptureFixture) -> list[str]:
    """Return the logged messages about falling back to a generic context."""
    return [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.WARNING
        and "does not recognise the system" in record.getMessage()
    ]


def test_unregistered_lmod_name_falls_back_to_the_platform_generic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cluster that names itself through Lmod but is not registered gets the
    generic context of its platform, remembering the name it reported.
    """
    monkeypatch.setenv("LMOD_SYSHOST", "frontera")

    selection = select_system()

    assert selection.context is LinuxSystemContext
    assert selection.source is SystemSource.FALLBACK
    assert selection.detected == "frontera"


def test_unregistered_lmod_name_warns_once_per_process(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Building the system manager twice on an unrecognised cluster logs one warning
    that names the cluster, the generic context and how to choose a system.
    """
    monkeypatch.setenv("LMOD_SYSHOST", "frontera")

    with caplog.at_level(logging.WARNING):
        CStarSystemManager()
        CStarSystemManager()

    (message,) = fallback_messages(caplog)
    assert "'frontera'" in message
    assert LinuxSystemContext.name in message
    assert ENV_CSTAR_SYSTEM in message
    assert manager.DOCS_MACHINES_URL in message


def test_unregistered_lmod_name_does_not_call_lmod(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A generic context on a cluster with Lmod neither resets modules nor looks for
    a module list the generic context does not have.
    """
    monkeypatch.setenv("LMOD_SYSHOST", "frontera")
    monkeypatch.setenv("LMOD_CMD", "/nonexistent/lmod")

    with mock.patch.object(CStarEnvironment, "_call_lmod") as call_lmod:
        sysmgr = CStarSystemManager()

    call_lmod.assert_not_called()
    assert sysmgr.name == LinuxSystemContext.name
    assert sysmgr.environment.uses_lmod is False


@pytest.mark.parametrize(
    ("node", "expected"),
    [
        pytest.param("login1.example.edu", "login1.example.edu", id="named"),
        pytest.param("", "unknown", id="unnamed"),
    ],
)
def test_lmod_without_a_system_name_falls_back_to_the_platform_generic(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    node: str,
    expected: str,
) -> None:
    """A machine with Lmod that names no system and matches no context is a cluster
    C-Star does not know: the generic context is a fallback, reported under the
    machine's network name (else "unknown"), that does not call Lmod.
    """
    monkeypatch.setenv("LMOD_CMD", "/nonexistent/lmod")
    monkeypatch.setattr(platform, "node", lambda: node)

    selection = select_system()
    with (
        caplog.at_level(logging.WARNING),
        mock.patch.object(CStarEnvironment, "_call_lmod") as call_lmod,
    ):
        sysmgr = CStarSystemManager()

    assert selection == SystemSelection(
        LinuxSystemContext, SystemSource.FALLBACK, expected
    )
    (message,) = fallback_messages(caplog)
    assert f"'{expected}'" in message
    call_lmod.assert_not_called()
    assert sysmgr.environment.uses_lmod is False


def test_lmod_with_a_matching_context_is_detected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A context that recognises its own machine is detected, not a fallback, when
    Lmod names no system.
    """
    monkeypatch.setenv("LMOD_CMD", "/nonexistent/lmod")
    monkeypatch.setenv("RCAC_CLUSTER", "anvil")

    assert select_system() == SystemSelection(AnvilSystemContext, SystemSource.DETECTED)


def test_registered_cluster_still_loads_its_modules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A registered cluster with Lmod resets the modules and loads its module list."""
    monkeypatch.setenv("LMOD_SYSHOST", "perlmutter")
    monkeypatch.setenv("LMOD_CMD", "/nonexistent/lmod")

    with mock.patch.object(CStarEnvironment, "_call_lmod") as call_lmod:
        sysmgr = CStarSystemManager()

    assert sysmgr.environment.uses_lmod is True
    assert call_lmod.call_args_list[0] == mock.call("reset")
    assert call_lmod.call_count > 1


@pytest.mark.parametrize("context", BUNDLED_CONTEXTS, ids=lambda ctx: ctx.name)
def test_only_the_platform_contexts_lack_a_module_list(
    monkeypatch: pytest.MonkeyPatch, context: type[SystemContext]
) -> None:
    """Every shipped context but the three platform generics has a module list, and
    uses Lmod where `LMOD_CMD` is set only when it does.
    """
    monkeypatch.setenv("LMOD_CMD", "/nonexistent/lmod")
    has_module_list = context.name not in GENERIC_CONTEXT_NAMES

    with mock.patch.object(CStarEnvironment, "_call_lmod"):
        env = CStarEnvironment(context.name, context.mpi_prefix, context.compiler)

    assert env.lmod_path.is_file() is has_module_list
    assert env.uses_lmod is has_module_list


def test_setting_selects_a_registered_context_over_detection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`CSTAR_SYSTEM` beats Lmod and a context that matches its own machine,
    ignoring case and surrounding whitespace.
    """
    monkeypatch.setenv("LMOD_SYSHOST", "perlmutter")
    monkeypatch.setenv("RCAC_CLUSTER", "anvil")
    monkeypatch.setenv(ENV_CSTAR_SYSTEM, " Bouchet ")

    selection = select_system()

    assert selection.context is BouchetSystemContext
    assert selection.source is SystemSource.SETTING
    assert ENV_CSTAR_SYSTEM in selection.describe()
    assert CStarSystemManager().name == "bouchet"


def test_blank_setting_is_detection(monkeypatch: pytest.MonkeyPatch) -> None:
    """A `CSTAR_SYSTEM` of only whitespace leaves the system to detection."""
    monkeypatch.setenv("LMOD_SYSHOST", "perlmutter")
    monkeypatch.setenv(ENV_CSTAR_SYSTEM, "  ")

    selection = select_system()

    assert selection.context is PerlmutterSystemContext
    assert selection.source is SystemSource.DETECTED


def test_unknown_setting_is_refused_listing_the_registered_systems(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A `CSTAR_SYSTEM` that names no registered system raises, listing every one
    that is registered, and the label that displays the system says why instead.
    """
    monkeypatch.setenv(ENV_CSTAR_SYSTEM, "frontera")

    with pytest.raises(CstarError) as excinfo:
        select_system()

    message = str(excinfo.value)
    assert ENV_CSTAR_SYSTEM in message
    for name in CTX_REGISTRY:
        assert name in message
    assert system_label().startswith("not determined:")


def test_no_generic_context_for_the_platform_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unrecognised system on a platform with no generic context raises, telling
    the user to set `CSTAR_SYSTEM`.
    """
    monkeypatch.setenv("LMOD_SYSHOST", "frontera")
    monkeypatch.setattr(
        HostNameEvaluator, "platform_hostname", property(lambda _: "darwin_x86_64")
    )

    with pytest.raises(CstarError, match=ENV_CSTAR_SYSTEM) as excinfo:
        select_system()

    assert "frontera" in str(excinfo.value)
    assert "darwin_x86_64" in str(excinfo.value)
    assert system_label().startswith("not determined:")


def test_undeterminable_system_label_does_not_raise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When no system name can be determined at all, selecting raises `OSError` and
    the label that displays the system says why instead.
    """
    monkeypatch.setattr(platform, "system", lambda: "")
    monkeypatch.setattr(platform, "machine", lambda: "")

    with pytest.raises(OSError, match="cannot determine your system name"):
        select_system()

    assert system_label().startswith("not determined:")


def test_unregistered_lmod_name_with_a_matching_context_is_detected(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A context that recognises its own machine is used, as detected and without a
    warning, when the Lmod name is not registered.
    """
    monkeypatch.setenv("LMOD_SYSHOST", "weird")
    monkeypatch.setenv("RCAC_CLUSTER", "anvil")

    with caplog.at_level(logging.WARNING):
        sysmgr = CStarSystemManager()

    assert sysmgr.selection == SystemSelection(
        AnvilSystemContext, SystemSource.DETECTED
    )
    assert fallback_messages(caplog) == []


def test_registered_system_is_detected_without_a_warning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A registered Lmod system name is detected as before, with no warning."""
    monkeypatch.setenv("LMOD_SYSTEM_NAME", "Perlmutter")

    with caplog.at_level(logging.WARNING):
        sysmgr = CStarSystemManager()

    assert sysmgr.selection == SystemSelection(
        PerlmutterSystemContext, SystemSource.DETECTED
    )
    assert fallback_messages(caplog) == []


def test_platform_generic_detected_without_lmod_is_not_a_fallback(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A machine with no Lmod and no matching cluster is detected as its platform's
    generic context, which is not a fallback.
    """
    monkeypatch.delenv("LMOD_CMD", raising=False)

    with caplog.at_level(logging.WARNING):
        selection = detect_system()
        warn_fallback(selection)

    assert selection == SystemSelection(LinuxSystemContext, SystemSource.DETECTED)
    assert fallback_messages(caplog) == []


def test_get_system_context_falls_back_without_a_warning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Asking only for the context of an unrecognised system returns the generic
    one and leaves the warning to the system manager.
    """
    monkeypatch.setenv("LMOD_SYSHOST", "frontera")

    with caplog.at_level(logging.WARNING):
        ctx = get_system_context()

    assert ctx.name == LinuxSystemContext.name
    assert fallback_messages(caplog) == []


def test_warn_fallback_warns_once_per_unrecognised_system(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The fallback warning is logged once for each distinct unrecognised system."""
    frontera = SystemSelection(LinuxSystemContext, SystemSource.FALLBACK, "frontera")
    stampede = SystemSelection(LinuxSystemContext, SystemSource.FALLBACK, "stampede")

    with caplog.at_level(logging.WARNING):
        for selection in (frontera, frontera, stampede, frontera):
            warn_fallback(selection)

    first, second = fallback_messages(caplog)
    assert "'frontera'" in first
    assert "'stampede'" in second


def test_warn_once_logs_each_message_once(caplog: pytest.LogCaptureFixture) -> None:
    """A system warning is logged once per process, and a different one still is."""
    with caplog.at_level(logging.WARNING):
        warn_once("first")
        warn_once("first")
        warn_once("second")

    assert [record.getMessage() for record in caplog.records] == ["first", "second"]


@pytest.mark.parametrize("source", [SystemSource.DETECTED, SystemSource.SETTING])
def test_warn_fallback_ignores_a_system_that_was_not_a_fallback(
    caplog: pytest.LogCaptureFixture, source: SystemSource
) -> None:
    """Only a fallback to the generic context is warned about."""
    with caplog.at_level(logging.WARNING):
        warn_fallback(SystemSelection(LinuxSystemContext, source, "frontera"))

    assert caplog.records == []


@pytest.mark.parametrize(
    ("source", "detected", "expected"),
    [
        pytest.param(SystemSource.DETECTED, "", ["linux_x86_64"], id="detected"),
        pytest.param(
            SystemSource.SETTING, "", ["linux_x86_64", ENV_CSTAR_SYSTEM], id="setting"
        ),
        pytest.param(
            SystemSource.FALLBACK,
            "frontera",
            ["linux_x86_64", "generic", "'frontera'"],
            id="fallback",
        ),
    ],
)
def test_selection_describes_how_the_context_was_chosen(
    source: SystemSource, detected: str, expected: list[str]
) -> None:
    """A selection names its context, and says how it was chosen unless detected."""
    description = SystemSelection(LinuxSystemContext, source, detected).describe()

    assert all(part in description for part in expected)
    if source is SystemSource.DETECTED:
        assert description == LinuxSystemContext.name


@pytest.mark.parametrize(
    ("context", "source", "expected"),
    [
        pytest.param(LinuxSystemContext, SystemSource.FALLBACK, "SLURM", id="fallback"),
        pytest.param(
            LinuxSystemContext, SystemSource.SETTING, "SLURM", id="setting-generic"
        ),
        pytest.param(
            LinuxSystemContext, SystemSource.DETECTED, "", id="detected-generic"
        ),
        pytest.param(
            AnvilSystemContext, SystemSource.DETECTED, "", id="detected-slurm"
        ),
        pytest.param(AnvilSystemContext, SystemSource.SETTING, "", id="setting-slurm"),
    ],
)
def test_unsupported_scheduler_is_asked_of_a_context_that_has_none_unless_detected(
    monkeypatch: pytest.MonkeyPatch,
    context: type[SystemContext],
    source: SystemSource,
    expected: str,
) -> None:
    """A visible scheduler is unsupported when the context in use has none and C-Star
    did not detect it: the fallback, or a generic context pinned by `CSTAR_SYSTEM`.
    A detected system, or one with a scheduler of its own, is trusted.
    """
    environ = {"PATH": "/somewhere"}
    seen = mock.Mock(return_value="SLURM")
    monkeypatch.setattr("cstar.system.manager.visible_scheduler", seen)

    selection = SystemSelection(context, source, "frontera")

    assert selection.unsupported_scheduler(environ) == expected
    if expected:
        seen.assert_called_once_with(environ)
    else:
        seen.assert_not_called()


@pytest.mark.parametrize("source", [SystemSource.FALLBACK, SystemSource.SETTING])
def test_generic_context_without_a_visible_scheduler_has_no_unsupported_scheduler(
    tmp_path: Path, source: SystemSource
) -> None:
    """A generic context on a machine that shows no scheduler has nothing unsupported."""
    selection = SystemSelection(LinuxSystemContext, source, "frontera")

    assert selection.unsupported_scheduler({"PATH": str(tmp_path)}) == ""


@pytest.mark.parametrize("name", ["anvil", " Linux_X86_64\n", "PERLMUTTER"])
def test_registered_context_ignores_case_and_whitespace(name: str) -> None:
    """A registered context is found by its name however it is cased or padded."""
    assert registered_context(name) is CTX_REGISTRY[name.strip().casefold()]


def test_registered_context_refuses_an_unknown_name() -> None:
    """An unknown name raises, quoting it and listing the registered systems."""
    with pytest.raises(CstarError, match="'frontera'") as excinfo:
        registered_context("frontera")

    for name in ("anvil", "linux_x86_64", "perlmutter"):
        assert name in str(excinfo.value)


@pytest.mark.parametrize(
    ("command", "family"), [(cmd, fam) for fam, cmd, _ in VISIBLE_SCHEDULERS]
)
def test_visible_scheduler_finds_a_submit_command_on_the_path(
    tmp_path: Path, command: str, family: str
) -> None:
    """A submit command on the environment's `PATH` names its scheduler family."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    submit = bin_dir / command
    submit.touch()
    submit.chmod(submit.stat().st_mode | stat.S_IXUSR)

    assert visible_scheduler({"PATH": str(bin_dir)}) == family


def test_visible_scheduler_searches_the_path_it_is_given() -> None:
    """The command is looked for on the environment's `PATH`, not the process's."""
    which = mock.Mock(return_value=None)

    visible_scheduler({"PATH": "/a:/b"}, which=which)

    assert which.call_args_list == [
        mock.call(command, path="/a:/b") for _, command, _ in VISIBLE_SCHEDULERS
    ]


@pytest.mark.parametrize(
    ("found", "environ", "expected"),
    [
        pytest.param("sbatch", {}, "SLURM", id="sbatch"),
        pytest.param("qsub", {}, "PBS", id="qsub"),
        pytest.param(None, {"SLURM_CONF": "/etc/slurm.conf"}, "SLURM", id="slurm-var"),
        pytest.param(None, {"PBS_SERVER": "head"}, "PBS", id="pbs-var"),
        pytest.param("qsub", {"SLURM_CONF": "/etc/slurm.conf"}, "SLURM", id="both"),
        pytest.param(None, {"PATH": "/a", "HOME": "/h"}, "", id="nothing"),
    ],
)
def test_visible_scheduler_names_the_family(
    found: str | None, environ: dict[str, str], expected: str
) -> None:
    """The scheduler family is named by a submit command found, or by a variable the
    scheduler sets, and is empty when the machine shows neither.
    """

    def which(command: str, path: str | None = None) -> str | None:
        return f"/bin/{command}" if command == found else None

    assert visible_scheduler(environ, which=which) == expected
