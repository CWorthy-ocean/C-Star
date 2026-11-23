from pathlib import Path
from unittest import mock

import pytest
import yaml

from cstar.orchestration.check import deep_check
from cstar.orchestration.models import Step, Workplan


async def test_deep_check_valid_workplan(
    hello_world_bp_path: Path,
) -> None:
    """Verify a resolvable workplan yields the transformed workplan and no
    problems, without asking for a launcher when it declares no external runs.

    Parameters
    ----------
    hello_world_bp_path : Path
        Fixture providing the path to a minimal hello-world blueprint
    """
    wp = Workplan(
        name="deep-check-valid",
        description="A resolvable single-step workplan.",
        steps=[
            Step(
                name="Say Hello",
                application="hello_world",
                blueprint=hello_world_bp_path,
            )
        ],
    )
    launcher_factory = mock.Mock()

    transformed, problems = await deep_check(wp, None, launcher_factory)

    assert problems == []
    assert transformed is not None
    assert [step.name for step in transformed.steps] == ["Say Hello"]
    launcher_factory.assert_not_called()


async def test_deep_check_requires_declared_runtime_vars(
    hello_world_bp_path: Path,
) -> None:
    """Verify a declared runtime variable that is not supplied is reported as
    missing, before any step is resolved.

    Parameters
    ----------
    hello_world_bp_path : Path
        Fixture providing the path to a minimal hello-world blueprint
    """
    wp = Workplan(
        name="deep-check-runtime-vars",
        description="A workplan declaring a runtime variable.",
        runtime_vars=["alpha"],
        steps=[
            Step(
                name="Say Hello",
                application="hello_world",
                blueprint=hello_world_bp_path,
            )
        ],
    )

    launcher_factory = mock.Mock()

    transformed, problems = await deep_check(wp, None, launcher_factory)

    assert transformed is None
    assert len(problems) == 1
    assert "missing keys: alpha" in problems[0]
    launcher_factory.assert_not_called()


async def test_deep_check_reports_unknown_directive(
    hello_world_bp_path: Path,
) -> None:
    """Verify a directive the application does not define is reported as a
    problem naming the step, rather than raised.

    Parameters
    ----------
    hello_world_bp_path : Path
        Fixture providing the path to a minimal hello-world blueprint
    """
    wp = Workplan(
        name="deep-check-invalid",
        description="A workplan with a directive its application does not define.",
        steps=[
            Step(
                name="Say Hello",
                application="hello_world",
                blueprint=hello_world_bp_path,
                directives={"no-such-directive": {}},
            )
        ],
    )

    transformed, problems = await deep_check(wp, None, mock.Mock())

    assert transformed is None
    assert len(problems) == 1
    assert "Say Hello" in problems[0]
    assert "no-such-directive" in problems[0]


async def test_deep_check_anchors_relative_blueprint_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    hello_world_bp_content: str,
) -> None:
    """Verify the deep check resolves a relative blueprint path against the
    workplan's directory, as `run` does, rather than the working directory.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for the workplan and blueprint
    monkeypatch : pytest.MonkeyPatch
        Fixture used to change the working directory
    hello_world_bp_content : str
        Fixture providing the content of a minimal hello-world blueprint
    """
    plans = tmp_path / "plans"
    plans.mkdir()
    (plans / "hw.yaml").write_text(hello_world_bp_content)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)

    wp = Workplan(
        name="deep-check-relative",
        description="A step whose blueprint sits beside the workplan.",
        steps=[
            Step(name="Say Hello", application="hello_world", blueprint="./hw.yaml")
        ],
    )

    transformed, problems = await deep_check(
        wp, None, mock.Mock(), wp_path=plans / "wp.yaml"
    )

    assert problems == []
    assert transformed is not None
    assert Path(transformed.steps[0].blueprint_path) == (plans / "hw.yaml").resolve()


async def test_deep_check_reports_local_codebase(
    tmp_path: Path, complete_blueprint_path: Path
) -> None:
    """Verify a `ucla_roms` blueprint whose `code.roms` is a local checkout is
    reported as a problem, so it fails before the job is queued.

    Parameters
    ----------
    tmp_path : Path
        Temporary directory for the blueprint
    complete_blueprint_path : Path
        Fixture providing the path to a valid, complete blueprint
    """
    data = yaml.safe_load(complete_blueprint_path.read_text())
    data["code"]["roms"]["location"] = "/home/user/ucla-roms"
    blueprint_path = tmp_path / "local-roms.yaml"
    blueprint_path.write_text(yaml.safe_dump(data))
    wp = Workplan(
        name="deep-check-local-codebase",
        description="A step whose ROMS codebase is a local checkout.",
        steps=[Step(name="Run", application="ucla_roms", blueprint=blueprint_path)],
    )

    transformed, problems = await deep_check(wp, None, mock.Mock())

    assert transformed is None
    assert any(
        "'/home/user/ucla-roms' is not a remote repository URL" in p for p in problems
    ), problems
