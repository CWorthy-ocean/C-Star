from pathlib import Path
from unittest import mock

import pytest

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
        name="Deep Check Valid",
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
        name="Deep Check Invalid",
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
        name="Deep Check Relative",
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
