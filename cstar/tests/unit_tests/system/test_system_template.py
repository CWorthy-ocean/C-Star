"""Tests for the commented starting system file `cstar system template` prints."""

import inspect
import typing as t

import pytest
import yaml
from pydantic import BaseModel

from cstar.system.spec import PBSSchedulerSpec, SystemSpec
from cstar.system.template import (
    MARKER_PATTERN,
    REQUIRED_NOTE,
    SKELETON,
    describe,
    render_system_template,
    spec_models,
    wrap_comment,
)

if t.TYPE_CHECKING:
    from collections.abc import Callable


def marker_refs() -> list[str]:
    """The reference of every marker in the skeleton, in order."""
    return [
        m["ref"] for line in SKELETON.splitlines() if (m := MARKER_PATTERN.match(line))
    ]


def field_owner(model: type[BaseModel], field: str) -> str:
    """The name of the class in `model`'s hierarchy that declares `field`."""
    for klass in model.__mro__:
        if field in inspect.get_annotations(klass):
            return klass.__name__
    raise KeyError(f"{model.__name__} has no field {field!r}")


def uncomment(text: str, header: str) -> str:
    """Uncomment the block that starts at the line ``# <header>``.

    The block is that line and the lines after it that go on with the same
    indentation followed by ``#`` and a deeper indentation.
    """
    lines = text.splitlines()
    start = next(i for i, ln in enumerate(lines) if ln.lstrip() == f"# {header}")
    indent = lines[start][: len(lines[start]) - len(lines[start].lstrip())]
    lines[start] = indent + header
    for i in range(start + 1, len(lines)):
        if not lines[i].startswith(f"{indent}#  "):
            break
        lines[i] = indent + lines[i][len(indent) + 2 :]
    return "\n".join(lines) + "\n"


def drop_block(text: str, header: str) -> str:
    """Remove the top-level block that starts at the line `header`, up to the
    next blank line.
    """
    lines = text.splitlines()
    start = lines.index(header)
    end = lines.index("", start)
    return "\n".join(lines[:start] + lines[end:]) + "\n"


def validate(text: str) -> SystemSpec:
    """Read a rendered system file as a spec."""
    return SystemSpec.model_validate(yaml.safe_load(text))


def test_rendered_template_validates_at_the_current_schema() -> None:
    """The file as printed is a valid system file declaring the current schema."""
    spec = validate(render_system_template())

    assert spec.schema_version == SystemSpec.model_fields["schema_version"].default
    assert spec.name == "mycluster"


def test_rendered_template_has_no_markers_left() -> None:
    """Every marker is replaced by text."""
    rendered = render_system_template()

    assert not [ln for ln in rendered.splitlines() if MARKER_PATTERN.match(ln)]
    assert "@schema_version@" not in rendered


def test_every_field_a_system_file_can_hold_has_a_marker() -> None:
    """A field added to any spec model fails here until the template describes it.

    Fields shared through a base class (the two scheduler families) are
    described once.
    """
    models = spec_models()
    fields = {
        (field_owner(model, name), name)
        for model in models.values()
        for name in model.model_fields
    }
    marked = {
        (field_owner(models[model], name), name)
        for model, _, name in (ref.partition(".") for ref in marker_refs())
        if name
    }

    assert fields - marked == set()


def test_every_marker_names_a_described_model_or_field() -> None:
    """A marker for a renamed field, or a field with no description, is caught."""
    models = spec_models()

    for ref in marker_refs():
        assert describe(ref, models)


def test_spec_models_reach_every_nested_model() -> None:
    """Both scheduler families and both detect rules are found from `SystemSpec`."""
    assert {
        "SystemSpec",
        "ToolchainSpec",
        "DetectEnv",
        "DetectPath",
        "ScratchSpec",
        "SlurmSchedulerSpec",
        "SlurmQueueSpec",
        "PBSSchedulerSpec",
        "PBSQueueSpec",
        "GpuNodesSpec",
    } <= set(spec_models())


def test_describe_marks_required_fields_only() -> None:
    """A required field's text ends with the note; an optional one's does not."""
    models = spec_models()

    assert describe("SystemSpec.name", models).endswith(REQUIRED_NOTE)
    assert not describe("SystemSpec.docs", models).endswith(REQUIRED_NOTE)


def test_describe_refuses_an_unknown_reference() -> None:
    """A typo in a marker fails loudly rather than printing nothing."""
    with pytest.raises(KeyError):
        describe("SystemSpec.nope", spec_models())


def test_wrap_comment_never_breaks_a_code_span() -> None:
    """A space inside backticks is not a place to wrap."""
    text = "word " * 12 + "`srun --kill-on-bad-exit=1` " + "word " * 5

    lines = wrap_comment(text, "    # ")

    assert any("`srun --kill-on-bad-exit=1`" in ln for ln in lines)
    assert all(ln.startswith("    # ") for ln in lines)


@pytest.mark.parametrize(
    ("header", "takes_effect"),
    [
        ("detect:", lambda spec: len(spec.detect) == 2),
        ("scratch:", lambda spec: spec.scratch is not None),
        ("gpu:", lambda spec: "gpu" in spec.toolchains),
        ("gpu_nodes:", lambda spec: spec.scheduler.gpu_nodes is not None),
    ],
)
def test_each_commented_out_block_validates_when_uncommented(
    header: str, takes_effect: "Callable[[SystemSpec], bool]"
) -> None:
    """Uncommenting an optional block as it stands gives a valid file that uses it."""
    spec = validate(uncomment(render_system_template(), header))

    assert takes_effect(spec)


def test_pbs_alternative_validates_in_place_of_the_slurm_block() -> None:
    """Swapping the SLURM block for the commented PBS one gives a valid PBS file."""
    rendered = render_system_template()
    text = uncomment(drop_block(rendered, "scheduler:"), "scheduler:")

    spec = validate(text)

    assert isinstance(spec.scheduler, PBSSchedulerSpec)
