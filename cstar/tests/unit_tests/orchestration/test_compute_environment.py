import typing as t

import pytest
from pydantic import ValidationError

from cstar.base.adapter import CstarAdaptationError
from cstar.base.exceptions import CstarExpectationFailed
from cstar.orchestration.compute_environment import (
    ComputeEnvironment,
    ComputeEnvironmentAdapter,
    resolve_compute_environment,
)
from cstar.orchestration.launch.slurm import SlurmComputeSpec
from cstar.orchestration.models import COMPUTE_OVERRIDE_NAMESPACES


def test_adapter_empty_not_expected() -> None:
    """Verify an empty mapping is "nothing for this adapter"."""
    with pytest.raises(CstarExpectationFailed):
        ComputeEnvironmentAdapter().adapt({})


def test_adapter_adapts_complete_environment() -> None:
    """Verify every documented key is adapted."""
    env = ComputeEnvironmentAdapter().adapt(
        {
            "launcher": "slurm",
            "system": "anvil",
            "slurm": {
                "account_name": "x-ees250129",
                "queue_name": "wholenode",
                "max_walltime": "04:00:00",
            },
        }
    )

    assert env.launcher == "slurm"
    assert env.system == "anvil"
    assert env.slurm == SlurmComputeSpec(
        account_name="x-ees250129", queue_name="wholenode", max_walltime="04:00:00"
    )


def test_adapter_defaults() -> None:
    """Verify unset keys take their defaults."""
    env = ComputeEnvironmentAdapter().adapt({"system": "anvil"})

    assert env.launcher == ""
    assert env.slurm is None


@pytest.mark.parametrize("launcher", ["local", "slurm", ""])
def test_launcher_names_are_accepted(launcher: str) -> None:
    """Verify the launcher names match those a step's compute overrides use."""
    assert ComputeEnvironment(launcher=launcher).launcher == launcher


def test_launcher_names_are_the_override_namespaces() -> None:
    """Verify every compute-override namespace is a selectable launcher."""
    assert {
        name
        for name in COMPUTE_OVERRIDE_NAMESPACES
        if ComputeEnvironment(launcher=name).launcher
    } == COMPUTE_OVERRIDE_NAMESPACES


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        pytest.param({"bogus": 1}, "bogus", id="unknown-key"),
        pytest.param({"launcher": "pbs"}, "pbs", id="bad-launcher"),
        pytest.param(
            {"slurm": {"max_walltime": "soon"}}, "max_walltime", id="bad-walltime"
        ),
    ],
)
def test_adapter_invalid_content(content: dict[str, t.Any], expected: str) -> None:
    """Verify invalid content raises an adaptation error naming the problem."""
    with pytest.raises(CstarAdaptationError, match=expected):
        ComputeEnvironmentAdapter().adapt(content)


def test_adapter_reports_all_problems() -> None:
    """Verify every problem is listed in the one error."""
    with pytest.raises(CstarAdaptationError) as ex:
        ComputeEnvironmentAdapter().adapt({"launcher": "pbs", "bogus": 1})

    assert "pbs" in str(ex.value)
    assert "bogus" in str(ex.value)


def test_resolve_empty_is_default() -> None:
    """Verify an empty mapping resolves to the default environment."""
    assert resolve_compute_environment({}) == ComputeEnvironment()


def test_resolve_invalid_raises() -> None:
    """Verify an invalid mapping is not defaulted."""
    with pytest.raises(CstarAdaptationError):
        resolve_compute_environment({"bogus": 1})


def test_legacy_template_keys_are_dropped_with_a_warning(caplog) -> None:
    """The pre-0.16 template keys never did anything; they load, warn, and vanish."""
    import logging

    with caplog.at_level(logging.WARNING):
        env = ComputeEnvironmentAdapter().adapt(
            {"num_nodes": 4, "num_cpus_per_process": 16, "launcher": "local"}
        )
    assert env.launcher == "local"
    assert "num_cpus_per_process" in caplog.text and "num_nodes" in caplog.text
    assert "legacy" in caplog.text


def test_adapter_rejects_unknown_slurm_key() -> None:
    """A misspelt `slurm` key is reported, not dropped: a typo'd `num_gpus` would
    otherwise run a CPU job.
    """
    with pytest.raises(CstarAdaptationError, match="num_gpu"):
        ComputeEnvironmentAdapter().adapt({"slurm": {"num_gpu": 2}})


def test_slurm_spec_unknown_key_is_rejected() -> None:
    """The spec forbids keys it does not define."""
    with pytest.raises(ValidationError, match="num_gpu"):
        SlurmComputeSpec.model_validate({"num_cpus": 4, "num_gpu": 2})


def test_slurm_spec_gpus_per_node_needs_gpus() -> None:
    """Naming GPUs per node for a CPU job is a contradiction."""
    with pytest.raises(ValidationError, match="gpus_per_node is set .* num_gpus=0"):
        SlurmComputeSpec(gpus_per_node=4)


def test_slurm_spec_single_node_gpus_must_fit_one_node() -> None:
    """A single-node job cannot ask for more GPUs than the planned node holds."""
    with pytest.raises(ValidationError, match="num_gpus=5 exceeds gpus_per_node=4"):
        SlurmComputeSpec(num_cpus=5, num_gpus=5, gpus_per_node=4, single_node=True)

    spec = SlurmComputeSpec(num_cpus=4, num_gpus=4, gpus_per_node=4, single_node=True)
    assert spec.num_gpus == 4


def test_slurm_spec_gpus_need_a_task_each() -> None:
    """More GPUs than tasks is rejected, naming `num_cpus`."""
    with pytest.raises(
        ValidationError, match=r"num_gpus=8 exceeds num_cpus=1.*num_cpus"
    ):
        SlurmComputeSpec(num_gpus=8)

    assert SlurmComputeSpec(num_cpus=8, num_gpus=8).num_gpus == 8


@pytest.mark.parametrize(
    "content",
    [{"num_gpus": -1}, {"num_gpus": 2, "gpus_per_node": 0}],
    ids=["negative-gpus", "zero-gpus-per-node"],
)
def test_slurm_spec_gpu_counts_are_bounded(content: dict[str, int]) -> None:
    """GPU counts must be non-negative (and `gpus_per_node` positive)."""
    with pytest.raises(ValidationError):
        SlurmComputeSpec.model_validate(content)


def test_slurm_spec_gpus_round_trip() -> None:
    """A GPU request survives the non-default dump the launcher overlays on its
    defaults, and a CPU spec dumps no GPU keys.
    """
    spec = SlurmComputeSpec(num_cpus=8, num_gpus=2)

    dumped = spec.model_dump(exclude_defaults=True)

    assert dumped == {"num_cpus": 8, "num_gpus": 2}
    assert SlurmComputeSpec.model_validate(dumped) == spec
    assert "num_gpus" not in SlurmComputeSpec(num_cpus=8).model_dump(
        exclude_defaults=True
    )
