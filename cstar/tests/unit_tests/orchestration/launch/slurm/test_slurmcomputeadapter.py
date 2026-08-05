import pytest

from cstar.base.adapter import CstarAdaptationError
from cstar.base.exceptions import CstarExpectationFailed
from cstar.orchestration.launch.slurm import SlurmComputeAdapter
from cstar.orchestration.models import KeyValueStore


@pytest.mark.parametrize(
    "compute_overrides",
    [
        pytest.param({}, id="empty overrides"),
        pytest.param(None, id="null overrides"),
    ],
)
def test_slurmcomputeadapter_adapt_null(
    compute_overrides: KeyValueStore | None,
) -> None:
    """Verify that an appropriate error is raised if the required model is null."""
    adapter = SlurmComputeAdapter()

    with pytest.raises(
        CstarExpectationFailed, match="Compute overrides were not supplied"
    ):
        _ = adapter.adapt(compute_overrides)  # type: ignore


@pytest.mark.parametrize(
    "compute_overrides",
    [
        pytest.param(
            {"local": {"max_walltime": "00:10:00"}}, id="no applicable overrides"
        ),
        pytest.param({"slurm": None}, id="null overrides"),
        pytest.param({"slurm": {}}, id="empty slurm overrides"),
    ],
)
def test_slurmcomputeadapter_adapt_empty_overrides(
    compute_overrides: KeyValueStore,
) -> None:
    """Verify that the adapter returns `None` if no slurm compute overrides can be located."""
    adapter = SlurmComputeAdapter()

    with pytest.raises(CstarAdaptationError, match="Unable to adapt model"):
        _ = adapter.adapt(compute_overrides)  # type: ignore


def test_slurmcomputeadapter_adapt_happy_path() -> None:
    """Verify that values pased via the compute overrides held on a step results in a
    correctly configured compute spec being returned.
    """
    exp_walltime = "00:10:00"

    # provide SLURM and local to verify the adapter uses the SLURM overrides
    model: KeyValueStore = {
        "local": {"max_walltime": "00:05:00"},
        "slurm": {"max_walltime": exp_walltime},
    }
    adapter = SlurmComputeAdapter()

    result = adapter.adapt(model)

    assert result
    assert result.max_walltime == exp_walltime
    # assert result.force_kill_timeout == exp_fk_timeout
