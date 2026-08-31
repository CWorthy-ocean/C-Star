import pytest

from cstar.base.exceptions import CstarExpectationFailed
from cstar.orchestration.launch.local import LocalComputeAdapter
from cstar.orchestration.models import KeyValueStore


@pytest.mark.parametrize(
    "compute_overrides",
    [
        pytest.param({}, id="empty overrides"),
        pytest.param(None, id="null overrides"),
    ],
)
def test_localcomputeadapter_adapt_null(
    compute_overrides: KeyValueStore | None,
) -> None:
    """Verify that an appropriate error is raised if the required model is null."""
    adapter = LocalComputeAdapter()

    with pytest.raises(
        CstarExpectationFailed,
        match="Compute overrides were not supplied to the LocalComputeAdapter",
    ):
        _ = adapter.adapt(compute_overrides)  # type: ignore


@pytest.mark.parametrize(
    "compute_overrides",
    [
        pytest.param(
            {"slurm": {"max_walltime": "00:10:00"}}, id="no applicable overrides"
        ),
        pytest.param({"local": None}, id="null overrides"),
        pytest.param({"local": {}}, id="empty local overrides"),
    ],
)
def test_localcomputeadapter_adapt_empty_overrides(
    compute_overrides: KeyValueStore,
) -> None:
    """Verify the adapter signals it cannot attempt adaptation when no local
    compute overrides can be located.
    """
    adapter = LocalComputeAdapter()

    with pytest.raises(CstarExpectationFailed, match="overrides were supplied"):
        _ = adapter.adapt(compute_overrides)


def test_localcomputeadapter_adapt_happy_path() -> None:
    """Verify that values pased via the compute overrides held on a step results in a
    correctly configured compute spec being returned.
    """
    exp_walltime = "00:05:00"
    exp_fk_timeout = "00:02"

    # provide SLURM and local to verify the adapter uses the local overrides
    model: KeyValueStore = {
        "local": {"max_walltime": exp_walltime, "force_kill_timeout": exp_fk_timeout},
        "slurm": {"max_walltime": "00:10:00", "force_kill_timeout": "45s"},
    }
    adapter = LocalComputeAdapter()

    result = adapter.adapt(model)

    assert result
    assert result.max_walltime == exp_walltime
    assert result.force_kill_timeout == exp_fk_timeout
