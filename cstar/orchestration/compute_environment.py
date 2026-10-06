import typing as t

from pydantic import Field, ValidationError, field_validator, model_validator

from cstar.base.adapter import ConfiguredModelAdapter, CstarAdaptationError
from cstar.base.exceptions import CstarExpectationFailed
from cstar.base.log import get_logger
from cstar.orchestration.launch.slurm import SlurmComputeSpec
from cstar.orchestration.models import (
    COMPUTE_OVERRIDE_NAMESPACES,
    ConfiguredBaseModel,
    KeyValueStore,
)

log = get_logger(__name__)

LEGACY_KEYS: t.Final[frozenset[str]] = frozenset({"num_nodes", "num_cpus_per_process"})
"""Keys the pre-0.16 workplan templates placed under `compute_environment`.

Nothing ever read them. They are dropped with a warning so workplans and
recorded runs written against those templates still load."""


class ComputeEnvironment(ConfiguredBaseModel):
    """The compute environment a workplan asks to run in."""

    launcher: str = Field(default="")
    """The launcher to run steps with; unset selects the launcher matching the
    current system (SLURM when the system has a scheduler, otherwise local)."""

    system: str = Field(default="")
    """The name of the system the workplan was written for. Informational: a
    mismatch with the current system produces a warning."""

    slurm: SlurmComputeSpec | None = Field(default=None)
    """Workplan-wide SLURM defaults, overridden by a step's own
    `compute_overrides["slurm"]`."""

    @model_validator(mode="before")
    @classmethod
    def _drop_legacy_keys(cls, data: t.Any) -> t.Any:
        """Drop the never-read legacy template keys, warning once per load."""
        if not isinstance(data, dict) or not (legacy := LEGACY_KEYS.intersection(data)):
            return data
        log.warning(
            "compute_environment key(s) %s are legacy and ignored; remove them",
            ", ".join(sorted(legacy)),
        )
        return {k: v for k, v in data.items() if k not in LEGACY_KEYS}

    @field_validator("launcher", mode="after")
    @classmethod
    def _known_launcher(cls, value: str) -> str:
        """Reject a launcher name that no launcher answers to."""
        if value and value not in COMPUTE_OVERRIDE_NAMESPACES:
            raise ValueError(
                f"unknown launcher {value!r}: expected one of "
                f"{', '.join(sorted(COMPUTE_OVERRIDE_NAMESPACES))} or unset"
            )
        return value


class ComputeEnvironmentAdapter(
    ConfiguredModelAdapter[KeyValueStore, ComputeEnvironment]
):
    """Adapts a workplan's `compute_environment` mapping into a `ComputeEnvironment`."""

    def adapt(self, model: KeyValueStore) -> ComputeEnvironment:
        """Adapt the input into a `ComputeEnvironment`.

        Parameters
        ----------
        model : KeyValueStore
            The `compute_environment` mapping to be adapted.

        Returns
        -------
        ComputeEnvironment

        Raises
        ------
        CstarExpectationFailed
            If the mapping is empty.
        CstarAdaptationError
            If the mapping contains unknown keys or invalid values; the
            message lists every problem.
        """
        if not model:
            msg = "No compute environment was supplied to the ComputeEnvironmentAdapter"
            raise CstarExpectationFailed(msg)

        try:
            return ComputeEnvironment.model_validate(model)
        except ValidationError as ex:
            problems = "; ".join(
                f"{'.'.join(str(loc) for loc in err['loc']) or '<root>'}: {err['msg']}"
                for err in ex.errors()
            )
            msg = f"Invalid compute_environment: {problems}"
            raise CstarAdaptationError(msg) from ex


def resolve_compute_environment(model: KeyValueStore) -> ComputeEnvironment:
    """Adapt a `compute_environment` mapping, treating an empty one as the
    default (no launcher or SLURM defaults requested).

    Parameters
    ----------
    model : KeyValueStore
        The workplan's `compute_environment` mapping.

    Returns
    -------
    ComputeEnvironment

    Raises
    ------
    CstarAdaptationError
        If the mapping is present but invalid.
    """
    try:
        return ComputeEnvironmentAdapter().adapt(model)
    except CstarExpectationFailed:
        return ComputeEnvironment()
