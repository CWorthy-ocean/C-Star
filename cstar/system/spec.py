"""A system's definition as data: the YAML file C-Star reads for each machine.

A system file names the machine, the toolchains it offers (the modules to load
and the environment variables to set for each), its scheduler and queues, how
C-Star recognises it, and where its scratch file system is when no variable
names it. `SystemSpec` validates one file; `cstar.system.manager` turns each
spec into a `SystemContext`. The machines C-Star ships are read from
``cstar/catalog/bundled/systems/<name>.yaml``.

This module imports nothing from `cstar.system.manager` or `cstar.catalog`: the
manager is imported by nearly everything, and the catalog scans its stack.
"""

import getpass
import glob
import hashlib
import os
import re
import typing as t
from dataclasses import dataclass
from pathlib import Path

import yaml
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PositiveInt,
    ValidationError,
    create_model,
    field_validator,
    model_validator,
)

from cstar.base.exceptions import CstarError
from cstar.base.log import get_logger
from cstar.system.environment import EnvSettingsBase, SlurmSettingsBase
from cstar.system.migration import VersionedDocument
from cstar.system.scheduler import (
    PBSQueue,
    PBSScheduler,
    Scheduler,
    SlurmPartition,
    SlurmQOS,
    SlurmQueue,
    SlurmScheduler,
    query_max_walltime_via_sacctmgr,
    query_max_walltime_via_sinfo,
)
from cstar.system.toolchain import GpuNodes, Toolchain, ToolchainName

if t.TYPE_CHECKING:
    from collections.abc import Callable

log = get_logger(__name__)

BUNDLED_SYSTEMS_DIR: t.Final = (
    Path(__file__).resolve().parents[1] / "catalog" / "bundled" / "systems"
)
"""The directory holding the system files C-Star ships, one ``<name>.yaml`` each."""

BUNDLED_LAYER: t.Final = "bundled"
"""The layer of a system file C-Star ships."""

SYSTEM_FILE_SUFFIX: t.Final = ".yaml"
"""The suffix of a system file; its stem is the system's name."""

USER_PLACEHOLDER: t.Final = "{user}"
"""Replaced by the current user's name in a scratch rule."""

REQUIRE_ACCOUNT: t.Final = "account"
"""A `require` entry: a job must name the account it is charged to."""
REQUIRE_QUEUE: t.Final = "queue"
"""A `require` entry: a job must name its queue."""

_REQUIRED_FIELDS: t.Final[dict[str, str]] = {
    REQUIRE_ACCOUNT: "SLURM_ACCOUNT",
    REQUIRE_QUEUE: "SLURM_QUEUE",
}
"""The settings field each `require` entry makes required."""

WALLTIME_QUERIES: t.Final[dict[str, "Callable[[str], str | None]"]] = {
    "sinfo": query_max_walltime_via_sinfo,
    "sacctmgr": query_max_walltime_via_sacctmgr,
}
"""The SLURM walltime sources a queue may name, and the query each runs."""

_WALLTIME_PATTERN: t.Final = r"^(\d+-)?\d{1,2}:\d{2}:\d{2}$"
_NAME_PATTERN: t.Final = r"^[a-z0-9_-]+$"
_VARIABLE_PATTERN: t.Final = r"^[A-Za-z_][A-Za-z0-9_]*$"


def current_user() -> str:
    """Best-effort current username; never raises (containers and CI may lack ``$USER``)."""
    if user := os.environ.get("USER"):
        return user
    try:
        return getpass.getuser()
    except Exception:
        return "unknown"


def path_is_dir(path: str) -> bool:
    """Tell whether `path` is a directory, for a ``detect`` path rule.

    The one place detection touches the file system, so a test can replace it.
    Never raises: an unreadable path is not a match.
    """
    try:
        return Path(path).is_dir()
    except OSError:
        return False


def _refuse_unquoted_walltime(value: t.Any) -> t.Any:
    """Explain the YAML trap behind a number where a walltime belongs."""
    if isinstance(value, int | float) and not isinstance(value, bool):
        raise ValueError(
            f"a walltime must be quoted: YAML reads an unquoted 12:00:00 as the "
            f'number {value!r}; write it as "HH:MM:SS" or "D-HH:MM:SS"'
        )
    return value


def _number_to_str(value: t.Any) -> t.Any:
    """Read an unquoted number in ``env`` as the string it spells."""
    if isinstance(value, int | float) and not isinstance(value, bool):
        return str(value)
    return value


Walltime = t.Annotated[
    str,
    BeforeValidator(_refuse_unquoted_walltime),
    Field(pattern=_WALLTIME_PATTERN),
]
"""A fixed walltime, ``HH:MM:SS`` or ``D-HH:MM:SS``, quoted in YAML."""

EnvValue = t.Annotated[str, BeforeValidator(_number_to_str)]
"""An environment variable's value; an unquoted number is read as a string."""

VariableName = t.Annotated[str, Field(pattern=_VARIABLE_PATTERN)]
"""The name of an environment variable."""


class _SpecModel(BaseModel):
    """The configuration every part of a system file shares."""

    model_config = ConfigDict(
        extra="forbid", str_strip_whitespace=True, use_attribute_docstrings=True
    )


class ToolchainSpec(_SpecModel):
    """A compiler, MPI and module environment the system offers for one kind of run."""

    compiler: str = Field(min_length=1)
    """The compiler passed to the ROMS build: `gnu`, `intel` or `nvfortran`."""
    mpi_prefix: str = Field(min_length=1)
    """The command that launches the model under MPI, e.g. `srun --kill-on-bad-exit=1`."""
    gpu_arch: str = Field(default="")
    """The NVIDIA compute capability of a GPU build, e.g. `cc80`; empty for CPU."""
    link_flags: str = Field(default="")
    """Extra linker flags a GPU build appends after the MPI libraries; `${VAR}`
    references are expanded when the build runs."""
    modules: list[t.Annotated[str, Field(min_length=1)]] = Field(default_factory=list)
    """The modules to load, in order, after `module reset`; each entry is the
    argument of one `module load`. Empty leaves the user's modules alone."""
    env: dict[VariableName, EnvValue] = Field(default_factory=dict)
    """The environment variables to set after the modules load, in order. A
    `${VAR}` reference sees earlier entries first, then the process environment;
    an unset variable expands to an empty string."""

    def build(self, name: ToolchainName) -> Toolchain:
        """Return the toolchain this entry describes under `name`."""
        return Toolchain(
            compiler=self.compiler,
            mpi_prefix=self.mpi_prefix,
            profile="" if name == "cpu" else name,
            gpu_arch=self.gpu_arch,
            link_flags=self.link_flags,
            modules=tuple(self.modules),
            env=dict(self.env),
        )


class DetectEnv(_SpecModel):
    """Recognise the machine by an environment variable's value."""

    env: VariableName
    """The variable to read."""
    equals: str = Field(min_length=1)
    """The value that identifies the machine."""

    def matches(self) -> bool:
        """Tell whether the variable holds the value in this process."""
        return os.environ.get(self.env, "") == self.equals


class DetectPath(_SpecModel):
    """Recognise the machine by a directory that exists only there, e.g. a site mount."""

    path: str = Field(min_length=1)
    """The directory to look for."""

    def matches(self) -> bool:
        """Tell whether the directory exists; an error reading it is no match."""
        return path_is_dir(self.path)


DetectRule = DetectEnv | DetectPath
"""One way of recognising the machine."""


class ScratchSpec(_SpecModel):
    """Where the scratch file system is, on a machine that exports no `SCRATCH`-style variable."""

    match: str = Field(min_length=1)
    """A glob for the scratch directory, with `~` and `{user}` expanded; the first
    directory it matches, in sorted order, is used."""
    subdir: str = Field(default="")
    """A path appended to the matched directory, with `{user}` expanded; it need not exist."""

    def resolve(self) -> Path | None:
        """Return the scratch root this rule finds here, or `None`.

        Never raises, because it runs while `CSTAR_DATA_HOME` is resolved: a
        failed look warns and returns `None`.
        """
        user = current_user()
        try:
            pattern = os.path.expanduser(self.match.replace(USER_PLACEHOLDER, user))
            found = sorted(Path(p) for p in glob.glob(pattern) if Path(p).is_dir())
        except (OSError, RuntimeError):
            log.warning(
                f"Failed to look for the scratch directory {self.match}; set SCRATCH "
                "or CSTAR_DATA_HOME to name the scratch file system explicitly."
            )
            return None
        if not found:
            return None
        if self.subdir:
            return found[0] / self.subdir.replace(USER_PLACEHOLDER, user)
        return found[0]


def _fixed_walltime(walltime: str) -> "Callable[[str], str | None]":
    """Return a walltime query that answers `walltime` for any queue."""

    def fixed_walltime(_query_name: str) -> str | None:
        return walltime

    return fixed_walltime


class SlurmQueueSpec(_SpecModel):
    """A SLURM partition or QOS jobs can be submitted to."""

    name: str = Field(min_length=1)
    """The name a job is submitted with."""
    kind: t.Literal["partition", "qos"]
    """Whether `name` is a partition (`--partition`) or a QOS (`--qos`)."""
    query_name: str = Field(default="")
    """The name the walltime query asks about; defaults to `name`."""
    max_walltime: t.Annotated[str, BeforeValidator(_refuse_unquoted_walltime)] = Field(
        default=""
    )
    """Where the queue's walltime limit comes from: `sinfo`, `sacctmgr`, or a
    fixed quoted `HH:MM:SS` or `D-HH:MM:SS`. Omitted, a partition asks `sinfo`
    and a QOS asks `sacctmgr`."""

    @field_validator("max_walltime")
    @classmethod
    def _known_walltime(cls, value: str) -> str:
        """Accept a walltime query, a fixed walltime, or nothing."""
        if (
            value
            and value not in WALLTIME_QUERIES
            and not re.fullmatch(_WALLTIME_PATTERN, value)
        ):
            raise ValueError(
                f"max_walltime must be one of {', '.join(WALLTIME_QUERIES)}, or "
                f"a quoted HH:MM:SS or D-HH:MM:SS; got {value!r}"
            )
        return value

    def build(self) -> SlurmQueue:
        """Return the queue this entry describes."""
        klass = SlurmPartition if self.kind == "partition" else SlurmQOS
        method: Callable[[str], str | None] | None = None
        if self.max_walltime in WALLTIME_QUERIES:
            method = WALLTIME_QUERIES[self.max_walltime]
        elif self.max_walltime:
            method = _fixed_walltime(self.max_walltime)
        return klass(
            name=self.name,
            query_name=self.query_name or None,
            max_walltime_method=method,
        )


class PBSQueueSpec(_SpecModel):
    """A PBS queue jobs can be submitted to."""

    name: str = Field(min_length=1)
    """The name a job is submitted with."""
    max_walltime: Walltime
    """The queue's walltime limit, a quoted `HH:MM:SS` or `D-HH:MM:SS`."""

    def build(self) -> PBSQueue:
        """Return the queue this entry describes."""
        return PBSQueue(name=self.name, max_walltime=self.max_walltime)


class GpuNodesSpec(_SpecModel):
    """The GPU nodes a system offers, and how a job reaches them."""

    gpus_per_node: PositiveInt
    """The number of GPUs on each GPU node."""
    queue: str = Field(min_length=1)
    """The queue GPU jobs go to by default; one of the scheduler's queues."""
    directives: dict[str, str] = Field(default_factory=dict)
    """Extra directives for GPU jobs, e.g. `{-C: gpu}`; they replace the
    scheduler's `directives` for GPU jobs."""
    cores_per_node: PositiveInt | None = Field(default=None)
    """The CPU cores on each GPU node."""
    account_required: bool = Field(default=False)
    """Whether the scheduler refuses a GPU job that names no account."""

    def build(self) -> GpuNodes:
        """Return the GPU nodes this entry describes."""
        return GpuNodes(
            gpus_per_node=self.gpus_per_node,
            queue=self.queue,
            directives=dict(self.directives),
            cores_per_node=self.cores_per_node,
            account_required=self.account_required,
        )


class _SchedulerSpec(_SpecModel):
    """What every scheduler family's entry holds."""

    primary_queue: str = Field(min_length=1)
    """The queue a job goes to when it names none; one of `queues`."""
    directives: dict[str, str] = Field(default_factory=dict)
    """Extra directives on every CPU job, as `flag: value`, e.g. `{-C: cpu}`."""
    max_cpus_per_node: PositiveInt | None = Field(default=None)
    """The CPUs per node used to lay out a job; omitted, the scheduler is asked."""
    require: list[t.Literal["account", "queue"]] = Field(default_factory=list)
    """Which of `account` and `queue` a job must name."""
    gpu_nodes: GpuNodesSpec | None = Field(default=None)
    """The system's GPU nodes; omitted when it has none."""

    @property
    def queue_names(self) -> list[str]:
        """The names of the scheduler's queues, in order."""
        return [q.name for q in getattr(self, "queues")]

    @model_validator(mode="after")
    def _queues_agree(self) -> "_SchedulerSpec":
        """Refuse duplicate queues, and a primary or GPU queue that is not listed."""
        names = self.queue_names
        problems = []
        if duplicates := sorted({n for n in names if names.count(n) > 1}):
            problems.append(f"queues listed more than once: {', '.join(duplicates)}")
        if self.primary_queue not in names:
            problems.append(f"primary_queue {self.primary_queue!r} is not in queues")
        if self.gpu_nodes and self.gpu_nodes.queue not in names:
            problems.append(
                f"gpu_nodes.queue {self.gpu_nodes.queue!r} is not in queues"
            )
        if problems:
            raise ValueError("; ".join(problems))
        return self


class SlurmSchedulerSpec(_SchedulerSpec):
    """A SLURM scheduler."""

    family: t.Literal["slurm"]
    """The scheduler family."""
    queues: list[SlurmQueueSpec] = Field(min_length=1)
    """The partitions and QOS jobs can be submitted to."""
    requires_task_distribution: bool = Field(default=False)
    """Whether a job must state its nodes and tasks per node, not only its CPUs."""

    def build(self, docs: str) -> Scheduler:
        """Return the scheduler this entry describes."""
        return SlurmScheduler(
            queues=[q.build() for q in self.queues],
            primary_queue_name=self.primary_queue,
            other_scheduler_directives=dict(self.directives),
            requires_task_distribution=self.requires_task_distribution,
            documentation=docs or None,
            max_cpus_per_node=self.max_cpus_per_node,
            gpu=self.gpu_nodes.build() if self.gpu_nodes else None,
        )


class PBSSchedulerSpec(_SchedulerSpec):
    """A PBS Professional or OpenPBS scheduler (`-l select=`); not Torque."""

    family: t.Literal["pbs"]
    """The scheduler family."""
    queues: list[PBSQueueSpec] = Field(min_length=1)
    """The queues jobs can be submitted to."""

    def build(self, docs: str) -> Scheduler:
        """Return the scheduler this entry describes; PBS always distributes tasks."""
        return PBSScheduler(
            queues=[q.build() for q in self.queues],
            primary_queue_name=self.primary_queue,
            other_scheduler_directives=dict(self.directives),
            requires_task_distribution=True,
            documentation=docs or None,
            max_cpus_per_node=self.max_cpus_per_node,
            gpu=self.gpu_nodes.build() if self.gpu_nodes else None,
        )


SchedulerSpec = t.Annotated[
    SlurmSchedulerSpec | PBSSchedulerSpec, Field(discriminator="family")
]
"""A scheduler entry, told apart by its `family`."""


class SystemSpec(_SpecModel, VersionedDocument):
    """One machine C-Star can run on, as its system file describes it."""

    SCHEMA_LABEL: t.ClassVar[str] = "system file"

    schema_version: str = Field(default="1.0.0")
    """The version of the system file schema the file follows."""
    name: str = Field(pattern=_NAME_PATTERN)
    """The system's name: lowercase letters, digits, `_` and `-`. It is the value
    of `CSTAR_SYSTEM` that selects it, and the file's name without `.yaml`."""
    docs: str = Field(default="")
    """Where the machine's documentation is."""
    detect: list[DetectRule] = Field(default_factory=list)
    """How C-Star recognises the machine; any rule matching selects it. Not
    needed when Lmod reports the system's name (`LMOD_SYSHOST` or
    `LMOD_SYSTEM_NAME`)."""
    toolchains: dict[ToolchainName, ToolchainSpec]
    """The toolchains the system offers, by name; `cpu` is required."""
    scheduler: SchedulerSpec | None = Field(default=None)
    """The batch scheduler jobs are submitted to; omitted on a workstation."""
    scratch: ScratchSpec | None = Field(default=None)
    """Where the scratch file system is, when no `SCRATCH`-style variable names it."""

    @field_validator("toolchains")
    @classmethod
    def _cpu_toolchain(
        cls, value: dict[ToolchainName, ToolchainSpec]
    ) -> dict[ToolchainName, ToolchainSpec]:
        """Require the `cpu` toolchain, the source of the system's compiler and MPI."""
        if "cpu" not in value:
            raise ValueError("the cpu toolchain is required")
        return value

    def settings_klass(self) -> type[EnvSettingsBase]:
        """Build the settings class that loads what a job on this system needs.

        A system with a scheduler gets a `SlurmSettingsBase` whose fields named
        by `require` are required (`min_length=1`, which `required_settings`
        reads) and whose others may be empty; a system without one needs nothing.
        Each call builds a new class, so callers build it once.
        """
        if self.scheduler is None:
            return EnvSettingsBase
        required = {_REQUIRED_FIELDS[r] for r in self.scheduler.require}
        optional: dict[str, t.Any] = {
            field: (str, Field(default="", frozen=True))
            for field in _REQUIRED_FIELDS.values()
            if field not in required
        }
        return create_model(
            f"{self.name.title().replace('_', '').replace('-', '')}Settings",
            __base__=SlurmSettingsBase,
            __doc__=f"The settings a job on {self.name} needs.",
            **optional,
        )


class SystemFileError(CstarError):
    """A system file that cannot be read or does not describe a system."""

    def __init__(self, path: Path, reason: str) -> None:
        """Name the file and why it was refused."""
        self.path = path
        self.reason = reason
        super().__init__(f"System file {path} is refused: {reason}")


@dataclass(frozen=True)
class SystemFile:
    """A system file as read: its spec, and where it came from."""

    spec: SystemSpec
    """The validated content."""
    path: Path
    """The file read."""
    layer: str
    """The catalog layer the file belongs to; `BUNDLED_LAYER` for one C-Star ships."""
    sha256: str
    """The digest of the file's bytes."""

    def record(self) -> dict[str, str]:
        """Describe the file for a build or run record."""
        return {
            "path": self.path.as_posix(),
            "layer": self.layer,
            "sha256": self.sha256,
            "schema_version": self.spec.schema_version,
        }


def read_system_file(path: Path, layer: str) -> SystemFile:
    """Read and validate one system file.

    Parameters
    ----------
    path : Path
        The file, named `<name>.yaml` after the system it describes.
    layer : str
        The catalog layer it belongs to.

    Returns
    -------
    SystemFile

    Raises
    ------
    SystemFileError
        If the file cannot be read, is not valid YAML, does not validate (a
        newer schema included), or names a system other than its file name.
    """
    try:
        content = path.read_bytes()
        data = yaml.safe_load(content)
        spec = SystemSpec.model_validate(data)
    except (OSError, yaml.YAMLError, ValidationError, CstarError) as ex:
        raise SystemFileError(path, str(ex)) from ex
    if spec.name != path.stem:
        raise SystemFileError(
            path, f"it describes {spec.name!r}, but the file must be named after it"
        )
    return SystemFile(spec, path, layer, hashlib.sha256(content).hexdigest())


def read_bundled_system_files() -> list[SystemFile]:
    """Read the system files C-Star ships, in file-name order.

    Raises
    ------
    SystemFileError
        If a bundled file is refused, which is a defect of the package.
    """
    return [
        read_system_file(path, BUNDLED_LAYER)
        for path in sorted(BUNDLED_SYSTEMS_DIR.glob(f"*{SYSTEM_FILE_SUFFIX}"))
    ]
