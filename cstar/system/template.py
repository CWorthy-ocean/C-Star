"""The commented starting system file that `cstar system template` prints.

The skeleton below is written by hand, so its example values and layout read
like a real file, but the comment above each key is the description of the
model field it sets: a line ``#: Model.field`` (or ``#: Model`` for the model's
own docstring) is replaced by that text, wrapped and indented like the marker.
A marker inside a commented-out block keeps the block's ``#`` prefix. The
schema version printed is the default of `SystemSpec.schema_version`. A unit
test checks that every field of every model a system file can hold has a
marker, and that the rendered file validates.
"""

import re
import textwrap
import typing as t

from pydantic import BaseModel

from cstar.system.spec import SystemSpec

MARKER_PATTERN: t.Final = re.compile(r"^(?P<lead>[ #]*)#: (?P<ref>\w+(?:\.\w+)?)$")
"""A line replaced by a description: the lead (spaces and ``#``) and the reference."""

SCHEMA_VERSION_TOKEN: t.Final = "@schema_version@"
"""Replaced by the schema version a new file declares."""

WRAP_WIDTH: t.Final = 80
"""The width descriptions are wrapped to, comment prefix included."""

REQUIRED_NOTE: t.Final = "Required."
"""Appended to the description of a field the model requires."""

_NO_BREAK: t.Final = "\0"
"""Stands in for a space inside a `code` span while a description is wrapped."""

SKELETON: t.Final = """\
# A C-Star system file: the modules, environment, scheduler and queues C-Star
# uses to build and run on one machine. Replace the example values with your
# site's. Uncomment a commented-out block only if the machine needs it, and
# delete any other key you do not need: a key C-Star does not know is refused.
# Test the file on the machine's login node with
#
#   cstar system check ./mycluster.yaml
#
# and see a machine C-Star knows with `cstar system list` and
# `cstar system show <name>`.

#: SystemSpec.schema_version
schema_version: @schema_version@

#: SystemSpec.name
name: mycluster

#: SystemSpec.docs
docs: https://hpc.example.edu/userguide

#: SystemSpec.detect
# detect:
#   #: DetectEnv
#   #: DetectEnv.env
#   - env: CLUSTER_NAME
#     #: DetectEnv.equals
#     equals: mycluster
#   #: DetectPath
#   #: DetectPath.path
#   - path: /mycluster/home

#: SystemSpec.scratch
# scratch:
#   #: ScratchSpec.match
#   match: "~/scratch"
#   #: ScratchSpec.subdir
#   subdir: "{user}"

#: SystemSpec.toolchains
toolchains:
  cpu:
    #: ToolchainSpec.compiler
    compiler: gnu
    #: ToolchainSpec.mpi_prefix
    mpi_prefix: srun --kill-on-bad-exit=1
    #: ToolchainSpec.modules
    # Pin a version on each entry, the site defaults included, so a change of
    # defaults does not change the build.
    modules:
      - gcc/12.2.0
      - openmpi/4.1.5
      - hdf5/1.14.1
      - netcdf-c/4.9.2
      - netcdf-fortran/4.6.1
      - parallel-netcdf/1.12.3  # optional: ParallelIO needs it
      - cmake/3.27.4  # ParallelIO builds with cmake, which the HPC conda environment does not provide
    #: ToolchainSpec.env
    env:
      # The install prefixes the ROMS build reads; each holds lib/ and bin/.
      # Take each from a variable the module sets (`module show <name>`).
      MPIHOME: ${OPENMPI_ROOT}
      NETCDFHOME: ${NETCDF_C_ROOT}
      # Only when netCDF-Fortran has a prefix of its own; else delete it.
      NETCDFFHOME: ${NETCDF_FORTRAN_ROOT}
      # Only with parallel-netcdf; else delete it.
      PNETCDFHOME: ${PNETCDF_ROOT}
      PATH: ${NETCDFHOME}/bin:${PATH}
      LIBRARY_PATH: ${LIBRARY_PATH}:${NETCDFHOME}/lib:${NETCDFFHOME}/lib
      # Linux: embed a RUNPATH at link time so the roms binary finds its
      # libraries without the modules loaded.
      LD_RUN_PATH: ${NETCDFFHOME}/lib:${NETCDFHOME}/lib:${PNETCDFHOME}/lib:${MPIHOME}/lib
      GIT_DISCOVERY_ACROSS_FILESYSTEM: "1"  # git on a distributed file system, e.g. lustre
  # A second toolchain, for GPU runs, takes the same keys plus these two.
  # gpu:
  #   compiler: nvfortran
  #   mpi_prefix: srun --kill-on-bad-exit=1
  #   #: ToolchainSpec.gpu_arch
  #   gpu_arch: cc80
  #   #: ToolchainSpec.link_flags
  #   link_flags: ""
  #   modules: []
  #   env: {}

#: SystemSpec.scheduler
scheduler:
  #: SlurmSchedulerSpec.family
  family: slurm
  #: SlurmSchedulerSpec.primary_queue
  primary_queue: standard
  #: SlurmSchedulerSpec.queues
  queues:
    #: SlurmQueueSpec.name
    - name: standard
      #: SlurmQueueSpec.kind
      kind: partition
      #: SlurmQueueSpec.query_name
      query_name: standard
      #: SlurmQueueSpec.max_walltime
      max_walltime: sinfo
    - {name: debug, kind: partition, max_walltime: "02:00:00"}
  #: SlurmSchedulerSpec.directives
  directives: {}
  #: SlurmSchedulerSpec.requires_task_distribution
  requires_task_distribution: false
  #: SlurmSchedulerSpec.max_cpus_per_node
  max_cpus_per_node: 128
  #: SlurmSchedulerSpec.require
  require: [account]
  #: SlurmSchedulerSpec.gpu_nodes
  # gpu_nodes:
  #   #: GpuNodesSpec.gpus_per_node
  #   gpus_per_node: 4
  #   #: GpuNodesSpec.queue
  #   queue: standard
  #   #: GpuNodesSpec.directives
  #   directives: {--gpu-bind: none}
  #   #: GpuNodesSpec.cores_per_node
  #   cores_per_node: 64
  #   #: GpuNodesSpec.account_required
  #   account_required: false

# On a PBS Professional or OpenPBS machine the scheduler block takes this shape
# instead; `directives`, `max_cpus_per_node`, `require` and `gpu_nodes` mean
# what they mean above, and a PBS queue takes no `kind` or `query_name`.
# `cstar system show derecho` is a complete example.
#
# scheduler:
#   #: PBSSchedulerSpec.family
#   family: pbs
#   primary_queue: main
#   #: PBSSchedulerSpec.queues
#   queues:
#     #: PBSQueueSpec.name
#     - name: main
#       #: PBSQueueSpec.max_walltime
#       max_walltime: "12:00:00"
"""
"""The template, before its markers are replaced."""


def spec_models() -> dict[str, type[BaseModel]]:
    """Every model a system file can hold, `SystemSpec` and the models in its
    fields, by class name.
    """
    found: dict[str, type[BaseModel]] = {}

    def visit(annotation: t.Any) -> None:
        if isinstance(annotation, type) and issubclass(annotation, BaseModel):
            model = t.cast("type[BaseModel]", annotation)
            if model.__name__ not in found:
                found[model.__name__] = model
                for field in model.model_fields.values():
                    visit(field.annotation)
            return
        for arg in t.get_args(annotation):
            visit(arg)

    visit(SystemSpec)
    return found


def describe(ref: str, models: dict[str, type[BaseModel]]) -> str:
    """The text a marker stands for: a model's docstring, or a field's
    description followed by `REQUIRED_NOTE` when the field is required.

    Raises
    ------
    KeyError
        If the reference names no model or field, or the field has no description.
    """
    model_name, _, field_name = ref.partition(".")
    model = models[model_name]
    field = model.model_fields[field_name] if field_name else None
    text = " ".join((field.description if field else model.__doc__ or "").split())
    if not text:
        raise KeyError(f"{ref} has no description")
    return f"{text} {REQUIRED_NOTE}" if field and field.is_required() else text


def wrap_comment(text: str, prefix: str) -> list[str]:
    """Wrap `text` into comment lines that start with `prefix`, never breaking
    inside a `code` span.
    """
    kept = re.sub(r"`[^`]*`", lambda m: m[0].replace(" ", _NO_BREAK), text)
    lines = textwrap.wrap(
        kept,
        width=WRAP_WIDTH,
        initial_indent=prefix,
        subsequent_indent=prefix,
        break_on_hyphens=False,
        break_long_words=False,
    )
    return [line.replace(_NO_BREAK, " ") for line in lines]


def render_system_template() -> str:
    """The commented starting system file, its markers replaced.

    Returns
    -------
    str
        A YAML document that validates as a `SystemSpec`.
    """
    models = spec_models()
    version = SystemSpec.model_fields["schema_version"].default
    lines: list[str] = []
    for line in SKELETON.replace(SCHEMA_VERSION_TOKEN, version).splitlines():
        if not (marker := MARKER_PATTERN.match(line)):
            lines.append(line)
            continue
        lines += wrap_comment(describe(marker["ref"], models), f"{marker['lead']}# ")
    return "\n".join(lines) + "\n"
