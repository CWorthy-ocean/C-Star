---
name: cstar-system
description: Write and test a C-Star system file for an HPC cluster C-Star does not know yet, so that C-Star loads the site's modules, builds UCLA-ROMS and submits jobs there. Use when someone wants to run C-Star on a new or unlisted cluster, when `cstar env show` or `cstar init` says the machine is not recognised, or when asked to describe a cluster, add a machine or system, or write a system file.
---

# Describe a cluster to C-Star

A *system file* is one YAML file that tells C-Star which modules to load on a
cluster, which variables point at MPI and netCDF, which queues exist and how
C-Star recognises the machine. This skill writes one for a machine C-Star does
not know, tests it as far as your access allows, and says how far that was.

`<skill-dir>` below is the directory that holds this file; its `scripts/`
folder holds `probe.sh` and `try-modules.sh`.

## Rules

- **Probing is read-only.** Look, list and query; change nothing on the
  machine. `sbatch --test-only` submits nothing and is fine.
- **Write only the system file and the scripts' output** (`cstar-probe.txt`
  and the like), plus what a level the user allowed writes (section 8: the
  example blueprint, and the run directory C-Star makes). Never overwrite an
  existing file of the same name without asking.
- **Never delete, move or overwrite** anything under scratch, project space,
  `CSTAR_DATA_HOME`, `CSTAR_PROJECT_HOME` or another user's files. Do not run
  `--clobber`, `cstar admin clean`, `cstar admin migrate-*` or
  `cstar run compress`.
- **Never install anything** into a base, system or shared environment, and
  never load modules in the user's own shell: `try-modules.sh` loads them in
  a shell of its own.
- **Ask before anything that uses an allocation or submits a job**: `salloc`,
  `srun`, `sbatch` (except `sbatch --test-only`), `qsub`, and
  `cstar blueprint submit` without `--pre-run`.
  Say what it will run, where, and roughly for how long.
- **Every value comes from this machine or its site's documentation**, never
  from another machine's file. Say where each came from in a comment.
- **When the user has to run something, give one short command per line**, or
  a script to run. Long pasted commands wrap and break.

## 1. Agree how you reach the machine

Ask the user which of these applies:

1. **You run on the machine** (a login node): run every command directly.
2. **Over ssh from the user's computer**: ask for the host name as their ssh
   configuration knows it, and run each command through a login shell, which
   sets up the `module` command:

   ```
   ssh <host> 'bash -l -s' < <skill-dir>/scripts/probe.sh > cstar-probe.txt
   ```

   For a few commands, send them on standard input:

   ```
   ssh <host> 'bash -l -s' <<'EOF'
   <commands>
   EOF
   ```

   If the site uses two-factor login, ask the user to open one connection
   that later ones reuse (ssh's `ControlMaster` and `ControlPersist`), so they
   authenticate once. `ssh -o BatchMode=yes <host> true` tells you whether a
   command will go through without a prompt.
3. **Public documentation only**: you cannot run anything there. Draft the
   file from the site's user guide, comment every value
   `# from <url>, unverified`, and hand the user the probe (section 3).

Ask as well:

- How C-Star is started on the machine: the line that activates the
  environment `cstar` is installed in (for example `conda activate cstar-env`).
  A login shell over ssh does not activate it. Below, `<activate>` stands for
  that line; put it before every `cstar` command you run there.
- The site's user guide URL, and a short lowercase name for the machine.
- Whether they want GPU runs, and which account (allocation) a test job may be
  charged to, if they want you to go as far as a job.

## 2. Check the installed C-Star

On the machine:

```
<activate>
cstar --version
cstar system template --help
```

If `cstar system template` is missing, the installed C-Star is older than
this skill. Ask the user to upgrade C-Star, or to use the copy of this skill
that matches their version. Do not write a file from memory. If `cstar` is not
installed on the machine at all, the probe can still run, but checking the
file needs C-Star there.

## 3. Probe the machine

Run `probe.sh` on a login node and keep its output:

- on the machine: `bash -l <skill-dir>/scripts/probe.sh > cstar-probe.txt`
- over ssh: the first command in section 1
- documentation only: give the user `probe.sh` and these steps:
  1. Copy `probe.sh` to the cluster's login node.
  2. Run `bash -l probe.sh > cstar-probe.txt` there.
  3. Send back `cstar-probe.txt`.

Read all of it. It prints, in labelled sections: the host and OS; Lmod's
variables (`LMOD_CMD`, `LMOD_SYSHOST`, `LMOD_SYSTEM_NAME`,
`LMOD_SYSTEM_DEFAULT_MODULES`); the modules loaded at login and those
available (compilers, MPI, HDF5, netCDF, parallel-netCDF, CMake), including
the ones a hierarchical Lmod hides; the scheduler commands on `PATH`; for
SLURM, the partitions (`sinfo`), each partition's limits and QOS (`scontrol
show partition`), your associations and the QOS limits (`sacctmgr`),
`ClusterName` and `MpiDefault`, and whether a job with no account and no
partition would be accepted (`sbatch --test-only`); for PBS, `qstat -Qf`;
scratch candidates; variables and top-level directories that could identify
the machine. It prints named variables only, never the whole environment.

Read the site's user guide too, for: the recommended compiler and MPI, how a
job picks its queue (`--partition` or `--qos`), whether an account is
required, the queue limits, the scratch file system, and any rule against
compiling on login nodes.

## 4. Decide what C-Star can do here

- **Lmod**: C-Star loads modules only through Lmod (`LMOD_CMD` is set in the
  probe). Without Lmod, write an *environment-only file*: `modules: []`, and
  `env` entries with absolute install prefixes. The user then loads the
  modules in their own shell before running `cstar`.
- **SLURM**: `family: slurm`.
- **PBS Professional or OpenPBS** (`qstat --version` prints `pbs_version`):
  `family: pbs`. C-Star cannot submit to PBS yet; tell the user that runs go
  through `cstar blueprint run` inside a PBS job.
- **Any other scheduler** (Torque, LSF, HTCondor, ...): leave out the
  `scheduler` block and tell the user that C-Star will not submit jobs here;
  runs go through `cstar blueprint run` inside an allocation.

Say plainly which of these applies before you go on.

## 5. Start the file

```
<activate>
cstar system template > <name>.yaml
```

The template is a valid file whose comments describe every key the installed
C-Star accepts; it is your schema. Keep its comments where they help, and
delete keys and commented-out blocks the machine does not need.

The `name` is short and lowercase (`[a-z0-9_-]`). If the probe shows
`LMOD_SYSHOST` (or, without it, `LMOD_SYSTEM_NAME`), use exactly that value:
C-Star then recognises the machine without a `detect` rule. The file is
`<name>.yaml`.

`cstar system list` shows the machines C-Star knows. `cstar system show
<name>` prints one of their files, to see the shape of a finished file only:
take no values from another machine.

## 6. Fill it in

### Modules (`toolchains.cpu.modules`)

The build needs a Fortran compiler, MPI, HDF5, netCDF-C, netCDF-Fortran, and
CMake (the ParallelIO build calls it, and the HPC conda environment has none).
Add parallel-netCDF when the site has one built with the same MPI: it lets
C-Star build ParallelIO.

1. Start from what is loaded at login (the probe's `module list`, or the
   modules marked `(L)` and `(D)`). List each one you need explicitly, with
   its version, and mark it `# (system default)`, so a change of site
   defaults does not change the build.
2. Prefer the MPI the site recommends, the one integrated with the scheduler.
3. In a hierarchical Lmod, the MPI-built libraries (netCDF with MPI, parallel
   HDF5, parallel-netCDF) appear only once a compiler and an MPI are loaded.
   Load those two first with `try-modules.sh` and read its "modules available
   now" section.
4. Take a netCDF-Fortran built with that compiler and MPI. `module spider
   <name>/<version>` shows what must be loaded before it.
5. Pick versions in this order: the defaults; else a version whose
   dependencies are the defaults; else the newest compiler, then repeat.
6. If the site has two builds of one library at the same version, check
   which one the netCDF-Fortran module pulls in, and load the module that
   brings the matching pair, not the library whose default is the other
   build.
7. Where module names carry build hashes (`name/1.2.3-abcd`), use the
   version-only name (`name/1.2.3`) if it loads, and say so in a comment.

Test each candidate set from `module reset`, as C-Star loads it:

```
bash -l <skill-dir>/scripts/try-modules.sh <module> <module> ...
ssh <host> 'bash -l -s -- <module> <module> ...' < <skill-dir>/scripts/try-modules.sh
```

A set is good when every module loads, none of them swaps out another module
you listed, and `mpifort` and `nf-config` resolve (and `pnetcdf-config` when
listed). In documentation-only mode, give the user the command and ask for
its output.

### Compiler and launcher

- `compiler`: `gnu` for GCC (also when the MPI wrappers wrap `gfortran`;
  `try-modules.sh` prints what `mpifort` wraps), `intel` for Intel or oneAPI,
  `nvfortran` for the NVIDIA HPC SDK.
- `mpi_prefix` on SLURM: `srun --kill-on-bad-exit=1`, plus `--mpi=<type>` when
  the site's documentation or the MPI requires one (the probe's `MpiDefault`
  and `srun --mpi=list`). Without SLURM, `mpirun` or `mpiexec`, as the site
  documents.

### Install prefixes (`toolchains.cpu.env`)

The ROMS build reads these variables, each an install prefix holding `lib/`:

- `MPIHOME`: the MPI.
- `NETCDFHOME`: netCDF-C, and netCDF-Fortran too when they share a prefix.
- `NETCDFFHOME`: netCDF-Fortran, only when its prefix is separate.
- `PNETCDFHOME`: parallel-netCDF, only when it is loaded.

1. Set each from a variable a module sets: `try-modules.sh` prints the
   location variables the modules added, which of `lib/` and `bin/` each
   holds, and the `setenv` lines of `module show`. Write `${VAR}`. Use the
   names you see; do not guess a naming convention.
2. When no variable exists, use `nc-config --prefix`, `nf-config --prefix` or
   `pnetcdf-config --prefix` and write the path itself, with a comment that it
   goes stale if the site rebuilds the module.
3. Keep the template's `PATH`, `LIBRARY_PATH` and `LD_RUN_PATH` lines, minus
   any variable you deleted, and `GIT_DISCOVERY_ACROSS_FILESYSTEM: "1"`.

### Scheduler and queues

- `queues`: the ones a C-Star job would use (general, shared, debug; GPU ones
  only for GPU runs). Leave out queues restricted to other groups (a
  partition's `AllowAccounts` or `AllowQos` that excludes the user).
- `kind`: `partition` if the site's documentation picks a queue with
  `--partition`, `qos` if with `--qos`.
- `primary_queue`: the default (marked `*` in `sinfo`), or the queue the
  documentation calls standard.
- `max_walltime`, for each queue:
  - a partition with a time limit in `sinfo`, no partition QOS, and no QOS
    with a smaller `MaxWall` that jobs there get by default (compare with the
    probe's `sacctmgr` sections): leave it out (the partition default asks
    `sinfo`);
  - a partition whose `scontrol show partition` line has `QoS=<q>`: that
    QOS's `MaxWall` is the limit; `max_walltime: sacctmgr` and
    `query_name: <q>` when `<q>` differs from the partition's name;
  - a partition whose jobs get a default QOS (the association's) with a
    `MaxWall` below the `sinfo` limit, or where `sinfo` says `infinite`:
    `sacctmgr`, with that QOS as `query_name`;
  - a QOS: leave it out (a QOS asks `sacctmgr`). If `sacctmgr` shows no
    `MaxWall` for the QOS a job names but the limit is carried by another QOS
    the site pairs with it, put that one in `query_name`, and say why in a
    comment;
  - nothing reports a limit but the documentation gives one: a fixed, quoted
    `"HH:MM:SS"` with the URL in a comment.

  `cstar system check` reports each queue's walltime; a queue that reports
  none usually has the wrong source. A partition with no limit at all
  (`infinite`, and no QOS caps it) also reports none and fails the check:
  give it the limit the documentation states, if any, as a fixed walltime;
  otherwise leave it, and tell the user that C-Star then asks for
  `CSTAR_SLURM_MAX_WALLTIME` (48 hours unless they set it).
- `directives`: only what every CPU job must carry, such as a constraint the
  site requires to select CPU nodes.
- `max_cpus_per_node`: only when every node of the primary queue has the same
  core count (the `cpus/node` column of `sinfo`); leave it out otherwise.
- `requires_task_distribution`: `true` only when the site requires jobs to
  state nodes and tasks per node.
- `require`: from the probe's `sbatch --test-only`, which names no account and
  no partition. Refused for the account: `account`. Refused for the
  partition, or no partition is marked default: `queue`. Accepted: `[]`,
  unless the site's documentation says every job must name an account. The
  test runs with the user's default account, if they have one, so say in the
  comment that `[]` relies on one, and give the test's date.
- PBS: list the queues the documentation tells users to submit to, each with
  the `resources_max.walltime` from `qstat -Qf` as a fixed, quoted walltime.
- `gpu_nodes` and a `gpu` toolchain: only when the user wants GPU runs; the
  template's commented-out blocks show the keys.

### Detection

1. Lmod reports the machine's name (`LMOD_SYSHOST`, or without it
   `LMOD_SYSTEM_NAME`): name the system exactly that and add no `detect`
   block.
2. Otherwise a variable with the same value on the login **and** the compute
   nodes. Avoid generic values (a SLURM cluster name like `slurm`).
   `SLURM_CLUSTER_NAME` exists only inside jobs, so pair it with a login-node
   variable as a second rule.
3. Otherwise a directory that exists only on this machine, such as a site
   mount (the probe's top-level directories).

The probe sees the login node only. Checking a compute node takes a job, so
ask first; if the user declines, say in a comment that the compute-node half
is unverified. With no reliable rule, leave `detect` out: `cstar init` then
sets `CSTAR_SYSTEM` for the user.

### Scratch

Add a `scratch` rule only when the probe shows no `SCRATCH`-style variable,
from the location the site documents.

### Comments

Say where each value came from: `# from module show <module> on <host>,
<date>`, or `# from <url>, unverified` until the machine confirms it.

## 7. Check the file (level 1)

If you wrote the file elsewhere, copy it to the machine (`scp <name>.yaml
<host>:`), then on a login node:

```
<activate>
cstar system check ./<name>.yaml
```

Each line is `PASS`, `FAIL` or `NOT CHECKED`. Fix every `FAIL` and run it
again:

- `prefixes`: a variable is empty or its prefix has no `lib/`; the module
  that sets it is not loaded, or it is the wrong variable.
- `mpi wrapper`: `MPIHOME` does not hold `bin/mpifort`.
- `nf-config`: not on `PATH`; check the `PATH` line and `NETCDFHOME`.
- `queue <name>`: the walltime source is wrong, or the queue has no limit
  (section 6).
- `permissions`: run `chmod o-w <name>.yaml`.

`NOT CHECKED` for detection means C-Star will not pick the system by itself
here; `cstar init` sets `CSTAR_SYSTEM`. When the check passes, add a dated
comment near the top of the file, for example:

```
# cstar system check passed on <host>, <date> (C-Star <version>).
```

## 8. Build and run (levels 2 and 3)

These need the file installed. The user runs, on the machine:

```
<activate>
cstar init --system-file ./<name>.yaml
```

It asks for the account and queue, copies the file into their catalog and
sets `CSTAR_SYSTEM` when C-Star does not recognise the machine.

**Level 2, a ROMS build.** Ask first: it downloads the example blueprint into
the current directory, then clones and compiles UCLA-ROMS, MARBL and
ParallelIO for a few minutes in a new run directory under `CSTAR_DATA_HOME`. Use the small example blueprint of the installed version
(`<version>` is the number on the `C-Star version:` line of `cstar --version`;
for a development install, use `main`):

```
<activate>
curl -fsSLO https://raw.githubusercontent.com/CWorthy-ocean/C-Star/<version>/docs/tutorials/wio_toy_blueprint.yaml
cstar blueprint submit wio_toy_blueprint.yaml --run-id <name>-check --pre-run
cstar run status <name>-check
```

The pre-run runs on the login node, and the command can return before it
finishes: repeat `cstar run status <name>-check` until the step is done, and read
`cstar run log <name>-check ucla_roms` if it failed. If the site forbids
compiling on login nodes, ask the user to run the same commands inside an
interactive allocation instead. When it succeeds, add a dated comment:
`# A ROMS build through C-Star succeeded on <host>, <date>.`

**Level 3, a job.** Ask first: it submits a job that runs the model on 10 MPI
ranks for a few minutes, charged to the account `cstar init` recorded
(`CSTAR_SLURM_ACCOUNT`). Name a short queue, and the walltime the job asks
for:

```
<activate>
CSTAR_SLURM_QUEUE=<debug queue> CSTAR_SLURM_MAX_WALLTIME=00:30:00 cstar blueprint submit wio_toy_blueprint.yaml --run-id <name>-check
cstar run status <name>-check
```

When the step completes, add a dated comment:
`# A job submitted through C-Star completed on <host>, <date>.` On PBS, or
with no scheduler block, level 3 is `cstar blueprint run
wio_toy_blueprint.yaml` inside an interactive job the user starts.

If a level fails, show the user the log, fix the file, and check it again.
Try the level again under a new run ID (`<name>-check-2`, which level 3 then
uses too): never clear or reuse the failed run's directory.

## 9. Finish

Tell the user the level the file reached (1, 2 or 3), what is unverified, and
where the file is. Then give them:

- To use it themselves: `cstar init --system-file ./<name>.yaml`
- To share it with their group, into a catalog directory the group can read
  and they can write:
  `cstar system add ./<name>.yaml --catalog /path/to/shared/cstar-catalog`.
  A colleague then runs `cstar init --system-file
  /path/to/shared/cstar-catalog/systems/<name>.yaml`.

Offer to draft a pull request to https://github.com/CWorthy-ocean/C-Star that
makes the machine part of C-Star, so nobody there needs the file. It holds:

- the file as `cstar/catalog/bundled/systems/<name>.yaml`, with its dated
  comments;
- a row in the table of `docs/deployments/machines.rst`, in alphabetical order,
  marked by the level reached;
- a release-note file `docs/changes/+<name>-system.feature.rst`, one line, for
  example "C-Star recognises <Machine> (<site>) and loads its modules.";
- the golden fixture, regenerated with
  `UPDATE_GOLDEN=1 python -m pytest cstar/tests/unit_tests/system/test_bundled_systems_golden.py`,
  and the diff reviewed.

`docs/developers/system-registration.rst` in the repository has the rules for
a bundled file. Draft the branch and the description only: the project's AI
policy asks the user to open the pull request, and to fill in its `AI
disclosure:` line, themselves.
