#!/usr/bin/env bash
# Describe a cluster for a C-Star system file. Read-only: it loads no modules,
# submits no job (`sbatch --test-only` only asks whether one would be
# accepted), and writes nothing; redirect its output to save it.
#
# Run it in a login shell on a login node, so the `module` command is set up:
#
#   bash -l probe.sh > cstar-probe.txt
#   ssh <host> 'bash -l -s' < probe.sh > cstar-probe.txt
#
# It prints named variables only, never the whole environment, which can hold
# tokens. It works on a machine without Lmod, SLURM or PBS, and says so.

# ShellCheck 0.11.0 reports SC2218 ("only defined later") for any function on
# the left of a pipeline, even one defined above it.
# shellcheck disable=SC2218

exec 2>&1
export LC_ALL=C

PROBE_VERSION=1
SECRET_NAMES='TOKEN|KEY|SECRET|PASS|AUTH|CRED|COOKIE|PROXY|SESSION'

section() { printf '\n=== %s ===\n' "$1"; }
have() { command -v "$1" >/dev/null 2>&1; }

# Bound each slow query; `timeout` is absent on some systems (macOS).
run() {
    if have timeout; then
        timeout 60 "$@"
    else
        "$@"
    fi
}

# Print NAME=value for each exported variable named, or that it is not set.
show_vars() {
    local name value
    for name in "$@"; do
        if value=$(printenv "$name"); then
            printf '%s=%s\n' "$name" "$value"
        else
            printf '%s is not set\n' "$name"
        fi
    done
}

# Print NAME=value for exported variables whose names match a pattern and do
# not look like they hold a secret.
show_matching_vars() {
    local name
    for name in $(compgen -e | grep -E "$1" | grep -Ev "$SECRET_NAMES" | sort); do
        printf '%s=%s\n' "$name" "$(printenv "$name")"
    done
}

# Make the `module` command available when the login shell did not.
if ! type module >/dev/null 2>&1; then
    if [ -n "${LMOD_PKG:-}" ] && [ -f "$LMOD_PKG/init/bash" ]; then
        # shellcheck disable=SC1091
        . "$LMOD_PKG/init/bash"
    elif [ -n "${LMOD_CMD:-}" ] && [ -x "$LMOD_CMD" ]; then
        # defined through eval: Lmod's own init defines it this way
        eval 'module() { eval "$("$LMOD_CMD" bash "$@")"; }'
    fi
fi
HAVE_MODULE=no
type module >/dev/null 2>&1 && HAVE_MODULE=yes

# A listing from Lmod (`module` is a shell function, which `timeout` cannot
# run, so call Lmod itself; it writes listings to stderr).
lmod_list() {
    if [ -n "${LMOD_CMD:-}" ]; then
        # the listing on stderr to the caller, the shell code on stdout dropped
        # shellcheck disable=SC2069
        run "$LMOD_CMD" bash "$@" 2>&1 >/dev/null
    else
        module "$@"
    fi
}

section "probe"
echo "cstar-system probe version $PROBE_VERSION"
date -u '+%Y-%m-%d %H:%M UTC'
echo "bash $BASH_VERSION"

section "host"
uname -srm
hostname
hostname -f 2>/dev/null
if [ -r /etc/os-release ]; then
    grep -E '^(PRETTY_NAME|VERSION_ID)=' /etc/os-release
fi
show_vars USER HOME SHELL

section "modules: Lmod"
show_vars LMOD_CMD LMOD_VERSION LMOD_PKG LMOD_SYSHOST LMOD_SYSTEM_NAME \
    LMOD_SYSTEM_DEFAULT_MODULES MODULESHOME MODULEPATH
echo "module command available: $HAVE_MODULE"
if [ -z "${LMOD_CMD:-}" ]; then
    if [ "$HAVE_MODULE" = yes ]; then
        echo "A module command exists but LMOD_CMD is not set: not Lmod (C-Star drives Lmod only)."
        module --version
    else
        echo "No Lmod and no module command on this machine."
    fi
fi

if [ "$HAVE_MODULE" = yes ]; then
    section "modules: loaded at login (module list)"
    lmod_list --terse list

    section "modules: available (module --terse avail, filtered)"
    echo "Filter: compilers, MPI, HDF5, netCDF, parallel-netCDF, CMake, CUDA, programming environments"
    lmod_list --terse avail |
        grep -Ei '^/|:$|gcc|gnu|intel|oneapi|nvhpc|nvidia|aocc|cce|prgenv|mpi|mpich|mvapich|hdf5|netcdf|pnetcdf|cmake|cuda|craype'

    if [ -n "${LMOD_CMD:-}" ]; then
        section "modules: everything Lmod can load (module --terse spider, filtered)"
        echo "Includes modules a hierarchical Lmod hides until a compiler and MPI are loaded."
        lmod_list --terse spider |
            grep -Ei 'gcc|gnu|intel|oneapi|nvhpc|nvidia|aocc|cce|prgenv|mpi|mpich|mvapich|hdf5|netcdf|pnetcdf|cmake|cuda' |
            sort -u
    fi
fi

section "compilers and tools on PATH at login"
for tool in gcc gfortran ifort ifx nvfortran ftn mpifort mpif90 mpicc srun mpirun mpiexec nc-config nf-config pnetcdf-config cmake; do
    printf '%-15s %s\n' "$tool" "$(command -v "$tool" || echo '-')"
done
have gfortran && gfortran --version | head -1
have mpifort && mpifort --version | head -1

section "scheduler commands"
for tool in sbatch srun sinfo sacctmgr scontrol squeue qsub qstat pbsnodes bsub condor_submit; do
    printf '%-15s %s\n' "$tool" "$(command -v "$tool" || echo '-')"
done
FAMILY=none
if have sbatch; then
    FAMILY=slurm
    run sbatch --version
fi
if have qsub; then
    [ "$FAMILY" = none ] && FAMILY=pbs-or-other
    echo "qstat --version:"
    run qstat --version
    echo "(PBS Professional and OpenPBS print pbs_version; Torque prints Version)"
fi
have bsub && echo "bsub found: LSF, which C-Star does not support"
echo "scheduler family guess: $FAMILY"

if [ "$FAMILY" = slurm ]; then
    section "slurm: configuration"
    run scontrol show config |
        grep -E '^(ClusterName|MpiDefault|SelectType|SelectTypeParameters|AccountingStorageEnforce|EnforcePartLimits|DefMemPerCPU|DefMemPerNode) '

    section "slurm: MPI types srun supports (srun --mpi=list)"
    run srun --mpi=list

    section "slurm: partitions (sinfo)"
    echo "partition avail timelimit nodes cpus/node memory/node gres features"
    run sinfo --noheader -o '%P %a %l %D %c %m %G %f' | sort -u

    section "slurm: partition limits (scontrol show partition)"
    echo "QoS= is a partition QOS whose MaxWall may cap the partition"
    run scontrol show partition --oneliner |
        awk '{
            out = ""
            for (i = 1; i <= NF; i++)
                if ($i ~ /^(PartitionName|Default|Hidden|State|QoS|MaxTime|DefaultTime|AllowQos|AllowAccounts|DenyAccounts|TotalNodes|MaxNodes|MaxCPUsPerNode|OverSubscribe)=/)
                    out = out $i " "
            print out
        }'

    section "slurm: your associations (sacctmgr show assoc)"
    run sacctmgr --noheader --parsable2 show assoc user="$USER" \
        format=Cluster,Account,Partition,QOS,DefaultQOS,MaxWall

    section "slurm: QOS limits (sacctmgr show qos)"
    run sacctmgr --parsable2 show qos format=Name,MaxWall,MaxTRESPU,MaxJobsPU,Flags

    section "slurm: would a job with no account and no partition be accepted? (sbatch --test-only, submits nothing)"
    run sbatch --test-only --nodes=1 --time=00:05:00 --job-name=cstar-probe --wrap=true
fi

if [ "$FAMILY" = pbs-or-other ]; then
    section "pbs: server (qstat -B)"
    run qstat -B

    section "pbs: queues (qstat -Qf)"
    run qstat -Qf

    section "pbs: node types (pbsnodes -a, counted)"
    run pbsnodes -a |
        grep -E 'resources_available\.(ncpus|ngpus|mem|Qlist)' | sort | uniq -c | head -40
fi

section "scratch and work file systems"
show_vars SCRATCH PSCRATCH CSCRATCH WORK PROJECT TMPDIR
echo "other variables named like a file system:"
show_matching_vars 'SCRATCH|WORK|PROJECT|STORAGE' |
    grep -Ev '^(SCRATCH|PSCRATCH|CSCRATCH|WORK|PROJECT)='
echo "scratch-like directories:"
for path in "$HOME"/scratch* /scratch /scratch/"$USER" /*/scratch; do
    [ -e "$path" ] && ls -ld "$path"
done

section "detection candidates"
echo "variables named like a cluster, site or host:"
show_matching_vars 'CLUSTER|SYSHOST|SYSTEM|SITE|MACHINE|HOSTNAME|_HOST$'
echo "top-level directories (a site mount can identify the machine):"
printf '%s ' /*/
echo

section "python and conda"
for tool in conda mamba micromamba python3 cstar; do
    printf '%-15s %s\n' "$tool" "$(command -v "$tool" || echo '-')"
done
show_vars CONDA_PREFIX CONDA_DEFAULT_ENV

section "gpus on this node"
if have nvidia-smi; then
    run nvidia-smi -L
else
    echo "nvidia-smi not found"
fi

section "end"
