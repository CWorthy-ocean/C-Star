#!/usr/bin/env bash
# Try a module set for a C-Star system file the way C-Star loads it: from
# `module reset`, in order, in this script's own shell (the caller's shell is
# not touched). Then report what the set provides: the variables the modules
# set, the MPI and netCDF tools and their prefixes, and whether each prefix
# holds lib/ and bin/. Read-only: it writes nothing; redirect its output to
# save it.
#
#   bash -l try-modules.sh gcc/12.2.0 openmpi/4.1.5 netcdf-fortran/4.6.1
#   ssh <host> 'bash -l -s -- gcc/12.2.0 openmpi/4.1.5' < try-modules.sh
#
# It prints only variables the modules added or changed whose names look like
# install locations, never the whole environment.

exec 2>&1
export LC_ALL=C

SECRET_NAMES='TOKEN|KEY|SECRET|PASS|AUTH|CRED|COOKIE|PROXY|SESSION'
PREFIX_NAMES='(ROOT|HOME|PREFIX|DIR|_BASE|_VERSION)$|^(NETCDF|PNETCDF|HDF5|MPI|OPAL|I_MPI|CRAY_|EBROOT|PE_|CUDA|NVHPC)'

section() { printf '\n=== %s ===\n' "$1"; }
have() { command -v "$1" >/dev/null 2>&1; }

[ "${1:-}" = "--" ] && shift
# Say why the script cannot go on, and stop with the code given.
stop() {
    printf '%s\n' "${@:2}"
    exit "$1"
}

[ "$#" -gt 0 ] || stop 2 "usage: bash -l try-modules.sh <module> [<module> ...]"

if ! type module >/dev/null 2>&1; then
    if [ -n "${LMOD_PKG:-}" ] && [ -f "$LMOD_PKG/init/bash" ]; then
        # shellcheck disable=SC1091
        . "$LMOD_PKG/init/bash"
    elif [ -n "${LMOD_CMD:-}" ] && [ -x "$LMOD_CMD" ]; then
        # defined through eval: Lmod's own init defines it this way
        eval 'module() { eval "$("$LMOD_CMD" bash "$@")"; }'
    fi
fi
if [ -z "${LMOD_CMD:-}" ] || ! type module >/dev/null 2>&1; then
    stop 1 "No Lmod here: C-Star loads modules through Lmod only, so a system file" \
        "for this machine lists no modules (see the skill's env-only file)."
fi

# NAME=value for every exported variable, one per line, sorted; kept in
# memory and compared, never printed whole.
snapshot() {
    local name
    for name in $(compgen -e | sort); do
        printf '%s=%s\n' "$name" "$(printenv "$name")"
    done
}

section "module reset"
module reset
before=$(snapshot)

section "module load, in order"
failed=0
for mod in "$@"; do
    # a module named without a version, or by its version only, loads under a
    # longer name; match the start of the loaded name
    if module load "$mod" && module --terse list 2>&1 | grep -q "^$mod"; then
        echo "loaded: $mod"
    else
        echo "FAILED: $mod"
        failed=1
    fi
done

section "module list"
module --terse list

section "variables the modules set that look like install locations"
after=$(snapshot)
changed=$(comm -13 <(printf '%s\n' "$before") <(printf '%s\n' "$after") |
    grep -E "^[A-Za-z_][A-Za-z0-9_]*=" | grep -Ev "^[^=]*($SECRET_NAMES)[^=]*=")
printf '%s\n' "$changed" | while IFS= read -r line; do
    name=${line%%=*}
    value=${line#*=}
    case "$name" in *PATH) continue ;; esac
    printf '%s\n' "$name" | grep -Eq "$PREFIX_NAMES" || continue
    note=""
    if [ -d "$value" ]; then
        [ -d "$value/lib" ] && note="$note lib/"
        [ -d "$value/lib64" ] && note="$note lib64/"
        [ -d "$value/bin" ] && note="$note bin/"
        [ -d "$value/include" ] && note="$note include/"
        note=" [holds:${note:- none of lib/ bin/ include/}]"
    fi
    printf '%s%s\n' "$line" "$note"
done

section "what each module sets (module show, setenv lines)"
for mod in "$@"; do
    echo "--- $mod"
    module show "$mod" 2>&1 | grep -E 'setenv|pushenv' | grep -Ev "$SECRET_NAMES"
done

section "modules available now (module --terse avail, filtered)"
echo "In a hierarchical Lmod, libraries built with this compiler and MPI appear here."
module --terse avail 2>&1 |
    grep -Ei '^/|:$|mpi|mpich|hdf5|netcdf|pnetcdf|cmake|cuda'

section "tools on PATH"
for tool in mpifort mpif90 mpicc srun mpirun mpiexec nc-config nf-config pnetcdf-config h5pfc h5fc cmake gfortran ifx ifort nvfortran ftn; do
    printf '%-15s %s\n' "$tool" "$(command -v "$tool" || echo '-')"
done

section "tool versions and prefixes"
if have mpifort; then
    echo "mpifort --version: $(mpifort --version 2>&1 | head -1)"
    echo "mpifort wraps: $( (mpifort -show 2>/dev/null || mpifort --showme 2>/dev/null) | head -1)"
fi
if have nc-config; then
    echo "nc-config --version: $(nc-config --version)"
    echo "nc-config --prefix: $(nc-config --prefix)"
    echo "nc-config --has-parallel: $(nc-config --has-parallel 2>/dev/null)"
    echo "nc-config --has-fortran: $(nc-config --has-fortran 2>/dev/null)"
fi
if have nf-config; then
    echo "nf-config --version: $(nf-config --version)"
    echo "nf-config --prefix: $(nf-config --prefix)"
    echo "nf-config --fc: $(nf-config --fc)"
fi
if have pnetcdf-config; then
    echo "pnetcdf-config --version: $(pnetcdf-config --version)"
    echo "pnetcdf-config --prefix: $(pnetcdf-config --prefix)"
fi
have cmake && echo "cmake --version: $(cmake --version | head -1)"

section "result"
if [ "$failed" -eq 0 ]; then
    echo "every module loaded"
else
    echo "some modules failed to load"
fi
exit "$failed"
