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

# ShellCheck 0.11.0 reports SC2218 ("only defined later") for functions
# defined above their use, once a script has more than one exit path.
# shellcheck disable=SC2218

# Names that may hold a secret, matched without regard to case; such a
# variable is never printed.
SECRET_NAMES='TOKEN|KEY|SECRET|PASS|PWD|(^|_)PAT($|_)|AUTH|CRED|COOKIE|PROXY|SESSION|PRIVATE|CERT|NETRC|LICENSE'
# Names of variables that usually hold an install location.
PREFIX_NAMES='(ROOT|HOME|PREFIX|DIR|_BASE)$|^(NETCDF|PNETCDF|HDF5|MPI|OPAL|I_MPI|CRAY_|EBROOT|CUDA|NVHPC)'

section() { printf '\n=== %s ===\n' "$1"; }
have() { command -v "$1" >/dev/null 2>&1; }

# Say why the script cannot go on, and stop with the code given.
stop() {
    printf '%s\n' "${@:2}"
    exit "$1"
}

# NAME=value for every exported variable, one per line, in line order (what
# `comm` needs); kept in memory and compared, never printed whole.
snapshot() {
    local name
    for name in $(compgen -e); do
        printf '%s=%s\n' "$name" "$(printenv "$name")"
    done | sort
}

# Tell whether `module --terse list` shows the module asked for. A name with
# a version may load under a longer one (`name/1.2.3` as `name/1.2.3-abcd`);
# a name without one loads as `name/<version>`.
is_loaded() {
    module --terse list 2>&1 | awk -v m="$1" '
        $0 == m || index($0, m "/") == 1 || (m ~ /\// && index($0, m) == 1) { f = 1 }
        END { exit !f }'
}

main() {
    # Nothing below reads standard input; under `bash -s` it holds the rest
    # of this script, which a command reading it would swallow.
    exec </dev/null 2>&1
    export LC_ALL=C

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

    section "module reset"
    module reset
    local before after
    before=$(snapshot)

    section "module load, in order"
    local mod failed=0
    for mod in "$@"; do
        if module load "$mod" && is_loaded "$mod"; then
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
    local line name value note
    comm -13 <(printf '%s\n' "$before") <(printf '%s\n' "$after") |
        grep -E '^[A-Za-z_][A-Za-z0-9_]*=' |
        while IFS= read -r line; do
            name=${line%%=*}
            value=${line#*=}
            case "$name" in *PATH) continue ;; esac
            printf '%s\n' "$name" | grep -Eq "$PREFIX_NAMES" || continue
            printf '%s\n' "$name" | grep -Eiq "$SECRET_NAMES" && continue
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
        module show "$mod" 2>&1 | grep -E 'setenv|pushenv' | grep -Eiv "$SECRET_NAMES"
    done

    section "modules available now (module --terse avail, filtered)"
    echo "In a hierarchical Lmod, libraries built with this compiler and MPI appear here."
    module --terse avail 2>&1 |
        grep -Ei '^/|:$|(^|/)(cpu|gpu)(/|$)|mpi|mpich|hdf5|netcdf|pnetcdf|cmake|cuda'

    section "tools on PATH"
    local tool
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
    return "$failed"
}

main "$@"
