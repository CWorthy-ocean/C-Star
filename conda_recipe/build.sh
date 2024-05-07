#!/bin/bash

# Use python setup.py in conjunction with manic to get externals

python -m pip install --no-deps --ignore-installed . -v
mv "${SRC_DIR}/externals" "${PREFIX}/externals/"
export MARBL_ROOT="${PREFIX}/externals/MARBL/"
export  ROMS_ROOT="${PREFIX}/externals/ucla-roms/"
#export MARBL_ROOT="$SRC_DIR/externals/MARBL/"
#export  ROMS_ROOT="$SRC_DIR/externals/ucla-roms/"

ARCH=$(uname -m)
OS=$(  uname -s)

function unsupported_error() {
    printf '%s\n' \
        "################################################################################" \
        "Unsupported system. Currently supported systems are:" \
        "- MacOS (ARM64)" \
        "- SDSC Expanse (https://www.sdsc.edu/support/user_guides/expanse.html)" \
        "- NCAR Derecho (https://ncar-hpc-docs.readthedocs.io/en/latest/compute-systems/derecho/)" \
        "- NERSC Perlmutter (https://docs.nersc.gov/systems/perlmutter/)" \
        "Generic support (gnu) will be added in a future release." \
        "To request support for your system, please raise an issue on GitHub:" \
        "https://github.com/CWorthy-ocean/C-Star/" \
        "################################################################################"
    exit 1
}


if [ "${ARCH}" = "arm64" ] && [ "${OS}" = "Darwin" ];then
    echo "Installing on MacOS (ARM64)"

    export MPIHOME=${BUILD_PREFIX}
    export NETCDFHOME=${BUILD_PREFIX}
    export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$NETCDFHOME/lib"
    export PATH="$ROMS_ROOT/Tools-Roms:$PATH"
    
    compiler=gnu

elif [ "{ARCH}" = "x86_64" ] && [ "${OS}" = "Linux" ];then
    compiler=intel
    if [ "${LMOD_SYSHOST}" = "expanse" ];then
	echo "Installing on Expanse"
	module purge
	module load slurm sdsc DefaultModules shared
	module load cpu/0.15.4  intel/19.1.1.217  mvapich2/2.3.4
	module load netcdf-c/4.7.4
	module load netcdf-fortran/4.5.3
	export NETCDFHOME=$NETCDF_FORTRANHOME
	export MPIHOME=$MVAPICH2HOME
	
    elif [ "${LMOD_SYSHOST}" = "derecho" ];then
	echo "Installing on Derecho"
	module purge
	module load intel
	module load netcdf
	module load cray-mpich/8.1.25
	export MPIHOME=/opt/cray/pe/mpich/8.1.25/ofi/intel/19.0/
	export NETCDFHOME=$NETCDF
	export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$NETCDFHOME/lib"	
	

    elif [ "${LMOD_SYSHOST}" = "perlmutter" ] ;then
	echo "Installing on Perlmutter"
	module purge
	module load PrgEnv-intel
	module load cray-hdf5
	module load cray-netcdf
	export MPIHOME=/opt/cray/pe/mpich/8.1.28/ofi/intel/2022.1/
	export NETCDFHOME=/opt/cray/pe/netcdf/4.9.0.9/intel/2023.2/
	export PATH=$PATH:$NETCDFHOME/bin
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$NETCDFHOME/lib
	export LIBRARY_PATH=$LIBRARY_PATH:$NETCDFHOME/lib
	
    else
	unsupported_error
    fi
else 	unsupported_error
fi						  
						 
# Compile MARBL
cd $MARBL_ROOT/src
make "${compiler}" USEMPI=TRUE

# Compile ROMS NHMG
cd $ROMS_ROOT/Work
make nhmg COMPILER="${compiler}"

cd $ROMS_ROOT/Tools-Roms
make COMPILER="${compiler}"

