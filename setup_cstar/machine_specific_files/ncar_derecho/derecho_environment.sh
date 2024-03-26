module purge
module load intel
module load netcdf
module load cray-mpich/8.1.25

export MPIHOME=/opt/cray/pe/mpich/8.1.25/ofi/intel/19.0/
export NETCDFHOME=$NETCDF

export PATH="./:$PATH"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$NETCDFHOME/lib"

# setup_cstar will append absolute paths for ROMS_ROOT and MARBL_ROOT here:
