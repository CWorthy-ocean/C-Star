#!/bin/bash

################################################################################
# MODULES
module purge
# Restore default SDSC modules
module load slurm sdsc DefaultModules shared

# - Prerequisite modules for netcdf (as told by using 'module spider netcdf')
module load cpu/0.15.4  intel/19.1.1.217  mvapich2/2.3.4
# - netCDF-c
module load netcdf-c/4.7.4
# - netCDF-Fortran
module load netcdf-fortran/4.5.3
# - ncview
module load ncview/2.1.8
################################################################################
# ENVIRONMENT
export PATH="$(pwd):$PATH" # Add C-Star/setup_cstar dir to path

# - set roms' environment variables to match expanse module paths:
# 1 - ucla-roms compilation
export NETCDFHOME=$NETCDF_FORTRANHOME
export MPIHOME=$MVAPICH2HOME
# 2 - older roms verions compilation
export NETCDF=$NETCDF_FORTRANHOME
export MPI_ROOT=$MVAPICH2HOME

################################################################################
# setup_cstar will append absolute paths for ROMS_ROOT and MARBL_ROOT here:
