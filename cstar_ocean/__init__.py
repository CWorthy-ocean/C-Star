################################################################################
# Build module environment at import time
# NOTE: need to set ROMS_ROOT,MARBL_ROOT,CSTAR_ROOT,CSTAR_SYSTEM, and maybe modify PATH on conda install
import os,sys,platform

if (platform.system()=='Linux') \
   and ('LMOD_DIR'     in list(os.environ)) \
   and ('LMOD_SYSHOST' in list(os.environ)):
    sys.path.append(os.environ['LMOD_DIR']+'/../init')
    from env_modules_python import module

    syshost=os.environ['LMOD_SYSHOST'].casefold()
    module('purge')
    # FIXME: change to match/case whenever Expanse updates emacs
    if syshost=='expanse':
        sdsc_default_modules='slurm sdsc DefaultModules shared'
        nc_prerequisite_modules='cpu/0.15.4 intel/19.1.1.217 mvapich2/2.3.4'
        module('load',sdsc_default_modules)
        module('load',nc_prerequisite_modules)
        module('load','netcdf-c/4.7.4')
        module('load','netcdf-fortran/4.5.3')
        
    elif syshost=='derecho':
        module('load' 'intel')
        module('load','netcdf')
        module('load','cray-mpich/8.1.25')

        #export MPIHOME=/opt/cray/pe/mpich/8.1.25/ofi/intel/19.0/
        #export NETCDFHOME=$NETCDF

        #export PATH="./:$PATH"
        #export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$NETCDFHOME/lib"

    elif syshost=='perlmutter':
        module('load','PrgEnv-intel')
        module('load','cray-hdf5')
        module('load','cray-netcdf')
        

################################################################################


from .cstar_ocean import cstar_welcome
