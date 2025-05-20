FROM ubuntu:24.04
# Install base dependencies
RUN apt update && apt install -y \
    build-essential gcc g++ gfortran binutils \
    libc6-dev libstdc++-14-dev m4 libxml2-dev \
    libcurl4-openssl-dev zlib1g-dev wget \
    python3 python-is-python3\
    git rsync \
    && rm -rf /var/lib/apt/lists/*
# Set base install directory
ENV INSTALL_BASE_DIR=/opt
WORKDIR $INSTALL_BASE_DIR
# Install Intel OneAPI C++ essentials
RUN wget -q https://registrationcenter-download.intel.com/akdlm/IRC_NAS/189a6c48-72de-4bef-a43c-eb1ee49787d6/intel-cpp-essentials-2025.0.1.27_offline.sh \
    && sh ./intel-cpp-essentials-2025.0.1.27_offline.sh -a --silent --cli --eula accept \
    && rm intel-cpp-essentials-2025.0.1.27_offline.sh
# Install Intel OneAPI Fortran essentials
RUN wget -q https://registrationcenter-download.intel.com/akdlm/IRC_NAS/9e86b555-f238-4dea-b4b2-01b243e42483/intel-fortran-essentials-2025.0.1.27_offline.sh \
    && sh ./intel-fortran-essentials-2025.0.1.27_offline.sh -a --silent --cli --eula accept \
    && rm intel-fortran-essentials-2025.0.1.27_offline.sh
## Obtain and hardcode OneAPI environment variables:
## Updated variables here using:
# RUN bash -c "source /opt/intel/oneapi/setvars.sh \
#     && printenv"
ENV TBBROOT=/opt/intel/oneapi/tbb/2022.0/env/.. \
    ONEAPI_ROOT=/opt/intel/oneapi \
    PKG_CONFIG_PATH=/opt/intel/oneapi/tbb/2022.0/env/../lib/pkgconfig:/opt/intel/oneapi/mpi/2021.14/lib/pkgconfig:/opt/intel/oneapi/mkl/2025.0/lib/pkgconfig:/opt/intel/oneapi/dpl/2022.7/lib/pkgconfig:/opt/intel/oneapi/compiler/2025.0/lib/pkgconfig \
    I_MPI_ROOT=/opt/intel/oneapi/mpi/2021.14 \
    FI_PROVIDER_PATH=/opt/intel/oneapi/mpi/2021.14/opt/mpi/libfabric/lib/prov:/usr/lib/x86_64-linux-gnu/libfabric \
    DPL_ROOT=/opt/intel/oneapi/dpl/2022.7 \
    MANPATH=/opt/intel/oneapi/mpi/2021.14/share/man:/opt/intel/oneapi/debugger/2025.0/share/man:/opt/intel/oneapi/compiler/2025.0/share/man: \
    TCM_ROOT=/opt/intel/oneapi/tcm/1.2 \
    GDB_INFO=/opt/intel/oneapi/debugger/2025.0/share/info/ \
    CMAKE_PREFIX_PATH=/opt/intel/oneapi/tbb/2022.0/env/..:/opt/intel/oneapi/mkl/2025.0/lib/cmake:/opt/intel/oneapi/dpl/2022.7/lib/cmake/oneDPL:/opt/intel/oneapi/compiler/2025.0 \
    CMPLR_ROOT=/opt/intel/oneapi/compiler/2025.0 \
    INFOPATH=/opt/intel/oneapi/debugger/2025.0/share/info \
    UMF_ROOT=/opt/intel/oneapi/umf/0.9 \
    LIBRARY_PATH=/opt/intel/oneapi/tcm/1.2/lib:/opt/intel/oneapi/umf/0.9/lib:/opt/intel/oneapi/tbb/2022.0/env/../lib/intel64/gcc4.8:/opt/intel/oneapi/mpi/2021.14/lib:/opt/intel/oneapi/mkl/2025.0/lib:/opt/intel/oneapi/compiler/2025.0/lib \
    OCL_ICD_FILENAMES=/opt/intel/oneapi/compiler/2025.0/lib/libintelocl.so \
    CLASSPATH=/opt/intel/oneapi/mpi/2021.14/share/java/mpi.jar \
    LD_LIBRARY_PATH=/opt/intel/oneapi/tcm/1.2/lib:/opt/intel/oneapi/umf/0.9/lib:/opt/intel/oneapi/tbb/2022.0/env/../lib/intel64/gcc4.8:/opt/intel/oneapi/mpi/2021.14/opt/mpi/libfabric/lib:/opt/intel/oneapi/mpi/2021.14/lib:/opt/intel/oneapi/mkl/2025.0/lib:/opt/intel/oneapi/debugger/2025.0/opt/debugger/lib:/opt/intel/oneapi/compiler/2025.0/opt/compiler/lib:/opt/intel/oneapi/compiler/2025.0/lib \
    MKLROOT=/opt/intel/oneapi/mkl/2025.0 \
    INSTALL_BASE_DIR=/opt \
    NLSPATH=/opt/intel/oneapi/compiler/2025.0/lib/compiler/locale/%l_%t/%N \
    PATH=/opt/intel/oneapi/mpi/2021.14/bin:/opt/intel/oneapi/mkl/2025.0/bin:/opt/intel/oneapi/dpcpp-ct/2025.0/bin:/opt/intel/oneapi/dev-utilities/2025.0/bin:/opt/intel/oneapi/debugger/2025.0/opt/debugger/bin:/opt/intel/oneapi/compiler/2025.0/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    INTEL_PYTHONHOME=/opt/intel/oneapi/debugger/2025.0/opt/debugger \
    CPATH=/opt/intel/oneapi/umf/0.9/include:/opt/intel/oneapi/tbb/2022.0/env/../include:/opt/intel/oneapi/mpi/2021.14/include:/opt/intel/oneapi/mkl/2025.0/include:/opt/intel/oneapi/dpl/2022.7/include:/opt/intel/oneapi/dpcpp-ct/2025.0/include:/opt/intel/oneapi/dev-utilities/2025.0/include
# Additional variables to ensure mpif90 wraps oneapi compilers
ENV I_MPI_F90=ifx \
    I_MPI_F77=ifx \
    I_MPI_FC=ifx
# Install HDF5
ENV HDF5_HOME=$INSTALL_BASE_DIR/hdf5-1.14.6-install
RUN wget -q https://github.com/HDFGroup/hdf5/archive/refs/tags/hdf5_1.14.6.tar.gz \
    && tar xvf hdf5_1.14.6.tar.gz \
    && rm hdf5_1.14.6.tar.gz
WORKDIR $INSTALL_BASE_DIR/hdf5-hdf5_1.14.6
RUN bash -c "source /opt/intel/oneapi/setvars.sh \
    && export HDF5_HOME=$INSTALL_BASE_DIR/hdf5-1.14.6-install \
    && mkdir -p $HDF5_HOME \
    && ./configure --prefix=$HDF5_HOME \
    CC=mpiicx FC=mpiifx CXX=mpiicpc \
    CFLAGS="-fPIC -O3 -march=x86-64" \
    FFLAGS="-fPIC -O3 -march=x86-64" \
    CXXFLAGS="-fPIC -O3 -march=x86-64" \
    --enable-fortran --enable-shared --with-pic"
RUN bash -c "source /opt/intel/oneapi/setvars.sh \
    && make -j$(nproc) && make install"
# Set HDF5 environment variables
ENV PATH=$HDF5_HOME/bin:$PATH \
    LD_LIBRARY_PATH=$HDF5_HOME/lib:$LD_LIBRARY_PATH \
    CPATH=$HDF5_HOME/include:$CPATH \
    LIBRARY_PATH=$HDF5_HOME/lib:$LIBRARY_PATH \
    LD_RUN_PATH=$HDF5_HOME/lib
# Install NetCDF-C
WORKDIR $INSTALL_BASE_DIR
RUN wget -q https://github.com/Unidata/netcdf-c/archive/refs/tags/v4.9.3.tar.gz \
    && tar xvf v4.9.3.tar.gz \
    && rm v4.9.3.tar.gz
WORKDIR $INSTALL_BASE_DIR/netcdf-c-4.9.3
ENV NETCDF_HOME=$INSTALL_BASE_DIR/netcdf-4.9.3-install
RUN bash -c "source /opt/intel/oneapi/setvars.sh \
    && icx --version \
    && ifx --version \
    && mpiifx --version \
    && mpiicx --version \
    && export NETCDF_HOME=$INSTALL_BASE_DIR/netcdf-4.9.3-install \
    && mkdir -p $NETCDF_HOME \
    && ./configure --prefix=$NETCDF_HOME \
    CC=mpiicx FC=mpiifx \
    CFLAGS="-O3 -align -march=x86-64 -I$HDF5_HOME/include" \
    LDFLAGS="-L$HDF5_HOME/lib" \
    --enable-netcdf-4 --enable-shared"
RUN bash -c "source /opt/intel/oneapi/setvars.sh \
    && make -j$(nproc) && make install"
RUN rm -rf $INSTALL_BASE_DIR/netcdf-c-4.9.3
# # Set NetCDF-C environment variables
ENV PATH=$NETCDF_HOME/bin:$PATH \
    LD_LIBRARY_PATH=$NETCDF_HOME/lib:$LD_LIBRARY_PATH \
    CPATH=$NETCDF_HOME/include:$CPATH \
    LIBRARY_PATH=$NETCDF_HOME/lib:$LIBRARY_PATH
# Install NetCDF-Fortran
WORKDIR $INSTALL_BASE_DIR
ENV NETCDF_FORTRAN_HOME=$INSTALL_BASE_DIR/netcdf-fortran-4.6.1-install
RUN wget -q https://github.com/Unidata/netcdf-fortran/archive/refs/tags/v4.6.1.tar.gz \
    && tar xvf v4.6.1.tar.gz \
    && rm v4.6.1.tar.gz
WORKDIR $INSTALL_BASE_DIR/netcdf-fortran-4.6.1
RUN bash -c "source /opt/intel/oneapi/setvars.sh \
    && export NETCDF_FORTRAN_HOME=$INSTALL_BASE_DIR/netcdf-fortran-4.6.1-install \
    && mkdir -p $NETCDF_FORTRAN_HOME \
    && ./configure --prefix=$NETCDF_FORTRAN_HOME \
    CC=mpiicx FC=mpiifx \
    CFLAGS="-O3 -align -march=x86-64 -I$NETCDF_HOME/include" \
    FFLAGS="-O3 -align -march=x86-64 -I$NETCDF_HOME/include" \
    LDFLAGS="-L$NETCDF_HOME/lib" \
    --enable-shared"
RUN bash -c "source /opt/intel/oneapi/setvars.sh \
    && make -j$(nproc) && make install"
# Set NetCDF-Fortran environment variables
RUN rm -rf $INSTALL_BASE_DIR/netcdf-fortran-4.6.1
ENV PATH=$NETCDF_FORTRAN_HOME/bin:$PATH \
    LD_LIBRARY_PATH=$NETCDF_FORTRAN_HOME/lib:$LD_LIBRARY_PATH \
    CPATH=$NETCDF_FORTRAN_HOME/include:$CPATH \
    LIBRARY_PATH=$NETCDF_FORTRAN_HOME/lib:$LIBRARY_PATH
# Install ROMS
ENV ROMS_ROOT=$INSTALL_BASE_DIR/ucla-roms 
ENV PATH=$PATH:$ROMS_ROOT/Tools-Roms/
RUN bash -c "source /opt/intel/oneapi/setvars.sh \
    && wget https://github.com/dafyddstephenson/ucla-roms/archive/refs/heads/update_makefiles.tar.gz \ 
    && tar xvf update_makefiles.tar.gz \
    && mv ucla-roms-update_makefiles $ROMS_ROOT  \
    && cd $ROMS_ROOT/Work \
    && make nhmg COMPILER=intel \
    && cd $ROMS_ROOT/Tools-Roms  \
    && make COMPILER=intel"
# Install MARBL
# (Makefile doesn't define ifx option but setting I_MPI_F90 to ifx above should fix this)
ENV MARBL_ROOT=$INSTALL_BASE_DIR/MARBL
WORKDIR $INSTALL_BASE_DIR
RUN bash -c "source /opt/intel/oneapi/setvars.sh \
    && wget https://github.com/marbl-ecosys/MARBL/archive/refs/tags/marbl0.45.0.tar.gz \ 
    && tar xvf marbl0.45.0.tar.gz \
    && mv MARBL-marbl0.45.0 $MARBL_ROOT  \
    && cd $MARBL_ROOT/src \
    && make intel USEMPI=TRUE"