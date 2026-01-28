This Containerfile was used while testing the build on Perlmutter with podman-hpc. It is _not_ currently meant to be built and used, but is left as an example of a working build that utilizes NERSC's vendor-optimized mpi, hdf5, and netcdf libraries during both build time and runtime.

Note that in initial testing, this container ran a very short simulation (1 week) in 18 seconds, compared to 49 seconds in the [regular "standalone" runner](ci/containers/runner) (no libs linked in at build time). The standing theory is that this is because of the cray-optimized netcdf/hdf5, since podman-hpc should be enabling the cray-optimized MPI in both cases. 

To build this container on Perlmutter, you should:

```commandline
# it may be unnecessary to do these module loads at build time, but doesn't hurt
module load cpu/1.0
module load cray-hdf5/1.12.2.9
module load cray-netcdf/4.9.0.9
module load PrgEnv-gnu/8.5.0

podman-hpc build -v /opt/cray:/opt/cray . -t runner_built_w_nersc_libs:latest
podman-hpc migrate runner_built_w_nersc_libs:latest
```

And then at runtime:

```commandline
salloc --nodes 2 --qos interactive --time 00:30:00 --constraint cpu --account m4632

export IMG_NAME=<your img from above build, migrated>

module load cpu/1.0
module load cray-hdf5/1.12.2.9
module load cray-netcdf/4.9.0.9
module load PrgEnv-gnu/8.5.0

srun  -N 2 -n 256  podman-hpc shared-run --mpi --scratch -v /pscratch/sd/e/eilerman/2node1wk/playground:/work -v /opt/cray:/opt/cray  $IMG_NAME bash -c "export LD_LIBRARY_PATH=/opt/cray/pe/mpich/8.1.30/ofi/gnu/12.3/lib/:$MPIHOME/lib:$NETCDF_DIR/lib:$HDF5_HOME/lib:/opt/cray/pe/hdf5/1.12.2.9/gnu/12.3/lib/:$LD_LIBRARY_PATH && cd /tmp && hostname && /opt/work_internal/compile_time_code/roms /work/ROMS/runtime_code/2node_test.in"
```