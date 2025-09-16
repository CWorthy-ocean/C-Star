# Working with C-Star Container Images

## Preparing to Build Images

1. A `Makefile` for building images can be found at `/ci/containers/Makefile`
2. Running the `Makefile` requires the following environment variables:
    - `CSTAR_CI_CONTAINER_REGISTRY` - The image repository to publish images to (e.g. `docker.io`)
    - `CSTAR_CI_CONTAINER_REGISTRY_ACCOUNT` - The account name to use when publishing and tagging images
3. The build will search for the following runtime engines and use the first available option:
    - `podman-hpc`
    - `podman`
    - `docker`

## Building Images

### All images

- Run `make all` from `/ci/containers/`

### Individual images

All images have a target in the `Makefile` that matches a directory holding a `Containerfile`.

The available targets are:

- `buildbase`
- `mpi5`
- `hdf5`
- `netcdf`
- `python`
- `roms`
- `roms-marbl`
- `runner`

These target names are used in all commands referencing a single image.

To build an image:

- Run `make <target>` (e.g. `make mpi5`)

## Running Images

After an image is built, a `Makefile` target can be used to enter an interactive shell.

Pass the name of the directory containing the `Containerfile` as the `img` argument:

- Run `make irun img=<target>`

## Fetching Images

To retrieve the latest version of all images, run:

- `make pullall`

To retrieve the latest version of a specific image, run:

- `make pull img=<target>`

## Running on multiple nodes on Perlmutter

Podman-hpc is necessary to facilitate MPI communications between containers. It's a bit tricky to get right. To date, we have not gotten it working with our openmpi build (MPI_Init hangs indefinitely when multiple ranks exist in the _same_ container, e.g. when using shared-run mode), but it does work using MPICH.

Two different versions of an MPICH build have succeeded: one that links in NERSC's optimized libraries at build time (see [here](ci/containers/all_w_nersc_libs)), and one that builds our own versions of mpich, hdf5, and netcdf from source (see the Makefile in this directory, culminating in the [runner container image](ci/containers/runner)). Instructions for building and running the NERSC-linked container are in a [separate README](ci/containers/all_w_nersc_libs/README.md).

For the multistage, built-from-source version, you should be able to clone this repo onto Perlmutter, and run `make all` from this directory. In initial tests, simulation-specific compile-time .opt files were added into the runner build directory and built in at container image build-time, so that we didn't need to recompile roms at runtime. 

With a built and migrated runner image, the following command worked at runtime:

```commandline
salloc --nodes 2 --qos interactive --time 00:30:00 --constraint cpu --account m4632

export IMG_NAME=<your img from above build, migrated>

module load cpu/1.0
module load cray-hdf5/1.12.2.9
module load cray-netcdf/4.9.0.9
module load PrgEnv-gnu/8.5.0

srun  -N 2 -n 256  podman-hpc shared-run --mpi --scratch -v /pscratch/sd/e/eilerman/2node1wk/playground:/work  $IMG_NAME bash -c "source /etc/profile && cd /tmp && hostname && /opt/work_internal/compile_time_code/roms /work/ROMS/runtime_code/2node_test.in"
```

Breaking down that last srun command, we launch against 2 nodes (`-N 2`) and want a total of 256 MPI ranks (`-n 256`). We call podman-hpc in "shared run" mode, which initiates one container per node, tells it to `sleep infinity` (by default, you can alter this if we need to add bootstrap steps later), and then executes each additional task into that single running container via a hidden `podman exec <command>` operation. In our case, this results in 2 nodes, 1 container each, with 128 roms executables running in each container.

For the rest of the command:
* `--mpi` activates the NERSC module at `/etc/podman_hpc/modules.d/mpich.yaml`, which is a shortcut for injecting a lot of environment variables and bind mounts from the node into the container
* `--scratch` is a NERSC-provided shortcut for mounting their SCRATCH filesystem
* `-v /pscratch/sd/e/eilerman/2node1wk/playground:/work` mounts my runtime files into the container (in retrospect, this wasn't needed after I found the `--scratch` shortcut)
* `$IMG_NAME` is just an env-var identifying our migrated image
* `bash -c` calls the rest of the commands in a single shell
* `source /etc/profile` sets internal environment variables pointing to libs and repos, established during the build. Note that in the current, official runner Containerfile, `entrypoint.sh` effectively does this. During testing, I had removed that, as podman-hpc perhaps doesn't work with entrypoint commands. You should be able to additionally skip an existing ENTRYPOINT directive with `--entrypoint=""` rather than removing it during build-time.
* `cd /tmp` was probably not necessary, but it's worth noting that when containers run on compute node, most of the container filesystem is read-only. Only `/tmp` and some other special directories can be written to inside the container.
* `hostname` was a sanity check that processes were starting on multiple nodes, and could be omitted when not debugging
* `/opt/work_internal/compile_time_code/roms` is the container-internal path to our pre-built roms executable
* `/work/ROMS/runtime_code/2node_test.in` is the mounted location of my runtime ROMS input.