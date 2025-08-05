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
