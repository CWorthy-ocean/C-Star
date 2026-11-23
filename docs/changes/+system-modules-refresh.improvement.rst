On Anvil, Bouchet and Elja, C-Star now loads the modules a build uses by name, including those the site loads by default, and on Anvil, Elja and Perlmutter it loads ``cmake``, which ParallelIO builds need:
- Anvil loads ``cmake/3.20.0`` and its default ``hdf5/1.10.7`` and ``netcdf-c/4.7.4`` explicitly; Elja loads ``CMake/3.26.3``; Perlmutter loads ``cmake/3.30.2``.
- Bouchet builds with the ``gompi-2024a`` toolchain (GCC 13.3.0, OpenMPI 5.0.3, HDF5 1.14.5, netCDF 4.9.2, netCDF-Fortran 4.6.1, PnetCDF 1.14.0) in place of ``gompi-2022b``, and finds MPI through ``EBROOTOPENMPI``.
- Elja loads ``GCC/12.3.0``, the compiler its netCDF-Fortran module already switched to, and names the HDF5 and netCDF modules that came with it.
- On Derecho, Elja, Expanse and Perlmutter, ``MPIHOME``, ``NETCDFHOME`` and ``PNETCDFHOME`` no longer end in ``/``. On Derecho, a missing ``parallel-netcdf`` module now stops the build with a message naming ``PNETCDFHOME`` instead of pointing it at ``/``.
