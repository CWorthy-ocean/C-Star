C-Star builds and submits GPU runs on the H100 nodes of TACC's Stampede3.
- The GPU build links the Fortran MPI library the MPI installation provides: MPICH's ``libmpifort`` or OpenMPI's ``libmpi_mpifh``.
- A system file's ``gpu_nodes`` can set ``request_gpus: false`` for GPU nodes that a job reaches through its partition alone, without asking for GPUs (``--gpus-per-node``).
