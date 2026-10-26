
Support Machines
================

C-Star aims to be deployable on a wide range of HPC systems, but configuration
is needed on each system. This process is becoming more automated over time.

Here are the current systems where we have tested so far. Some systems have support
for building and running UCLA-ROMS via C-Star, but have not been tested or are not
yet supported for full workplan orchestration.

If you have a system that you would like to use with C-Star that isn't supported,
check out :doc:`../developers/system-registration`, or reach out to the repo maintainers.
We would love to help you get set up!

+--------------------------------------------------------------------------------------+------------+-------------------------+
| Name of system                                                                       | UCLA-ROMS  | Orchestration Support   |
|                                                                                      | build/run  |                         |
|                                                                                      | support    |                         |
+======================================================================================+============+=========================+
| `Anvil (RCAC) <https://www.rcac.purdue.edu/anvil>`__                                 | ✅         | ✅                      |
+--------------------------------------------------------------------------------------+------------+-------------------------+
| `Bouchet (YCRC) <https://docs.ycrc.yale.edu/clusters/bouchet/>`__                    | ✅         | ✅                      |
+--------------------------------------------------------------------------------------+------------+-------------------------+
| `Derecho (NCAR)                                                                      | ✅         | ❌ (PBS not currently supported)  |
| <https://ncar-hpc-docs.readthedocs.io/en/latest/compute-systems/derecho/>`__         |            |                         |
+--------------------------------------------------------------------------------------+------------+-------------------------+
| `Elja (IRHPC) <https://wiki.irei.is>`__                                              | ✅         | ✅                      |
+--------------------------------------------------------------------------------------+------------+-------------------------+
| `Expanse (SDSC) <https://www.sdsc.edu/support/user_guides/expanse.html>`__           | ✅         | Untested                |
+--------------------------------------------------------------------------------------+------------+-------------------------+
| `Perlmutter (NERSC) <https://docs.nersc.gov/systems/perlmutter/architecture/>`__     | ✅         | ✅                      |
+--------------------------------------------------------------------------------------+------------+-------------------------+
| MacOS ARM64 (Apple Silicon)                                                          | ✅         | ✅                      |
+--------------------------------------------------------------------------------------+------------+-------------------------+
| Linux (x86_64 and aarch 64)                                                          | ✅         | ✅                      |
+--------------------------------------------------------------------------------------+------------+-------------------------+