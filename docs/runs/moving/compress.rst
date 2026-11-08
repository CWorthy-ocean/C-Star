.. _runs-compress:

Compressing a finished run
==========================

Model output is written uncompressed: a run with ParallelIO writes CDF-5 files
in double precision, and a year of a large domain is terabytes. ``cstar run
compress`` rewrites the data files of a finished run as chunked, deflated
netCDF-4, in place, and can also store variables in single precision.

.. code-block:: console

   cstar run compress <run-directory> --dry-run
   cstar run compress <run-directory>

How files are grouped
---------------------

C-Star does not need to know what a model writes. It reads the header of every
data file the run's completed steps recorded (or, for runs recorded before
products were, every ``*.nc`` file in a step's directory), and groups the files
into **file types**: files with the same name once dates and timestamps are
replaced by ``*`` (``output/output_cdr.*.nc``), and the same variables. Files
without a variable that spans two or more non-time dimensions are left out.

The questions
-------------

For each file type, the command shows the number of files, their size, and how
many variables are protected, then asks:

1. **Compress it?** Yes rewrites the files as netCDF-4 with deflate and
   chunking, which is lossless on its own. Restart, nesting boundary
   (``child_bry``) and forge files default to no: other runs read them as
   input, and a CPU ROMS build with ParallelIO cannot read netCDF-4 input.
2. **Float32-compress non-protected variables?** Double-precision variables
   that are not protected are stored as single precision.
3. **Additionally bit-reduce (quantize) them?** Asked only after a yes to the
   previous question: values are rounded (BitGroom) to the number of
   significant digits you give, which deflates far better.
4. **Chunking (x, y, z)?** The default is derived per variable (one time
   record per chunk, chunks under 1 MiB); enter ``x,y,z`` to override.

Then, once for the run: whether to delete each original once its compressed
copy is verified (otherwise it is moved to ``<run>/uncompressed_originals/``),
and whether to run here or submit a SLURM job.

Protected variables
-------------------

An application names the variables compression must keep exact; they are only
chunked and deflated, never cast or quantized. Coordinates and time variables
are always kept exact.

- ``ucla_roms``: the inputs of the :ref:`CDR metrics <blueprints-cdr-metrics>`
  (``FG_CO2``, ``FG_ALT_CO2``, ``hDIC``, ``hDIC_ALT_CO2``, ``ALK_source``,
  ``DIC_source`` and the averaging window times), and both members of every
  carbonate perturbation pair (``ALK*``, ``DIC*``, ``hALK*``, ``int_z_*``,
  ``pH``, ``pCO2SURF``, ``CO3`` and their ``*_ALT_CO2`` baselines, including
  the boundary forms such as ``ALK_north``). An analysis takes the difference of
  a pair, which after months is far below single-precision resolution.
- ``forge``: every variable. Grids and forcing feed a model, so they are only
  compressed losslessly.

The list is written into the blueprint for each file type and can be edited.

The compress blueprint
----------------------

The answers are written to ``<run>/compress/B_compress_<run-id>_<time>.yaml``, a
blueprint of the ``compress`` application. Each file type is described by a
filename glob and a few variables it must hold, not by dimension sizes, so the
same blueprint can be applied to another run:

.. code-block:: console

   cstar run compress <other-run> --answers <run>/compress/B_compress_....yaml

Only file types the blueprint does not match are asked about.

Running as a job
----------------

Compressing terabytes takes hours. With ``--submit`` the command also asks for
the job's cores (files compressed at once), walltime, queue and account, writes
a one-step workplan ``W_compress_<run-id>_<time>.yaml`` carrying them in the
step's ``compute_overrides``, and submits it with ``cstar workplan submit``.
``--now`` compresses in the current process instead.

Each file is written to ``<name>.partial``, compared with its original (exact
variables bit for bit, reduced ones within their tolerance), and only then
renamed over the original. A file that fails the check is left as it was. The
run's step records get each file's new size, and a ``compression`` note, and
``<run>/compress-report.yaml`` lists every file. Running the command again skips
files that are already compressed.
