.. _runs-compress:

Compressing a finished run
==========================

Model output is written uncompressed: a run with ParallelIO writes CDF-5 files
in double precision, and a year of a large domain is terabytes. ``cstar run
compress`` rewrites the data files of a finished run as chunked, deflated
netCDF-4, in place, and can also store variables in single precision.

.. code-block:: console

   cstar run compress <run> --dry-run
   cstar run compress <run>

``<run>`` is a run-id or the path of a run directory. A run attached read-only
is refused: ``cstar run attach <run> --own`` takes ownership first.

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
   (``child_bry``), carbonate sensitivity (``_cdrgas``) and forge files default
   to no: other runs read them as input, and a CPU ROMS build with ParallelIO
   cannot read netCDF-4 input.
2. **Float32-compress non-protected variables?** Double-precision variables
   that are not protected are stored as single precision.
3. **Additionally bit-reduce (quantize) them?** Asked only after a yes to the
   previous question: values are rounded (BitGroom) to the number of
   significant digits you give, which deflates far better.
4. **Chunking (x, y, z)?** The default is derived per variable (one time
   record per chunk, chunks under 1 MiB); enter ``x,y,z`` to override.

Then, once for the run: whether to delete each original once its compressed
copy is verified (otherwise it is moved to ``<run>/uncompressed_originals/``),
and whether to run here or submit a SLURM job. Each worker holds one record of
one variable at a time (about 3.5 times the largest record in memory); the
command shows that estimate when it asks how many files to compress at once.

Protected variables
-------------------

An application names the variables compression must keep exact; they are only
chunked and deflated, never cast or quantized. Coordinates and time variables
are always kept exact.

- ``ucla_roms`` and ``ucla_roms_gpu``: both members of every carbonate perturbation pair (``ALK*``,
  ``DIC*``, ``hALK*``, ``int_z_*``, ``pH``, ``pCO2SURF``, ``CO3`` and their
  ``*_ALT_CO2`` baselines, including the boundary forms such as ``ALK_north``),
  since an analysis takes the difference of a pair, which after months is far
  below single-precision resolution. And every variable another step reads:
  the inputs of the :ref:`CDR metrics <blueprints-cdr-metrics>` (``FG_CO2``,
  ``FG_ALT_CO2``, ``hDIC``, ``hDIC_ALT_CO2``, ``ALK_source``, ``DIC_source`` and
  the averaging window times), the carbonate sensitivities a CDR-LiTE run reads
  (``ddic_dco2``, ``ddic_dalk``), and the boundary fluxes and layer thicknesses
  the upscaler builds CDR forcing from (``*_add_<side>``, ``h_<side>``).
- ``forge``: every variable. Grids and forcing feed a model, so they are only
  compressed losslessly.

The list is written into the blueprint for each file type and can be edited.

The compress blueprint
----------------------

The settings are written to ``<run>/compress/B_compress_<run-id>_<time>.yaml``,
a blueprint of the ``compress`` application. Each file type is described by a
filename glob and a few variables it must hold, not by dimension sizes, so the
same blueprint can be applied to another run:

.. code-block:: console

   cstar run compress <other-run> --blueprint <run>/compress/B_compress_....yaml

Only file types the blueprint does not match are asked about. In a blueprint
edited by hand, ``significant_digits`` applies only with ``float32: true`` and
is refused without it.

Running as a job
----------------

Compressing terabytes takes hours. With ``--submit`` the command also asks for
the job's cores (files compressed at once), walltime, queue and account, writes
a one-step workplan ``W_compress_<run-id>_<time>.yaml`` carrying them in the
step's ``compute_overrides``, and submits it with ``cstar workplan submit``.
``--now`` compresses in the current process instead. ``--workers N`` sets the
number of files compressed at once (the job's cores with ``--submit``) without
the question, and overrides ``--yes``'s default of 1 and a reused blueprint's
saved value.

Compressing here on a login node, or on a machine with a scheduler C-Star cannot
submit to, prints a warning before it asks how many files to compress at once:
login nodes may stop long or memory-heavy processes, so submit a job, or keep
the session alive with ``tmux`` or ``screen``. ``cstar blueprint run`` of a compress blueprint logs the same
warning. ``CSTAR_ALLOW_LOGIN_NODE_RUN=1`` silences it.

Each file is written to ``<name>.partial`` and compared with its original on
the first and last record of each variable (exact variables bit for bit,
reduced ones within their tolerance; a lossless rewrite either succeeds or
fails outright, so the check samples rather than re-reads every record), and
only then renamed over the original. A file that fails the check is left as it
was. The run's step records get each file's new size, and a ``compression``
note, and ``<run>/compress-report.yaml`` lists every file. Running the command
again skips files that are already compressed.

If a run is interrupted, no original is lost: a file is only replaced by a
verified copy, and each file's step record is written as soon as it finishes.
Running the command again removes any leftover ``.partial`` files and skips the
files already compressed. One case is not covered: if only the parent process is
killed while workers are mid-file, a file a worker finishes afterwards is not
recorded, and ``cstar run check`` reports its size until you fix the record by
hand.
