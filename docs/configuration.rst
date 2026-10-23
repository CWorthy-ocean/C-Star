Configuration
=============

.. _configuration_file:

Configuration file
------------------

The simplest way to set C-Star up is a configuration file, ``config.env``,
in ``~/.config/cstar``. If ``CSTAR_CONFIG_HOME`` is set, C-Star looks there
instead, and otherwise under ``XDG_CONFIG_HOME/cstar`` when that is set.

The file has one ``NAME=value`` per line. Blank lines and lines starting
with ``#`` are ignored, ``~`` and ``$VAR`` in a value are expanded, and a
value that contains spaces needs quotes:

.. code-block:: bash

   # Scheduler settings for this cluster
   CSTAR_SLURM_ACCOUNT=my-allocation
   CSTAR_SLURM_QUEUE=mpi

The names are the ``CSTAR_*`` variables listed in the table below. Every
``cstar`` command reads the file before it runs, and so does C-Star when you
use it from Python or a notebook.

An exported variable always beats the file. If a setting in the file seems
to be ignored, look for an old ``export`` line in your shell profile and
remove it. The full order is under :ref:`configuration_precedence`.

C-Star refuses to run if the file contains a name it does not accept. The
error names the file and every offending name, so you can fix them in one
pass. Not accepted are:

- names it does not recognise, which are usually typos;
- feature flags (``CSTAR_FF_*``), which are developer toggles and must be
  exported;
- ``CSTAR_CONFIG_HOME``, because it says where to find the file;
- settings that C-Star no longer reads.

The easiest way to create the file is ``cstar init``, which asks for the
scheduler settings and the data locations with the current value of each as
the default, writes the answers, and lists every other setting as a commented
line you can fill in later. You can also edit the file by hand, or let C-Star
change one setting:

.. code-block:: console

   cstar env set CSTAR_SLURM_ACCOUNT my-allocation   # add or update a line
   cstar env unset CSTAR_SLURM_ACCOUNT               # comment the line out
   cstar env show                                    # the file's path and every value in effect

``cstar env set`` creates the file the first time you use it. ``cstar env
show`` marks each value with where it came from: ``[env]`` for an exported
variable, ``[config]`` for the file, ``[default]`` for anything else.

.. _configuration_precedence:

Where a setting can be set, and which wins
------------------------------------------

Most settings can be given in several places. From lowest to highest
precedence, a later one replaces an earlier one:

1. C-Star's built-in default.
2. The system's own default: the scratch file system named by one of the
   ``CSTAR_SCRATCH_DIRS`` variables or known to C-Star for the machine, and
   the ``XDG_*`` and ``PROJECT`` variables that stand in for the directory
   settings.
3. The configuration file.
4. An exported ``CSTAR_*`` variable.
5. A command-line option, where the command has one (``--log-level``, for
   example).

The SLURM settings (``CSTAR_SLURM_ACCOUNT``, ``CSTAR_SLURM_QUEUE``,
``CSTAR_SLURM_MAX_WALLTIME``) have no command-line option but two more layers
above the environment: the workplan's ``compute_environment.slurm`` block,
then each step's ``compute_overrides.slurm``. The value that results for
each step is recorded in the transformed workplan in the run directory. See
:ref:`workplan_compute_environment`.

Two things to know when running workplans:

- ``cstar workplan submit --resume`` re-exports the settings recorded when the
  run started, except the data and state homes, so a resumed run keeps its
  original settings even if your shell or configuration file changed since.
- On a system that requires a SLURM account and queue, C-Star asks for them
  from the environment only when neither the workplan's ``compute_environment``
  nor every step's ``compute_overrides`` supplies them.

Environment Variables
---------------------

User-settable environment variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following variables control C-Star behavior. Put them in the
configuration file above, or export them from your ``.bash_profile`` or
``.zshrc`` if you prefer to keep them with your shell setup.

.. tip::
    Use ``cstar env show --display export`` to generate a starter script.

.. envvar-table::
    cstar.base.env
    cstar.base.utils
    cstar.orchestration.utils
    cstar.base.feature
