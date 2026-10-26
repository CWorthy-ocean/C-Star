Configuration
=============

On a laptop, C-Star works out of the box: run output goes under
``~/cstar``, and nothing needs to be set before your first blueprint. On a
cluster, C-Star needs to know which scheduler account and queue to submit
jobs to, and it is worth confirming where it will put data. One command
covers both cases:

.. code-block:: console

   cstar init

It asks for the scheduler settings (listing your SLURM associations and
partitions when it can), shows where run output, the shared source-data
cache and your catalog will go, writes the answers to a configuration file,
and offers to install the shell function behind ``cstar run cd`` and to
register a Jupyter kernel for the environment. Every prompt defaults to the
value in effect now, so running it again later reviews and updates your
setup. ``cstar init --yes`` accepts every default without asking.

That is all most people need. The rest of this page is for when you want to
see or change an individual setting: every option C-Star reads, the
configuration file ``cstar init`` writes, and how settings from different
places combine.

Settings
--------

The following variables control C-Star behavior. Put them in the
:ref:`configuration file <configuration_file>`, or export them from your
``.bash_profile`` or ``.zshrc`` if you prefer to keep them with your shell
setup. ``cstar env show`` prints the value of each in effect and where it
came from.

.. tip::
    Use ``cstar env show --display export`` to generate a starter script.

.. envvar-table::
    cstar.base.env
    cstar.base.utils
    cstar.orchestration.utils
    cstar.base.feature

.. _configuration_file:

The configuration file
----------------------

The configuration file is ``config.env`` in ``~/.config/cstar``. If
``CSTAR_CONFIG_HOME`` is set, C-Star looks there instead, and otherwise under
``XDG_CONFIG_HOME/cstar`` when that is set.

The file has one ``NAME=value`` per line. Blank lines and lines starting
with ``#`` are ignored, ``~`` and ``$VAR`` in a value are expanded, and a
value that contains spaces needs quotes:

.. code-block:: bash

   # Scheduler settings for this cluster
   CSTAR_SLURM_ACCOUNT=my-allocation
   CSTAR_SLURM_QUEUE=mpi

The names are the ``CSTAR_*`` variables in the table above. Every ``cstar``
command reads the file before it runs, and so does C-Star when you use it
from Python or a notebook.

``cstar init`` is the easiest way to create the file. You can also edit it by
hand, or let C-Star change one setting:

.. code-block:: console

   cstar env set CSTAR_SLURM_ACCOUNT my-allocation   # add or update a line
   cstar env unset CSTAR_SLURM_ACCOUNT               # comment the line out
   cstar env show                                    # the file's path and every value in effect

``cstar env set`` creates the file the first time you use it. ``cstar env
show`` marks each value with where it came from: ``[env]`` for an exported
variable, ``[config]`` for the file, ``[default]`` for anything else.

An exported variable always beats the file. If a setting in the file seems
to be ignored, look for an old ``export`` line in your shell profile and
remove it. The full order is under :ref:`configuration_precedence`.

A line C-Star does not recognize, such as a typo, is skipped with a
warning naming it, and every other line still applies.

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
