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

When the same setting is given more than one way, the first of these wins:

1. A command-line option.
2. An environment variable you have exported.
3. The configuration file.
4. The system's own default, such as the scratch file system for
   ``CSTAR_DATA_HOME`` on a cluster.
5. C-Star's built-in default.

An exported variable always beats the file. If a setting in the file seems
to be ignored, look for an old ``export`` line in your shell profile and
remove it.

C-Star refuses to run if the file contains a name it does not accept. The
error names the file and every offending name, so you can fix them in one
pass. Not accepted are:

- names it does not recognise, which are usually typos;
- feature flags (``CSTAR_FF_*``), which are developer toggles and must be
  exported;
- ``CSTAR_CONFIG_HOME``, because it says where to find the file;
- settings that C-Star no longer reads.

You can edit the file by hand, or let C-Star do it:

.. code-block:: console

   cstar env set CSTAR_SLURM_ACCOUNT my-allocation   # add or update a line
   cstar env unset CSTAR_SLURM_ACCOUNT               # comment the line out
   cstar env show                                    # the file's path and every value in effect

``cstar env set`` creates the file the first time you use it. ``cstar env
show`` marks each value with where it came from: ``[env]`` for an exported
variable, ``[config]`` for the file, ``[default]`` for anything else.

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
