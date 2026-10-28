.. _runs-promote:

Promoting a run to share with your group
========================================

A run on most HPCs will exist on the HPC's scratch space, which is often
purged on a regular schedule. Once a run is finished and worth keeping,
``promote`` places it in the **project runs
area**: ``cstar/runs`` under ``CSTAR_PROJECT_HOME``, where it outlasts your
scratch space and your collaborators can access it.

.. _workplan_promote_run:

Promoting a run
---------------

.. code-block:: console

   cstar run promote <run-id> --dry-run
   cstar run promote <run-id>

The run is named by its run ID, by ``run://<uuid>`` or by its directory.
The copy goes to ``<project runs area>/<run-id>``; pass ``--to <dir>`` to
put it at ``<dir>/<run-id>`` instead.

What is copied is a plan read from the run's manifest, not the whole
directory: the manifest, the transformed workplan, the ``attempts/``
directories, each step's record, logs and the small files in its ``work/``
directory, and every product the step records list. The layout is kept, so
every path in the manifest stays valid, and nothing in the files is
rewritten. The large parts of a ROMS step are left behind unless you ask for
them:

``--include builds``
   Each step's compile-time code, runtime code and cloned codebases.
``--include temp-output``
   The partitioned output pieces of a run without ParallelIO.
``--include all``
   The whole directory.

``promote`` never overwrites an existing run, and never writes into the
source. After the copy, the destination is checked as ``cstar run check``
does; pass ``--verify`` to compare the content of every file too (see
:doc:`manifest`). Absolute paths in the copied files that name the source are
listed for information, as ``cstar run check`` lists paths that name the run
itself. An absolute path to another place in your data home is listed too,
and counted as inherited from the source rather than as a problem of the copy:
it does not stop the promotion.


Copy, link, materialize or move
-------------------------------

``--mode`` chooses what ``promote`` does; the default is ``copy``:

``copy``
   A copy of the plan's files. The run in your data home is untouched and
   stays tracked here; the copy in the project runs area is found by its run
   ID, by its uuid or by its directory (see :ref:`workplan_external_runs`),
   and ``cstar run ls --shared`` lists it as ``project``.
``link``
   No copy; ``<project runs area>/<run-id>`` becomes a symlink to the run
   where it is, which registers the run with the project without spending
   the space. ``cstar run ls --shared`` lists it as ``project, linked``.
   Note that this method does not protect a run on a scratch directory from
   being purged.
``materialize``
   Replaces a run that was promoted with ``--mode link`` with a hard copy.
   The run the link pointed at is left in place, and can be deleted if desired.
``move``
   A copy of the whole directory, whatever ``--include`` says, after which
   the source directory is removed, once move has been fully verified.
   The run will then be tracked as if it were an external run that was
   :doc:`attached <attach>`: it is inspected and referenced, never
   relaunched, and it is read-only when the destination is in the project
   runs area (``cstar run attach <dir> --own`` takes ownership).

A copy or a link carries the file permissions of the source. The project
runs area is shared, so set your umask, or the group and permissions of the
area, so that your collaborators can read what you promote; C-Star changes
no permissions.

.. _workplan_linked_promotions:

Linked promotions
^^^^^^^^^^^^^^^^^

A run promoted with ``--mode link`` may stay linked for as long as you
like: C-Star never expires the link or asks you to replace it. The link is
only as durable as the run it points at in your data home, and that run is
yours: before your scratch space is purged, or before you delete or move
the run there, materialize it, which replaces the link with a copy under
the same uuid (add ``--to <dir>`` if you linked it with ``--to``):

.. code-block:: console

   cstar run promote <run-id> --mode materialize

Permissions work as for a copy: collaborators who follow the link read the
run in your data home directly, so that run must be readable by the
project's group, and the directories above it searchable.

Using a promoted run
--------------------

A promoted run is found by every command that takes a run: ``cstar run show
<run-id>``, ``run://<uuid>``, or its directory, and ``cstar run ls
--shared`` lists the area. Another workplan can continue from it or take its
outputs through a ``runs`` alias (see :ref:`workplan_external_runs`), and
``cstar run attach`` registers it on a machine where it is not yet tracked
(see :doc:`attach`). A run in the project runs area is never relaunched in
place: to extend it, start a new run that references it
(:ref:`workplan_extending_run`).
