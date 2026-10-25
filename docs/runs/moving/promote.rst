.. _runs-promote:

Promoting a run to share with your group
========================================

A run in your data home lives on scratch, which is purged. Once a run is
finished and worth keeping, ``promote`` places it in the **project runs
area**: ``cstar/runs`` under ``CSTAR_PROJECT_HOME`` (``$PROJECT/cstar/runs``
on Anvil; see :doc:`../../hpc`), where it outlasts your scratch space and
your collaborators find it.

.. _workplan_promote_run:

Promoting a run
---------------

.. code-block:: console

   cstar run promote <run-id> --dry-run
   cstar run promote <run-id>

The run is named by its run ID, by ``run://<uuid>`` or by its directory.
The copy goes to ``<project runs area>/<run-id>``; pass ``--to <dir>`` to
put it at ``<dir>/<run-id>`` instead. Without a project runs area and
without ``--to``, ``promote`` stops with an error. ``--dry-run`` prints the
plan, with the number of files and the size of each group, and copies
nothing.

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
   The per-rank output pieces of a run without ParallelIO.
``--include all``
   The whole directory.

``promote`` never overwrites. A destination that holds another run (a
manifest with another uuid) is refused, and so is one that already holds
this run. A product the step records list that is missing from the source, a
run directory holding more than one transformed workplan, and a destination
file system without room for the plan are refused before anything is
copied, so run ``cstar run check`` on the run first. Symlinks are copied as
links: a link inside the run stays relative, and a link that leaves the run
is written absolute so it still names the file where it is.

After the copy, the destination is checked as ``cstar run check`` does, and
its ``gathered_output`` is rebuilt when the source has one. A product that
is missing or whose size changed, a carried input that differs, a step
directory that is missing or a dangling link fails the promotion with exit
code 1. Pass ``--verify`` to compare the content of every product too: the
digest of each product that has none is computed from the source, recorded
in the copy's step records and manifest, and the copy is compared against
every recorded digest (see :doc:`manifest`). The source is never written
into.

The exit code is 0 when the run was promoted, 1 when it was promoted but
the copy has problems of its own (or the source misses products and nothing
was copied), and 2 for bad arguments, a destination that is taken and a move
that is refused.

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
   the space. ``cstar run ls --shared`` lists it as ``project, linked``: it
   is as durable as its target.
``materialize``
   Replaces that link with a copy under the same uuid. The run the link
   pointed at is left in place; delete it yourself, or promote with
   ``--mode move`` to begin with.
``move``
   A copy of the whole directory, whatever ``--include`` says, after which
   the source directory is removed, once the destination passed the check;
   a move loses nothing. A run tracked here is then tracked as
   :doc:`attached <attach>`: it is inspected and referenced, never
   relaunched, and it is read-only when the destination is in the project
   runs area (``cstar run attach <dir> --own`` takes ownership). ``move``
   refuses a run that is attached read-only, a run that is not tracked here
   and was found by its run ID or uuid (pass its directory to move it
   deliberately) and a run whose latest attempt has not finished; the other
   modes warn about an unfinished attempt and go ahead.

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
