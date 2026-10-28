.. _runs-attach:

Attaching to a moved or shared run
==================================

C-Star tracks runs per machine and per user: a run started here is known here by its
run ID, and ``cstar run status`` reads its live state. A run that arrived
some other way, copied or rsynced from another machine, transferred with
Globus, promoted by a colleague into the project runs area, or whose
tracking record was lost, is readable through its directory but is not
tracked. ``attach`` registers it, so that the ``cstar run`` commands and
other workplans can refer to it by name.

.. _workplan_attach_run:

Attaching a run
---------------

.. code-block:: console

   cstar run attach /path/to/copy/of/the/run

``attach`` reads the run's manifest, step records and transformed
workplan, and writes one tracking record in your state home. It writes
nothing into the run directory.

It first checks the run as ``cstar run check`` does (see
:ref:`workplan_check_run`). Pass ``--verify`` to also
compare the content of each product that records a digest (see
:doc:`manifest`), and ``--dry-run`` to see what ``attach`` would do without
writing the record.

Once attached, the run is known here: ``status``, ``show``, ``gather``,
``ls``, ``path``, a bare run ID and ``run://<uuid>`` all find it, and a
workplan can take its finished steps through a ``runs`` alias (see
:ref:`workplan_external_runs`). The run has no scheduler state on this
machine, so ``status`` shows what its manifest and step records say, and
``show`` and ``check`` read its directory as it is.

The run is attached under the run ID its manifest names. If you already have
a run ID with that name, ``attach`` refuses; pass ``--as <run-id>`` to
attach the run under a free one.
A copy attached ``--as`` another run ID keeps the run's uuid, so
``run://<uuid>`` resolves to the run tracked here first, then to attached
copies; name a copy by its run ID or directory to use it.


Owned or read-only
------------------

An attached run is **owned** or **read-only**, which decides whether a
command may write into its directory. It is read-only by default when its
directory is in the project runs area or belongs to another user, and owned
otherwise. Pass ``--own`` or ``--read-only`` when attaching to override the
default; on a run that is already attached, this will change the mode and nothing else.

A run attached as read-only refuses ``cstar run gather``, ``cstar run check
--repair``, ``cstar run check --record-digests`` and ``cstar admin clean
--run-id`` (the last removes the run's tracking record and leaves its
directory; the project runs area is left alone whatever the mode).

What an attached run is not
---------------------------

An attached run is never relaunched: ``cstar workplan submit --run-id`` and
``--resume`` refuse it, whether it is owned or read-only. It is a record to
inspect and a source to build on. To continue from it, start a new workplan
that references it by ``run://<uuid>`` or by its directory and takes what it
needs from its finished steps; see :ref:`workplan_extending_run`.

Future work may enable relaunching a partially completed run; if this feature
would be helpful for you, please reach out to the developers.
