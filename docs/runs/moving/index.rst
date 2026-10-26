.. _runs-moving:

Moving runs
===========

A run is a directory, and a directory can be copied, transferred to another
machine, or placed where a group shares it. C-Star keeps enough in that
directory for the copy to be understood wherever it lands: what the run
was, what each step did, and what it left behind. These pages describe that
record and the commands that move a run and pick it up again.

.. toctree::
   :maxdepth: 1

   The run manifest and moving runs <manifest>
   Promoting a run to share with your group <promote>
   Attaching to a moved or shared run <attach>

In short:

- Every run writes a :doc:`manifest <manifest>`, and every step a record of
  its own. Paths in them are relative, so a run directory stays readable when
  it is moved or copied as a unit, and ``cstar run show <directory>`` reads
  it anywhere.
- :doc:`promote` copies, links or moves a finished run into your project's
  shared runs area, where it outlasts your scratch space and your
  collaborators can find it.
- :doc:`attach` registers a run that arrived by copy, so that the ``cstar
  run`` commands and other workplans can refer to it by name in your environment.
