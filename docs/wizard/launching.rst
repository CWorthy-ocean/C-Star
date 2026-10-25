.. _wizard:

Launching the wizard
====================

The wizard is C-Star's point-and-click interface. It has two pages. The
**Blueprint** page builds a :doc:`forge blueprint
<../applications/forge/blueprint>` without writing YAML: it presents the
choices as a series of sections, fills each from the :doc:`catalog
<../catalog>`, shows the resolved blueprint for review, and saves or
downloads it. The **Workplan** page composes blueprints into a
:doc:`workplan <../workplans/index>`, checks it as you edit, and submits it.
The wizard only writes files; nothing is downloaded or generated until you
run what it wrote.

As a web app
------------

.. code-block:: console

   cstar wizard

This serves the wizard with `Voila <https://voila.readthedocs.io>`__ at
``http://localhost:8866`` and opens it in your browser. ``--port`` picks
another port; any other options are passed through to Voila, for example
``--no-browser`` on a machine without one. (``cstar forge wizard`` is
equivalent.)

From a login node
-----------------

Login nodes have no browser. Serve the wizard there and forward the port
from your laptop:

.. code-block:: console

   # on the login node
   cstar wizard --no-browser
   # on your laptop
   ssh -N -L 8866:localhost:8866 <user>@<login-node>

then open ``http://localhost:8866`` locally. Alternatively, build the
blueprint on your laptop and copy the YAML to the cluster: a forge blueprint
does not depend on the machine it was written on. The Workplan page is more
useful on the cluster itself, where it can see your earlier runs and submit
to the scheduler.

As a Jupyter notebook
---------------------

The same wizard is available as a notebook, which is convenient where a
Jupyter server is already provided, such as an HPC OnDemand portal:

.. code-block:: console

   cstar forge copy-notebook          # writes ~/cstar/forge-blueprint-wizard.ipynb
   cstar env register-kernel          # once per environment, so Jupyter can find it

Open the copy in Jupyter and select the registered kernel. Re-run
``copy-notebook --force`` after upgrading C-Star to refresh the copy;
``--dest`` places it elsewhere. A copy is used rather than the installed
file because Jupyter saves executed output back into the notebook.

Where your work goes
--------------------

Blueprints and workplans you save, and specs you register, land in your own
writable catalog layer, ``~/cstar/catalog`` by default: blueprints under
``blueprints/forge/``, workplans under ``workplans/``, and specs in one
directory per spec kind. The bundled examples stay visible in the dropdowns,
marked ``(bundled)``, and cannot be overwritten: saving an edited bundled
spec means giving it a new name. Set ``CSTAR_CATALOG`` to use another
location or to add a shared group catalog. See :doc:`../catalog`.
