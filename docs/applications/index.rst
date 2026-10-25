.. _applications-index:

Applications and blueprints
===========================

Everything C-Star does, it does by running an **application** on a
**blueprint**. The pages in this section describe each application that ships
with C-Star, what its blueprint contains, and what it leaves behind.

An application is a thing C-Star can run. It might be an ocean model, a tool
that prepares model inputs, a small data transformation between two model
runs, or an analysis of a run's output. Each application declares the
blueprint it accepts and knows how to turn one into a result. The
applications that ship with C-Star are:

``ucla_roms``
   Runs a UCLA-ROMS simulation, with or without MARBL biogeochemistry. It
   stages the inputs, fetches and compiles the model code, fills in the
   namelist, runs the model and tidies up the output. See
   :doc:`ucla_roms/index`.
``forge``
   Builds a new UCLA-ROMS domain: the grid, initial conditions and forcing
   files, the model settings, and the UCLA-ROMS blueprint that runs them. See
   :doc:`forge/index`.
``nest_ic`` and ``upscaler``
   Two data transformations for nested experiments: the first turns a parent
   run's restart into initial conditions for a child grid, the second turns a
   child's boundary fluxes into carbon dioxide removal forcing for the parent.
   See :doc:`transformations/index`.
``cdr_metrics`` and ``plotter``
   Analyses of a run's output: the CO2 uptake and efficiency of a carbon
   dioxide removal release, and a small plotting demonstration. See
   :doc:`analytics/index`.

A blueprint is a YAML file that holds every input an application needs: for
a simulation, which model code to build, which input files to use and which
settings to apply; for a domain, which region, resolution, datasets and run
window. Given the same blueprint, an application produces the same result on
a given machine, which is what makes C-Star results shareable and
reproducible. Every blueprint has the same small core (a name, the
application it is for, and where to work), and each application adds the
fields it needs. :doc:`blueprints` describes the core and the commands that
check and run a blueprint.

Blueprints are also the unit a :doc:`workplan <../workplans/index>` is built
from: each step of a workplan runs one application on one blueprint, and
steps can hand files and even whole blueprints to one another.

Writing your own application
----------------------------

The applications here are not a closed set. A post-processing script, a
different model, or a transformation your project needs can be wrapped as a
C-Star application with a blueprint of its own, and then used in workplans
alongside the built-in ones. The small applications in this section
(``hello_world``, ``nest_ic``, ``plotter``) are written exactly that way and
double as examples. :doc:`../custom_applications` in the developer
documentation walks through it.
