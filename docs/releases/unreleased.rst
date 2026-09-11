.. _unreleased:

Unreleased
----------

.. note::
    This release is currently in development

Breaking Changes
~~~~~~~~~~~~~~~~


- C-Star now requires ``roms_tools>=5.0,<6`` (previously ``>=4.0.1,<5``). The only roms-tools API C-Star itself calls that changed in 5.0 is ``InitialConditions`` call in the ``NestIc`` application; everything else is unaffected. (`#684 <https://github.com/CWorthy-ocean/C-Star/pull/684>`_)

New features
~~~~~~~~~~~~


- Blueprints passed to ``cstar workplan run ...`` will be executed in the context of a workplan. (`#675 <https://github.com/CWorthy-ocean/C-Star/pull/675>`_)

Bug Fixes
~~~~~~~~~


- The per-step "Job Details" block in the ``cstar workplan run`` summary showed another step's status and task/job ID; each step now reports its own. (`#678 <https://github.com/CWorthy-ocean/C-Star/pull/678>`_)

Improvements
~~~~~~~~~~~~


- Fix parameter typo and include capability to re-attach in main help text. (`#675 <https://github.com/CWorthy-ocean/C-Star/pull/675>`_)

Miscellaneous
~~~~~~~~~~~~~

- N/A
