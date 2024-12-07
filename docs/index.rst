Welcome to the C-Star Documentation!
====================================

**C-Star** is an open-source modeling system developed by a team of ocean/biogeochemical modelers and scientific software engineers at `[\C\]Worthy <https://cworthy.org>`_. It is designed to support Monitoring, Reporting, and Verification (MRV) for research and commercial ocean-based Carbon Dioxide Removal (CDR) projects. C-Star aims to provide an accessible, common, framework for creating, sharing, and reproducing ocean biogeochemical simulations.

We are designing and building C-Star with these high-level principles in mind:

- **Scientific Integrity:** The C-Star modeling system utilizes trusted biogeochemical ocean models that have been developed in the public domain through decades of scientific R&D. Community involvement and iteration ensures that we are tracking the best-available science.
- **Transparency and Accessibility of our code:** Facilitates broad trust and adoption by both academic and commercial actors.
- **Reproducible and auditable:** Modeling simulations used to underpin carbon removal claims must be shareable and reproducible by a range of users.
- **Ease of use:** Ensures consistent application by diverse user groups including the commercial sector.
- **Standardization:** Ensures a consistent level of quality across CDR projects.

A key strength of C-Star lies in its ability to run regional simulations using a `“blueprint” <https://c-star.readthedocs.io/en/latest/terminology.html#term-blueprint>`_ that consolidates all the necessary data to define a model setup. This enables the creation of curated databases containing both scientifically validated and research-grade blueprints. These blueprints offer users the flexibility to easily reproduce simulations, making the modeling process more accessible and consistent.

.. toctree::
   :maxdepth: 1
   :caption: Getting Started

   Installing C-Star <installation>

.. toctree::
   :maxdepth: 1
   :caption: Terminology

   terminology

.. toctree::
   :maxdepth: 1
   :caption: Examples

   Building a Case and exporting it as a blueprint <1_building_a_case_and_exporting_it_as_a_blueprint>
   Importing and running a Case from a blueprint <2_importing_and_running_a_case_from_a_blueprint>
   Restarting and continuing a Case <3_restarting_and_continuing_a_case>
   Preparing input datasets for a Case with ROMS using roms-tools <4_preparing_roms_input_datasets>

.. toctree::
   :maxdepth: 1
   :caption: Deployment

   machines

.. toctree::
   :maxdepth: 1
   :caption: Reference

   api

.. toctree::
   :maxdepth: 1
   :caption: For Developers

   contributing
   releases
