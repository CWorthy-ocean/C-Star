.. _unreleased:

Unreleased
----------

.. note::
    This release is currently in development

Breaking Changes
~~~~~~~~~~~~~~~~


- Formally drop support for python 3.10 and 3.11 (`#613 <https://github.com/CWorthy-ocean/C-Star/pull/613>`_)

New features
~~~~~~~~~~~~


- Adds github action to finalize release notes ahead of a new release (`#612 <https://github.com/CWorthy-ocean/C-Star/pull/612>`_)

Bug Fixes
~~~~~~~~~


- Fixes incorrect backtick usage in the release notes, and updates the PR template scraper to use backticks correctly (`#612 <https://github.com/CWorthy-ocean/C-Star/pull/612>`_)

Improvements
~~~~~~~~~~~~


- Harden ROMS build process by handling separate NETCDFF paths, ensuring linked libraries come from a single source, and baking library locations into the roms binary (`#611 <https://github.com/CWorthy-ocean/C-Star/pull/611>`_)

Miscellaneous
~~~~~~~~~~~~~

- Rename default environment from cstar_env to cstar-env (`#613 <https://github.com/CWorthy-ocean/C-Star/pull/613>`_)
- Move environment yamls to root directory (`#613 <https://github.com/CWorthy-ocean/C-Star/pull/613>`_)
- Overhaul installation instructions (`#613 <https://github.com/CWorthy-ocean/C-Star/pull/613>`_)
- no-op changes to ``UTC`` usage to satisfy new linting under python 3.12 (`#613 <https://github.com/CWorthy-ocean/C-Star/pull/613>`_)
