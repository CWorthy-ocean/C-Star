.. _unreleased:

Unreleased
----------

.. note::
    This release is currently in development

Breaking Changes
~~~~~~~~~~~~~~~~

- N/A

New features
~~~~~~~~~~~~


- ``cstar wp ...`` is now shorthand for ``cstar workplan ...`` (the whole subcommand tree, e.g. ``cstar wp run``, ``cstar wp ls``). (`#697 <https://github.com/CWorthy-ocean/C-Star/pull/697>`_)
- ``cstar bp ...`` is now shorthand for ``cstar blueprint ...`` (e.g. ``cstar bp run``, ``cstar bp check``). (`#697 <https://github.com/CWorthy-ocean/C-Star/pull/697>`_)
- ``cstar workplan list`` now works as an alternate name for ``cstar workplan ls``, with an identical set of options. (`#697 <https://github.com/CWorthy-ocean/C-Star/pull/697>`_)
- ``CSTAR_CATALOG`` is a registered C-Star environment variable (shown by ``cstar env show`` under File System Configuration): ``os.pathsep``-separated catalog locations for forge blueprint authoring (local paths, GitHub or http URLs), stacked over the bundled catalog, with the first entry as the writable user layer. Default ``<CSTAR_DATA_HOME>/catalog``. Nothing reads it yet; forge's catalog switches to it when it moves here. (`#698 <https://github.com/CWorthy-ocean/C-Star/pull/698>`_)

Bug Fixes
~~~~~~~~~

- N/A

Improvements
~~~~~~~~~~~~


- ``cstar workplan --help`` and ``cstar blueprint --help`` now name their available alias, so the shorthand is discoverable from the help output. (`#697 <https://github.com/CWorthy-ocean/C-Star/pull/697>`_)
- Ruff selects ``RUF`` in addition to ``I, F, E, W, D, UP, TCH``, matching cstar-forge exactly (``RUF043`` regex metacharacters in ``pytest.raises(match=)`` and ``RUF059`` unused unpacked names are ignored in both repos). The findings were mechanical: stale ``noqa`` comments, two ``[0]`` slices on list copies, two chained comparisons, one f-string conversion, four curly apostrophes in docstrings. (`#698 <https://github.com/CWorthy-ocean/C-Star/pull/698>`_)
- ``flake8-type-checking`` is configured with ``runtime-evaluated-base-classes`` (``pydantic.BaseModel``, ``pydantic_settings.BaseSettings``) and ``runtime-evaluated-decorators`` (``dataclasses.dataclass``), so TCH never moves an import that Pydantic evaluates at runtime into a ``TYPE_CHECKING`` block. Nothing in ``cstar/`` trips this today; forge's executor (``host: HostPaths | None`` where ``HostPaths`` is a dataclass) does, and broke without it. (`#698 <https://github.com/CWorthy-ocean/C-Star/pull/698>`_)
- ``network`` and ``slow`` pytest markers are registered for forge's test suite; test selection in this repo remains by path. (`#698 <https://github.com/CWorthy-ocean/C-Star/pull/698>`_)

Miscellaneous
~~~~~~~~~~~~~

- pre-commit: ``types-ujson`` added to the mypy hook (forge's GLORYS subchunking imports ``ujson``, and typeshed knows the stub package, so ``ignore_missing_imports`` does not silence it); ``check-added-large-files`` raised from 100 kB to 200 kB, matching forge (the hook fires only on files newly staged as added; two tracked test files already exceed 100 kB). (`#698 <https://github.com/CWorthy-ocean/C-Star/pull/698>`_)
- ``MANIFEST.in`` points at ``cstar/additional_files`` instead of the long-gone ``cstar_ocean/additional_files/ROMS_Makefiles``. (`#698 <https://github.com/CWorthy-ocean/C-Star/pull/698>`_)
