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


- ``bgc_mode: cdr_lite`` builds ucla-roms without MARBL and with the ``CDR_LITE`` key, from a ModelSpec, ``build_forge_blueprint(bgc_mode=...)`` or the wizard's BGC dropdown. (`#754 <https://github.com/CWorthy-ocean/C-Star/pull/754>`_)

  - The mode needs a CDR forcing whose releases target the CDR-lite tracers (simple-mode releases are tagged ``tracer_set: cdr_lite`` automatically; YAML and netCDF forcings must use it) and ucla-roms >= 0.10.0, the first release whose ``_cdrgas`` output is forcing-ready and whose CDR tracers need no ``_flx`` surface-flux variables.
  - ``nt_cdr_oae``/``nt_cdr_dor`` are read off the generated CDR forcing's tracer axis, ``nt_bgc`` is 0, and the ``_cdrtrc`` output stream is switched on so the CDR tracers have an output.
  - BGC-type forcing items, the online carbonate-sensitivity knob (needs MARBL) and volume releases are rejected at resolve time with a message naming them.

- New workplan directive ``carbonate-sensitivity-from`` sets a step's ``forcing.carbonate_sensitivity`` from the ``_cdrgas`` files of an earlier ROMS-MARBL step (``step:``) or a directory or file (``path:``), with ``;``-joined multiple sources like ``nest-from``. (`#754 <https://github.com/CWorthy-ocean/C-Star/pull/754>`_)

  - The wizard's Workplan page edits it and labels its DAG edges; ``cstar workplan check`` validates its config at schedule time.
  - The producing step must be a MARBL run with ``cdr_gas_exch_output.do_cdr_gas_exch_output`` on.

- roms_marbl blueprint schema 3.2.0 adds the optional ``forcing.carbonate_sensitivity`` dataset (additive: 3.1.0 files load and run unchanged). (`#754 <https://github.com/CWorthy-ocean/C-Star/pull/754>`_)

  - A ``CDR_LITE`` build that reads the sensitivities from files fails during setup, before the compile, when no staged forcing file provides the four variables ROMS reads, naming the directive as the usual fix.

- Forge blueprints may list carbonate sensitivity files up front (``carbonate_sensitivity: files: [...]``, a directory or paths in ``build_forge_blueprint``, a directory in the wizard); Forge checks their variables, stages copies (CDF-5 under ParallelIO) and lists them in the emitted blueprint. (`#754 <https://github.com/CWorthy-ocean/C-Star/pull/754>`_)
- ``cstar workplan path <run-id> [step]`` prints the directory of a run, or of one of its steps, and nothing else, so it composes with other shell commands. (`#755 <https://github.com/CWorthy-ocean/C-Star/pull/755>`_)

  - Example: ``dir=$(cstar workplan path <run-id>) && cd "$dir"``.
  - Unknown runs or steps, and directories that no longer exist, are reported on stderr with a non-zero exit code.

- ``cstar workplan cd <run-id> [step]`` (also ``cstar wp cd``) moves your shell to that directory once the shell function is installed; without it, the command prints the ``cd`` line and the one setup command. (`#755 <https://github.com/CWorthy-ocean/C-Star/pull/755>`_)
- ``cstar env shell-init --install`` sets up that shell function in one step: it detects zsh or bash, saves the function in the C-Star config directory, and adds a marked block that sources it to ``~/.zshrc`` or ``~/.bashrc``. (`#755 <https://github.com/CWorthy-ocean/C-Star/pull/755>`_)

  - Pass ``zsh`` or ``bash`` to choose the shell; if neither can be detected, the command asks you to name it.
  - Re-run it after upgrading C-Star to refresh the function; ``--uninstall`` removes the block and the function file.
  - Without ``--install``, the command prints the function, for people who manage their own dotfiles.


Bug Fixes
~~~~~~~~~


- ``cstar workplan run`` no longer rejects a step whose blueprint path contains a ``{{ }}`` placeholder (a runtime variable or ``{{output_dir: step@alias}}``); the path is filled when the workplan is prepared, as documented and as ``cstar workplan check`` already allowed. (`#752 <https://github.com/CWorthy-ocean/C-Star/pull/752>`_)
- A relative ``blueprint:`` path in a workplan step no longer fails at launch with "Blueprint file not found"; it is resolved against the workplan file's directory, whether ``cstar workplan run`` is invoked from that directory or elsewhere. (`#753 <https://github.com/CWorthy-ocean/C-Star/pull/753>`_)

  - ``cstar workplan check`` resolves relative paths the same way, so a workplan that passes ``check`` is one ``run`` accepts.
  - The transformed workplan written into the run directory records the resolved absolute path.
  - ``{{placeholder}}`` paths are filled first and then anchored; ``inline``, ``from_step``, remote URIs and absolute paths are left untouched.

- ``nest-from`` (and the new directive) now accept a ``path`` that names a single boundary file, as the docs always said; a file path used to be reported as "no boundary files located". (`#754 <https://github.com/CWorthy-ocean/C-Star/pull/754>`_)
- A generated CDR forcing that is reused without ``--clobber`` is now described from the file on disk (release count, release family, tracer axis), so the namelist can no longer disagree with the data ROMS reads. (`#754 <https://github.com/CWorthy-ocean/C-Star/pull/754>`_)
- Completing a step name in ``cstar workplan log``, ``path``, or ``cd`` after an unknown or mistyped run-id printed a traceback in the terminal; it now offers no completions. (`#759 <https://github.com/CWorthy-ocean/C-Star/pull/759>`_)
- Forge input generation no longer hangs intermittently with the progress bar frozen mid-save (an xarray netCDF lock-order deadlock, fixed upstream in xarray 2026.9). (`#758 <https://github.com/CWorthy-ocean/C-Star/pull/758>`_)
- Physics-only runs given a river file with BGC tracers no longer get a warning that the extra tracers go unused; on ucla-roms 0.9.0 and later that run aborts at startup. (`#757 <https://github.com/CWorthy-ocean/C-Star/pull/757>`_)
- The default ``roms-marbl-0.10-default`` model now builds ucla-roms 0.10.1, which fixes PIO restart writes stalling on multi-node runs until the job hit its time limit ([CWorthy-ocean/ucla-roms#388](https://github.com/CWorthy-ocean/ucla-roms/pull/388)). (`#762 <https://github.com/CWorthy-ocean/C-Star/pull/762>`_)


Improvements
~~~~~~~~~~~~


- An unknown ``bgc_mode`` value is rejected at resolve time instead of silently building a physics-only run. (`#754 <https://github.com/CWorthy-ocean/C-Star/pull/754>`_)
- The CDR forcing's tracer axis is checked against the build's tracer count at generation, in every mode, so a file built for a different tracer layout fails there rather than inside ROMS. (`#754 <https://github.com/CWorthy-ocean/C-Star/pull/754>`_)
- CDR-lite tracer counts in a build without ``CDR_LITE`` (which ROMS aborts on with "Forcing type not supported") are rejected when the blueprint is resolved or validated. (`#754 <https://github.com/CWorthy-ocean/C-Star/pull/754>`_)
- A stored ``cdr_lite`` blueprint pinned to a ucla-roms release below the mode's minimum fails at validation, before any input is generated. (`#754 <https://github.com/CWorthy-ocean/C-Star/pull/754>`_)
- ``cstar workplan log`` and ``gather`` report an empty or invalid run-id with the run-tracking message ("A valid run-id was not provided") instead of a separate pre-check message. (`#755 <https://github.com/CWorthy-ocean/C-Star/pull/755>`_)
- Forge rejects a river forcing file whose tracer count ucla-roms would abort on, naming the file and the accepted lengths, before ROMS is built. (`#757 <https://github.com/CWorthy-ocean/C-Star/pull/757>`_)

  - From ucla-roms 0.9.0 (and on branch or commit pins), the accepted lengths are every model tracer, or every tracer except the CDR tracers, the same in every river file.
  - On older pins a shorter file is rejected and a longer one warns that the extra tracers are ignored.

- Forge rejects, at blueprint resolution, a roms-tools-generated river that ROMS cannot read: one with passive tracers, or a MARBL run without ``include_bgc``. (`#757 <https://github.com/CWorthy-ocean/C-Star/pull/757>`_)
- A river file with too few tracers fails at the river step instead of after the remaining inputs are generated. (`#757 <https://github.com/CWorthy-ocean/C-Star/pull/757>`_)

Miscellaneous
~~~~~~~~~~~~~

- The workplan docs and the ``Step.blueprint_path`` docstring state that relative paths are resolved against the workplan file's location. (`#753 <https://github.com/CWorthy-ocean/C-Star/pull/753>`_)
- New bundled ModelSpec ``roms-marbl-0.10-default`` (ucla-roms 0.10.0; same namelist tier and templates as the 0.9 spec) is the wizard's and the integration suite's default; ``roms-marbl-0.9-default`` remains for 0.9.1. (`#754 <https://github.com/CWorthy-ocean/C-Star/pull/754>`_)
- The Tier 2 e2e workplan grows to four steps: the MARBL run writes gas-exchange output, a ``cdr_lite`` forge step and a CDR-LiTE run read it back through the ``carbonate-sensitivity-from`` directive. (`#754 <https://github.com/CWorthy-ocean/C-Star/pull/754>`_)
- Tier 1 integration case ``cdr_lite`` (physics-only ForcingSpec, simple-mode CDR forcing, golden namelist), skipped on ucla-roms pins the mode's own version gate rejects. (`#754 <https://github.com/CWorthy-ocean/C-Star/pull/754>`_)
- Docs: directive reference, forge blueprint and input-data developer docs, wizard and spec docs, terminology, API list; published schema ``roms_marbl_schema.3.2.0.json`` and blueprint template. (`#754 <https://github.com/CWorthy-ocean/C-Star/pull/754>`_)
- The workplans guide has a new "Finding a Run's Directory" section covering ``path``, ``cd`` and the shell setup. (`#755 <https://github.com/CWorthy-ocean/C-Star/pull/755>`_)
- ``shellingham`` is now a declared dependency; it was already installed as a requirement of ``typer``. (`#755 <https://github.com/CWorthy-ocean/C-Star/pull/755>`_)
- Tests now use the real ``roms_marbl`` application: the test conftest no longer replaces it with a ``SleepApplication`` subclass registered under the same name. (`#756 <https://github.com/CWorthy-ocean/C-Star/pull/756>`_)
- C-Star now requires ``xarray>=2026.9`` and ``roms-tools>=5.1.1`` (the first roms-tools release without the old ``xarray<2025.8`` cap); existing environments will upgrade both. (`#758 <https://github.com/CWorthy-ocean/C-Star/pull/758>`_)
- The weekly extended integration workflow now tests ucla-roms 0.10.1 instead of 0.10.0. (`#762 <https://github.com/CWorthy-ocean/C-Star/pull/762>`_)
