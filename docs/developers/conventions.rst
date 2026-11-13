Design and CLI conventions
==========================

These are the rules code review applies to C-Star pull requests. Each one
gives the rule, the reason, and where it applies; a pull request number points
at the change where the rule came up. Read this page before you add a class,
a model field, a flag, a shared constant, a CLI option, or a new mechanism.
Formatting, imports, and types are checked by ``pre-commit`` (see
:doc:`../contributing`), and renaming or removing anything a user may have
written down follows "Deprecations and compatibility" on the same page.

General principles
------------------

These are the default shape for new code. Each has an exception, and a
deliberate exception with a stated reason is fine.

- **One reason to change.** A class or module does one job. A name that needs
  "and", or an adapter that both parses a format and performs I/O, is a sign
  to split it. A small, coherent facade is not three jobs.
- **Depend on the existing seams.** Extend the ``Directive`` and ``Transform``
  protocols, the ``ModelAdapter`` family, and the application registry
  (``@register_application``) rather than importing a concrete class from
  higher-level code. A protocol with one implementation and no test double is
  speculative: stay concrete until a second implementation exists.
- **Climb the abstraction ladder one rung at a time.** Two classes that share
  data share a module constant; shared behavior is a module function; only an
  interface that callers dispatch on becomes a base class or protocol. Do not
  add a base class, hook, or plugin point in case it is needed later.
- **Make invalid states unrepresentable.** Put validity in the type and its
  validator, so consumers never see an invalid combination. Two fields that
  must agree, checked at every use, belong in one validator. Where an old
  document must still load, normalize it once at the boundary.
- **Build what the task needs.** A parameter no caller passes, or a hook with
  one implementation, is waiting for a caller that may never come. Factor out
  an expression on its third copy, except a literal that encodes a contract (a
  configuration key, a file name, a wire-format token), which gets a name at
  its second.
- **No surprises.** A property or ``get_*`` method does not write files or
  submit jobs, and a ``validate_*`` function does not repair what it checks.

Design rules
------------

Borrow a value from a peer only for its identity
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A class must not reference an unrelated sibling just to reuse one of its
values. Ask: if the sibling were deleted, would this code still make sense? If
it would, the value belongs to neither class.

- *Why:* the reference couples two independent classes, so renaming one
  silently changes the other.
- *Fix:* move the value to the smallest scope both can see. In
  ``cstar/applications/ucla_roms/transforms.py``, ``SOURCE_KEY_PATH`` and
  ``SOURCE_KEY_STEP`` are module constants that both ``ContinuanceDirective``
  and ``NestingDirective`` use (#682). ``NestingDirective`` checking for a
  ``ContinuanceDirective`` in ``validate_directives`` is fine: that rule is
  about the sibling itself.

A new kind of value must not make consumers branch on it
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A feature that needs ``isinstance`` checks, or "is this ``None``?" checks, in
many places is shaped wrong at the source. Count the places that branch on the
new type. More than the type itself, one validator, and one boundary means the
special case has leaked.

- *Why:* each consumer becomes aware of the feature, and the next consumer
  forgets.
- *Where:* deferred blueprints (#646) are visible only at
  ``DeferredBlueprintRef``, one validator, and ``resolve_deferred_blueprint``;
  every step applies its overrides through the ``apply-overrides`` directive.
- *Not covered:* ``isinstance`` on untyped external data (YAML, JSON, CLI
  strings) at a parse boundary, and ``| None`` for data that is stored or
  passed along without changing behavior.

Compute a fact once, where it becomes known, and store it on its owner
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Do not make each consumer re-derive a value, and do not use ``None`` to mean
"work it out later". Where the value may not exist yet, its one accessor
raises a typed error.

- *Why:* a derivation repeated in every consumer drifts.
- *Where:* the workplan transformer records the SLURM CPU count on the step,
  so ``SlurmLauncher`` reads a plain integer (#646). ``LiveStep.blueprint``
  raises ``BlueprintDeferredError`` when the content is not available yet.
  Step sizes are measured once, when a step finishes, and stored as
  ``WorkplanRun.step_sizes``, with ``WorkplanRun.size_mb`` as the accessor
  (#681).

Fail before compute is spent
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Validate configuration at the earliest point the answer is known, and raise.
Never substitute a default with only a log line.

- *Why:* a user who finds out from a compute-node log has already lost the
  allocation.
- *Where:* invalid resource or compute settings stop SLURM submission (#646);
  ``NestingDirective.validate_directives`` runs at schedule time, so a
  conflicting ``rst_path`` and ``continue-from`` are reported before the job
  starts.

One source for each default and each derived name
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The model field's ``Field(default=...)`` is the source of its default. Code
that builds a "default instance" does not restate it. Derived state is a
read-only ``@property`` on the model that owns the underlying field.

- *Why:* when the authoritative value changes, every copy must change with it.
- *Where:* ``Step.clobber`` is a property that reads ``workflow_overrides``,
  not a second field kept in sync. In Forge, only the blueprint's ``name`` (and
  dates and process counts) is stored; the case name, namelist title, output
  root name, and run output directory are derived from it. A blueprint's
  current schema version is the default of its ``schema_version`` field,
  exposed as ``ApplicationDefinition.schema_version``; never restate it.

One action, one layer
~~~~~~~~~~~~~~~~~~~~~

The same effect must not happen, and the same check must not run, at two
levels of the stack. If deleting a line changes nothing because another layer
does the same thing, one of them is wrong.

- *Why:* duplicated actions disagree as soon as one of them changes.
- *Where:* the step subprocess handles ``--clobber`` cleanup through its own
  CLI flag, so the launcher does not clean as well (#635).

Run-scoped state travels as parameters and model fields
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Environment variables and module globals are for process-level flags and
configuration only. Never add an environment variable to carry state for one
run.

- *Why:* two runs in one process could disagree about the value, and nothing
  records which value a run used.
- *Where:* state that travels from the CLI to orchestration is passed as
  function arguments and recorded on the models. ``Step`` already has
  ``blueprint_overrides``, ``compute_overrides``, ``workflow_overrides``, and
  ``directives`` for this; per-step clobber is carried in
  ``workflow_overrides`` (#635). Look for an existing extension point before
  adding a mechanism.
- A selection that affects a run is written into the transformed workplan in
  ``prepare_workplan`` before it is serialized, so the run's artifacts show
  what was chosen.

Do not change the value shape of an existing flag
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Boolean environment flags such as ``CSTAR_DISABLE_BUILD_VERIFICATION`` are on
only when set to ``"1"``, as read by ``is_flag_enabled`` in
``cstar/base/feature.py``.

- *Why:* any other value silently reads as off.
- *Where:* add a new variable rather than giving an existing flag a second
  meaning.

Guard an invariant where the data is consumed
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

List every route by which data reaches its consumer, and make sure each one
establishes the invariant. A new field must reach every place its sibling
fields are read, written, serialized, migrated, and loaded back.

- *Why:* an upstream step that fixes the data protects only the paths that
  pass through it.
- *Where:* migrating an old blueprint in one command's callback does not cover
  a reload by run ID or a programmatic run, so the migration check sits where
  the blueprint is loaded. In Forge, a new forcing field must also reach
  ``sources_to_forcing_override`` and the wizard's load-back.
- A transform that renames or splits steps must remap every step reference:
  ``depends_on``, a deferred ``from_step``, and step references in directives
  such as ``continue-from``. Until it does, refuse the input with a "not yet
  supported" error. Leaving part of the work for a follow-up is fine when the
  follow-up is written down.

Shared helpers are generic; shared literals use the existing names
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A helper in a shared or base module takes the varying key or target as a
parameter rather than wrapping one hardcoded value. Before adding a string
literal, check whether a constant family (``ARG_*``, ``SOURCE_KEY_*``,
``ENV_*``) already names it.

- *Why:* a single-purpose wrapper in a shared module invites a second one
  beside it, and an unnamed copy of a named literal drifts.

Pydantic models and adapters
----------------------------

- New models set at least ``ConfigDict(extra="forbid",
  str_strip_whitespace=True, use_attribute_docstrings=True)``. *Why:* unknown
  keys are errors, stray whitespace is not data, and attribute docstrings
  become field descriptions (#646).
- Write defaults as ``Field(default=...)``, not ``= value``. *Why:* the keyword
  form keeps IDE constructor hints intact.
- Prefer ``str = Field(default="")`` over ``str | None`` when an empty string
  means "not set". *Why:* the same truthiness checks work without a ``None``
  case.
- Validators that do not mutate accept and return ``Sequence[...]``, not
  ``list[...]``.
- Wire-format parsing (URI schemes, for example) tolerates case and
  surrounding whitespace in the scheme (``strip().casefold()``); the payload
  stays case-sensitive.
- Compute adapters own their key lookup; callers never check which keys an
  adapter reads. ``adapt()`` raises ``CstarExpectationFailed`` when there is
  nothing for that adapter (callers skip it quietly, at debug level) and
  ``CstarAdaptationError`` when the input is present but invalid (callers
  apply their failure policy).

Typer CLI
---------

- Shared option names and help strings are ``ARG_*`` constants in
  ``cstar/entrypoint/utils.py``. Reuse or extend that family; do not repeat an
  option string inline. *Why:* one option spelled two ways is two options.
- Compose per-option callbacks with ``cb_pipeline(...)`` from
  ``cstar/cli/common.py`` instead of nesting calls. Reusable callbacks live in
  the same module.
- A repeatable option is ``list[str]`` with a ``= []`` default, not
  ``list[str] | None``. *Why:* Typer's ``default_factory`` cannot be used in
  the middle of a signature, because of Python's rules on default ordering.
- Do not pass ``param_hint`` to a ``typer.BadParameter`` raised from that
  parameter's own callback; Typer already knows the parameter.
- Validation reports every bad value in one error, not only the first.

Style
-----

- ``slugify`` (``cstar/base/utils.py``) already strips whitespace and raises
  on an empty result. Do not strip before calling it; check for an empty
  value first only when you need a friendlier message.
- Normalization and bookkeeping messages are logged at ``debug``, not
  ``info``. Log one combined message per subject rather than one per item.
- Match the docstring and comment density of the surrounding code, and keep
  comprehensions tight.
- Inside a module that consistently does something another way, follow the
  module and propose the change separately.
