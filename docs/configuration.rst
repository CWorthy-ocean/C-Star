Configuration
=============

Environment Variables
---------------------

User-settable environment variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following environment variables can be set by the user to control C-Star behavior.

Frequent users may want to set preferred values for these variables in their ``.bash_profile`` or ``.zshrc`` files.


+------------------------------+-----------------------+-----------------------+
| Variable                     | Default               | Effect                |
+==============================+=======================+=======================+
| CSTAR_HOME                   | ~/.cstar              | The default location  |
|                              |                       | for storage of C-Star |
|                              |                       | configuration.        |
+------------------------------+-----------------------+-----------------------+
| CSTAR_OUTDIR                 | ~/.cstar/assets       | The default location  |
|                              |                       | for storage of C-Star |
|                              |                       | outputs.              |
+------------------------------+-----------------------+-----------------------+
| CSTAR_NPROCS_POST            | os.cpu_count() / 3    | The number of         |
|                              |                       | parallel processes to |
|                              |                       | use for post-run join |
|                              |                       | operations.           |
+------------------------------+-----------------------+-----------------------+
| CSTAR_FRESH_CODEBASES        | 0                     | If 1, CSTAR will make |
|                              |                       | fresh codebase        |
|                              |                       | directories and       |
|                              |                       | clones for each run.  |
|                              |                       | If 0 (default),       |
|                              |                       | common codebases in   |
|                              |                       | ROMS_ROOT/MARBL_ROOT  |
|                              |                       | are used (those       |
|                              |                       | variables default to  |
|                              |                       | locations within this |
|                              |                       | package directory).   |
+------------------------------+-----------------------+-----------------------+
| CSTAR_CLOBBER_WORKING_DIR    | 0                     | If 1, clear the       |
|                              |                       | working directory     |
|                              |                       | dictated in the       |
|                              |                       | blueprint before      |
|                              |                       | launching a SLURM     |
|                              |                       | job. Use at your own  |
|                              |                       | risk.                 |
+------------------------------+-----------------------+-----------------------+
| CSTAR_SLURM_ACCOUNT          | None (must be set for | The account name to   |
|                              | SLURM usage)          | be passed to SLURM    |
|                              |                       | for compute           |
|                              |                       | accounting.           |
+------------------------------+-----------------------+-----------------------+
| CSTAR_SLURM_QUEUE            | None (must be set for | The SLURM queue or    |
|                              | SLURM usage)          | partition to use for  |
|                              |                       | jobs.                 |
+------------------------------+-----------------------+-----------------------+
| CSTAR_SLURM_MAX_WALLTIME     | “48:00:00”            | Maximum walltime to   |
|                              |                       | set for jobs          |
|                              |                       | submitted to SLURM.   |
+------------------------------+-----------------------+-----------------------+
| CSTAR_ORCH_TRX_FREQ          | monthly               | The timespan used when|
|                              |                       | automatically         |
|                              |                       | splitting             |
|                              |                       | simulations. Accepts: |
|                              |                       | monthly, weekly,      |
|                              |                       | daily                 |
+------------------------------+-----------------------+-----------------------+


Developer-only environment variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

These variables are set internally or during testing and are documented here for developers reference.

+------------------------------+-----------------------+-----------------------+
| Variable                     | Default               | Effect                |
+==============================+=======================+=======================+
+------------------------------+-----------------------+-----------------------+
| CSTAR_RUNID                  | None, set by CLI      | The :term:`run ID`    |
+------------------------------+-----------------------+-----------------------+
| CSTAR_CMD_CONVERTER_OVERRIDE | None                  | Testing only. If set, |
|                              |                       | submit a custom       |
|                              |                       | command as the        |
|                              |                       | execution command to  |
|                              |                       | SLURM jobs, instead   |
|                              |                       | of the default        |
|                              |                       | application command.  |
+------------------------------+-----------------------+-----------------------+


Feature-flag Environment Variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Feature flags enable unsupported, experimental features in C-Star. They are subject to change without notice and should be used with caution.

+------------------------------+-----------------------+-----------------------+
| Variable                     | Default               | Effect                |
+==============================+=======================+=======================+
+------------------------------+-----------------------+-----------------------+
| CSTAR_FF_DEVELOPER_MODE      | 0                     | If 1, enable all      |
|                              |                       | experimental          |
|                              |                       | features.             |
+------------------------------+-----------------------+-----------------------+
| CSTAR_FF_CLI_TEMPLATE_CREATE | 0                     | If 1, enable the      |
|                              |                       | `cstar template       |
|                              |                       | create` command.      |
+------------------------------+-----------------------+-----------------------+
| CSTAR_FF_CLI_WORKPLAN_GEN    | 0                     | If 1, enable the      |
|                              |                       | `cstar workplan       |
|                              |                       | generate` command.    |
+------------------------------+-----------------------+-----------------------+
| CSTAR_FF_CLI_WORKPLAN_PLAN   | 0                     | If 1, enable the      |
|                              |                       | `cstar workplan       |
|                              |                       | plan` command.        |
+------------------------------+-----------------------+-----------------------+
| CSTAR_FF_CLI_WORKPLAN_STATUS | 0                     | If 1, enable the      |
|                              |                       | `cstar workplan       |
|                              |                       | status` command.      |
+------------------------------+-----------------------+-----------------------+
| CSTAR_FF_ORCH_TRANSFORM_AUTO | 0                     | If 1, enable the      |
|                              |                       | time-splitting of     |
|                              |                       | simulations when run  |
|                              |                       | by the orchestrator.  |
+------------------------------+-----------------------+-----------------------+
