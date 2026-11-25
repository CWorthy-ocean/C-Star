A parent grid can now have several named child grids, and its run writes each child's boundary data under the child's name.
- A Forge blueprint lists them by name in ``domain.children``, with one shared ``nest_period``; the wizard adds them with **+ Add child grid**.
- ``nest-from`` takes ``child_name`` to read that child's ``<name>_bry.*.nc`` files; it can be left out when the parent has one child.
- A name starts with a letter and uses letters, digits and ``-``, at most 20 characters; more than one child needs ucla-roms 0.12.0 or later, which reads up to eight.
