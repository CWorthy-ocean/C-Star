The Forge wizard's grid previews failed with ``unexpected keyword argument 'metadata'`` in 0.19.0 when the bundled ``GoA-10th_deg`` or ``NEP-4th_deg`` domain was the grid, its nesting child or its parent.
- A Forge run of a GoA or NEP blueprint saved from the wizard failed the same way unless the domain had a parent grid.
