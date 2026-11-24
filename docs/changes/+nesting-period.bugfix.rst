A Forge parent run now extracts data for its nesting child at the period in the child domain's ``metadata`` block; before, the namelist used 3600 s unless ``domain.metadata_child`` repeated the period.
- Picking a child domain preset in the wizard now fills the extraction period and pressure-flux settings from the preset, and saved blueprints and domains no longer keep a copy of its ``metadata`` block.
- A blueprint saved by the 0.19.0 wizard with a ``GoA-10th_deg`` or ``NEP-4th_deg`` child is reported as edited by hand each time it runs until it is saved again, for example from the wizard.
