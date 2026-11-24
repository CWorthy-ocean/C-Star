The Forge wizard's Description field moved under the blueprint name in Review and export, and defaults to a one-line summary of the blueprint instead of "Generated blueprint".
- The summary names the grid with its size and spacing, the run dates, the model and forcing presets, the BGC mode and any CDR release, and updates until you type your own.
- A loaded blueprint keeps its description, unless it is "Generated blueprint" or an unedited summary.
- Catalog specs saved from the wizard take the description only when you typed one; ``build_forge_blueprint`` called without a ``description`` gives the same summary.
