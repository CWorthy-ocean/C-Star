import yaml
from pathlib import Path


def _modify_roms_tools_yaml(
    input_file: str | Path, output_file: str | Path, new_entries: dict = {}
):
    with open(input_file, "r") as F:
        _, header, yaml_data = F.read().split("---", 2)
        yaml_dict = yaml.safe_load(yaml_data)

    for key, value in new_entries.items():
        # First level describes roms tools class
        rtclasses = yaml_dict.keys()
        for rtclass in rtclasses:
            if key not in yaml_dict[rtclass].keys():
                raise ValueError(
                    f"Cannot replace entry {key} in "
                    + f"roms_tools yaml file {input_file}. "
                    + "No such entry."
                )

            yaml_dict[rtclass][key] = value

    with open(output_file, "w") as F:
        F.write(f"---{header}---\n" + yaml.dump(yaml_dict))
