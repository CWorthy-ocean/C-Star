from pathlib import Path


def get_output_root_name(dot_in_path: Path) -> str:
    output_root_name = ""

    with open(dot_in_path) as fp:
        located = False
        while not located:
            line = fp.readline()
            if "output_root_name" in line:
                located = True

        while not output_root_name:
            line = fp.readline()
            if ":" in line:
                raise RuntimeError(
                    f"Unable to locate `output_root_main` in {dot_in_path}"
                )
            output_root_name = line.strip()

    return output_root_name


def find_roms_dot_in(directory: Path) -> Path:
    dot_in_files = directory.glob("*.in")
    # assert len(list(dot_in_files)) == 1, "Expected exactly one .in file in directory"
    return next(dot_in_files)


if __name__ == "__main__":
    d = Path("/anvil/scratch/x-smaticka/sims_runtime/GoM_wrkplan")
    output_base_name = find_roms_dot_in(d)
    print(f"{output_base_name=!r}")
