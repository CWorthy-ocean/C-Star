import typer

from cstar.cli.blueprint import app as app_blueprint
from cstar.cli.template import app as app_template
from cstar.cli.version import common_callback
from cstar.cli.workplan import app as app_workplan


def main() -> None:
    """Main entrypoint for the complete C-Star CLI."""
    app = typer.Typer(callback=common_callback)

    subcommands: list[tuple[typer.Typer, str]] = [
        (app_blueprint, "blueprint"),
        (app_template, "template"),
        (app_workplan, "workplan"),
    ]

    try:
        for command_app, command_name in subcommands:
            if command_app.registered_groups or command_app.registered_commands:
                app.add_typer(
                    command_app,
                    name=command_name,
                    callback=common_callback,
                )
        app()
    except Exception as ex:
        print(f"An error occurred while handling request: {ex}")


if __name__ == "__main__":
    main()
