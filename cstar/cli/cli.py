import typer

app = typer.Typer()


def main() -> None:
    """Main entrypoint for the complete C-Star CLI."""
    try:
        app()
    except Exception as ex:
        print(f"An error occurred while handling request: {ex}")


if __name__ == "__main__":
    main()
