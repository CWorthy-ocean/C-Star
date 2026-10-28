import typer

from cstar.cli.admin.clean import app as app_clean
from cstar.cli.admin.migrate_catalog import app as app_migrate_catalog
from cstar.cli.admin.migrate_outputs import app as app_migrate_outputs

app = typer.Typer(
    name="admin",
    help="Perform administrative tasks related to your C-Star installation and runs.",
)

app.add_typer(app_migrate_catalog)
app.add_typer(app_migrate_outputs)
app.add_typer(app_clean)
