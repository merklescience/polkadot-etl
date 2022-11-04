"""polkadotetl CLI built using Typer"""

from typer import Typer

app = Typer()


@app.command()
def export_blocks():
    """Exports blocks from the polkadot sidecar API into a newline-separated jsons file"""


@app.command()
def enrich():
    """Enriches the exported jsons into another file of exported jsons."""


def cli():
    """Helper function to run the cli."""
    app()
