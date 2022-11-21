"""Merkle Science Specific subcommands for the CLI"""
from datetime import datetime
from types import SimpleNamespace
from pathlib import Path
from decouple import config

import typer
from polkadotetl.constants import SIDECAR_RETRIES
from polkadotetl.cli.datasources import bigquery


app = typer.Typer()


@app.callback(invoke_without_command=False)
def main(
    ctx: typer.Context,
):
    """Set command-level values
    Reference: https://jacobian.org/til/common-arguments-with-typer/
    This way, duplicate arguments are centralized.
    """
    # store the common context
    ctx.obj = SimpleNamespace(
        sidecar_url=config("POLKADOT_SIDECAR_URL"),
    )

@app.command()
def convert_to_bigquery_schema(
    input_dir: Path,
    output_dir: Path,
    start: int,
    stop: int,
    raise_error: bool = False
    ):
    """This command takes a directory containing raw responses from the sidecar,
    then applies a transformation on it to make it so that it can be written to Google BigQuery
    as a table.

    The data cannot be written "as is" because of the `events` field and the `args`
    fields, which have varying schema.
    NOTE: While this is "merkle-specific", it might be prudent to put this in the OSS version
    so that it can be used everywhere.
    """
    bigquery.convert_to_bigquery_schema(input_dir, output_dir, start, stop, raise_error)
