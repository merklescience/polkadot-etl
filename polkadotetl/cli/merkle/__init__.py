"""Merkle Science Specific subcommands for the CLI"""
from datetime import datetime
from types import SimpleNamespace
from pathlib import Path
from decouple import config

import typer
from polkadotetl.constants import SIDECAR_RETRIES
from polkadotetl.cli.merkle import bigquery


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
        clickhouse_url=config("CLICKHOUSE_URL"),
        clickhouse_user=config("CLICKHOUSE_USER"),
        clickhouse_password=config("CLICKHOUSE_PASSWORD"),
        clickhouse_db=config("CLICKHOUSE_AUTH_DB"),
        clickhouse_port=config("CLICKHOUSE_HTTP_PORT"),
        tigergraph_url=config("POLKADOT_TIGERGRAPH_URL"),
        sidecar_url=config("POLKADOT_SIDECAR_URL"),
    )


@app.command()
def write_to_clickhouse(
    ctx: typer.Context,
    start_block: int = typer.Option(None, help="Start Block"),
    end_block: int = typer.Option(None, help="End Block"),
    start_timestamp: datetime = typer.Option(None, help="Start timestamp"),
    end_timestamp: datetime = typer.Option(None, help="End timestamp"),
    retries: int = typer.Option(
        SIDECAR_RETRIES, help="Number of retries for the requests"
    ),
):
    """This is a custom function that collects data from the sidecar and streams to Merkle Science Data Stores"""
    # TODO: Perhaps this is best abstracted away into an internal tool since we are open sourcing
    # this?
    # ctx.obj contains all the credentials and config that's stored in the `main` function
    # above, so this can be used to call the necessary functions within a pipeline.

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
    

