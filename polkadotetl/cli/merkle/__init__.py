"""Merkle Science Specific subcommands for the CLI"""
from datetime import datetime
from types import SimpleNamespace
from decouple import config

import typer
from polkadotetl.constants import SIDECAR_RETRIES


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
        clickhouse_url=config('CLICKHOUSE_URL'),
        clickhouse_user=config('CLICKHOUSE_USER'),
        clickhouse_password=config('CLICKHOUSE_PASSWORD'),
        clickhouse_db=config('CLICKHOUSE_AUTH_DB'),
        clickhouse_port=config('CLICKHOUSE_HTTP_PORT'),
        tigergraph_url=config('POLKADOT_TIGERGRAPH_URL'),
        sidecar_url=config('POLKADOT_SIDECAR_URL'),
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
    

