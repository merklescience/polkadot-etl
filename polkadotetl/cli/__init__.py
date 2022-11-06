"""polkadotetl CLI built using Typer"""
from pathlib import Path
import glob
import json
import logging
import os
import sys
import warnings

import typer
from polkadotetl.constants import SIDECAR_RETRIES
from polkadotetl.warnings import NoTransactionsWarning
from polkadotetl.logger import logger
from polkadotetl.exceptions import InvalidInput

app = typer.Typer()


@app.callback(invoke_without_command=False)
def main(
    ctx: typer.Context,
    log_level: str = typer.Option(
        "INFO",
        help="Set the loglevel for this application. Accepted values are python loglevels. See here: https://docs.python.org/3/library/logging.html#levels",
    ),
):

    if not isinstance(logging.getLevelName(log_level), int):
        logger.error("Invalid log level. Unable to continue.")
        raise typer.Exit(2)
    logger.remove()
    logger.add(sys.stdout, colorize=True, filter="polkadotetl", level=log_level)
    logger.debug("Running: {}".format(ctx.invoked_subcommand))


@app.command()
def export_blocks(
    sidecar_url: str = typer.Argument(
        ...,
        envvar="POLKADOT_SIDECAR_URL",
        help="Fully qualified URL to the polkadot sidecar. Provide the API key within the query parameters as well, if required.",
    ),
    output_directory: Path = typer.Argument(
        ...,
        exists=True,
        writable=True,
        resolve_path=True,
        dir_okay=True,
        file_okay=False,
    ),
    start_block: int = typer.Option(None, help="Start Block"),
    end_block: int = typer.Option(None, help="End Block"),
    start_timestamp: str = typer.Option(None),
    end_timestamp: str = typer.Option(None),
    retries: int = typer.Option(
        SIDECAR_RETRIES, help="Number of retries for the requests"
    ),
):
    """Exports blocks from the polkadot sidecar API into a newline-separated jsons file"""
    from polkadotetl.export import export_blocks

    logger.debug(f"{start_block=}, {end_block=}, {start_timestamp=}, {end_timestamp=}")
    try:
        export_blocks(
            output_directory,
            sidecar_url,
            start_block,
            end_block,
            start_timestamp,
            end_timestamp,
            retries,
        )
    except InvalidInput as e:
        logger.error("Invalid input provided to CLI.")
        raise typer.Exit(1) from e


@app.command()
def enrich(
    block_response_path: Path = typer.Argument(
        ...,
        exists=True,
        file_okay=False,
        dir_okay=True,
        resolve_path=True,
        readable=True,
        help="Process all jsons in this directory. Note that you should only keep response jsons in this directory, or you will face errors.",
    ),
    output_file: Path = typer.Argument(
        ...,
        exists=False,
        file_okay=True,
        dir_okay=False,
        writable=True,
        resolve_path=True,
        help="Write all resulting jsons to this file, with a new line separating each json.",
    ),
    quiet: int = typer.Option(0, "--quiet", "-q", count=True),
    overwrite: bool = typer.Option(
        False,
        "--overwrite/--no-overwrite",
        "-w/-N",
        help="Overwrite the output file if it exists.",
    ),
):
    """Enriches all Polkadot block response files from a folder and writes the results into a single, new-line-separated
    file of jsons. This can be directly uploaded to BigQuery."""
    from polkadotetl.enrich import enrich_block

    if quiet > 0:
        warnings.filterwarnings("ignore", category=NoTransactionsWarning)
    response_files = glob.glob(os.path.join(block_response_path, "*.json"))
    if output_file.exists() and not overwrite:
        logger.error("`{}` exists. Use --overwrite if you want to do replace the file.")
        raise typer.Exit(1)
    enriched_transactions = 0
    logger.info("Processing {:,} response files.".format(len(response_files)))
    with open(output_file, "w") as output_file_buffer:
        for response_file in response_files:
            with open(response_file, "r") as file_buffer:
                polkadot_response = json.load(file_buffer)
                transactions = enrich_block(polkadot_response)
                for txn in transactions:
                    enriched_transactions += 1
                    output_file_buffer.write("{}\n".format(json.dumps(txn)))

    logger.info(
        "Completed processing all files in `{}` and wrote them to `{}`. Total number of transactions: {:,}".format(
            block_response_path, output_file, enriched_transactions
        )
    )


def cli():
    """Helper function to run the cli."""
    app()
