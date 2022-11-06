"""polkadotetl CLI built using Typer"""
import os
import json
import glob
from pathlib import Path
import warnings

import typer

from polkadotetl.warnings import NoTransactionsWarning
from polkadotetl.logger import logger

app = typer.Typer()


@app.command()
def export_blocks():
    """Exports blocks from the polkadot sidecar API into a newline-separated jsons file"""


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
    if quiet < 2:
        logger.info("Processing {:,} response files.".format(len(response_files)))
    with open(output_file, "w") as output_file_buffer:
        for response_file in response_files:
            with open(response_file, "r") as file_buffer:
                polkadot_response = json.load(file_buffer)
                transactions = enrich_block(polkadot_response)
                for txn in transactions:
                    enriched_transactions += 1
                    output_file_buffer.write("{}\n".format(json.dumps(txn)))

    if quiet < 2:
        logger.info(
            "Completed processing all files in `{}` and wrote them to `{}`. Total number of transactions: {:,}".format(
                block_response_path, output_file, enriched_transactions
            )
        )


def cli():
    """Helper function to run the cli."""
    app()
