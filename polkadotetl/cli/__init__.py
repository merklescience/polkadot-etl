"""polkadotetl CLI built using Typer"""
import os
import json
import glob
from pathlib import Path
import warnings

from typer import Typer

from polkadotetl.warnings import NoTransactionsWarning
from polkadotetl.logger import logger

app = Typer()


@app.command()
def export_blocks():
    """Exports blocks from the polkadot sidecar API into a newline-separated jsons file"""


@app.command()
def enrich(
    block_response_path: Path,
    output_file: Path,
    quiet: bool = False,
):
    """Enriches all Polkadot block response files from a folder and writes the results into a single, new-line-separated
    file of jsons. This can be directly uploaded to BigQuery."""
    from polkadotetl.enrich import enrich_block

    if quiet:
        warnings.filterwarnings("ignore", category=NoTransactionsWarning)
    response_files = glob.glob(os.path.join(block_response_path, "*.json"))
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
