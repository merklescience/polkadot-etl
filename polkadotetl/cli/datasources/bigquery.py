"""Polkadot Block Processor"""
import json
import random
import glob
from pathlib import Path

import typer
from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn

from polkadotetl.exceptions import PolkadotSidecarError, PruningError



def convert_to_bigquery_schema(
    input_dir: Path, output_dir: Path, start: int, stop: int, raise_error: bool = False
):
    """This function cleans the raw sidecar response and makes it so that it can write it to BigQuery.
    1. Read Json.
    2. Remove data fields wherever pallet='parainherent' and pallet='timestamp'.
    3. "flatten" the data fields for pallet='system' and method='extrinsicSuccess|extrinsicFailed'
    The specific schema it writes to is in `schema.json`, found in the root level of this repository.
    """
    assert (
        input_dir != output_dir
    ), "Please don't use the same folder for input and output."
    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"batch.{start}.{stop}.json"
    with open(output_file, "w") as fw:

        with Progress(
            SpinnerColumn(),
            *Progress.get_default_columns(),
            TimeElapsedColumn(),
        ) as progress:
            task = progress.add_task("Processing", total=stop - start + 1)
            for file_number in range(start, stop + 1):
                file_path = input_dir / f"{file_number}.json"
                if not file_path.exists():
                    continue
                try:
                    with open(file_path, "r") as f:
                        block_response = json.load(f)
                except json.JSONDecodeError as e:
                    progress.console.print(f"JSONDecodeError Processing: {file_path}, {e}")
                    continue
                try:
                    process(block_response)
                except PruningError as e:
                    progress.console.print(f"PruningError Processing: {file_path}, {e}")
                    continue
                except Exception as e:
                    progress.console.print(f"Error Processing: {file_path}, {e}")
                    if raise_error:
                        raise e
                    else:
                        continue
                fw.write("{}\n".format(json.dumps(block_response)))
                progress.advance(task)
