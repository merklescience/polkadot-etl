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


def process(block_response: dict):
    """Processes a single block response"""
    # first ensure this has the extrinsics
    if "extrinsics" not in block_response.keys():
        raise SubstrateSidecarError("Not a valid Substrate Block Response. Missing extrinsics")

    for ix, extrinsic in enumerate(block_response["extrinsics"]):
        # convert the `signature field` to a STRING.
        signature = extrinsic.get("signature")
        if signature is not None and not isinstance(signature, str):
            block_response["extrinsics"][ix]["signature"] = json.dumps(signature)
        success = extrinsic.get("success", False)
        if success in [True, "true"]:
            success = True
        else:
            if isinstance(success, str) and "Unable to fetch Events, cannot confirm extrinsic status. Check pruning settings on the node." in success:
                raise PruningError("Check pruning settings for this block.")
            success = False
        block_response["extrinsics"][ix]["success"] = success

        # Next, make sure that the `data` fields everywhere only have a list of strings
        for iy, event in enumerate(block_response["extrinsics"][ix]["events"]):
            data = event["data"]
            for iz, item in enumerate(data):
                if not isinstance(item, str):
                    block_response["extrinsics"][ix]["events"][iy]["data"][
                        iz
                    ] = json.dumps(item)

    for key in ["onInitialize", "onFinalize"]:
        if key not in block_response.keys():
            raise SubstrateSidecarError(f"Not a valid Substrate Block Response. Missing {key}")
        for ix, event in enumerate(block_response[key]["events"]):
            for iy, item in enumerate(event["data"]):
                if not isinstance(item, str):
                    block_response[key]["events"][ix]["data"][iy] = json.dumps(item)

    # next, serialize `extrinsics[].args`
    for ix, extrinsic in enumerate(block_response["extrinsics"]):
        block_response["extrinsics"][ix]["args"] = json.dumps(extrinsic["args"])
