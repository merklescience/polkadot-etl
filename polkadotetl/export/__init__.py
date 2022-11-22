"""Functions to export polkadot blocks from a sidecar"""
from datetime import datetime
from pathlib import Path
from typing import Optional

from polkadotetl.constants import SIDECAR_RETRIES
from polkadotetl.export.internals import (
    InputType,
    validate_inputs,
    export_blocks_by_number,
    export_blocks_by_timestamp,
)


def export_blocks(
        output_directory: Path,
        sidecar_url: str,
        start_block: Optional[int] = None,
        end_block: Optional[int] = None,
        start_timestamp: Optional[datetime] = None,
        end_timestamp: Optional[datetime] = None,
        retries: int = SIDECAR_RETRIES,
):
    """Exports all blocks from a sidecar into a folder of jsons"""
    input_type = validate_inputs(start_block, end_block, start_timestamp, end_timestamp)

    if input_type == InputType.BLOCKS:
        export_blocks_by_number(
            output_directory,
            sidecar_url,
            start_block,
            end_block,
            retries,
        )
    else:
        export_blocks_by_timestamp(
            output_directory,
            sidecar_url,
            start_timestamp,
            end_timestamp,
            retries,
        )


def get_latest_block(
        sidecar_url: str,
):
    requestor = sidecar.PolkadotRequestor()
    get_block = requestor.build_requestor(sidecar.get_block)
    end_block_response = get_block(sidecar_url, "head")
    print(end_block_response)
    return int(end_block_response["number"])

def get_latest_block(
        sidecar_url: str,
):
    requestor = sidecar.PolkadotRequestor()
    get_block = requestor.build_requestor(sidecar.get_block)
    end_block_response = get_block(sidecar_url, "head")
    print(end_block_response)
    return int(end_block_response["number"])
