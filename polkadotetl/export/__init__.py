"""Functions to export polkadot blocks from a sidecar"""
from pathlib import Path
from typing import Optional
import tenacity
import requests

from polkadotetl.logger import logger
from polkadotetl.constants import SIDECAR_RETRIES
from polkadotetl.exceptions import InvalidInput


def export_blocks(
    output_directory: Path,
    sidecar_url: str,
    start_block: Optional[int] = None,
    end_block: Optional[int] = None,
    start_timestamp: Optional[int] = None,
    end_timestamp: Optional[int] = None,
    retries: int = SIDECAR_RETRIES,
):
    """Exports all blocks from a sidecar into a folder of jsons"""
    invalid_input = False
    if start_block is None:
        if start_timestamp is None:
            logger.error("Need to specify either the block number or the timestamp")
            invalid_input = True
        elif end_timestamp is None:
            invalid_input = True
            if end_block is not None:
                logger.error("Cannot mix and match start timestamp with end block.")
            else:
                logger.error("Need to specify the end timestamp.")
    elif end_block is None:
        invalid_input = True
        if start_timestamp is not None:
            logger.error("Cannot specify both the start block and the start timestamp.")
        if end_timestamp is not None:
            logger.error("Cannot mix and match start block with end timestamp.")
        else:
            logger.error("Need to specify an end block if you specify a start block.")
    else:
        if start_timestamp is not None or end_timestamp is not None:
            logger.error(
                "Cannot specify both the blocks and the timestamps. Pick one pair."
            )
            invalid_input = True
    if invalid_input:
        raise InvalidInput(
            "Cannot use the provided inputs to the function. Check the logs for more information."
        )
