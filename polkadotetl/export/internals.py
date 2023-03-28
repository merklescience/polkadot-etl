from collections import defaultdict
from datetime import datetime
from enum import Enum
import json
import pytz
from pathlib import Path
from typing import Optional
from polkadotetl.logger import logger
from polkadotetl.exceptions import InvalidInput, NoBlockAtTimestamp
from polkadotetl.constants import POLKADOT_BLOCK_CREATION_TIME_SECONDS, SIDECAR_RETRIES
from polkadotetl.export import sidecar
from tenacity import RetryError
from math import floor


class InputType(Enum):
    """Determines which input mode to use."""

    BLOCKS = 0
    TIMESTAMP = 1


def validate_inputs(
    start_block: Optional[int] = None,
    end_block: Optional[int] = None,
    start_timestamp: Optional[int] = None,
    end_timestamp: Optional[int] = None,
) -> InputType:
    """check the start stop parameters to check whether to use the blocks or the timestamps."""
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
        else:
            return InputType.TIMESTAMP
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
        else:
            return InputType.BLOCKS
    if invalid_input:
        raise InvalidInput(
            "Cannot use the provided inputs to the function. Check the logs for more information."
        )


def get_block_for_timestamp(
    sidecar_url: str,
    timestamp: datetime,
    threshold_in_seconds=NEAREST_BLOCK_THRESHOLD_IN_SECONDS,
    is_strict_boundary=False
    # If set to False, the first block after `timestamp` parameter will be returned
    # else, the first block before `timestamp` parameter is returned
):
    """Returns the nearest block number for a particular timestamp.
    `threshold_in_seconds` controls the closeness of the block.
    """
    start_block_number = 1

    if not timestamp.tzinfo:
        timestamp = pytz.utc.localize(timestamp)
        # if no timezone is passed assume UTC

    requestor = sidecar.PolkadotRequestor()
    get_block = requestor.build_requestor(sidecar.get_block)
    end_block_response = get_block(sidecar_url, "head")
    end_block_number = int(end_block_response["number"])
    low = start_block_number
    high = end_block_number
    mid = low
    logger.debug(
        f"Searching for a block that occurs at {timestamp:} between {low:,} and {high:,} blocks."
    )
    # NOTE: use binary search algorithm to get the block number for this timestamp
    searched = defaultdict(int)
    last_mid = None

    while high - low > 1:
        mid = (high + low) // 2
        logger.debug(f"Checking if block #{mid:,} happens at {timestamp.timestamp():,}")
        response = get_block(sidecar_url, mid)
        # get the timestamp out of the first extrinsic
        current_timestamp = int(response["extrinsics"][0]["args"]["now"]) * 1.000000 / 1000.000000
        # divide by 1000 because it is in milliseconds
        actual_timestamp = datetime.utcfromtimestamp(current_timestamp)
        difference = timestamp - pytz.utc.localize(actual_timestamp)
        logger.info(
            f"Block #{mid:,} happens at {current_timestamp:,}. Difference=`{difference}`"
        )

        if (current_timestamp - timestamp.timestamp()) < -1.000000 * NEAREST_BLOCK_THRESHOLD_IN_SECONDS:
            low = mid + 1
        elif (current_timestamp - timestamp.timestamp()) > 1.0000000 * NEAREST_BLOCK_THRESHOLD_IN_SECONDS:
            high = mid - 1
        else:
            low = mid

    response = get_block(sidecar_url, mid - 3)
    waypast_timestamp = (int(response["extrinsics"][0]["args"]["now"]) * 1.000000 / 1000.000000)

    response = get_block(sidecar_url, mid - 2)
    past_timestamp = (int(response["extrinsics"][0]["args"]["now"]) * 1.000000 / 1000.000000)

    response = get_block(sidecar_url, mid - 1)
    previous_timestamp = (int(response["extrinsics"][0]["args"]["now"]) * 1.000000 / 1000.000000)

    response = get_block(sidecar_url, mid)
    current_timestamp = (int(response["extrinsics"][0]["args"]["now"]) * 1.000000 / 1000.000000)

    response = get_block(sidecar_url, mid + 1)
    next_timestamp = (int(response["extrinsics"][0]["args"]["now"]) * 1.000000 / 1000.000000)

    response = get_block(sidecar_url, mid + 2)
    future_timestamp = (int(response["extrinsics"][0]["args"]["now"]) * 1.000000 / 1000.000000)

    if not is_strict_boundary:
        if future_timestamp >= timestamp.timestamp() and abs(future_timestamp - timestamp.timestamp()) < 0.1:
            return mid + 2
        elif next_timestamp >= timestamp.timestamp() and abs(next_timestamp - timestamp.timestamp()) < 0.1:
            return mid + 1
        elif current_timestamp >= timestamp.timestamp() and abs(current_timestamp - timestamp.timestamp()) < 0.1:
            return mid
        elif previous_timestamp >= timestamp.timestamp() and abs(previous_timestamp - timestamp.timestamp()) < 0.1:
            return mid - 1
        elif past_timestamp >= timestamp.timestamp() and abs(past_timestamp - timestamp.timestamp()) < 0.1:
            return mid - 2


    if is_strict_boundary:
        if waypast_timestamp < timestamp.timestamp() and abs(waypast_timestamp - timestamp.timestamp()) <= POLKADOT_BLOCK_CREATION_TIME_SECONDS:
            return mid - 3
        elif past_timestamp < timestamp.timestamp() and abs(past_timestamp - timestamp.timestamp()) <= POLKADOT_BLOCK_CREATION_TIME_SECONDS:
            return mid - 2
        elif previous_timestamp < timestamp.timestamp() and abs(previous_timestamp - timestamp.timestamp()) <= POLKADOT_BLOCK_CREATION_TIME_SECONDS:
            return mid - 1
        elif current_timestamp < timestamp.timestamp() and abs(current_timestamp - timestamp.timestamp()) <= POLKADOT_BLOCK_CREATION_TIME_SECONDS:
            return mid
        elif next_timestamp < timestamp.timestamp() and abs(next_timestamp - timestamp.timestamp()) <= POLKADOT_BLOCK_CREATION_TIME_SECONDS:
            return mid + 1
        elif future_timestamp < timestamp.timestamp() and abs(future_timestamp - timestamp.timestamp()) <= POLKADOT_BLOCK_CREATION_TIME_SECONDS:
            return mid + 2


def export_blocks_by_timestamp(
    output_directory: Path,
    sidecar_url: str,
    start_timestamp: Optional[datetime] = None,
    end_timestamp: Optional[datetime] = None,
    retries: int = SIDECAR_RETRIES,
):
    """Exports blocks from the sidecar by block timestamp"""
    # TODO: Implement this function
    # NOTE: Internally calls the export_blocks_by_number
    if start_timestamp > end_timestamp:
        message = f"Start timestamp has to be before end timestamp. {start_timestamp=:} and {end_timestamp=:}"
        logger.error(message)
        raise InvalidInput(message)
    start_block = get_block_for_timestamp(sidecar_url=sidecar_url, timestamp=start_timestamp, is_strict_boundary=False)
    logger.debug(f"Start block for timestamp: {start_timestamp} is {start_block}")
    end_block = get_block_for_timestamp(sidecar_url=sidecar_url, timestamp=end_timestamp, is_strict_boundary=True)
    logger.debug(f"end block for timestamp: {end_timestamp} is {end_block}")
    logger.info(f"Getting blocks between {start_timestamp} and {end_timestamp}")
    export_blocks_by_number(
        output_directory,
        sidecar_url,
        start_block,
        end_block,
        retries,
    )


def export_blocks_by_number(
    output_directory: Path,
    sidecar_url: str,
    start_block: int,
    end_block: int,
    retries: int = SIDECAR_RETRIES,
):
    """Exports blocks from the sidecar by block number"""
    if start_block > end_block:
        message = f"Start block number has to be smaller than end block number. {start=:,} and {end=:,}"
        logger.error(message)
        raise InvalidInput(message)
    requestor = sidecar.PolkadotRequestor(retries=retries)
    get_block = requestor.build_requestor(sidecar.get_block)
    logger.info(
        f"Getting {end_block - start_block + 1:,} blocks between {start_block:,} and {end_block:,}"
    )
    for block_number in range(start_block, end_block + 1):
        try:
            response = get_block(sidecar_url, block_number)
            response_json_path = output_directory / f"{block_number}.json"
            with open(response_json_path, "w") as file_buffer:
                file_buffer.write(json.dumps(response))
                logger.debug(
                    f"Wrote block response of block #{block_number} to {response_json_path}."
                )
        except RetryError:
            logger.error(f"Unable to export block {block_number} due to retry failures")

    logger.debug(f"Wrote {end_block - start_block + 1} blocks to {output_directory}.")


def get_latest_block(
        sidecar_url: str,
):
    requestor = sidecar.PolkadotRequestor()
    get_block = requestor.build_requestor(sidecar.get_block)
    end_block_response = get_block(sidecar_url, "head")
    print(end_block_response)

    latest_block_timestamp = str(datetime.utcfromtimestamp(
        int(end_block_response["extrinsics"][0]["args"]["now"]) / 1000)
                                 .strftime('%Y-%m-%d %H:%M:%S %Z'))

    return int(end_block_response["number"]), latest_block_timestamp
