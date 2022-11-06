from datetime import datetime
from enum import Enum
from typing import Optional
from polkadotetl.logger import logger
from polkadotetl.exceptions import InvalidInput


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


def get_block_for_timestamp(timestamp: datetime):
    """Returns a block number for a particular timestamp"""
    # TODO: implement a helper function to get a block number given a timestamp
