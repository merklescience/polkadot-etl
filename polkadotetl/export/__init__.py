"""Functions to export polkadot blocks from a sidecar"""
from pathlib import Path
from typing import Optional

from polkadotetl.constants import SIDECAR_RETRIES
from polkadotetl.export.internals import InputType, validate_inputs


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
    input_type = validate_inputs(start_block, end_block, start_timestamp, end_timestamp)

    if input_type == InputType.BLOCKS:
        ...
    else:
        ...
