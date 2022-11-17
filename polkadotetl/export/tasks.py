"""Celery file"""
import json
import pathlib
from celery import Celery
import json
from polkadotetl.export import sidecar
from polkadotetl.logger import logger

app = Celery("tasks", broker="pyamqp://guest@localhost", backend="rpc://")


@app.task
def get_block_and_write_to_file(
    sidecar_url: str, block_number: int, output_directory: str
):
    """Gets a block from the sidecar and writes to file"""
    requestor = sidecar.PolkadotRequestor(retries=5)
    get_block = requestor.build_requestor(sidecar.get_block)

    response_json_path = pathlib.Path(output_directory) / f"{block_number}.json"
    if response_json_path.exists():
        with open(response_json_path) as file_buffer:
            try:
                block_response_data = json.load(file_buffer)
                if block_response_data.get("extrinsics") is not None:
                    logger.debug(
                        f"Ignoring block #{block_number} as it's already written to a file"
                    )
                    return
                else:
                    pass
            except Exception:
                pass
    response = get_block(sidecar_url, block_number)
    with open(response_json_path, "w") as file_buffer:
        file_buffer.write(json.dumps(response))
