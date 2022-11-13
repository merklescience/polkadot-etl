"""Celery file"""
import pathlib
from celery import Celery
import json
from polkadotetl.export import sidecar

app = Celery("tasks", broker="pyamqp://guest@localhost", backend='rpc://')


@app.task
def get_block_and_write_to_file(sidecar_url: str, block_number: int, output_directory: str):
    """Gets a block from the sidecar and writes to file"""
    requestor = sidecar.PolkadotRequestor(retries=5)
    get_block = requestor.build_requestor(sidecar.get_block)
    response_json_path = pathlib.Path(output_directory) / f"{block_number}.json"
    response = get_block(sidecar_url, block_number)
    with open(response_json_path, "w") as file_buffer:
        file_buffer.write(json.dumps(response))
