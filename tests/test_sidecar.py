"""Tests for the sidecar requestor"""
from decouple import config

SIDECAR_URL = "https://merkle-polkadot-01.bdnodes.net?auth=yibdyXgPzZj9lgTE3vVV_nsFWKcRGhhGvOmFqPrnIqU"


def test_requestor_function_with_sidecar():
    """test that the requestor function can actually query the sidecar"""
    from polkadotetl.export import sidecar

    requestor = sidecar.PolkadotRequestor()
    get_block = requestor.build_requestor(sidecar.get_block)
    block_response = get_block(SIDECAR_URL, 10000000)
    assert block_response is None


def test_get_block_for_timestamp():
    from polkadotetl.export import internals
    import datetime

    timestamp = datetime.datetime.now() - datetime.timedelta(days=5)
    block = internals.get_block_for_timestamp(SIDECAR_URL, timestamp)
    print(block)
    assert isinstance(block, int), "Block number is invalid"
