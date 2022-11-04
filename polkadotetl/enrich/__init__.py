"""Functions to help enrich blocks"""
import warnings

from polkadotetl.core.types import TransferTypes
from polkadotetl.constants import POLKADOT_TREASURY
from polkadotetl.warnings import NoTransactionsWarning



def enrich_block(sidecar_block_response: dict) -> list[dict]:
    """This function helps enrich block responses from the sidecar.

    This function returns a list of transactions"""
    block_number = sidecar_block_response["number"]
    token_address = "DOT"

    # First filter out the extrinsics where extrinsics.method.pallet
    # is in "paraInherent", "timestamp"
    extrinsics = sidecar_block_response["extrinsics"]
    block_timestamp = extrinsics[0]["args"]["now"]
    ignored_method_pallets = [
        "paraInherent",
        "timestamp",
    ]
    # use only some events from every extrinsics
    required_events = [
        "balances.BalanceSet",
        "balances.Deposit",
        "balances.DustLost",
        "balances.ReserveRepatriated",
        "balances.Slashed",
        "balances.Transfer",
        "claims.Claimed",
        "identity.SubIdentityAdded",
        "identity.SubIdentityRemoved",
        "identity.SubIdentityRevoked",
        "staking.Reward",
        "staking.Rewarded",
        "treasury.Deposit",
    ]
    txns = []
    for extrinsic in extrinsics:
        # ignore extrinsics where the method.pallet is not required
        if extrinsic["method"]["pallet"] in ignored_method_pallets:
            continue
        events = extrinsic["events"]
        txn_hash = extrinsic["hash"]
        for event in events:
            event_type = "{}.{}".format(
                event["method"]["pallet"], event["method"]["method"]
            )
            if event_type not in required_events:
                continue
            status = extrinsic["success"]
            signature = extrinsic["signature"]
            if isinstance(signature["signer"], dict):
                signer = signature["signer"]["id"]
            elif isinstance(signature["signer"], str):
                signer = signature["signer"]
            else:
                raise TypeError("Signature signer is not a string or a dictionary. Value:{}".format(signature))
            if event_type == "balances.Transfer":
                sender_address = signer
                # second item in the data list
                receiver_address = event["data"][1]
                coin_value = event["data"][2]
                fee = 0
                type = TransferTypes.NORMAL
            elif event_type == "treasury.Deposit":
                sender_address = signer
                # second item in the data list
                receiver_address = POLKADOT_TREASURY
                coin_value = 0
                fee = event["data"][0]
                type = TransferTypes.FEE
            elif event_type in ("staking.Reward", "staking.Rewarded"):
                sender_address = None
                receiver_address = event["data"][0]
                coin_value = event["data"][1]
                fee = 0
                type = TransferTypes.NO_SENDER
            elif event_type == "claims.Claimed":
                sender_address = None
                receiver_address = event["data"][0]
                coin_value = event["data"][2]
                fee = 0
                type = TransferTypes.NO_SENDER
            elif event_type in (
                "identity.SubIdentityAdded",
                "identity.SubIdentityRevoked",
                "identity.SubIdentityRemoved",
                "balances.ReserveRepatriated",
            ):
                sender_address = event["data"][0]
                receiver_address = event["data"][1]
                coin_value = event["data"][2]
                fee = 0
                type = TransferTypes.NORMAL
            elif event_type == "balances.Slashed":
                sender_address = event["data"][0]
                receiver_address = POLKADOT_TREASURY
                coin_value = event["data"][1]
                fee = 0
                type = TransferTypes.NORMAL
            elif event_type == "balances.DustLost":
                sender_address = event["data"][0]
                receiver_address = None
                coin_value = event["data"][1]
                fee = 0
                type = TransferTypes.NO_RECEIVER
            elif event_type == "balances.BalanceSet":
                sender_address = None
                receiver_address = event["data"][0]
                coin_value = event["data"][1]
                fee = 0
                type = TransferTypes.BALANCES_SET_BY_ROOT
            elif event_type == "balances.Deposit":
                # multiple cases
                data = event["data"]
                author_id = sidecar_block_response["authorId"]
                validator = signer
                address = data[0]
                if address in (author_id, validator, POLKADOT_TREASURY):
                    sender_address = validator
                    receiver_address = address
                    coin_value = 0
                    fee = data[1]
                    type = TransferTypes.FEE
                else:
                    sender_address = None
                    receiver_address = address
                    coin_value = data[1]
                    fee = 0
                    type = TransferTypes.NO_SENDER
            else:
                raise NotImplementedError(f"Invalid event type {event_type}")
            txn = dict(
                block_number=block_number,
                sender_address=sender_address,
                receiver_address=receiver_address,
                coin_value=coin_value,
                fee=fee,
                type=type.value,
                txn_hash=txn_hash,
                status=status,
                block_timestamp=block_timestamp,
                token_address=token_address,
            )
            if txn not in txns:
                txns.append(txn)
    if len(txns) == 0:
        warnings.warn(f"Block #{block_number} doesn't have any transactions with relevant events.", NoTransactionsWarning)
    return txns
