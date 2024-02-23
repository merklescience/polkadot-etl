"""Functions to help enrich blocks"""
import warnings
import datetime

from polkadotetl.logger import logger
from polkadotetl.core.types import TransferTypes
from polkadotetl.constants import POLKADOT_TREASURY, DECIMAL_AFTER_REDENOMINATION, REWARD_DESTINATION_STASH, REWARD_DESTINATION_STAKED, REWARD_DESTINATION_CONTROLLER, REWARD_DESTINATION_ACCOUNT
from polkadotetl.exceptions import BlockNotFinalized
from polkadotetl.warnings import NoTransactionsWarning


def enrich_block(sidecar_block_response: dict):
    """This function helps enrich block responses from the sidecar.

    This function returns a list of transactions"""
    block_number = sidecar_block_response["number"]
    token_address = "0x0000"
    if not sidecar_block_response["finalized"]:
        message = f"Block #{block_number} is not yet finalized. Cannot be enriched."
        logger.error(message)
        raise BlockNotFinalized(message)

    # First filter out the extrinsics where extrinsics.method.pallet
    # is in "paraInherent", "timestamp"
    extrinsics = sidecar_block_response["extrinsics"]
    block_timestamp = extrinsics[0]["args"]["now"]
    ignored_extrinsic_pallets = [
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
        "balances.TransferAllowDeath",
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
        if extrinsic["method"]["pallet"] in ignored_extrinsic_pallets:
            continue
        extrinsic_type = "{}.{}".format(
            extrinsic["method"]["pallet"], extrinsic["method"]["method"]
        )
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
            if signature is None:
                signer = None
            elif isinstance(signature["signer"], dict):
                signer = signature["signer"]["id"]
            elif isinstance(signature["signer"], str):
                signer = signature["signer"]
            else:
                raise TypeError(
                    "Signature signer is not a string or a dictionary. Value:{}".format(
                        signature
                    )
                )
            if event_type in ("balances.Transfer", "balances.TransferAllowDeath"):
                sender_address = signer
                # second item in the data list
                receiver_address = event["data"][1]
                if status:
                    coin_value = float(event["data"][2]) / DECIMAL_AFTER_REDENOMINATION
                else:
                    coin_value = 0
                fee = 0
                type_ = TransferTypes.NORMAL
            elif event_type == "treasury.Deposit":
                sender_address = signer
                # second item in the data list
                receiver_address = POLKADOT_TREASURY
                coin_value = 0
                fee = float(event["data"][0]) / DECIMAL_AFTER_REDENOMINATION
                type_ = TransferTypes.FEE
            elif event_type == "staking.Reward":
                sender_address = None
                receiver_address = event["data"][0]
                if status:
                    coin_value = float(event["data"][1]) / DECIMAL_AFTER_REDENOMINATION
                else:
                    coin_value = 0
                fee = 0
                type_ = TransferTypes.NO_SENDER
            elif event_type == "staking.Rewarded":
            ### https://github.com/paritytech/polkadot-sdk/blob/master/substrate/frame/staking/src/lib.rs#L401
                sender_address = None
                if (REWARD_DESTINATION_STASH in event["data"][1]):
                    receiver_address = event["data"][0]
                elif (REWARD_DESTINATION_STAKED in event["data"][1]):
                    receiver_address = event["data"][0]
                elif (REWARD_DESTINATION_CONTROLLER in event["data"][1]):
                    receiver_address = event["data"][1]["Controller"]
                elif (REWARD_DESTINATION_ACCOUNT in event["data"][1]):
                    receiver_address = event["data"][1]["Account"]
                else:
                    receiver_address = event["data"][0]
                if status:
                    if (REWARD_DESTINATION_STASH in event["data"][1]):
                        coin_value = 0
                    else:
                        coin_value = float(event["data"][2]) / DECIMAL_AFTER_REDENOMINATION
                else:
                    coin_value = 0
                fee = 0
                type_ = TransferTypes.NO_SENDER   
            elif event_type == "claims.Claimed":
                sender_address = None
                receiver_address = event["data"][0]
                if status:
                    coin_value = float(event["data"][2]) / DECIMAL_AFTER_REDENOMINATION
                else:
                    coin_value = 0
                fee = 0
                type_ = TransferTypes.NO_SENDER
            elif event_type in (
                "identity.SubIdentityAdded",
                "identity.SubIdentityRevoked",
                "identity.SubIdentityRemoved",
                "balances.ReserveRepatriated",
            ):
                sender_address = event["data"][0]
                receiver_address = event["data"][1]
                if status:
                    coin_value = float(event["data"][2]) / DECIMAL_AFTER_REDENOMINATION
                else:
                    coin_value = 0
                fee = 0
                type_ = TransferTypes.NORMAL
            elif event_type == "balances.Slashed":
                sender_address = event["data"][0]
                receiver_address = POLKADOT_TREASURY
                if status:
                    coin_value = float(event["data"][1]) / DECIMAL_AFTER_REDENOMINATION
                else:
                    coin_value = 0
                fee = 0
                type_ = TransferTypes.NORMAL
            elif event_type == "balances.DustLost":
                sender_address = event["data"][0]
                receiver_address = None
                if status:
                    coin_value = float(event["data"][1]) / DECIMAL_AFTER_REDENOMINATION
                else:
                    coin_value = 0
                fee = 0
                type_ = TransferTypes.NO_RECEIVER
            elif event_type == "balances.BalanceSet":
                sender_address = None
                receiver_address = event["data"][0]
                if status:
                    coin_value = float(event["data"][1]) / DECIMAL_AFTER_REDENOMINATION
                else:
                    coin_value = 0
                fee = 0
                type_ = TransferTypes.BALANCES_SET_BY_ROOT
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
                    fee = float(data[1]) / DECIMAL_AFTER_REDENOMINATION
                    type_ = TransferTypes.FEE
                else:
                    sender_address = None
                    receiver_address = address
                    if status:
                        coin_value = float(data[1]) / DECIMAL_AFTER_REDENOMINATION
                    else:
                        coin_value = 0
                    fee = 0
                    type_ = TransferTypes.NO_SENDER
            else:
                raise NotImplementedError(f"Invalid event type_ {event_type}")
            txn = dict(
                block=block_number,
                transaction_hash=txn_hash,
                sender_address=sender_address,
                receiver_address=receiver_address,
                type=type_.value,
                token_address=token_address,
                coin_value=str(coin_value),
                fee=str(fee),
                block_timestamp=int(block_timestamp)/1000,
                log_index=0
            )
            if txn not in txns:
                txns.append(txn)
    if len(txns) == 0:
        warnings.warn(
            f"Block #{block_number} doesn't have any transactions with relevant events.",
            NoTransactionsWarning,
        )
    return txns
