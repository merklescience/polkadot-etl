import enum


class TransferTypes(enum.Enum):
    """This defines the types of transfers in Polkadot"""

    NORMAL = 0
    FEE = 1
    NO_SENDER = 2
    NO_RECEIVER = 3
    BALANCES_SET_BY_ROOT = 4
