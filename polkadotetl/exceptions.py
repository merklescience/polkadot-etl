from requests.exceptions import RequestException


class InvalidInput(Exception):
    """Raised when there is an invalid input to the CLI."""


class PolkadotSidecarError(RequestException):
    """Raised when the `extrinsics` field is absent in the blocks response,
    or when the `code` field appears in it."""


class BlockNotFinalized(Exception):
    """Raised when the `finalized` field in the block response is `false`."""


class NoBlockAtTimestamp(Exception):
    """Raised when there is no block at a given timestamp."""


class InvalidBlockNumber(Exception):
    """Raised when a user queries an invalid block. This is mostly necessary
    because the sidecar uses /blocks/head for the head block, and the function
    takes integers, but accounts just for this one string."""

class PruningError(Exception):
    """Raised when the `success` field of an extrinsic has errors,
    specifically when the field has the string:

        'Unable to fetch Events, cannot confirm extrinsic status. Check pruning settings on the node.'
    """
