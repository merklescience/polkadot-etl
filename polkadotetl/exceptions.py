from requests.exceptions import RequestException


class InvalidInput(Exception):
    """Raised when there is an invalid input to the CLI."""


class PolkadotSidecarError(RequestException):
    """Raised when the `extrinsics` field is absent in the blocks response,
    or when the `code` field appears in it."""
