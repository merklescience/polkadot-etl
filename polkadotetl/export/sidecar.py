from typing import Callable, Optional
import requests
from requests.exceptions import InvalidURL, RequestException

from polkadotetl.constants import SIDECAR_RETRIES, SIDECAR_RETRY_DELAY_IN_SECONDS
from polkadotetl.exceptions import PolkadotSidecarError, InvalidBlockNumber
from polkadotetl.logger import logger


class PolkadotRequestor:
    """PolkadotRequestor
    This class builds a `requests` client
    that can handle the polkadot sidecar API.
    It can query the sidecar and account for
    common errors and retry the endpoint by accounting
    for a backoff as well.

    This class uses `tenacity` for the retry methods."""

    def __init__(
        self,
        retries: int = SIDECAR_RETRIES,
        retry_max_delay: int = SIDECAR_RETRY_DELAY_IN_SECONDS,
    ):
        # TODO: maybe account for headers instead of using a URL with query parameters.
        self.retries = retries
        self.retry_max_delay = retry_max_delay

    def build_requestor(self, request_function: Callable) -> Callable:
        """Creates a retrying function that can query the sidecar API
        for the blocks."""
        import logging
        from tenacity import (
            after_log,
            retry,
            retry_if_exception_type,
            stop_after_attempt,
            wait_exponential,
        )

        @retry(
            stop=stop_after_attempt(self.retries),
            wait=wait_exponential(multiplier=1, min=1, max=self.retry_max_delay),
            # TODO: Figure out how to use a list of custom exceptions as arguments
            # to `build_requestor`
            retry=(
                retry_if_exception_type(RequestException)
                | retry_if_exception_type(PolkadotSidecarError)
            ),
            after=after_log(logger, logging.WARNING),
        )
        def retry_function(*args, **kwargs) -> dict:
            """This function fires a function with retrying configurations"""
            return request_function(*args, **kwargs)

        return retry_function


def validate_url(url):
    """Internal function to validate the sidecar url."""
    from urllib.parse import urlparse

    try:
        result = urlparse(url)
        if all([result.scheme, result.netloc]):
            return
        logger.error(f"{url} is an invalid URL.")
        raise InvalidURL(f"{url} is an invalid URL.")
    except Exception as err:
        logger.error(f"{url} is an invalid URL.")
        raise InvalidURL(f"{url} is an invalid URL.") from err


def get_block(sidecar_url, block_number):
    """Gets 1 block response from the polkadot sidecar"""
    from urllib.parse import urlparse, urljoin

    validate_url(sidecar_url)
    if not isinstance(block_number, int) and block_number != "head":
        raise InvalidBlockNumber(f"`{block_number}` is invalid.")
    url = urlparse(sidecar_url)
    base_block_url = urljoin(sidecar_url, f"blocks/{block_number}")
    # NOTE: Do not log raw block_url since it will probably have the API key.
    # base_block_url will not have this parameter.
    if url.query != "":
        block_url = f"{base_block_url}?{url.query}"
    else:
        block_url = base_block_url
    response = requests.get(block_url)
    if isinstance(block_number, int) and response.status_code != 200:
        message = f"Received response for block #{block_number:,} from {base_block_url}. Status Code: {response.status_code}"
    elif response.status_code != 200:
        message = f"Received response for HEAD block from {base_block_url}. Status Code: {response.status_code}"
    # logger.debug(message)
    response.raise_for_status()
    block_response = response.json()
    if code := block_response.get("code") is not None:
        if isinstance(block_number, int):
            message = f"Got error code {code} querying for block #{block_number:,}"
        else:
            message = f"Got error code {code} querying for HEAD block."
        logger.error(message)
        raise PolkadotSidecarError(message)
    if "extrinsics" not in block_response.keys():
        if isinstance(block_number, int):
            message = (
                f"Error getting block number #{block_number:,} from {base_block_url}"
            )
        else:
            message = f"Error getting HEAD block from {base_block_url}"
        logger.error(message)
        raise PolkadotSidecarError(message)
    return block_response
