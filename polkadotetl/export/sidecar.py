from typing import Optional
import requests
from requests.exceptions import InvalidURL, RequestException

from polkadotetl.constants import SIDECAR_RETRIES, SIDECAR_RETRY_DELAY_IN_SECONDS
from polkadotetl.exceptions import PolkadotSidecarError
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
        sidecar_url: str,
        retries: int = SIDECAR_RETRIES,
        retry_max_delay: int = SIDECAR_RETRY_DELAY_IN_SECONDS,
    ):
        # TODO: maybe account for headers instead of using a URL with query parameters.
        self.__validate_url(sidecar_url)
        self.sidecar_url = sidecar_url
        self.retries = retries
        self.retry_max_delay = retry_max_delay
        self.request_block = self.__build_requestor()

    def __build_requestor(self):
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
            retry=(
                retry_if_exception_type(RequestException)
                | retry_if_exception_type(PolkadotSidecarError)
            ),
            after=after_log(logger, logging.WARNING),
        )
        def request_block(block_number) -> dict:
            """This function requests for a block from the polkadot sidecar URL,
            and it will retry the requests as configured
            Takes a block number and returns a dictionary of the response."""
            from urllib.parse import urlparse, urljoin

            url = urlparse(self.sidecar_url)
            base_block_url = urljoin(self.sidecar_url, f"blocks/{block_number}")
            # NOTE: Do not log block_url since it will probably have the API key.
            block_url = f"{base_block_url}?{url.query}"
            logger.debug(f"Requesting block #{block_number} from {base_block_url}")
            response = requests.get(block_url)
            logger.debug(
                f"Received response for block #{block_number} from {base_block_url}. Status Code: {response.status_code}"
            )
            response.raise_for_status()
            block_response = response.json()
            if code := block_response.get("code") is not None:
                message = f"Got error code {code} querying for block #{block_number}"
                logger.error(message)
                raise PolkadotSidecarError(message)
            if "extrinsics" not in block_response.keys():
                message = (
                    f"Error getting block number #{block_number} from {base_block_url}"
                )
                logger.error(message)
                raise PolkadotSidecarError(message)
            logger.debug(
                f"Received a valid block response for block #{block_number} from {base_block_url}"
            )
            return block_response

        return request_block

    def __validate_url(self, url):
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

    # TODO: I need to build a requestor that can
    # retry the requests based on parameters
    # this will even need to take a validator function for
    # the retries.
