"""Optiply target sink class, which handles writing streams."""

from __future__ import annotations

import backoff
import json
import logging
import os
from datetime import datetime
import requests
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from singer_sdk.sinks import RecordSink

from target_optiply.auth import OptiplyAuthenticator

logger = logging.getLogger(__name__)

class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder for datetime objects."""

    def default(self, obj):
        """Encode datetime objects."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class OptiplySink(RecordSink):
    """Optiply target sink class."""

    base_url = os.environ.get("optiply_base_url", "https://api.optiply.com/v1")

    def __init__(
        self,
        target: Any,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]] = None,
    ) -> None:
        """Initialize the sink.

        Args:
            target: The target instance.
            stream_name: The name of the stream.
            schema: The schema for the stream.
            key_properties: The key properties for the stream.
        """
        super().__init__(target, stream_name, schema, key_properties)
        self._authenticator = None
        self._session = None
        self._access_token = None
        self._token_expires_at = None

    @property
    def authenticator(self) -> OptiplyAuthenticator:
        """Get the authenticator instance.

        Returns:
            The authenticator instance.
        """
        if self._authenticator is None:
            self._authenticator = OptiplyAuthenticator(self.config)
        return self._authenticator

    def http_headers(self) -> Dict[str, str]:
        """Get the HTTP headers for the request.

        Returns:
            The HTTP headers.
        """
        return {
            "Content-Type": "application/vnd.api+json",
            "Accept": "application/vnd.api+json"
        }

    def validate_response(self, response: requests.Response) -> None:
        """Validate the response from the API.

        Args:
            response: The response to validate.

        Raises:
            FatalAPIError: If the response indicates a fatal error.
            RetriableAPIError: If the response indicates a retriable error.
        """
        if response.status_code >= 500:
            raise RetriableAPIError(f"Server error: {response.text}")
        elif response.status_code == 404:
            logger.warning(f"Resource not found (404): {response.url}")
            return
        elif response.status_code >= 400:
            raise FatalAPIError(f"Client error: {response.text}")

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ReadTimeout),
        max_tries=5,
        factor=2,
    )
    def _request(
        self, http_method, endpoint, params=None, request_data=None, headers=None
    ) -> requests.PreparedRequest:
        """Prepare a request object."""
        url = self.url(endpoint)
        headers = {**self.http_headers(), **self.authenticator.auth_headers}

        response = requests.request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data
        )
        self.validate_response(response)
        return response

    def url(self, endpoint: str = "") -> str:
        """Get the URL for the given endpoint.

        Args:
            endpoint: The endpoint to get the URL for.

        Returns:
            The URL for the endpoint.
        """
        # Add accountId and couplingId as query parameters if they exist
        params = {}
        if "account_id" in self.config:
            params["accountId"] = self.config["account_id"]
        if "coupling_id" in self.config:
            params["couplingId"] = self.config["coupling_id"]
        
        url = f"{self.base_url}/{endpoint}"
        if params:
            query_string = "&".join(f"{k}={v}" for k, v in params.items())
            url = f"{url}?{query_string}"
        return url