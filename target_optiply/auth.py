"""Authentication module for Optiply target."""

import json
from datetime import datetime
from typing import Optional
from base64 import b64encode
from typing import Any, Dict, Optional

import logging
import requests

from target_optiply.client import OptiplySink

logger = logging.getLogger(__name__)

class OptiplyAuthenticator:
    """API Authenticator for OAuth 2.0 flows."""

    def __init__(self, config: Dict[str, Any]) -> None:
        """Initialize authenticator.

        Args:
            config: The configuration dictionary.
        """
        self._config = config
        self._access_token = None
        self._token_expires_at = None
        self._refresh_token = None

    @property
    def auth_headers(self) -> dict:
        """Get the authentication headers.

        Returns:
            The authentication headers.
        """
        if not self.is_token_valid():
            self.update_access_token()
        result = {}
        result["Authorization"] = f"Bearer {self._access_token}"
        return result

    def is_token_valid(self) -> bool:
        """Check if the current token is valid.

        Returns:
            True if the token is valid, False otherwise.
        """
        if not self._access_token or not self._token_expires_at:
            return False
        # Add a 5-minute buffer before expiration
        return datetime.now().timestamp() < (self._token_expires_at - 300)

    def update_access_token(self) -> None:
        """Update the access token."""
        try:
            # Get the credentials from config
            client_id = self._config.get("client_id")
            client_secret = self._config.get("client_secret")
            username = self._config.get("username")
            password = self._config.get("password")

            if not all([client_id, client_secret, username, password]):
                raise ValueError("Missing required credentials in config")

            # Create basic auth token
            auth_string = f"{client_id}:{client_secret}"
            basic_token = b64encode(auth_string.encode()).decode()

            # Make the token request
            response = requests.post(
                f"{OptiplySink.auth_url}?grant_type=password",
                headers={
                    "Authorization": f"Basic {basic_token}",
                    "Content-Type": "application/x-www-form-urlencoded"
                },
                data={
                    "username": username,
                    "password": password
                }
            )
            response.raise_for_status()

            # Parse the response
            token_data = response.json()
            self._access_token = token_data.get("access_token")
            self._refresh_token = token_data.get("refresh_token")
            expires_in = token_data.get("expires_in", 3600)  # Default to 1 hour
            self._token_expires_at = datetime.now().timestamp() + expires_in

            logger.info("Successfully updated access token")

        except requests.exceptions.RequestException as e:
            logger.error(f"Error updating access token: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error updating access token: {str(e)}")
            raise

    def get_auth_session(self) -> None:
        """Get the authentication session.

        Returns:
            None since we use headers for authentication.
        """
        return None