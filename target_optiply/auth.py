import json
from datetime import datetime
from typing import Optional
from base64 import b64encode
from typing import Any, Dict, Optional

import logging
import requests


class OptiplyAuthenticator:
    """API Authenticator for OAuth 2.0 flows."""@property
    def auth_headers(self) -> dict:
        if not self.is_token_valid():
            self.update_access_token()
        result = {}
        result["Authorization"] = f"Bearer {self._config.get('access_token')}"
        return result