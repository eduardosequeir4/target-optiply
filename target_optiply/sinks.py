"""Optiply target sink class, which handles writing streams."""

from __future__ import annotations

import json
import logging
import time
import urllib.parse
from datetime import datetime
from typing import Any, Dict

import requests
from singer_sdk.sinks import RecordSink

# Set up logging
logger = logging.getLogger(__name__)

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class OptiplySink(RecordSink):
    """Optiply target sink class."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the sink.

        Args:
            *args: Arguments to pass to the parent class.
            **kwargs: Keyword arguments to pass to the parent class.
        """
        super().__init__(*args, **kwargs)
        self._session = requests.Session()
        self._auth_token = None
        self._refresh_token = None
        self._token_expires_at = None
        self._token_type = None
        self._scope = None
        self._auth_url = self.config["auth_url"]  # Get auth URL from config
        self._api_base_url = self.config["api_url"]  # Get API URL from config
        self._last_response = None  # Store the last response
        
        # Set default headers
        self._session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "curl/8.4.0",  # Match curl user agent
        })
        
        # Get initial token
        self._authenticate()

    def _authenticate(self) -> None:
        """Authenticate with the Optiply API."""
        credentials = self.config["api_credentials"]
        
        try:
            # Prepare the data in the exact format from the curl example
            data = {
                "username": credentials["username"],
                "password": credentials["password"]
            }
            
            # Use the Basic token from config
            authorization = f"Basic {credentials['basic_token']}"
            
            response = self._session.post(
                f"{self._auth_url}?grant_type=password",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Authorization": authorization,
                },
                data=data  # Send data as form-urlencoded
            )
            response.raise_for_status()
            token_data = response.json()
            
            self._auth_token = token_data["access_token"]
            self._refresh_token = token_data["refresh_token"]
            self._token_type = token_data["token_type"]
            self._scope = token_data["scope"]
            # Set expiration time (subtract 60 seconds for safety margin)
            self._token_expires_at = time.time() + token_data["expires_in"] - 60
            
            # Update authorization header
            self._session.headers["Authorization"] = f"{self._token_type.capitalize()} {self._auth_token}"
            logger.info("Successfully authenticated with Optiply API")
        except requests.exceptions.RequestException as e:
            logger.error(f"Authentication failed: {str(e)}")
            raise Exception(f"Failed to authenticate with Optiply API: {str(e)}") from e

    def _refresh_token_if_needed(self) -> None:
        """Refresh the token if it's expired or about to expire."""
        current_time = time.time()
        if not self._auth_token or (self._token_expires_at and current_time >= self._token_expires_at):
            if self._refresh_token:
                try:
                    response = self._session.post(
                        f"{self._auth_url}?grant_type=refresh_token",
                        headers={
                            "Content-Type": "application/x-www-form-urlencoded",
                            "Authorization": self._authorization,
                        },
                        data=f"refresh_token={urllib.parse.quote(self._refresh_token)}",
                    )
                    response.raise_for_status()
                    token_data = response.json()
                    
                    self._auth_token = token_data["access_token"]
                    self._refresh_token = token_data.get("refresh_token", self._refresh_token)
                    self._token_type = token_data["token_type"]
                    self._scope = token_data["scope"]
                    self._token_expires_at = time.time() + token_data["expires_in"] - 60
                    
                    # Update authorization header
                    self._session.headers["Authorization"] = f"{self._token_type.capitalize()} {self._auth_token}"
                    logger.info("Successfully refreshed token")
                except requests.exceptions.RequestException as e:
                    logger.error(f"Token refresh failed: {str(e)}")
                    # If refresh fails, try to get a new token
                    self._authenticate()
            else:
                self._authenticate()

    def _ensure_valid_token(self) -> None:
        """Ensure we have a valid token before making a request."""
        current_time = time.time()
        if not self._auth_token or (self._token_expires_at and current_time >= self._token_expires_at):
            self._refresh_token_if_needed()

    def _prepare_payload(self, record: dict, context: dict) -> dict:
        """Prepare the payload for the Optiply API."""
        # Map fields from the record
        placed = record.get("transaction_date")
        if isinstance(placed, datetime):
            placed = placed.isoformat()
        if placed and not placed.endswith('Z'):
            placed = placed.replace('+00:00', '') + 'Z'

        supplier_id = record.get("supplier_remoteId")
        account_id = self.config["api_credentials"]["account_id"]
        
        # Calculate total value from line items
        total_value = 0
        if "line_items" in record:
            try:
                line_items = json.loads(record["line_items"])
                for item in line_items:
                    total_value += float(item.get("subtotalValue", 0))
            except (json.JSONDecodeError, ValueError) as e:
                logger.error(f"Error parsing line items: {str(e)}")
                return None

        # Check mandatory fields only for POST requests
        http_method = context.get("http_method", "POST").upper()
        if http_method == "POST":
            missing_fields = []
            if not placed:
                missing_fields.append("placed (transaction_date)")
            if not total_value:
                missing_fields.append("totalValue (from line_items)")
            if not supplier_id or (isinstance(supplier_id, str) and not supplier_id.strip()):
                missing_fields.append("supplierId (supplier_remoteId)")
            if not account_id:
                missing_fields.append("accountId")

            if missing_fields:
                logger.error(f"Record skipped due to missing mandatory fields: {', '.join(missing_fields)}")
                return None

        # Build attributes dictionary with all possible fields
        attributes = {}
        
        # Add fields only if they have values
        if placed:
            attributes["placed"] = placed
        if total_value:
            attributes["totalValue"] = float(total_value)
        if supplier_id and (isinstance(supplier_id, str) and supplier_id.strip()):
            attributes["supplierId"] = int(supplier_id)
        if account_id:
            attributes["accountId"] = int(account_id)
        
        # Handle dates
        if "completed" in record:
            completed_date = record["completed"]
            if isinstance(completed_date, datetime):
                completed_date = completed_date.isoformat()
            if not completed_date.endswith('Z'):
                completed_date = completed_date.replace('+00:00', '') + 'Z'
            attributes["completed"] = completed_date

        # Handle line items for POST requests
        if "line_items" in record:
            try:
                line_items = json.loads(record["line_items"])
                order_lines = []
                for item in line_items:
                    order_line = {
                        "quantity": int(item.get("quantity", 0)),
                        "subtotalValue": float(item.get("subtotalValue", 0)),
                        "productId": int(item.get("productId", 0))
                    }
                    # Only add non-zero values
                    order_line = {k: v for k, v in order_line.items() if v != 0}
                    if order_line:  # Only add if there are non-zero values
                        order_lines.append(order_line)
                
                if order_lines:
                    attributes["orderLines"] = order_lines
            except (json.JSONDecodeError, ValueError) as e:
                logger.error(f"Error parsing line items: {str(e)}")
                return None

        return {
            "data": {
                "type": "buyOrders",
                "attributes": attributes
            }
        }

    def process_record(self, record: dict, context: dict) -> None:
        """Process a record and write it to the Optiply API."""
        try:
            # Prepare the payload
            payload = self._prepare_payload(record, context)
            if payload is None:  # If payload preparation failed due to missing mandatory fields
                return

            logger.info(f"Request payload: {json.dumps(payload, indent=2)}")

            # Make the request
            self._ensure_valid_token()
            url = f"{self._api_base_url}/buyOrders"
            params = {
                "accountId": str(self.config["api_credentials"]["account_id"]),
                "couplingId": str(self.config["api_credentials"]["coupling_id"])
            }
            
            # Use the authenticated session with the correct headers
            self._session.headers.update({
                "Content-Type": "application/vnd.api+json",
                "Accept": "application/json",
                "User-Agent": "curl/8.4.0"
            })
            
            # Convert payload to JSON string using DateTimeEncoder
            payload_json = json.dumps(payload, cls=DateTimeEncoder)
            
            # Get HTTP method from context, default to POST if not specified
            http_method = context.get("http_method", "POST").upper()
            
            # Validate HTTP method
            if http_method not in ["POST", "PATCH"]:
                logger.error(f"Unsupported HTTP method: {http_method}. Only POST and PATCH are supported.")
                return
            
            # Validate remoteId for PATCH requests
            if http_method == "PATCH":
                remote_id = record.get("remoteId")
                if not remote_id:
                    logger.error("PATCH method requires a remoteId in the record")
                    return
                url = f"{url}/{remote_id}"
                logger.info(f"Using PATCH for record with remoteId: {remote_id}")
            else:
                logger.info("Using POST for record")
            
            # Make the request using the specified HTTP method
            if http_method == "PATCH":
                response = self._session.patch(
                    url,
                    params=params,
                    data=payload_json,
                    verify=False
                )
            else:
                response = self._session.post(
                    url,
                    params=params,
                    data=payload_json,
                    verify=False
                )
            
            self._last_response = response  # Store the response
            
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Response body: {response.text}")
            
            # Raise an exception if the request failed
            response.raise_for_status()

        except Exception as e:
            logger.error(f"Failed to process record: {str(e)}")
            # Don't raise the exception, just log it and continue
            return

class ProductSink(RecordSink):
    """Optiply target sink class for products."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the sink.

        Args:
            *args: Arguments to pass to the parent class.
            **kwargs: Keyword arguments to pass to the parent class.
        """
        super().__init__(*args, **kwargs)
        self._session = requests.Session()
        self._auth_token = None
        self._refresh_token = None
        self._token_expires_at = None
        self._token_type = None
        self._scope = None
        self._auth_url = self.config["auth_url"]
        self._api_base_url = self.config["api_url"]
        self._last_response = None
        
        # Set default headers
        self._session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "curl/8.4.0",
        })
        
        # Get initial token
        self._authenticate()

    def _authenticate(self) -> None:
        """Authenticate with the Optiply API."""
        credentials = self.config["api_credentials"]
        
        try:
            data = {
                "username": credentials["username"],
                "password": credentials["password"]
            }
            
            authorization = f"Basic {credentials['basic_token']}"
            
            response = self._session.post(
                f"{self._auth_url}?grant_type=password",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Authorization": authorization,
                },
                data=data
            )
            response.raise_for_status()
            token_data = response.json()
            
            self._auth_token = token_data["access_token"]
            self._refresh_token = token_data["refresh_token"]
            self._token_type = token_data["token_type"]
            self._scope = token_data["scope"]
            self._token_expires_at = time.time() + token_data["expires_in"] - 60
            
            self._session.headers["Authorization"] = f"{self._token_type.capitalize()} {self._auth_token}"
            logger.info("Successfully authenticated with Optiply API")
        except requests.exceptions.RequestException as e:
            logger.error(f"Authentication failed: {str(e)}")
            raise Exception(f"Failed to authenticate with Optiply API: {str(e)}") from e

    def _refresh_token_if_needed(self) -> None:
        """Refresh the token if it's expired or about to expire."""
        current_time = time.time()
        if not self._auth_token or (self._token_expires_at and current_time >= self._token_expires_at):
            if self._refresh_token:
                try:
                    response = self._session.post(
                        f"{self._auth_url}?grant_type=refresh_token",
                        headers={
                            "Content-Type": "application/x-www-form-urlencoded",
                            "Authorization": self._authorization,
                        },
                        data=f"refresh_token={urllib.parse.quote(self._refresh_token)}",
                    )
                    response.raise_for_status()
                    token_data = response.json()
                    
                    self._auth_token = token_data["access_token"]
                    self._refresh_token = token_data.get("refresh_token", self._refresh_token)
                    self._token_type = token_data["token_type"]
                    self._scope = token_data["scope"]
                    self._token_expires_at = time.time() + token_data["expires_in"] - 60
                    
                    self._session.headers["Authorization"] = f"{self._token_type.capitalize()} {self._auth_token}"
                    logger.info("Successfully refreshed token")
                except requests.exceptions.RequestException as e:
                    logger.error(f"Token refresh failed: {str(e)}")
                    self._authenticate()
            else:
                self._authenticate()

    def _ensure_valid_token(self) -> None:
        """Ensure we have a valid token before making a request."""
        current_time = time.time()
        if not self._auth_token or (self._token_expires_at and current_time >= self._token_expires_at):
            self._refresh_token_if_needed()

    def _prepare_payload(self, record: dict, context: dict) -> dict:
        """Prepare the payload for the Optiply API."""
        # Check mandatory fields only for POST requests
        http_method = context.get("http_method", "POST").upper()
        if http_method == "POST":
            mandatory_fields = ["name", "unlimitedStock", "stockLevel"]
            missing_fields = []
            for field in mandatory_fields:
                if field not in record or record[field] is None or (isinstance(record[field], str) and not record[field].strip()):
                    missing_fields.append(field)
            if missing_fields:
                logger.error(f"Record skipped due to missing mandatory fields: {', '.join(missing_fields)}")
                return None

        # Build attributes dictionary with all possible fields
        attributes = {}
        
        # Add fields only if they have values
        if "name" in record and record["name"]:
            attributes["name"] = record["name"]
        if "unlimitedStock" in record:
            attributes["unlimitedStock"] = bool(record["unlimitedStock"])
        if "stockLevel" in record:
            attributes["stockLevel"] = int(record["stockLevel"])
            
        # Add optional fields only if they have non-zero values
        if "skuCode" in record and record["skuCode"]:
            attributes["skuCode"] = record["skuCode"]
            
        if "eanCode" in record and record["eanCode"]:
            attributes["eanCode"] = record["eanCode"]
            
        if "articleCode" in record and record["articleCode"]:
            attributes["articleCode"] = record["articleCode"]
            
        if "price" in record and float(record.get("price", 0)) != 0:
            attributes["price"] = float(record["price"])
            
        if "minimumPurchaseQuantity" in record and int(record.get("minimumPurchaseQuantity", 0)) != 0:
            attributes["minimumPurchaseQuantity"] = int(record["minimumPurchaseQuantity"])
            
        if "lotSize" in record and int(record.get("lotSize", 0)) != 0:
            attributes["lotSize"] = int(record["lotSize"])
            
        if "availability" in record and record["availability"]:
            attributes["availability"] = record["availability"]
            
        if "availabilityDate" in record and record["availabilityDate"]:
            attributes["availabilityDate"] = record["availabilityDate"]
            
        if "preferred" in record:
            attributes["preferred"] = bool(record["preferred"])
            
        if "deliveryTime" in record and record["deliveryTime"]:
            attributes["deliveryTime"] = record["deliveryTime"]
            
        if "status" in record and record["status"]:
            attributes["status"] = record["status"]
            
        if "freeStock" in record and int(record.get("freeStock", 0)) != 0:
            attributes["freeStock"] = int(record["freeStock"])
            
        if "weight" in record and float(record.get("weight", 0)) != 0:
            attributes["weight"] = float(record["weight"])
            
        if "volume" in record and float(record.get("volume", 0)) != 0:
            attributes["volume"] = float(record["volume"])

        return {
            "data": {
                "type": "products",
                "attributes": attributes
            }
        }

    def process_record(self, record: dict, context: dict) -> None:
        """Process a record and write it to the Optiply API."""
        try:
            # Prepare the payload
            payload = self._prepare_payload(record, context)
            if payload is None:  # If payload preparation failed due to missing mandatory fields
                # Get the missing fields from the last error message
                missing_fields = []
                for field in ["name", "unlimitedStock", "stockLevel"]:
                    if field not in record or record[field] is None or (isinstance(record[field], str) and not record[field].strip()):
                        missing_fields.append(field)
                logger.error(f"Record skipped due to missing mandatory fields: {', '.join(missing_fields)}")
                return

            logger.info(f"Request payload: {json.dumps(payload, indent=2)}")

            # Make the request
            self._ensure_valid_token()
            url = f"{self._api_base_url}/products"
            params = {
                "accountId": str(self.config["api_credentials"]["account_id"]),
                "couplingId": str(self.config["api_credentials"]["coupling_id"])
            }
            
            # Use the authenticated session with the correct headers
            self._session.headers.update({
                "Content-Type": "application/vnd.api+json",
                "Accept": "application/json",
                "User-Agent": "curl/8.4.0"
            })
            
            # Convert payload to JSON string using DateTimeEncoder
            payload_json = json.dumps(payload, cls=DateTimeEncoder)
            
            # Get HTTP method from context, default to POST if not specified
            http_method = context.get("http_method", "POST").upper()
            
            # Validate HTTP method
            if http_method not in ["POST", "PATCH"]:
                logger.error(f"Unsupported HTTP method: {http_method}. Only POST and PATCH are supported.")
                return
            
            # Validate remoteId for PATCH requests
            if http_method == "PATCH":
                remote_id = record.get("remoteId")
                if not remote_id:
                    logger.error("PATCH method requires a remoteId in the record")
                    return
                url = f"{url}/{remote_id}"
                logger.info(f"Using PATCH for record with remoteId: {remote_id}")
            else:
                logger.info("Using POST for record")
            
            # Make the request using the specified HTTP method
            if http_method == "PATCH":
                response = self._session.patch(
                    url,
                    params=params,
                    data=payload_json,
                    verify=False
                )
            else:
                response = self._session.post(
                    url,
                    params=params,
                    data=payload_json,
                    verify=False
                )
            
            self._last_response = response
            
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Response body: {response.text}")
            
            response.raise_for_status()

        except Exception as e:
            logger.error(f"Failed to process record: {str(e)}")
            return

class SupplierSink(RecordSink):
    """Optiply target sink class for suppliers."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the sink.

        Args:
            *args: Arguments to pass to the parent class.
            **kwargs: Keyword arguments to pass to the parent class.
        """
        super().__init__(*args, **kwargs)
        self._session = requests.Session()
        self._auth_token = None
        self._refresh_token = None
        self._token_expires_at = None
        self._token_type = None
        self._scope = None
        self._auth_url = self.config["auth_url"]
        self._api_base_url = self.config["api_url"]
        self._last_response = None
        
        # Set default headers
        self._session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "curl/8.4.0",
        })
        
        # Get initial token
        self._authenticate()

    def _authenticate(self) -> None:
        """Authenticate with the Optiply API."""
        credentials = self.config["api_credentials"]
        
        try:
            data = {
                "username": credentials["username"],
                "password": credentials["password"]
            }
            
            authorization = f"Basic {credentials['basic_token']}"
            
            response = self._session.post(
                f"{self._auth_url}?grant_type=password",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Authorization": authorization,
                },
                data=data
            )
            response.raise_for_status()
            token_data = response.json()
            
            self._auth_token = token_data["access_token"]
            self._refresh_token = token_data["refresh_token"]
            self._token_type = token_data["token_type"]
            self._scope = token_data["scope"]
            self._token_expires_at = time.time() + token_data["expires_in"] - 60
            
            self._session.headers["Authorization"] = f"{self._token_type.capitalize()} {self._auth_token}"
            logger.info("Successfully authenticated with Optiply API")
        except requests.exceptions.RequestException as e:
            logger.error(f"Authentication failed: {str(e)}")
            raise Exception(f"Failed to authenticate with Optiply API: {str(e)}") from e

    def _refresh_token_if_needed(self) -> None:
        """Refresh the token if it's expired or about to expire."""
        current_time = time.time()
        if not self._auth_token or (self._token_expires_at and current_time >= self._token_expires_at):
            if self._refresh_token:
                try:
                    response = self._session.post(
                        f"{self._auth_url}?grant_type=refresh_token",
                        headers={
                            "Content-Type": "application/x-www-form-urlencoded",
                            "Authorization": self._authorization,
                        },
                        data=f"refresh_token={urllib.parse.quote(self._refresh_token)}",
                    )
                    response.raise_for_status()
                    token_data = response.json()
                    
                    self._auth_token = token_data["access_token"]
                    self._refresh_token = token_data.get("refresh_token", self._refresh_token)
                    self._token_type = token_data["token_type"]
                    self._scope = token_data["scope"]
                    self._token_expires_at = time.time() + token_data["expires_in"] - 60
                    
                    self._session.headers["Authorization"] = f"{self._token_type.capitalize()} {self._auth_token}"
                    logger.info("Successfully refreshed token")
                except requests.exceptions.RequestException as e:
                    logger.error(f"Token refresh failed: {str(e)}")
                    self._authenticate()
            else:
                self._authenticate()

    def _ensure_valid_token(self) -> None:
        """Ensure we have a valid token before making a request."""
        current_time = time.time()
        if not self._auth_token or (self._token_expires_at and current_time >= self._token_expires_at):
            self._refresh_token_if_needed()

    def _prepare_payload(self, record: dict, context: dict) -> dict:
        """Prepare the payload for the Optiply API."""
        # Build attributes dictionary with all possible fields
        attributes = {}
        
        # Add string fields only if they have non-empty values
        name = record.get("name", "")
        if name:
            attributes["name"] = name
            
        # Add email list if present
        emails = record.get("emails", [])
        if emails:
            attributes["emails"] = emails
            
        # Add type if present and not default
        supplier_type = record.get("type", "vendor")
        if supplier_type != "vendor":
            attributes["type"] = supplier_type
        
        # Add numeric fields only if they have non-zero values
        minimum_order_value = float(record.get("minimumOrderValue", 0.0))
        if minimum_order_value != 0.0:
            attributes["minimumOrderValue"] = minimum_order_value
            
        fixed_costs = float(record.get("fixedCosts", 0.0))
        if fixed_costs != 0.0:
            attributes["fixedCosts"] = fixed_costs
            
        delivery_time = int(record.get("deliveryTime", 0))
        if delivery_time != 0:
            attributes["deliveryTime"] = delivery_time
            
        user_replenishment_period = int(record.get("userReplenishmentPeriod", 0))
        if user_replenishment_period != 0:
            attributes["userReplenishmentPeriod"] = user_replenishment_period
            
        lost_sales_reaction = int(record.get("lostSalesReaction", 0))
        if lost_sales_reaction != 0:
            attributes["lostSalesReaction"] = lost_sales_reaction
            
        lost_sales_mov_reaction = int(record.get("lostSalesMovReaction", 0))
        if lost_sales_mov_reaction != 0:
            attributes["lostSalesMovReaction"] = lost_sales_mov_reaction
            
        backorder_threshold = int(record.get("backorderThreshold", 0))
        if backorder_threshold != 0:
            attributes["backorderThreshold"] = backorder_threshold
            
        backorders_reaction = int(record.get("backordersReaction", 0))
        if backorders_reaction != 0:
            attributes["backordersReaction"] = backorders_reaction
            
        max_load_capacity = int(record.get("maxLoadCapacity", 0))
        if max_load_capacity != 0:
            attributes["maxLoadCapacity"] = max_load_capacity
            
        container_volume = float(record.get("containerVolume", 0.0))
        if container_volume != 0.0:
            attributes["containerVolume"] = container_volume
        
        # Add boolean fields (include False values for PATCH)
        if "reactingToLostSales" in record:
            attributes["reactingToLostSales"] = bool(record["reactingToLostSales"])
        if "backorders" in record:
            attributes["backorders"] = bool(record["backorders"])

        return {
            "data": {
                "type": "suppliers",
                "attributes": attributes
            }
        }

    def process_record(self, record: dict, context: dict) -> None:
        """Process a record and write it to the Optiply API."""
        try:
            # Prepare the payload
            payload = self._prepare_payload(record, context)
            logger.info(f"Request payload: {json.dumps(payload, indent=2)}")

            # Make the request
            self._ensure_valid_token()
            url = f"{self._api_base_url}/suppliers"
            params = {
                "accountId": str(self.config["api_credentials"]["account_id"]),
                "couplingId": str(self.config["api_credentials"]["coupling_id"])
            }
            
            # Use the authenticated session with the correct headers
            self._session.headers.update({
                "Content-Type": "application/vnd.api+json",
                "Accept": "application/json",
                "User-Agent": "curl/8.4.0"
            })
            
            # Convert payload to JSON string using DateTimeEncoder
            payload_json = json.dumps(payload, cls=DateTimeEncoder)
            
            # Get HTTP method from context, default to POST if not specified
            http_method = context.get("http_method", "POST").upper()
            
            # Validate HTTP method
            if http_method not in ["POST", "PATCH"]:
                logger.error(f"Unsupported HTTP method: {http_method}. Only POST and PATCH are supported.")
                return
            
            # Validate remoteId for PATCH requests
            if http_method == "PATCH":
                remote_id = record.get("remoteId")
                if not remote_id:
                    logger.error("PATCH method requires a remoteId in the record")
                    return
                url = f"{url}/{remote_id}"
                logger.info(f"Using PATCH for record with remoteId: {remote_id}")
            else:
                logger.info("Using POST for record")
            
            # Make the request using the specified HTTP method
            if http_method == "PATCH":
                response = self._session.patch(
                    url,
                    params=params,
                    data=payload_json,
                    verify=False
                )
            else:
                response = self._session.post(
                    url,
                    params=params,
                    data=payload_json,
                    verify=False
                )
            
            self._last_response = response
            
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Response body: {response.text}")
            
            response.raise_for_status()

        except Exception as e:
            logger.error(f"Failed to process record: {str(e)}")
            return

class SupplierProductSink(RecordSink):
    """Optiply target sink class for supplier products."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the sink.

        Args:
            *args: Arguments to pass to the parent class.
            **kwargs: Keyword arguments to pass to the parent class.
        """
        super().__init__(*args, **kwargs)
        self._session = requests.Session()
        self._auth_token = None
        self._refresh_token = None
        self._token_expires_at = None
        self._token_type = None
        self._scope = None
        self._auth_url = self.config["auth_url"]
        self._api_base_url = self.config["api_url"]
        self._last_response = None
        
        # Set default headers
        self._session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "curl/8.4.0",
        })
        
        # Get initial token
        self._authenticate()

    def _authenticate(self) -> None:
        """Authenticate with the Optiply API."""
        credentials = self.config["api_credentials"]
        
        try:
            data = {
                "username": credentials["username"],
                "password": credentials["password"]
            }
            
            authorization = f"Basic {credentials['basic_token']}"
            
            response = self._session.post(
                f"{self._auth_url}?grant_type=password",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Authorization": authorization,
                },
                data=data
            )
            response.raise_for_status()
            token_data = response.json()
            
            self._auth_token = token_data["access_token"]
            self._refresh_token = token_data["refresh_token"]
            self._token_type = token_data["token_type"]
            self._scope = token_data["scope"]
            self._token_expires_at = time.time() + token_data["expires_in"] - 60
            
            self._session.headers["Authorization"] = f"{self._token_type.capitalize()} {self._auth_token}"
            logger.info("Successfully authenticated with Optiply API")
        except requests.exceptions.RequestException as e:
            logger.error(f"Authentication failed: {str(e)}")
            raise Exception(f"Failed to authenticate with Optiply API: {str(e)}") from e

    def _refresh_token_if_needed(self) -> None:
        """Refresh the token if it's expired or about to expire."""
        current_time = time.time()
        if not self._auth_token or (self._token_expires_at and current_time >= self._token_expires_at):
            if self._refresh_token:
                try:
                    response = self._session.post(
                        f"{self._auth_url}?grant_type=refresh_token",
                        headers={
                            "Content-Type": "application/x-www-form-urlencoded",
                            "Authorization": self._authorization,
                        },
                        data=f"refresh_token={urllib.parse.quote(self._refresh_token)}",
                    )
                    response.raise_for_status()
                    token_data = response.json()
                    
                    self._auth_token = token_data["access_token"]
                    self._refresh_token = token_data.get("refresh_token", self._refresh_token)
                    self._token_type = token_data["token_type"]
                    self._scope = token_data["scope"]
                    self._token_expires_at = time.time() + token_data["expires_in"] - 60
                    
                    self._session.headers["Authorization"] = f"{self._token_type.capitalize()} {self._auth_token}"
                    logger.info("Successfully refreshed token")
                except requests.exceptions.RequestException as e:
                    logger.error(f"Token refresh failed: {str(e)}")
                    self._authenticate()
            else:
                self._authenticate()

    def _ensure_valid_token(self) -> None:
        """Ensure we have a valid token before making a request."""
        current_time = time.time()
        if not self._auth_token or (self._token_expires_at and current_time >= self._token_expires_at):
            self._refresh_token_if_needed()

    def _prepare_payload(self, record: dict, context: dict) -> dict:
        """Prepare the payload for the Optiply API."""
        # Check mandatory fields only for POST requests
        http_method = context.get("http_method", "POST").upper()
        if http_method == "POST":
            mandatory_fields = ["name", "productId", "supplierId"]
            missing_fields = []
            for field in mandatory_fields:
                if field not in record or record[field] is None or (isinstance(record[field], str) and not record[field].strip()):
                    missing_fields.append(field)
            if missing_fields:
                logger.error(f"Record skipped due to missing mandatory fields: {', '.join(missing_fields)}")
                return None

        # Build attributes dictionary with all possible fields
        attributes = {}
        
        # Add fields only if they have values
        if "name" in record and record["name"]:
            attributes["name"] = record["name"]
        if "productId" in record:
            attributes["productId"] = int(record["productId"])
        if "supplierId" in record:
            attributes["supplierId"] = int(record["supplierId"])
            
        # Add optional fields only if they have non-zero values
        if "skuCode" in record and record["skuCode"]:
            attributes["skuCode"] = record["skuCode"]
            
        if "eanCode" in record and record["eanCode"]:
            attributes["eanCode"] = record["eanCode"]
            
        if "articleCode" in record and record["articleCode"]:
            attributes["articleCode"] = record["articleCode"]
            
        if "price" in record and float(record.get("price", 0)) != 0:
            attributes["price"] = float(record["price"])
            
        if "minimumPurchaseQuantity" in record and int(record.get("minimumPurchaseQuantity", 0)) != 0:
            attributes["minimumPurchaseQuantity"] = int(record["minimumPurchaseQuantity"])
            
        if "lotSize" in record and int(record.get("lotSize", 0)) != 0:
            attributes["lotSize"] = int(record["lotSize"])
            
        if "availability" in record and record["availability"]:
            attributes["availability"] = record["availability"]
            
        if "availabilityDate" in record and record["availabilityDate"]:
            attributes["availabilityDate"] = record["availabilityDate"]
            
        if "preferred" in record:
            attributes["preferred"] = bool(record["preferred"])
            
        if "deliveryTime" in record and record["deliveryTime"]:
            attributes["deliveryTime"] = record["deliveryTime"]
            
        if "status" in record and record["status"]:
            attributes["status"] = record["status"]
            
        if "freeStock" in record and int(record.get("freeStock", 0)) != 0:
            attributes["freeStock"] = int(record["freeStock"])
            
        if "weight" in record and float(record.get("weight", 0)) != 0:
            attributes["weight"] = float(record["weight"])
            
        if "volume" in record and float(record.get("volume", 0)) != 0:
            attributes["volume"] = float(record["volume"])

        return {
            "data": {
                "type": "supplierProducts",
                "attributes": attributes
            }
        }

    def process_record(self, record: dict, context: dict) -> None:
        """Process a record and write it to the Optiply API."""
        try:
            # Prepare the payload
            payload = self._prepare_payload(record, context)
            if payload is None:  # If payload preparation failed due to missing mandatory fields
                # Get the missing fields from the last error message
                missing_fields = []
                for field in ["name", "productId", "supplierId"]:
                    if field not in record or record[field] is None or (isinstance(record[field], str) and not record[field].strip()):
                        missing_fields.append(field)
                logger.error(f"Record skipped due to missing mandatory fields: {', '.join(missing_fields)}")
                return

            logger.info(f"Request payload: {json.dumps(payload, indent=2)}")

            # Make the request
            self._ensure_valid_token()
            url = f"{self._api_base_url}/supplierProducts"
            params = {
                "accountId": str(self.config["api_credentials"]["account_id"]),
                "couplingId": str(self.config["api_credentials"]["coupling_id"])
            }
            
            # Use the authenticated session with the correct headers
            self._session.headers.update({
                "Content-Type": "application/vnd.api+json",
                "Accept": "application/json",
                "User-Agent": "curl/8.4.0"
            })
            
            # Convert payload to JSON string using DateTimeEncoder
            payload_json = json.dumps(payload, cls=DateTimeEncoder)
            
            # Get HTTP method from context, default to POST if not specified
            http_method = context.get("http_method", "POST").upper()
            
            # Validate HTTP method
            if http_method not in ["POST", "PATCH"]:
                logger.error(f"Unsupported HTTP method: {http_method}. Only POST and PATCH are supported.")
                return
            
            # Validate remoteId for PATCH requests
            if http_method == "PATCH":
                remote_id = record.get("remoteId")
                if not remote_id:
                    logger.error("PATCH method requires a remoteId in the record")
                    return
                url = f"{url}/{remote_id}"
                logger.info(f"Using PATCH for record with remoteId: {remote_id}")
            else:
                logger.info("Using POST for record")
            
            # Make the request using the specified HTTP method
            if http_method == "PATCH":
                response = self._session.patch(
                    url,
                    params=params,
                    data=payload_json,
                    verify=False
                )
            else:
                response = self._session.post(
                    url,
                    params=params,
                    data=payload_json,
                    verify=False
                )
            
            self._last_response = response
            
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Response body: {response.text}")
            
            response.raise_for_status()

        except Exception as e:
            logger.error(f"Failed to process record: {str(e)}")
            return

class BuyOrderLineSink(RecordSink):
    """Optiply target sink class for buy order lines."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the sink.

        Args:
            *args: Arguments to pass to the parent class.
            **kwargs: Keyword arguments to pass to the parent class.
        """
        super().__init__(*args, **kwargs)
        self._session = requests.Session()
        self._auth_token = None
        self._refresh_token = None
        self._token_expires_at = None
        self._token_type = None
        self._scope = None
        self._auth_url = self.config["auth_url"]
        self._api_base_url = self.config["api_url"]
        self._last_response = None
        
        # Set default headers
        self._session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "curl/8.4.0",
        })
        
        # Get initial token
        self._authenticate()

    def _authenticate(self) -> None:
        """Authenticate with the Optiply API."""
        credentials = self.config["api_credentials"]
        
        try:
            data = {
                "username": credentials["username"],
                "password": credentials["password"]
            }
            
            authorization = f"Basic {credentials['basic_token']}"
            
            response = self._session.post(
                f"{self._auth_url}?grant_type=password",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Authorization": authorization,
                },
                data=data
            )
            response.raise_for_status()
            token_data = response.json()
            
            self._auth_token = token_data["access_token"]
            self._refresh_token = token_data["refresh_token"]
            self._token_type = token_data["token_type"]
            self._scope = token_data["scope"]
            self._token_expires_at = time.time() + token_data["expires_in"] - 60
            
            self._session.headers["Authorization"] = f"{self._token_type.capitalize()} {self._auth_token}"
            logger.info("Successfully authenticated with Optiply API")
        except requests.exceptions.RequestException as e:
            logger.error(f"Authentication failed: {str(e)}")
            raise Exception(f"Failed to authenticate with Optiply API: {str(e)}") from e

    def _refresh_token_if_needed(self) -> None:
        """Refresh the token if it's expired or about to expire."""
        current_time = time.time()
        if not self._auth_token or (self._token_expires_at and current_time >= self._token_expires_at):
            if self._refresh_token:
                try:
                    response = self._session.post(
                        f"{self._auth_url}?grant_type=refresh_token",
                        headers={
                            "Content-Type": "application/x-www-form-urlencoded",
                            "Authorization": self._authorization,
                        },
                        data=f"refresh_token={urllib.parse.quote(self._refresh_token)}",
                    )
                    response.raise_for_status()
                    token_data = response.json()
                    
                    self._auth_token = token_data["access_token"]
                    self._refresh_token = token_data.get("refresh_token", self._refresh_token)
                    self._token_type = token_data["token_type"]
                    self._scope = token_data["scope"]
                    self._token_expires_at = time.time() + token_data["expires_in"] - 60
                    
                    self._session.headers["Authorization"] = f"{self._token_type.capitalize()} {self._auth_token}"
                    logger.info("Successfully refreshed token")
                except requests.exceptions.RequestException as e:
                    logger.error(f"Token refresh failed: {str(e)}")
                    self._authenticate()
            else:
                self._authenticate()

    def _ensure_valid_token(self) -> None:
        """Ensure we have a valid token before making a request."""
        current_time = time.time()
        if not self._auth_token or (self._token_expires_at and current_time >= self._token_expires_at):
            self._refresh_token_if_needed()

    def _prepare_payload(self, record: dict, context: dict) -> dict:
        """Prepare the payload for the Optiply API."""
        # Check mandatory fields only for POST requests
        http_method = context.get("http_method", "POST").upper()
        if http_method == "POST":
            mandatory_fields = ["buyOrderId", "quantity", "productId", "subtotalValue"]
            missing_fields = []
            for field in mandatory_fields:
                if field not in record or record[field] is None or (isinstance(record[field], str) and not record[field].strip()):
                    missing_fields.append(field)
            if missing_fields:
                logger.error(f"Record skipped due to missing mandatory fields: {', '.join(missing_fields)}")
                return None

        # Build attributes dictionary with all possible fields
        attributes = {}
        
        # Add fields only if they have values
        if "buyOrderId" in record:
            attributes["buyOrderId"] = int(record["buyOrderId"])
        if "quantity" in record:
            attributes["quantity"] = int(record["quantity"])
        if "productId" in record:
            attributes["productId"] = int(record["productId"])
        if "subtotalValue" in record:
            attributes["subtotalValue"] = float(record["subtotalValue"])

        return {
            "data": {
                "type": "buyOrderLines",
                "attributes": attributes
            }
        }

    def process_record(self, record: dict, context: dict) -> None:
        """Process a record and write it to the Optiply API."""
        try:
            # Prepare the payload
            payload = self._prepare_payload(record, context)
            if payload is None:  # If payload preparation failed due to missing mandatory fields
                # Get the missing fields from the last error message
                missing_fields = []
                for field in ["buyOrderId", "quantity", "productId", "subtotalValue"]:
                    if field not in record or record[field] is None or (isinstance(record[field], str) and not record[field].strip()):
                        missing_fields.append(field)
                logger.error(f"Record skipped due to missing mandatory fields: {', '.join(missing_fields)}")
                return

            logger.info(f"Request payload: {json.dumps(payload, indent=2)}")

            # Make the request
            self._ensure_valid_token()
            url = f"{self._api_base_url}/buyOrderLines"
            params = {
                "accountId": str(self.config["api_credentials"]["account_id"]),
                "couplingId": str(self.config["api_credentials"]["coupling_id"])
            }
            
            # Use the authenticated session with the correct headers
            self._session.headers.update({
                "Content-Type": "application/vnd.api+json",
                "Accept": "application/json",
                "User-Agent": "curl/8.4.0"
            })
            
            # Convert payload to JSON string using DateTimeEncoder
            payload_json = json.dumps(payload, cls=DateTimeEncoder)
            
            # Get HTTP method from context, default to POST if not specified
            http_method = context.get("http_method", "POST").upper()
            
            # Validate HTTP method
            if http_method not in ["POST", "PATCH"]:
                logger.error(f"Unsupported HTTP method: {http_method}. Only POST and PATCH are supported.")
                return
            
            # Validate remoteId for PATCH requests
            if http_method == "PATCH":
                remote_id = record.get("remoteId")
                if not remote_id:
                    logger.error("PATCH method requires a remoteId in the record")
                    return
                url = f"{url}/{remote_id}"
                logger.info(f"Using PATCH for record with remoteId: {remote_id}")
            else:
                logger.info("Using POST for record")
            
            # Make the request using the specified HTTP method
            if http_method == "PATCH":
                response = self._session.patch(
                    url,
                    params=params,
                    data=payload_json,
                    verify=False
                )
            else:
                response = self._session.post(
                    url,
                    params=params,
                    data=payload_json,
                    verify=False
                )
            
            self._last_response = response
            
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Response body: {response.text}")
            
            response.raise_for_status()

        except Exception as e:
            logger.error(f"Failed to process record: {str(e)}")
            return

class SellOrderSink(RecordSink):
    """Optiply target sink class for sell orders."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the sink.

        Args:
            *args: Arguments to pass to the parent class.
            **kwargs: Keyword arguments to pass to the parent class.
        """
        super().__init__(*args, **kwargs)
        self._session = requests.Session()
        self._auth_token = None
        self._refresh_token = None
        self._token_expires_at = None
        self._token_type = None
        self._scope = None
        self._auth_url = self.config["auth_url"]
        self._api_base_url = self.config["api_url"]
        self._last_response = None
        
        # Set default headers
        self._session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "curl/8.4.0",
        })
        
        # Get initial token
        self._authenticate()

    def _authenticate(self) -> None:
        """Authenticate with the Optiply API."""
        credentials = self.config["api_credentials"]
        
        try:
            data = {
                "username": credentials["username"],
                "password": credentials["password"]
            }
            
            authorization = f"Basic {credentials['basic_token']}"
            
            response = self._session.post(
                f"{self._auth_url}?grant_type=password",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Authorization": authorization,
                },
                data=data
            )
            response.raise_for_status()
            token_data = response.json()
            
            self._auth_token = token_data["access_token"]
            self._refresh_token = token_data["refresh_token"]
            self._token_type = token_data["token_type"]
            self._scope = token_data["scope"]
            self._token_expires_at = time.time() + token_data["expires_in"] - 60
            
            self._session.headers["Authorization"] = f"{self._token_type.capitalize()} {self._auth_token}"
            logger.info("Successfully authenticated with Optiply API")
        except requests.exceptions.RequestException as e:
            logger.error(f"Authentication failed: {str(e)}")
            raise Exception(f"Failed to authenticate with Optiply API: {str(e)}") from e

    def _refresh_token_if_needed(self) -> None:
        """Refresh the token if it's expired or about to expire."""
        current_time = time.time()
        if not self._auth_token or (self._token_expires_at and current_time >= self._token_expires_at):
            if self._refresh_token:
                try:
                    response = self._session.post(
                        f"{self._auth_url}?grant_type=refresh_token",
                        headers={
                            "Content-Type": "application/x-www-form-urlencoded",
                            "Authorization": self._authorization,
                        },
                        data=f"refresh_token={urllib.parse.quote(self._refresh_token)}",
                    )
                    response.raise_for_status()
                    token_data = response.json()
                    
                    self._auth_token = token_data["access_token"]
                    self._refresh_token = token_data.get("refresh_token", self._refresh_token)
                    self._token_type = token_data["token_type"]
                    self._scope = token_data["scope"]
                    self._token_expires_at = time.time() + token_data["expires_in"] - 60
                    
                    self._session.headers["Authorization"] = f"{self._token_type.capitalize()} {self._auth_token}"
                    logger.info("Successfully refreshed token")
                except requests.exceptions.RequestException as e:
                    logger.error(f"Token refresh failed: {str(e)}")
                    self._authenticate()
            else:
                self._authenticate()

    def _ensure_valid_token(self) -> None:
        """Ensure we have a valid token before making a request."""
        current_time = time.time()
        if not self._auth_token or (self._token_expires_at and current_time >= self._token_expires_at):
            self._refresh_token_if_needed()

    def _prepare_payload(self, record: dict, context: dict) -> dict:
        """Prepare the payload for the Optiply API."""
        # Check mandatory fields only for POST requests
        http_method = context.get("http_method", "POST").upper()
        if http_method == "POST":
            mandatory_fields = ["placed", "totalValue"]
            missing_fields = []
            for field in mandatory_fields:
                if field not in record or record[field] is None or (isinstance(record[field], str) and not record[field].strip()):
                    missing_fields.append(field)
            if missing_fields:
                logger.error(f"Record skipped due to missing mandatory fields: {', '.join(missing_fields)}")
                return None

        # Build attributes dictionary with all possible fields
        attributes = {}
        
        # Add fields only if they have values
        if "placed" in record:
            placed = record["placed"]
            if isinstance(placed, datetime):
                placed = placed.isoformat()
            if not placed.endswith('Z'):
                placed = placed.replace('+00:00', '') + 'Z'
            attributes["placed"] = placed
            
        if "totalValue" in record:
            attributes["totalValue"] = float(record["totalValue"])
        
        # Handle dates
        if "completed" in record:
            completed_date = record["completed"]
            if isinstance(completed_date, datetime):
                completed_date = completed_date.isoformat()
            if not completed_date.endswith('Z'):
                completed_date = completed_date.replace('+00:00', '') + 'Z'
            attributes["completed"] = completed_date

        # Handle line items for POST requests
        if "orderLines" in record:
            order_lines = []
            for item in record["orderLines"]:
                order_line = {
                    "quantity": int(item.get("quantity", 0)),
                    "subtotalValue": float(item.get("subtotalValue", 0)),
                    "productId": int(item.get("productId", 0))
                }
                # Only add non-zero values
                order_line = {k: v for k, v in order_line.items() if v != 0}
                if order_line:  # Only add if there are non-zero values
                    order_lines.append(order_line)
            
            if order_lines:
                attributes["orderLines"] = order_lines

        return {
            "data": {
                "type": "sellOrders",
                "attributes": attributes
            }
        }

    def process_record(self, record: dict, context: dict) -> None:
        """Process a record and write it to the Optiply API."""
        try:
            # Prepare the payload
            payload = self._prepare_payload(record, context)
            if payload is None:  # If payload preparation failed due to missing mandatory fields
                # Get the missing fields from the last error message
                missing_fields = []
                for field in ["placed", "totalValue"]:
                    if field not in record or record[field] is None or (isinstance(record[field], str) and not record[field].strip()):
                        missing_fields.append(field)
                logger.error(f"Record skipped due to missing mandatory fields: {', '.join(missing_fields)}")
                return

            logger.info(f"Request payload: {json.dumps(payload, indent=2)}")

            # Make the request
            self._ensure_valid_token()
            url = f"{self._api_base_url}/sellOrders"
            params = {
                "accountId": str(self.config["api_credentials"]["account_id"]),
                "couplingId": str(self.config["api_credentials"]["coupling_id"])
            }
            
            # Use the authenticated session with the correct headers
            self._session.headers.update({
                "Content-Type": "application/vnd.api+json",
                "Accept": "application/json",
                "User-Agent": "curl/8.4.0"
            })
            
            # Convert payload to JSON string using DateTimeEncoder
            payload_json = json.dumps(payload, cls=DateTimeEncoder)
            
            # Get HTTP method from context, default to POST if not specified
            http_method = context.get("http_method", "POST").upper()
            
            # Validate HTTP method
            if http_method not in ["POST", "PATCH"]:
                logger.error(f"Unsupported HTTP method: {http_method}. Only POST and PATCH are supported.")
                return
            
            # Validate remoteId for PATCH requests
            if http_method == "PATCH":
                remote_id = record.get("remoteId")
                if not remote_id:
                    logger.error("PATCH method requires a remoteId in the record")
                    return
                url = f"{url}/{remote_id}"
                logger.info(f"Using PATCH for record with remoteId: {remote_id}")
            else:
                logger.info("Using POST for record")
            
            # Make the request using the specified HTTP method
            if http_method == "PATCH":
                response = self._session.patch(
                    url,
                    params=params,
                    data=payload_json,
                    verify=False
                )
            else:
                response = self._session.post(
                    url,
                    params=params,
                    data=payload_json,
                    verify=False
                )
            
            self._last_response = response
            
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Response body: {response.text}")
            
            response.raise_for_status()

        except Exception as e:
            logger.error(f"Failed to process record: {str(e)}")
            return

class SellOrderLineSink(RecordSink):
    """Optiply target sink class for sell order lines."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the sink.

        Args:
            *args: Arguments to pass to the parent class.
            **kwargs: Keyword arguments to pass to the parent class.
        """
        super().__init__(*args, **kwargs)
        self._session = requests.Session()
        self._auth_token = None
        self._refresh_token = None
        self._token_expires_at = None
        self._token_type = None
        self._scope = None
        self._auth_url = self.config["auth_url"]
        self._api_base_url = self.config["api_url"]
        self._last_response = None
        
        # Set default headers
        self._session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "curl/8.4.0",
        })
        
        # Get initial token
        self._authenticate()

    def _authenticate(self) -> None:
        """Authenticate with the Optiply API."""
        credentials = self.config["api_credentials"]
        
        try:
            data = {
                "username": credentials["username"],
                "password": credentials["password"]
            }
            
            authorization = f"Basic {credentials['basic_token']}"
            
            response = self._session.post(
                f"{self._auth_url}?grant_type=password",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Authorization": authorization,
                },
                data=data
            )
            response.raise_for_status()
            token_data = response.json()
            
            self._auth_token = token_data["access_token"]
            self._refresh_token = token_data["refresh_token"]
            self._token_type = token_data["token_type"]
            self._scope = token_data["scope"]
            self._token_expires_at = time.time() + token_data["expires_in"] - 60
            
            self._session.headers["Authorization"] = f"{self._token_type.capitalize()} {self._auth_token}"
            logger.info("Successfully authenticated with Optiply API")
        except requests.exceptions.RequestException as e:
            logger.error(f"Authentication failed: {str(e)}")
            raise Exception(f"Failed to authenticate with Optiply API: {str(e)}") from e

    def _refresh_token_if_needed(self) -> None:
        """Refresh the token if it's expired or about to expire."""
        current_time = time.time()
        if not self._auth_token or (self._token_expires_at and current_time >= self._token_expires_at):
            if self._refresh_token:
                try:
                    response = self._session.post(
                        f"{self._auth_url}?grant_type=refresh_token",
                        headers={
                            "Content-Type": "application/x-www-form-urlencoded",
                            "Authorization": self._authorization,
                        },
                        data=f"refresh_token={urllib.parse.quote(self._refresh_token)}",
                    )
                    response.raise_for_status()
                    token_data = response.json()
                    
                    self._auth_token = token_data["access_token"]
                    self._refresh_token = token_data.get("refresh_token", self._refresh_token)
                    self._token_type = token_data["token_type"]
                    self._scope = token_data["scope"]
                    self._token_expires_at = time.time() + token_data["expires_in"] - 60
                    
                    self._session.headers["Authorization"] = f"{self._token_type.capitalize()} {self._auth_token}"
                    logger.info("Successfully refreshed token")
                except requests.exceptions.RequestException as e:
                    logger.error(f"Token refresh failed: {str(e)}")
                    self._authenticate()
            else:
                self._authenticate()

    def _ensure_valid_token(self) -> None:
        """Ensure we have a valid token before making a request."""
        current_time = time.time()
        if not self._auth_token or (self._token_expires_at and current_time >= self._token_expires_at):
            self._refresh_token_if_needed()

    def _prepare_payload(self, record: dict, context: dict) -> dict:
        """Prepare the payload for the Optiply API."""
        # Check mandatory fields only for POST requests
        http_method = context.get("http_method", "POST").upper()
        if http_method == "POST":
            mandatory_fields = ["quantity", "productId", "sellOrderId", "subtotalValue"]
            missing_fields = []
            for field in mandatory_fields:
                if field not in record or record[field] is None or (isinstance(record[field], str) and not record[field].strip()):
                    missing_fields.append(field)
            if missing_fields:
                logger.error(f"Record skipped due to missing mandatory fields: {', '.join(missing_fields)}")
                return None

        # Build attributes dictionary with all possible fields
        attributes = {}
        
        # Add fields only if they have values
        if "quantity" in record:
            attributes["quantity"] = int(record["quantity"])
        if "productId" in record:
            attributes["productId"] = int(record["productId"])
        if "sellOrderId" in record:
            attributes["sellOrderId"] = int(record["sellOrderId"])
        if "subtotalValue" in record:
            attributes["subtotalValue"] = float(record["subtotalValue"])

        return {
            "data": {
                "type": "sellOrderLines",
                "attributes": attributes
            }
        }

    def process_record(self, record: dict, context: dict) -> None:
        """Process a record and write it to the Optiply API."""
        try:
            # Prepare the payload
            payload = self._prepare_payload(record, context)
            if payload is None:  # If payload preparation failed due to missing mandatory fields
                # Get the missing fields from the last error message
                missing_fields = []
                for field in ["quantity", "productId", "sellOrderId", "subtotalValue"]:
                    if field not in record or record[field] is None or (isinstance(record[field], str) and not record[field].strip()):
                        missing_fields.append(field)
                logger.error(f"Record skipped due to missing mandatory fields: {', '.join(missing_fields)}")
                return

            logger.info(f"Request payload: {json.dumps(payload, indent=2)}")

            # Make the request
            self._ensure_valid_token()
            url = f"{self._api_base_url}/sellOrderLines"
            params = {
                "accountId": str(self.config["api_credentials"]["account_id"]),
                "couplingId": str(self.config["api_credentials"]["coupling_id"])
            }
            
            # Use the authenticated session with the correct headers
            self._session.headers.update({
                "Content-Type": "application/vnd.api+json",
                "Accept": "application/json",
                "User-Agent": "curl/8.4.0"
            })
            
            # Convert payload to JSON string using DateTimeEncoder
            payload_json = json.dumps(payload, cls=DateTimeEncoder)
            
            # Get HTTP method from context, default to POST if not specified
            http_method = context.get("http_method", "POST").upper()
            
            # Validate HTTP method
            if http_method not in ["POST", "PATCH"]:
                logger.error(f"Unsupported HTTP method: {http_method}. Only POST and PATCH are supported.")
                return
            
            # Validate remoteId for PATCH requests
            if http_method == "PATCH":
                remote_id = record.get("remoteId")
                if not remote_id:
                    logger.error("PATCH method requires a remoteId in the record")
                    return
                url = f"{url}/{remote_id}"
                logger.info(f"Using PATCH for record with remoteId: {remote_id}")
            else:
                logger.info("Using POST for record")
            
            # Make the request using the specified HTTP method
            if http_method == "PATCH":
                response = self._session.patch(
                    url,
                    params=params,
                    data=payload_json,
                    verify=False
                )
            else:
                response = self._session.post(
                    url,
                    params=params,
                    data=payload_json,
                    verify=False
                )
            
            self._last_response = response
            
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Response body: {response.text}")
            
            response.raise_for_status()

        except Exception as e:
            logger.error(f"Failed to process record: {str(e)}")
            return