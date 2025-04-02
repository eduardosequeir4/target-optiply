"""Optiply target sink class, which handles writing streams."""

from __future__ import annotations

import backoff
import json
import logging
import time
import urllib.parse
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.sinks import RecordSink
import singer_sdk.typing as th

from target_optiply.auth import OptiplyAuthenticator
from target_optiply.client import OptiplySink

logger = logging.getLogger(__name__)

class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder for datetime objects."""

    def default(self, obj):
        """Encode datetime objects."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class BaseOptiplySink(OptiplySink):
    """Base sink for Optiply streams."""

    def __init__(self, target: str, stream_name: str, schema: Dict, key_properties: List[str]):
        super().__init__(target, stream_name, schema, key_properties)
        self.endpoint = self.stream_name.lower()

    def get_url(
        self,
        context: Optional[dict] = None,
    ) -> str:
        """Get the stream's API URL."""
        base_url = self.url().split('?')[0].rstrip('/')  # Remove query params and trailing slash
        endpoint = self.endpoint
        
        # Extract query parameters from the original URL
        url_parts = self.url().split('?')
        query_params = {}
        if len(url_parts) > 1:
            query_params = dict(param.split('=') for param in url_parts[1].split('&'))

        # Construct the base URL with endpoint
        if context and context.get("http_method") == "PATCH":
            record_id = context.get("record", {}).get("id")
            if record_id:
                url = f"{base_url}/{endpoint}/{record_id}"
            else:
                url = f"{base_url}/{endpoint}"
        else:
            url = f"{base_url}/{endpoint}"

        # Add query parameters if they exist
        if query_params:
            query_string = "&".join(f"{k}={v}" for k, v in query_params.items())
            url = f"{url}?{query_string}"

        return url

    def _prepare_payload(
        self,
        context: Optional[dict] = None,
        record: Optional[dict] = None,
    ) -> dict:
        """Prepare the payload for the API request."""
        if context and context.get("http_method") == "PATCH":
            # For PATCH requests, only include the fields that are being updated
            attributes = {}
            if "expectedDeliveryDate" in record:
                attributes["expectedDeliveryDate"] = record["expectedDeliveryDate"]
            return {
                "data": {
                    "type": self.stream_name,
                    "attributes": attributes
                }
            }
        else:
            # For POST requests, include all fields
            return {
                "data": {
                    "type": self.stream_name,
                    "attributes": record
                }
            }

    def process_record(self, record: Dict, context: Dict = None) -> None:
        """Process a record."""
        # Create context if not provided
        if context is None:
            context = {}

        # Set http_method based on presence of id field
        http_method = "PATCH" if "id" in record else "POST"
        context["http_method"] = http_method
        context["record"] = record  # Add record to context for URL construction
        self.logger.info(f"HTTP Method from context: {http_method}")
        self.logger.info(f"Context: {context}")
        self.logger.info(f"Record: {record}")

        # For POST requests, check mandatory fields
        if http_method == "POST":
            mandatory_fields = self.get_mandatory_fields()
            missing_fields = []
            for field in mandatory_fields:
                if field not in record or record[field] is None or (isinstance(record[field], str) and not record[field].strip()):
                    missing_fields.append(field)
            if missing_fields:
                self.logger.error(f"Record skipped due to missing mandatory fields: {', '.join(missing_fields)}")
                return None

        # Prepare the payload
        payload = self._prepare_payload(context, record)
        if not payload:
            return

        # Get the URL for the request
        url = self.get_url(context)
        self.logger.info(f"Request URL: {url}")

        # Make the request
        response = requests.request(
            method=http_method,
            url=url,
            headers={**self.http_headers(), **self.authenticator.auth_headers},
            json=payload
        )
        self.logger.info(f"Response status: {response.status_code}")
        self.logger.info(f"Response body: {response.text}")

        # Validate the response
        if response.status_code >= 400:
            raise FatalAPIError(f"Client error: {response.text}")

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink."""
        return []

class ProductSink(BaseOptiplySink):
    """Optiply target sink class for products."""

    def __init__(self, target: str, stream_name: str, schema: Dict, key_properties: List[str]):
        super().__init__(target, stream_name, schema, key_properties)
        self.endpoint = "products"  # Case-sensitive endpoint

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["name", "stockLevel", "unlimitedStock"]

    def get_field_mappings(self) -> Dict[str, str]:
        """Get the field mappings for this sink.

        Returns:
            The field mappings dictionary.
        """
        return {
            "name": "name",
            "skuCode": "skuCode",
            "price": "price",
            "stockLevel": "stockLevel",
            "unlimitedStock": "unlimitedStock",
            "status": "status",
            "remoteDataSyncedToDate": "remoteDataSyncedToDate"
        }

    def _prepare_payload(self, record: Dict, context: Dict) -> Dict:
        """Prepare the payload for the Optiply API."""
        # Get http_method from context
        http_method = context.get("http_method", "POST").upper()
        self.logger.info(f"HTTP Method from context: {http_method}")
        self.logger.info(f"Context: {context}")
        self.logger.info(f"Record: {record}")

        # For POST requests, check mandatory fields
        if http_method == "POST":
            mandatory_fields = self.get_mandatory_fields()
            missing_fields = []
            for field in mandatory_fields:
                if field not in record or record[field] is None or (isinstance(record[field], str) and not record[field].strip()):
                    missing_fields.append(field)
            if missing_fields:
                self.logger.error(f"Record skipped due to missing mandatory fields: {', '.join(missing_fields)}")
                return None

        # Build attributes using field mappings
        attributes = self.build_attributes(record, self.get_field_mappings())

        return {
            "data": {
                "type": self.get_endpoint(),
                "attributes": attributes
            }
        }

class SupplierSink(BaseOptiplySink):
    """Optiply target sink class for suppliers."""

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["name"]

    def get_field_mappings(self) -> Dict[str, str]:
        """Get the field mappings for this sink.

        Returns:
            The field mappings dictionary.
        """
        return {
            "name": "name",
            "remoteId": "remoteId",
            "leadTime": "leadTime",
            "minimumOrderValue": "minimumOrderValue",
            "orderCosts": "orderCosts",
            "status": "status"
        }

class SupplierProductSink(BaseOptiplySink):
    """Optiply target sink class for supplier products."""

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["supplierId", "productId"]

    def get_field_mappings(self) -> Dict[str, str]:
        """Get the field mappings for this sink.

        Returns:
            The field mappings dictionary.
        """
        return {
            "supplierId": "supplierId",
            "productId": "productId",
            "remoteId": "remoteId",
            "price": "price",
            "minimumPurchaseQuantity": "minimumPurchaseQuantity",
            "lotSize": "lotSize",
            "availability": "availability",
            "availabilityDate": "availabilityDate",
            "preferred": "preferred",
            "deliveryTime": "deliveryTime",
            "status": "status"
        }

class BuyOrderSink(BaseOptiplySink):
    """BuyOrder sink class."""

    def __init__(self, target: str, stream_name: str, schema: Dict, key_properties: List[str]):
        super().__init__(target, stream_name, schema, key_properties)
        self.endpoint = "buyOrders"  # Case-sensitive endpoint

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink."""
        return ["transaction_date", "supplier_remoteId", "line_items"]

    def _prepare_payload(self, context: dict, record: dict) -> dict:
        """Prepare the payload for the API request."""
        self.logger.info("Preparing payload for BuyOrders")
        self.logger.info(f"Context: {context}")
        self.logger.info(f"Record: {record}")

        # Get the HTTP method from context
        http_method = context.get("http_method", "POST")
        self.logger.info(f"HTTP Method from context: {http_method}")

        # For PATCH requests, only include the fields that are present in the record
        if http_method == "PATCH":
            attributes = {}
            if "expectedDeliveryDate" in record:
                attributes["expectedDeliveryDate"] = record["expectedDeliveryDate"].isoformat()
            return {
                "data": {
                    "type": "buyOrders",
                    "attributes": attributes
                }
            }

        # Get accountId and couplingId from URL parameters
        url_parts = self.url().split('?')
        if len(url_parts) > 1:
            params = dict(param.split('=') for param in url_parts[1].split('&'))
            account_id = int(params.get('accountId'))
            coupling_id = int(params.get('couplingId'))
        else:
            raise ValueError("accountId and couplingId are required in the URL parameters")

        # For POST requests, build attributes only with present fields
        attributes = {
            "createdFromPublicApi": True,
            "accountId": account_id,
            "couplingId": coupling_id
        }

        # Add placed date if present
        if "transaction_date" in record:
            attributes["placed"] = record["transaction_date"] if isinstance(record["transaction_date"], str) else record["transaction_date"].isoformat()

        # Add supplierId if present
        if "supplier_remoteId" in record:
            attributes["supplierId"] = int(record["supplier_remoteId"])

        # Parse line items if present
        if "line_items" in record:
            line_items = json.loads(record["line_items"])
            buy_order_lines = []
            total_value = 0
            for item in line_items:
                subtotal_value = float(item["subtotalValue"])
                total_value += subtotal_value
                buy_order_lines.append({
                    "type": "buyOrderLines",
                    "attributes": {
                        "quantity": item["quantity"],
                        "subtotalValue": str(subtotal_value),
                        "productId": item["productId"]
                    }
                })
            attributes["totalValue"] = str(total_value)

            # Create the payload with included buyOrderLines
            return {
                "data": {
                    "type": "buyOrders",
                    "attributes": attributes
                },
                "included": buy_order_lines
            }

        # If no line items, return just the data without included
        return {
            "data": {
                "type": "buyOrders",
                "attributes": attributes
            }
        }

class BuyOrderLineSink(BaseOptiplySink):
    """Optiply target sink class for buy order lines."""

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["buyOrderId", "productId", "quantity", "price"]

    def get_field_mappings(self) -> Dict[str, str]:
        """Get the field mappings for this sink.

        Returns:
            The field mappings dictionary.
        """
        return {
            "buyOrderId": "buyOrderId",
            "productId": "productId",
            "quantity": "quantity",
            "price": "price",
            "status": "status"
        }

class SellOrderSink(BaseOptiplySink):
    """Optiply target sink class for sell orders."""

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["placed", "totalValue"]

    def get_field_mappings(self) -> Dict[str, str]:
        """Get the field mappings for this sink.

        Returns:
            The field mappings dictionary.
        """
        return {
            "placed": "placed",
            "totalValue": "totalValue",
            "remoteId": "remoteId",
            "completed": "completed",
            "status": "status"
        }

class SellOrderLineSink(BaseOptiplySink):
    """Optiply target sink class for sell order lines."""

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["sellOrderId", "productId", "quantity", "price"]

    def get_field_mappings(self) -> Dict[str, str]:
        """Get the field mappings for this sink.

        Returns:
            The field mappings dictionary.
        """
        return {
            "sellOrderId": "sellOrderId",
            "productId": "productId",
            "quantity": "quantity",
            "price": "price",
            "status": "status"
        }