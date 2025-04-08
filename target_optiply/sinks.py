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
        endpoint = self.endpoint
        
        # Extract query parameters from the original URL
        url_parts = self.url().split('?')
        query_params = {}
        if len(url_parts) > 1:
            query_params = dict(param.split('=') for param in url_parts[1].split('&'))

        # Construct the base URL with endpoint
        if context and context.get("http_method") == "PATCH":
            record = context.get("record", {})
            record_id = record.get("id")
            if record_id:
                url = f"{self.base_url}/{endpoint}/{record_id}"
            else:
                url = f"{self.base_url}/{endpoint}"
        else:
            url = f"{self.base_url}/{endpoint}"

        # Add query parameters if they exist
        if query_params:
            query_string = "&".join(f"{k}={v}" for k, v in query_params.items())
            url = f"{url}?{query_string}"

        return url

    def get_field_mappings(self) -> Dict[str, str]:
        """Get the field mappings for this sink.

        Returns:
            The field mappings dictionary.
        """
        return {}

    def build_attributes(self, record: Dict, field_mappings: Dict[str, str]) -> Dict:
        """Build attributes dictionary from record using field mappings.

        Args:
            record: The record to transform
            field_mappings: Dictionary mapping record fields to API fields

        Returns:
            Dictionary of attributes for the API request
        """
        attributes = {}
        for record_field, api_field in field_mappings.items():
            if record_field in record and record[record_field] is not None:
                value = record[record_field]
                # Handle datetime objects
                if isinstance(value, datetime):
                    value = value.isoformat()
                attributes[api_field] = value
        return attributes

    def _prepare_payload(
        self,
        context: Optional[dict] = None,
        record: Optional[dict] = None,
    ) -> dict:
        """Prepare the payload for the API request."""
        self.logger.info(f"Preparing payload for {self.stream_name}")
        self.logger.info(f"Context: {context}")
        self.logger.info(f"Record: {record}")

        # Get the HTTP method from context
        http_method = context.get("http_method", "POST")
        self.logger.info(f"HTTP Method from context: {http_method}")

        # Build attributes using field mappings
        attributes = self.build_attributes(record, self.get_field_mappings())

        # Add any additional attributes from the record
        self._add_additional_attributes(record, attributes)

        return {
            "data": {
                "type": self.endpoint,
                "attributes": attributes
            }
        }

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        """Add any additional attributes that are not covered by field mappings.
        
        This method can be overridden by subclasses to add custom attributes.

        Args:
            record: The record to transform
            attributes: The attributes dictionary to update
        """
        pass

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

class ProductsSink(BaseOptiplySink):
    """Products sink class."""

    endpoint = "products"
    field_mappings = {
        "id": "id",
        "name": "name",
        "skuCode": "skuCode",
        "price": "price",
        "stockLevel": "stockLevel",
        "status": "status"
    }

    def __init__(self, target: Any, stream_name: str, schema: Dict, key_properties: List[str]):
        """Initialize the sink."""
        super().__init__(target, stream_name, schema, key_properties)
        # Override key_properties to make id optional for creation
        self.key_properties = []

    def _add_additional_attributes(self, record: dict, attributes: dict) -> None:
        """Add any additional attributes from the record."""
        # No additional attributes needed for products
        pass

    def get_url(self, context: Optional[dict] = None) -> str:
        """Get the URL for the API request."""
        record = context.get("record", {}) if context else {}
        
        # If we have an ID, it's a PATCH request
        if "id" in record:
            return f"{self.base_url}/{record['id']}?accountId={self.config['account_id']}&couplingId={self.config['coupling_id']}"
        
        # Otherwise it's a POST request
        return f"{self.base_url}?accountId={self.config['account_id']}&couplingId={self.config['coupling_id']}"

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

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        """Add any additional attributes that are not covered by field mappings.
        
        This method can be overridden by subclasses to add custom attributes.
        
        Args:
            record: The record to transform
            attributes: The attributes dictionary to update
        """
        pass

class SupplierSink(BaseOptiplySink):
    """Optiply target sink class for suppliers."""

    def __init__(self, target: str, stream_name: str, schema: Dict, key_properties: List[str]):
        super().__init__(target, stream_name, schema, key_properties)
        self.endpoint = "suppliers"  # Case-sensitive endpoint

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

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        """Add any additional attributes that are not covered by field mappings.
        
        This method can be overridden by subclasses to add custom attributes.
        
        Args:
            record: The record to transform
            attributes: The attributes dictionary to update
        """
        pass

class SupplierProductSink(BaseOptiplySink):
    """Optiply target sink class for supplier products."""

    def __init__(self, target: str, stream_name: str, schema: Dict, key_properties: List[str]):
        super().__init__(target, stream_name, schema, key_properties)
        self.endpoint = "supplierProducts"  # Case-sensitive endpoint

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["supplierId", "productId", "name"]

    def get_field_mappings(self) -> Dict[str, str]:
        """Get the field mappings for this sink.

        Returns:
            The field mappings dictionary.
        """
        return {
            "supplierId": "supplierId",
            "productId": "productId",
            "name": "name",
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

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        """Add any additional attributes that are not covered by field mappings.
        
        This method can be overridden by subclasses to add custom attributes.
        
        Args:
            record: The record to transform
            attributes: The attributes dictionary to update
        """
        pass

class BuyOrderSink(BaseOptiplySink):
    """BuyOrder sink class."""

    def __init__(self, target: str, stream_name: str, schema: Dict, key_properties: List[str]):
        super().__init__(target, stream_name, schema, key_properties)
        self.endpoint = "buyOrders"  # Case-sensitive endpoint

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink."""
        return ["transaction_date", "supplier_remoteId", "line_items"]

    def get_field_mappings(self) -> Dict[str, str]:
        """Get the field mappings for this sink.

        Returns:
            The field mappings dictionary.
        """
        return {
            "transaction_date": "placed",
            "supplier_remoteId": "supplierId",
            "expectedDeliveryDate": "expectedDeliveryDate"
        }

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        """Add any additional attributes that are not covered by field mappings.
        
        This method can be overridden by subclasses to add custom attributes.
        
        Args:
            record: The record to transform
            attributes: The attributes dictionary to update
        """
        if "transaction_date" in record:
            attributes["placed"] = record["transaction_date"] if isinstance(record["transaction_date"], str) else record["transaction_date"].isoformat()

        if "supplier_remoteId" in record:
            attributes["supplierId"] = int(record["supplier_remoteId"])

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

            attributes["buyOrderLines"] = buy_order_lines

class BuyOrderLineSink(BaseOptiplySink):
    """Optiply target sink class for buy order lines."""

    def __init__(self, target: str, stream_name: str, schema: Dict, key_properties: List[str]):
        super().__init__(target, stream_name, schema, key_properties)
        self.endpoint = "buyOrderLines"  # Case-sensitive endpoint

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

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        """Add any additional attributes that are not covered by field mappings.
        
        This method can be overridden by subclasses to add custom attributes.
        
        Args:
            record: The record to transform
            attributes: The attributes dictionary to update
        """
        pass

class SellOrderSink(BaseOptiplySink):
    """Optiply target sink class for sell orders."""

    def __init__(self, target: str, stream_name: str, schema: Dict, key_properties: List[str]):
        super().__init__(target, stream_name, schema, key_properties)
        self.endpoint = "sellOrders"  # Case-sensitive endpoint

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

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        """Add any additional attributes that are not covered by field mappings.
        
        This method can be overridden by subclasses to add custom attributes.
        
        Args:
            record: The record to transform
            attributes: The attributes dictionary to update
        """
        pass

class SellOrderLineSink(BaseOptiplySink):
    """Optiply target sink class for sell order lines."""

    def __init__(self, target: str, stream_name: str, schema: Dict, key_properties: List[str]):
        super().__init__(target, stream_name, schema, key_properties)
        self.endpoint = "sellOrderLines"  # Case-sensitive endpoint

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

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        """Add any additional attributes that are not covered by field mappings.
        
        This method can be overridden by subclasses to add custom attributes.
        
        Args:
            record: The record to transform
            attributes: The attributes dictionary to update
        """
        pass