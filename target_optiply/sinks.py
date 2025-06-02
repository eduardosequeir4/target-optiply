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

    endpoint = None
    field_mappings = {}

    def __init__(self, target: str, stream_name: str, schema: Dict, key_properties: List[str]):
        super().__init__(target, stream_name, schema, key_properties)
        self.endpoint = self.stream_name.lower() if not self.endpoint else self.endpoint

    def get_url(
        self,
        context: Optional[dict] = None,
    ) -> str:
        """Get the stream's API URL."""
        endpoint = self.endpoint
        record = context.get("record", {}) if context else {}
        
        # Construct the base URL with endpoint
        if context and context.get("http_method") == "PATCH" and "id" in record:
            url = f"{self.base_url}/{endpoint}/{record['id']}"
        else:
            url = f"{self.base_url}/{endpoint}"

        # Add query parameters
        query_params = {}
        
        # Add account and coupling IDs if they exist in config
        if hasattr(self, 'config'):
            if 'account_id' in self.config:
                query_params['accountId'] = self.config['account_id']
            if 'coupling_id' in self.config:
                query_params['couplingId'] = self.config['coupling_id']

        # Add any additional query parameters from the original URL
        url_parts = self.url().split('?')
        if len(url_parts) > 1:
            additional_params = dict(param.split('=') for param in url_parts[1].split('&'))
            query_params.update(additional_params)

        # Construct final URL with query parameters
        if query_params:
            query_string = "&".join(f"{k}={v}" for k, v in query_params.items())
            url = f"{url}?{query_string}"

        return url

    def get_field_mappings(self) -> Dict[str, str]:
        """Get the field mappings for this sink.

        Returns:
            The field mappings dictionary.
        """
        return self.field_mappings

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
        "name": "name",
        "skuCode": "skuCode",
        "eanCode": "eanCode",
        "articleCode": "articleCode",
        "price": "price",
        "unlimitedStock": "unlimitedStock",
        "stockLevel": "stockLevel",
        "notBeingBought": "notBeingBought",
        "resumingPurchase": "resumingPurchase",
        "status": "status",
        "assembled": "assembled",
        "minimumStock": "minimumStock",
        "maximumStock": "maximumStock",
        "ignored": "ignored",
        "manualServiceLevel": "manualServiceLevel",
        "createdAtRemote": "createdAtRemote",
        "stockMeasurementUnit": "stockMeasurementUnit"
    }

    def __init__(self, target: Any, stream_name: str, schema: Dict, key_properties: List[str]):
        """Initialize the sink."""
        super().__init__(target, stream_name, schema, key_properties)
        # Override key_properties to make id optional for creation
        self._key_properties = []

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["name", "stockLevel", "unlimitedStock"]

class SupplierSink(BaseOptiplySink):
    """Optiply target sink class for suppliers."""

    endpoint = "suppliers"
    field_mappings = {
        "name": "name",
        "emails": "emails",
        "minimumOrderValue": "minimumOrderValue",
        "fixedCosts": "fixedCosts",
        "deliveryTime": "deliveryTime",
        "userReplenishmentPeriod": "userReplenishmentPeriod",
        "reactingToLostSales": "reactingToLostSales",
        "lostSalesReaction": "lostSalesReaction",
        "lostSalesMovReaction": "lostSalesMovReaction",
        "backorders": "backorders",
        "backorderThreshold": "backorderThreshold",
        "backordersReaction": "backordersReaction",
        "maxLoadCapacity": "maxLoadCapacity",
        "containerVolume": "containerVolume",
        "ignored": "ignored",
        "globalLocationNumber": "globalLocationNumber",
        "type": "type"
    }

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["name"]

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        """Add any additional attributes that are not covered by field mappings.
        
        This method can be overridden by subclasses to add custom attributes.
        
        Args:
            record: The record to transform
            attributes: The attributes dictionary to update
        """
        # Handle emails field - convert from JSON string to array
        if "emails" in record and isinstance(record["emails"], str):
            try:
                attributes["emails"] = json.loads(record["emails"])
            except json.JSONDecodeError:
                self.logger.warning(f"Could not parse emails JSON string: {record['emails']}")
                attributes["emails"] = []

        # Convert boolean strings to actual booleans
        boolean_fields = ["reactingToLostSales", "backorders", "ignored"]
        for field in boolean_fields:
            if field in record and isinstance(record[field], str):
                attributes[field] = record[field].lower() == "true"

        # Convert numeric strings to actual numbers
        numeric_fields = [
            "minimumOrderValue", "fixedCosts", "deliveryTime", 
            "userReplenishmentPeriod", "lostSalesReaction", 
            "lostSalesMovReaction", "backorderThreshold", 
            "backordersReaction", "maxLoadCapacity", "containerVolume"
        ]
        for field in numeric_fields:
            if field in record and isinstance(record[field], str):
                try:
                    attributes[field] = float(record[field])
                except ValueError:
                    self.logger.warning(f"Could not convert {field} to number: {record[field]}")

        # Validate type field
        if "type" in record and record["type"] not in ["vendor", "producer"]:
            self.logger.warning(f"Invalid type value: {record['type']}. Must be 'vendor' or 'producer'")
            attributes["type"] = "vendor"  # Default to vendor if invalid

        # Validate globalLocationNumber length
        if "globalLocationNumber" in record and len(record["globalLocationNumber"]) != 13:
            self.logger.warning(f"Invalid globalLocationNumber length: {len(record['globalLocationNumber'])}. Must be 13 characters")
            attributes.pop("globalLocationNumber", None)  # Remove if invalid

        # Remove remoteDataSyncedToDate as it's not accepted by the API
        attributes.pop("remoteDataSyncedToDate", None)

class SupplierProductSink(BaseOptiplySink):
    """Optiply target sink class for supplier products."""

    endpoint = "supplierProducts"
    field_mappings = {
        "name": "name",
        "skuCode": "skuCode",
        "eanCode": "eanCode",
        "articleCode": "articleCode",
        "price": "price",
        "minimumPurchaseQuantity": "minimumPurchaseQuantity",
        "lotSize": "lotSize",
        "availability": "availability",
        "availabilityDate": "availabilityDate",
        "preferred": "preferred",
        "productId": "productId",
        "supplierId": "supplierId",
        "deliveryTime": "deliveryTime",
        "status": "status",
        "freeStock": "freeStock",
        "weight": "weight",
        "volume": "volume"
    }

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["name", "productId", "supplierId"]

class BuyOrderSink(BaseOptiplySink):
    """Optiply target sink class for buy orders."""

    endpoint = "buyOrders"
    field_mappings = {
        "placed": "placed",
        "completed": "completed",
        "expectedDeliveryDate": "expectedDeliveryDate",
        "totalValue": "totalValue",
        "supplierId": "supplierId",
        "accountId": "accountId",
        "assembly": "assembly"
    }

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["placed", "totalValue", "supplierId", "accountId"]

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        """Add any additional attributes that are not covered by field mappings.
        
        This method can be overridden by subclasses to add custom attributes.
        
        Args:
            record: The record to transform
            attributes: The attributes dictionary to update
        """
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
                        "productId": item["productId"],
                        "expectedDeliveryDate": item.get("expectedDeliveryDate")
                    }
                })
            attributes["totalValue"] = str(total_value)
            attributes["orderLines"] = buy_order_lines

class BuyOrderLineSink(BaseOptiplySink):
    """Optiply target sink class for buy order lines."""

    endpoint = "buyOrderLines"
    field_mappings = {
        "quantity": "quantity",
        "subtotalValue": "subtotalValue",
        "productId": "productId",
        "buyOrderId": "buyOrderId",
        "expectedDeliveryDate": "expectedDeliveryDate"
    }

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["subtotalValue", "productId", "quantity", "buyOrderId"]

class SellOrderSink(BaseOptiplySink):
    """Optiply target sink class for sell orders."""

    endpoint = "sellOrders"
    field_mappings = {
        "placed": "placed",
        "totalValue": "totalValue"
    }

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["totalValue", "placed"]

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        """Add any additional attributes that are not covered by field mappings.
        
        This method can be overridden by subclasses to add custom attributes.
        
        Args:
            record: The record to transform
            attributes: The attributes dictionary to update
        """
        if "line_items" in record:
            line_items = json.loads(record["line_items"])
            sell_order_lines = []
            total_value = 0
            for item in line_items:
                subtotal_value = float(item["subtotalValue"])
                total_value += subtotal_value
                sell_order_lines.append({
                    "type": "sellOrderLines",
                    "attributes": {
                        "quantity": item["quantity"],
                        "subtotalValue": str(subtotal_value),
                        "productId": item["productId"]
                    }
                })
            attributes["totalValue"] = str(total_value)
            attributes["orderLines"] = sell_order_lines

class SellOrderLineSink(BaseOptiplySink):
    """Optiply target sink class for sell order lines."""

    endpoint = "sellOrderLines"
    field_mappings = {
        "quantity": "quantity",
        "subtotalValue": "subtotalValue",
        "productId": "productId",
        "sellOrderId": "sellOrderId"
    }

    def get_mandatory_fields(self) -> List[str]:
        """Get the list of mandatory fields for this sink.

        Returns:
            The list of mandatory fields.
        """
        return ["subtotalValue", "sellOrderId", "productId", "quantity"]
