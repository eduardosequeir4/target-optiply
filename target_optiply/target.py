"""Optiply target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_optiply.sinks import (
    OptiplySink,
    ProductSink,
    SupplierSink,
    SupplierProductSink,
    BuyOrderLineSink,
    SellOrderSink,
    SellOrderLineSink,
)


class TargetOptiply(Target):
    """Target for Optiply API."""

    name = "target-optiply"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_credentials",
            th.ObjectType(
                th.Property(
                    "username",
                    th.StringType,
                    description="Optiply API username",
                ),
                th.Property(
                    "password",
                    th.StringType,
                    description="Optiply API password",
                ),
                th.Property(
                    "account_id",
                    th.IntegerType,
                    description="Optiply account ID",
                ),
                th.Property(
                    "coupling_id",
                    th.IntegerType,
                    description="Optiply coupling ID",
                ),
            ),
            description="Optiply API credentials",
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://api-accept.optiply.com/v1",
            description="Optiply API URL",
        ),
    ).to_dict()

    def get_sink_class(self, stream_name: str) -> type[OptiplySink | ProductSink | SupplierSink | SupplierProductSink | BuyOrderLineSink | SellOrderSink | SellOrderLineSink]:
        """Get sink class for the given stream name."""
        if stream_name == "BuyOrders":
            return OptiplySink
        elif stream_name == "Products":
            return ProductSink
        elif stream_name == "Suppliers":
            return SupplierSink
        elif stream_name == "SupplierProducts":
            return SupplierProductSink
        elif stream_name == "BuyOrderLines":
            return BuyOrderLineSink
        elif stream_name == "SellOrders":
            return SellOrderSink
        elif stream_name == "SellOrderLines":
            return SellOrderLineSink
        else:
            raise ValueError(f"Unsupported stream: {stream_name}")


if __name__ == "__main__":
    TargetOptiply.cli()
