"""Optiply target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_optiply.sinks import (
    OptiplySink,
    ProductSink,
    SupplierSink,
    SupplierProductSink,
    BuyOrderSink,
    BuyOrderLineSink,
    SellOrderSink,
    SellOrderLineSink,
)


class TargetOptiply(Target):
    """Target for Optiply API."""

    name = "target-optiply"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "config",
            th.ObjectType(
                th.Property(
                    "client_id",
                    th.StringType,
                    description="Optiply API client ID",
                ),
                th.Property(
                    "client_secret",
                    th.StringType,
                    description="Optiply API client secret",
                ),
                th.Property(
                    "account_id",
                    th.IntegerType,
                    description="Optiply account ID",
                ),
                th.Property(
                    "couplingId",
                    th.IntegerType,
                    description="Optiply coupling ID",
                ),
                th.Property(
                    "base_url",
                    th.StringType,
                    default="https://dashboard.optiply.nl/api/v1",
                    description="Optiply API URL",
                ),
                th.Property(
                    "auth_url",
                    th.StringType,
                    default="https://dashboard.optiply.nl/api/auth/oauth/token",
                    description="Optiply Auth URL",
                ),
            ),
            description="Optiply configuration",
        ),
    ).to_dict()

    def get_sink_class(self, stream_name: str) -> type[OptiplySink | ProductSink | SupplierSink | SupplierProductSink | BuyOrderLineSink | SellOrderSink | SellOrderLineSink]:
        """Get sink class for the given stream name."""
        if stream_name == "BuyOrders":
            return BuyOrderSink
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
