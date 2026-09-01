"""Registry of techmart_ai table specs."""
from __future__ import annotations

from ..spark.framework import SparkTableSpec
from .anomalies import AI_ANOMALY_CATALOG_SPEC
from .fact_sales_forecast import FACT_SALES_FORECAST_SPEC
from .product_review import PRODUCT_REVIEW_SPEC, PRODUCT_REVIEW_STAGING_SPEC
from .service_case import SERVICE_CASE_SPEC, SERVICE_CASE_STAGING_SPEC

AI_SPECS: list[SparkTableSpec] = [
    FACT_SALES_FORECAST_SPEC,
    AI_ANOMALY_CATALOG_SPEC,
    PRODUCT_REVIEW_STAGING_SPEC,
    SERVICE_CASE_STAGING_SPEC,
    PRODUCT_REVIEW_SPEC,
    SERVICE_CASE_SPEC,
]
