from techmart.ai.registry import AI_SPECS


def test_registry_covers_ai_tables():
    names = {s.name for s in AI_SPECS}
    assert {"fact_sales_forecast", "product_review", "service_case", "ai_anomaly_catalog",
            "_product_review_staging", "_service_case_staging"} <= names
    assert all(s.schema == "ai" for s in AI_SPECS)
