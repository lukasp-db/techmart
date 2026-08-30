from techmart.spark.session import get_spark


def test_get_spark_returns_session(spark):
    assert spark.version.startswith("3.")


def test_get_spark_is_idempotent(spark):
    again = get_spark("techmart-tests")
    assert again is spark
