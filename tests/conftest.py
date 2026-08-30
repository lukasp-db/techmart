import pytest

from techmart.spark.session import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("techmart-tests")
    yield session
    session.stop()
