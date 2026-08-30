import techmart


def test_package_exposes_version():
    assert isinstance(techmart.__version__, str)
    assert techmart.__version__.count(".") >= 2
