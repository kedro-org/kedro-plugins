import pytest

# from kedro.extras.datasets.snowflake import SnowParkDataSet


class TestSnowParkDataSet:
    @pytest.mark.snowflake
    def test_dummy_snowflake(self):
        assert int(1) == int(0)
