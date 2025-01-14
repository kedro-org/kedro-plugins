from kedro_datasets.databricks import ManagedTableDataset


class TestManagedTableDataset:
    def test_describe(self):
        unity_ds = ManagedTableDataset(table="test")
        assert unity_ds._describe() == {
            "catalog": None,
            "database": "default",
            "table": "test",
            "write_mode": None,
            "dataframe_type": "spark",
            "primary_key": None,
            "version": "None",
            "owner_group": None,
            "partition_columns": None,
        }
