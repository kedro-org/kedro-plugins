from unittest.mock import MagicMock

from kedro_telemetry.collectors import collect_dataset_properties


class TestCollectDatasetProperties:
    def test_old_catalog_with_list_method(self):
        # catalog.list() was replaced with catalog.keys() in `kedro >= 1.0`
        catalog = MagicMock()
        catalog.list.return_value = [
            "dataset1",
            "params:my_param",
            "dataset2",
            "parameters",
        ]
        del catalog.keys

        # dataset types are not available for the old catalog
        assert collect_dataset_properties(catalog) == {"number_of_datasets": 2}

    def test_new_catalog_counts_dataset_types_and_skips_parameters(self):
        catalog = MagicMock()
        catalog.keys.return_value = [
            "datasetA",
            "params:global",
            "datasetB",
            "parameters",
        ]
        catalog.get_type.return_value = "kedro.io.memory_dataset.MemoryDataset"
        del catalog.list

        assert collect_dataset_properties(catalog) == {
            "number_of_datasets": 2,
            "dataset_type_count.kedro.io.memory_dataset.MemoryDataset": 2,
        }

    def test_user_defined_dataset_types_are_bucketed_as_custom(self):
        types = {
            "a": "kedro_datasets.pandas.CSVDataset",
            "b": "kedro_datasets_experimental.foo.FooDataset",
            "c": "my_project.datasets.SecretDataset",
            "d": None,
        }
        catalog = MagicMock()
        catalog.keys.return_value = list(types)
        catalog.get_type.side_effect = types.get

        result = collect_dataset_properties(catalog)

        assert result == {
            "number_of_datasets": 4,
            "dataset_type_count.kedro_datasets.pandas.CSVDataset": 1,
            "dataset_type_count.kedro_datasets_experimental.foo.FooDataset": 1,
            "dataset_type_count.custom": 2,
        }
