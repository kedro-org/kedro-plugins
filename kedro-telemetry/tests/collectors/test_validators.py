from unittest.mock import MagicMock

from kedro_telemetry.collectors import collect_validator_properties


class TestCollectValidatorProperties:
    def test_validator_specs_counted_with_pii_bucketing(self):
        class Spec:
            def __init__(self, class_path):
                self.class_path = class_path

        catalog = MagicMock()
        catalog.validator_specs = {
            "datasetA": Spec("pandera.pandas.DataFrameModel"),
            "datasetB": Spec("my_project.schemas.CompaniesSchema"),
            "datasetC": Spec("my_project.validators.check_rows"),
        }

        result = collect_validator_properties(catalog)

        assert result == {
            "number_of_validated_datasets": 3,
            "validator_type_count.pandera": 1,
            "validator_type_count.custom": 2,
        }
        assert not any("my_project" in key for key in result)

    def test_catalog_without_validator_support_emits_no_validator_fields(self):
        catalog = MagicMock()
        del catalog.validator_specs

        assert collect_validator_properties(catalog) == {}

    def test_catalog_without_validators_emits_no_validator_fields(self):
        catalog = MagicMock()
        catalog.validator_specs = {}

        assert collect_validator_properties(catalog) == {}
