import sys
from unittest.mock import MagicMock, patch

import pytest
from kedro.io.core import DatasetError

from kedro_datasets.polars import IcebergDataset

_skip_on_314 = pytest.mark.skipif(
    sys.version_info >= (3, 14),
    reason="PyIceberg does not support Python 3.14",
)


@pytest.fixture
def table_name():
    return "default.test_table"


@pytest.fixture
def catalog_properties():
    return {"type": "glue"}


@pytest.fixture
def dummy_polars_df():
    mock_df = MagicMock()
    mock_df.to_arrow.return_value = MagicMock()
    return mock_df


@pytest.fixture
def iceberg_dataset(table_name, catalog_properties):
    return IcebergDataset(
        table_name=table_name,
        catalog_name="glue_catalog",
        catalog_properties=catalog_properties,
    )


@_skip_on_314
class TestPolarsIcebergDataset:
    def test_invalid_write_mode(self, table_name, catalog_properties):
        """Test that initializing with an unsupported write mode raises DatasetError."""
        with pytest.raises(DatasetError, match="Write mode 'invalid' is not supported"):
            IcebergDataset(
                table_name=table_name,
                catalog_properties=catalog_properties,
                save_args={"mode": "invalid"},
            )

    def test_describe(self, iceberg_dataset, table_name, catalog_properties):
        """Test the _describe method output."""
        description = iceberg_dataset._describe()
        assert description == {
            "table_name": table_name,
            "catalog_name": "glue_catalog",
            "catalog_properties": catalog_properties,
            "load_args": {},
            "save_args": {"mode": "overwrite"},
        }

    def test_describe_excludes_credentials(self, table_name, catalog_properties):
        """Test that _describe excludes credentials for security."""
        dataset = IcebergDataset(
            table_name=table_name,
            catalog_name="glue_catalog",
            catalog_properties=catalog_properties,
            credentials={"user": "test-user", "role_arn": "arn:aws:iam::role/test"},
        )
        description = dataset._describe()
        assert "credentials" not in description
        assert "test-user" not in str(description)

    def test_credentials_passed_to_catalog(self, table_name, mocker):
        """Test that credentials are merged into catalog properties when loading catalog."""
        mock_load_catalog = mocker.patch("pyiceberg.catalog.load_catalog")
        dataset = IcebergDataset(
            table_name=table_name,
            catalog_name="rest_catalog",
            catalog_properties={"type": "rest", "uri": "https://catalog.example.com"},
            credentials={"user": "test-user"},
        )
        dataset._get_catalog()
        mock_load_catalog.assert_called_once_with(
            "rest_catalog",
            type="rest",
            uri="https://catalog.example.com",
            user="test-user",
        )

    def test_exists_true(self, iceberg_dataset, mocker):
        """Test _exists when the table exists in the catalog."""
        mock_catalog = MagicMock()
        mock_catalog.table_exists.return_value = True
        mocker.patch.object(iceberg_dataset, "_get_catalog", return_value=mock_catalog)

        assert iceberg_dataset._exists() is True
        mock_catalog.table_exists.assert_called_once_with(iceberg_dataset._table_name)

    def test_exists_false(self, iceberg_dataset, mocker):
        """Test _exists returns False when table does not exist."""
        mock_catalog = MagicMock()
        mock_catalog.table_exists.return_value = False
        mocker.patch.object(iceberg_dataset, "_get_catalog", return_value=mock_catalog)

        assert iceberg_dataset._exists() is False

    def test_exists_returns_false_on_error(self, iceberg_dataset, mocker):
        """Test _exists returns False on catalog errors."""
        mocker.patch.object(
            iceberg_dataset,
            "_get_catalog",
            side_effect=Exception("Connection failed"),
        )
        assert iceberg_dataset._exists() is False

    def test_load(self, iceberg_dataset, dummy_polars_df, mocker):
        """Test loading table data via polars.scan_iceberg."""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mocker.patch.object(iceberg_dataset, "_get_catalog", return_value=mock_catalog)

        mock_lazy = MagicMock()
        mock_lazy.collect.return_value = dummy_polars_df
        mocker.patch("polars.scan_iceberg", return_value=mock_lazy, create=True)

        loaded = iceberg_dataset._load()
        assert loaded == dummy_polars_df
        mock_catalog.load_table.assert_called_once_with(iceberg_dataset._table_name)

    def test_load_with_args(self, table_name, catalog_properties, mocker):
        """Test that load_args are forwarded to polars.scan_iceberg."""
        dataset = IcebergDataset(
            table_name=table_name,
            catalog_properties=catalog_properties,
            load_args={"snapshot_id": 12345},
        )
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mocker.patch.object(dataset, "_get_catalog", return_value=mock_catalog)

        mock_lazy = MagicMock()
        mock_scan = mocker.patch(
            "polars.scan_iceberg", return_value=mock_lazy, create=True
        )

        dataset._load()
        mock_scan.assert_called_once_with(mock_table, snapshot_id=12345)

    def test_missing_polars_raises_error(self, iceberg_dataset):
        """Test that missing polars module raises DatasetError with install message."""
        with patch.dict("sys.modules", {"polars": None}):
            with pytest.raises(DatasetError, match="Polars is required"):
                iceberg_dataset._load()

    def test_save_overwrite(self, iceberg_dataset, dummy_polars_df, mocker):
        """Test saving data with overwrite mode."""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mocker.patch.object(iceberg_dataset, "_get_catalog", return_value=mock_catalog)

        iceberg_dataset._save(dummy_polars_df)
        mock_catalog.load_table.assert_called_once_with(iceberg_dataset._table_name)
        dummy_polars_df.write_iceberg.assert_called_once_with(
            mock_table, mode="overwrite"
        )

    def test_save_append(self, table_name, catalog_properties, dummy_polars_df, mocker):
        """Test saving data with append mode."""
        dataset = IcebergDataset(
            table_name=table_name,
            catalog_properties=catalog_properties,
            save_args={"mode": "append"},
        )
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mocker.patch.object(dataset, "_get_catalog", return_value=mock_catalog)

        dataset._save(dummy_polars_df)
        dummy_polars_df.write_iceberg.assert_called_once_with(mock_table, mode="append")

    def test_missing_pyiceberg_raises_error(self, iceberg_dataset):
        """Test that missing pyiceberg module raises DatasetError with install message."""
        with patch.dict("sys.modules", {"pyiceberg": None, "pyiceberg.catalog": None}):
            with pytest.raises(DatasetError, match="PyIceberg is required"):
                iceberg_dataset._get_catalog()
