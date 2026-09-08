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
    mock_lazy = MagicMock()
    mock_lazy.collect.return_value = mock_df
    mock_df.lazy.return_value = mock_lazy
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
        assert description == {
            "table_name": table_name,
            "catalog_name": "glue_catalog",
            "catalog_properties": catalog_properties,
            "load_args": {},
            "save_args": {"mode": "overwrite"},
        }

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

    def test_exists_surfaces_connection_error(self, iceberg_dataset, mocker):
        """Test _exists surfaces real connection or authentication errors."""
        mocker.patch.object(
            iceberg_dataset,
            "_get_catalog",
            side_effect=ConnectionError("Failed to reach catalog"),
        )
        with pytest.raises(ConnectionError, match="Failed to reach catalog"):
            iceberg_dataset._exists()

    def test_load_native_scan_iceberg(self, iceberg_dataset, dummy_polars_df, mocker):
        """Test loading table data via native polars.scan_iceberg."""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mocker.patch.object(iceberg_dataset, "_get_catalog", return_value=mock_catalog)

        mock_lazy = MagicMock()
        mock_lazy.collect.return_value = dummy_polars_df
        mocker.patch("polars.scan_iceberg", return_value=mock_lazy, create=True)

        loaded = iceberg_dataset.load()
        assert loaded == dummy_polars_df
        mock_catalog.load_table.assert_called_once_with(iceberg_dataset._table_name)

    def test_load_pyiceberg_scan_fallback(
        self, iceberg_dataset, dummy_polars_df, mocker
    ):
        """Test loading table data falls back to PyIceberg scan when scan_iceberg is unavailable/fails."""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_scan = MagicMock()
        mock_scan.to_polars.return_value = dummy_polars_df
        mock_table.scan.return_value = mock_scan
        mock_catalog.load_table.return_value = mock_table

        mocker.patch.object(iceberg_dataset, "_get_catalog", return_value=mock_catalog)
        mocker.patch(
            "polars.scan_iceberg", side_effect=NotImplementedError, create=True
        )

        loaded = iceberg_dataset.load()
        assert loaded == dummy_polars_df
        mock_catalog.load_table.assert_called_once_with(iceberg_dataset._table_name)
        mock_table.scan.assert_called_once()

    def test_load_invalid_load_args_raises_type_error(
        self, table_name, catalog_properties, mocker
    ):
        """Test that invalid load_args raise TypeError and are not silently swallowed."""
        dataset = IcebergDataset(
            table_name=table_name,
            catalog_properties=catalog_properties,
            load_args={"invalid_arg": 123},
        )
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mocker.patch.object(dataset, "_get_catalog", return_value=mock_catalog)
        mocker.patch(
            "polars.scan_iceberg",
            side_effect=TypeError(
                "scan_iceberg() got an unexpected keyword argument 'invalid_arg'"
            ),
            create=True,
        )

        with pytest.raises(DatasetError, match="unexpected keyword argument"):
            dataset.load()

    def test_missing_polars_raises_error(self, iceberg_dataset):
        """Test that missing polars module raises DatasetError with install message."""
        with patch.dict("sys.modules", {"polars": None}):
            with pytest.raises(DatasetError, match="Polars is required"):
                iceberg_dataset.load()

    def test_save_new_table(self, iceberg_dataset, dummy_polars_df, mocker):
        """Test saving data to a new table (table does not exist yet)."""
        mock_catalog = MagicMock()
        mock_catalog.table_exists.return_value = False
        mock_table = MagicMock()
        mock_catalog.create_table.return_value = mock_table

        mocker.patch.object(iceberg_dataset, "_get_catalog", return_value=mock_catalog)

        iceberg_dataset.save(dummy_polars_df)
        mock_catalog.create_table.assert_called_once()
        mock_table.append.assert_called_once()

    def test_save_overwrite_existing_table(
        self, iceberg_dataset, dummy_polars_df, mocker
    ):
        """Test saving data with overwrite mode to an existing table."""
        mock_catalog = MagicMock()
        mock_catalog.table_exists.return_value = True
        mock_table = MagicMock()
        mock_catalog.load_table.return_value = mock_table

        mocker.patch.object(iceberg_dataset, "_get_catalog", return_value=mock_catalog)

        iceberg_dataset.save(dummy_polars_df)
        mock_catalog.load_table.assert_called_once_with(iceberg_dataset._table_name)
        mock_table.overwrite.assert_called_once()

    def test_save_lazy_frame(self, iceberg_dataset, dummy_polars_df, mocker):
        """Test saving a Polars LazyFrame collects before writing."""
        mock_catalog = MagicMock()
        mock_catalog.table_exists.return_value = True
        mock_table = MagicMock()
        mock_catalog.load_table.return_value = mock_table

        mocker.patch.object(iceberg_dataset, "_get_catalog", return_value=mock_catalog)

        lazy_df = dummy_polars_df.lazy()
        iceberg_dataset.save(lazy_df)
        mock_table.overwrite.assert_called_once()

    def test_save_append_existing_table(
        self, table_name, catalog_properties, dummy_polars_df, mocker
    ):
        """Test saving data with append mode to an existing table."""
        dataset = IcebergDataset(
            table_name=table_name,
            catalog_properties=catalog_properties,
            save_args={"mode": "append"},
        )
        mock_catalog = MagicMock()
        mock_catalog.table_exists.return_value = True
        mock_table = MagicMock()
        mock_catalog.load_table.return_value = mock_table

        mocker.patch.object(dataset, "_get_catalog", return_value=mock_catalog)

        dataset.save(dummy_polars_df)
        mock_table.append.assert_called_once()

    def test_missing_pyiceberg_raises_error(self, iceberg_dataset):
        """Test that missing pyiceberg module raises DatasetError with install message."""
        with patch.dict("sys.modules", {"pyiceberg": None, "pyiceberg.catalog": None}):
            with pytest.raises(DatasetError, match="PyIceberg is required"):
                iceberg_dataset._get_catalog()
