import pandas as pd
import pytest
from kedro.io import DataSetError
from pandas.testing import assert_frame_equal

from kedro_datasets.pandas import DeltaTableDataSet


@pytest.fixture
def filepath(tmp_path):
    return (tmp_path / "delta-table").as_posix()


@pytest.fixture
def dummy_dataframe():
    return pd.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})


@pytest.fixture
def deltatable_data_set(filepath, load_args, save_args, fs_args):
    return DeltaTableDataSet(
        filepath=filepath, load_args=load_args, save_args=save_args, fs_args=fs_args
    )


class TestDeltaTableDataSet:
    def test_save_and_load(self, deltatable_data_set, dummy_dataframe):
        """Test saving and reloading the data set."""
        deltatable_data_set.save(dummy_dataframe)
        reloaded = deltatable_data_set.load()
        assert_frame_equal(dummy_dataframe, reloaded)

    def test_overwrite_with_same_schema(self, deltatable_data_set, dummy_dataframe):
        """Test saving with the default overwrite mode with new data of same schema."""
        deltatable_data_set.save(dummy_dataframe)
        new_df = pd.DataFrame({"col1": [0, 0], "col2": [1, 1], "col3": [2, 2]})
        deltatable_data_set.save(new_df)
        reloaded = deltatable_data_set.load()
        assert_frame_equal(new_df, reloaded)

    def test_overwrite_with_diff_schema(self, deltatable_data_set, dummy_dataframe):
        """Test saving with the default overwrite mode with new data of
        different schema."""
        deltatable_data_set.save(dummy_dataframe)
        new_df = pd.DataFrame({"new_col": [1, 2]})
        pattern = "Schema of data does not match table schema"
        with pytest.raises(DataSetError, match=pattern):
            deltatable_data_set.save(new_df)

    @pytest.mark.parametrize("save_args", [{"overwrite_schema": True}], indirect=True)
    def test_overwrite_both_data_and_schema(self, deltatable_data_set, dummy_dataframe):
        """Test saving to overwrite both data and schema."""
        deltatable_data_set.save(dummy_dataframe)
        new_df = pd.DataFrame({"new_col": [1, 2]})
        deltatable_data_set.save(new_df)
        reloaded = deltatable_data_set.load()
        assert_frame_equal(new_df, reloaded)

    def test_versioning(self, filepath, dummy_dataframe):
        """Test Delta Table versioning."""
        deltatable_data_set = DeltaTableDataSet(filepath)
        deltatable_data_set.save(dummy_dataframe)
        new_df = pd.DataFrame({"col1": [0, 0], "col2": [1, 1], "col3": [2, 2]})
        deltatable_data_set.save(new_df)

        deltatable_data_set0 = DeltaTableDataSet(filepath, load_args={"version": 0})
        version_0 = deltatable_data_set0.load()
        assert_frame_equal(dummy_dataframe, version_0)

        deltatable_data_set1 = DeltaTableDataSet(filepath, load_args={"version": 1})
        version_1 = deltatable_data_set1.load()
        assert_frame_equal(new_df, version_1)
