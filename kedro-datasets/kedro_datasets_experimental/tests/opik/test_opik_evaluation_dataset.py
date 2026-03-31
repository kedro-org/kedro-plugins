import json
from unittest.mock import Mock, call, patch

import pytest
import yaml
from kedro.io import DatasetError
from opik.rest_api.core.api_error import ApiError

from kedro_datasets_experimental.opik.opik_evaluation_dataset import (
    OpikEvaluationDataset,
)


def make_api_error(status_code: int) -> ApiError:
    """Return an ApiError with the given status code."""
    return ApiError(status_code=status_code, headers={}, body={})


@pytest.fixture
def mock_opik():
    """Mock Opik client instance."""
    with patch("kedro_datasets_experimental.opik.opik_evaluation_dataset.Opik") as mock_class:
        instance = Mock()
        mock_class.return_value = instance
        yield instance


@pytest.fixture
def mock_credentials():
    """Valid Opik credentials for testing."""
    return {
        "api_key": "opik_test_key",  # pragma: allowlist secret
        "workspace": "test-workspace",
    }


@pytest.fixture
def eval_items():
    """Sample evaluation dataset items with IDs."""
    return [
        {
            "id": "item_001",
            "input": {"question": "What is AI?"},
            "expected_output": {"answer": "Artificial Intelligence"},
        },
        {
            "id": "item_002",
            "input": {"question": "What is ML?"},
            "expected_output": {"answer": "Machine Learning"},
        },
    ]


@pytest.fixture
def eval_items_no_id():
    """Evaluation items without IDs."""
    return [
        {"input": {"question": "What is AI?"}, "expected_output": {"answer": "AI"}},
        {"input": {"question": "What is ML?"}, "expected_output": {"answer": "ML"}},
    ]


@pytest.fixture
def filepath_json(tmp_path, eval_items):
    """Temporary JSON file with evaluation items."""
    filepath = tmp_path / "eval.json"
    filepath.write_text(json.dumps(eval_items))
    return str(filepath)


@pytest.fixture
def filepath_yaml(tmp_path, eval_items):
    """Temporary YAML file with evaluation items."""
    filepath = tmp_path / "eval.yaml"
    filepath.write_text(yaml.dump(eval_items))
    return str(filepath)


@pytest.fixture
def mock_remote_dataset():
    """Mock Opik Dataset object."""
    ds = Mock()
    ds.name = "test-dataset"
    return ds


@pytest.fixture
def dataset_local(filepath_json, mock_credentials, mock_opik, mock_remote_dataset):
    """OpikEvaluationDataset with local sync policy."""
    mock_opik.get_dataset.return_value = mock_remote_dataset
    return OpikEvaluationDataset(
        dataset_name="test-dataset",
        credentials=mock_credentials,
        filepath=filepath_json,
        sync_policy="local",
    )


@pytest.fixture
def dataset_remote(mock_credentials, mock_opik, mock_remote_dataset):
    """OpikEvaluationDataset with remote sync policy and no filepath."""
    mock_opik.get_dataset.return_value = mock_remote_dataset
    return OpikEvaluationDataset(
        dataset_name="test-dataset",
        credentials=mock_credentials,
        sync_policy="remote",
    )


class TestOpikEvaluationDatasetInit:
    """Test OpikEvaluationDataset initialisation."""

    def test_init_minimal_params(self, mock_credentials, mock_opik):
        """Minimal required params store expected defaults."""
        ds = OpikEvaluationDataset(
            dataset_name="my-dataset",
            credentials=mock_credentials,
        )
        assert ds._dataset_name == "my-dataset"
        assert ds._filepath is None
        assert ds._sync_policy == "local"
        assert ds._metadata is None

    def test_init_all_params(self, filepath_json, mock_credentials, mock_opik):
        """All params are stored correctly."""
        meta = {"project": "test"}
        ds = OpikEvaluationDataset(
            dataset_name="my-dataset",
            credentials=mock_credentials,
            filepath=filepath_json,
            sync_policy="remote",
            metadata=meta,
        )
        assert ds._sync_policy == "remote"
        assert ds._metadata == meta
        assert ds._filepath is not None

    def test_init_missing_api_key(self, mock_opik):
        """Missing api_key raises DatasetError."""
        with pytest.raises(DatasetError, match="Missing required Opik credential: 'api_key'"):
            OpikEvaluationDataset(
                dataset_name="ds",
                credentials={"workspace": "w"},
            )

    @pytest.mark.parametrize("empty_value", ["", "   "])
    def test_init_empty_api_key(self, mock_opik, empty_value):
        """Empty api_key raises DatasetError."""
        with pytest.raises(DatasetError, match="Opik credential 'api_key' cannot be empty"):
            OpikEvaluationDataset(
                dataset_name="ds",
                credentials={"api_key": empty_value},
            )

    def test_init_empty_optional_credential(self, mock_opik):
        """Empty optional credential (workspace) raises DatasetError."""
        with pytest.raises(DatasetError, match="Opik credential 'workspace' cannot be empty if provided"):
            OpikEvaluationDataset(
                dataset_name="ds",
                credentials={"api_key": "key", "workspace": ""},  # pragma: allowlist secret
            )

    def test_init_invalid_sync_policy(self, mock_credentials, mock_opik):
        """Invalid sync_policy raises DatasetError."""
        with pytest.raises(DatasetError, match="Invalid sync_policy 'invalid'"):
            OpikEvaluationDataset(
                dataset_name="ds",
                credentials=mock_credentials,
                sync_policy="invalid",
            )

    def test_init_unsupported_filepath_extension(self, tmp_path, mock_credentials, mock_opik):
        """Unsupported file extension raises DatasetError."""
        bad_file = tmp_path / "data.txt"
        bad_file.write_text("content")
        with pytest.raises(DatasetError, match="Unsupported file extension '.txt'"):
            OpikEvaluationDataset(
                dataset_name="ds",
                credentials=mock_credentials,
                filepath=str(bad_file),
            )

    def test_init_client_failure_raises_dataset_error(self, mock_credentials):
        """Opik client construction failure is wrapped in DatasetError."""
        with patch("kedro_datasets_experimental.opik.opik_evaluation_dataset.Opik") as mock_class:
            mock_class.side_effect = Exception("Connection refused")
            with pytest.raises(DatasetError, match="Failed to initialise Opik client"):
                OpikEvaluationDataset(
                    dataset_name="ds",
                    credentials=mock_credentials,
                )


class TestFiledatasetProperty:
    """Test the file_dataset lazy property."""

    def test_json_returns_json_dataset(self, dataset_local):
        """JSON filepath resolves to JSONDataset."""
        assert dataset_local.file_dataset.__class__.__name__ == "JSONDataset"

    def test_yaml_returns_yaml_dataset(self, filepath_yaml, mock_credentials, mock_opik, mock_remote_dataset):
        """YAML filepath resolves to YAMLDataset."""
        mock_opik.get_dataset.return_value = mock_remote_dataset
        ds = OpikEvaluationDataset(
            dataset_name="ds",
            credentials=mock_credentials,
            filepath=filepath_yaml,
        )
        assert ds.file_dataset.__class__.__name__ == "YAMLDataset"

    def test_is_cached(self, dataset_local):
        """Repeated access returns the same object."""
        assert dataset_local.file_dataset is dataset_local.file_dataset

    def test_no_filepath_raises(self, dataset_remote):
        """Accessing file_dataset without a filepath raises DatasetError."""
        with pytest.raises(DatasetError, match="filepath must be provided"):
            _ = dataset_remote.file_dataset


class TestGetOrCreateRemoteDataset:
    """Test the _get_or_create_remote_dataset helper."""

    def test_returns_existing_dataset(self, dataset_local, mock_opik, mock_remote_dataset):
        """Returns the dataset when it already exists."""
        mock_opik.get_dataset.return_value = mock_remote_dataset
        result = dataset_local._get_or_create_remote_dataset()
        assert result is mock_remote_dataset
        mock_opik.create_dataset.assert_not_called()

    def test_creates_dataset_on_404(self, dataset_local, mock_opik, mock_remote_dataset):
        """Creates a new dataset when get_dataset raises 404."""
        mock_opik.get_dataset.side_effect = make_api_error(404)
        mock_opik.create_dataset.return_value = mock_remote_dataset

        result = dataset_local._get_or_create_remote_dataset()

        mock_opik.create_dataset.assert_called_once()
        assert result is mock_remote_dataset

    def test_non_404_api_error_raises_dataset_error(self, dataset_local, mock_opik):
        """Non-404 API error from get_dataset is wrapped in DatasetError."""
        mock_opik.get_dataset.side_effect = make_api_error(500)
        with pytest.raises(DatasetError, match="Opik API error while fetching dataset"):
            dataset_local._get_or_create_remote_dataset()

    def test_create_dataset_api_error_raises_dataset_error(self, dataset_local, mock_opik):
        """API error during create_dataset is wrapped in DatasetError."""
        mock_opik.get_dataset.side_effect = make_api_error(404)
        mock_opik.create_dataset.side_effect = make_api_error(400)
        with pytest.raises(DatasetError, match="Opik API error while creating dataset"):
            dataset_local._get_or_create_remote_dataset()


class TestValidateItems:
    """Test the _validate_items static method."""

    def test_valid_items_pass(self, eval_items):
        """Items with 'input' keys pass validation without error."""
        OpikEvaluationDataset._validate_items(eval_items)  # no exception

    def test_empty_list_passes(self):
        """Empty item list is valid."""
        OpikEvaluationDataset._validate_items([])

    def test_missing_input_raises_dataset_error(self):
        """Item missing 'input' raises DatasetError with index."""
        items = [{"input": {"q": "ok"}}, {"expected_output": "missing input"}]
        with pytest.raises(DatasetError, match="index 1.*missing required 'input'"):
            OpikEvaluationDataset._validate_items(items)


class TestUploadItems:
    """Test the _upload_items method."""

    def test_ids_are_stripped_before_insert(self, dataset_local, mock_remote_dataset, eval_items):
        """Human-readable IDs are removed; Opik receives items without 'id'."""
        dataset_local._upload_items(mock_remote_dataset, eval_items)

        inserted = mock_remote_dataset.insert.call_args[0][0]
        assert all("id" not in item for item in inserted)

    def test_non_id_fields_are_preserved(self, dataset_local, mock_remote_dataset, eval_items):
        """input and expected_output fields are passed through unchanged."""
        dataset_local._upload_items(mock_remote_dataset, eval_items)

        inserted = mock_remote_dataset.insert.call_args[0][0]
        assert inserted[0]["input"] == eval_items[0]["input"]
        assert inserted[0]["expected_output"] == eval_items[0]["expected_output"]

    def test_items_without_id_are_passed_unchanged(self, dataset_local, mock_remote_dataset, eval_items_no_id):
        """Items that have no 'id' field are inserted as-is."""
        dataset_local._upload_items(mock_remote_dataset, eval_items_no_id)

        inserted = mock_remote_dataset.insert.call_args[0][0]
        assert inserted == eval_items_no_id

    def test_dataset_insert_is_called_once(self, dataset_local, mock_remote_dataset, eval_items):
        """dataset.insert() is called exactly once."""
        dataset_local._upload_items(mock_remote_dataset, eval_items)
        mock_remote_dataset.insert.assert_called_once()


class TestSyncLocalToRemote:
    """Test the _sync_local_to_remote helper."""

    def test_returns_dataset_unchanged_when_no_filepath(self, dataset_remote, mock_remote_dataset):
        """No-op when filepath is not configured."""
        result = dataset_remote._sync_local_to_remote(mock_remote_dataset)
        assert result is mock_remote_dataset

    def test_returns_dataset_unchanged_when_file_missing(self, tmp_path, mock_credentials, mock_opik, mock_remote_dataset):
        """No-op when local file does not exist."""
        mock_opik.get_dataset.return_value = mock_remote_dataset
        ds = OpikEvaluationDataset(
            dataset_name="ds",
            credentials=mock_credentials,
            filepath=str(tmp_path / "nonexistent.json"),
        )
        result = ds._sync_local_to_remote(mock_remote_dataset)
        assert result is mock_remote_dataset

    def test_returns_dataset_unchanged_for_empty_file(self, tmp_path, mock_credentials, mock_opik, mock_remote_dataset):
        """No-op when local file contains an empty list."""
        empty_file = tmp_path / "empty.json"
        empty_file.write_text("[]")
        mock_opik.get_dataset.return_value = mock_remote_dataset

        ds = OpikEvaluationDataset(
            dataset_name="ds",
            credentials=mock_credentials,
            filepath=str(empty_file),
        )
        result = ds._sync_local_to_remote(mock_remote_dataset)
        assert result is mock_remote_dataset
        mock_remote_dataset.insert.assert_not_called()

    def test_calls_upload_items(self, dataset_local, mock_opik, mock_remote_dataset, eval_items):
        """Loads local items and passes them to _upload_items."""
        mock_opik.get_dataset.return_value = mock_remote_dataset

        with patch.object(dataset_local, "_upload_items") as mock_upload:
            dataset_local._sync_local_to_remote(mock_remote_dataset)
            mock_upload.assert_called_once_with(mock_remote_dataset, eval_items)

    def test_returns_refreshed_dataset(self, dataset_local, mock_opik, mock_remote_dataset):
        """Returns the result of a fresh get_dataset call after upload."""
        refreshed = Mock()
        mock_opik.get_dataset.return_value = refreshed

        with patch.object(dataset_local, "_upload_items"):
            result = dataset_local._sync_local_to_remote(mock_remote_dataset)

        assert result is refreshed

    def test_warns_about_items_without_id(self, tmp_path, mock_credentials, mock_opik, mock_remote_dataset, eval_items_no_id):
        """Logs a warning when items have no 'id' field."""
        filepath = tmp_path / "eval.json"
        filepath.write_text(json.dumps(eval_items_no_id))
        mock_opik.get_dataset.return_value = mock_remote_dataset

        ds = OpikEvaluationDataset(
            dataset_name="ds",
            credentials=mock_credentials,
            filepath=str(filepath),
        )

        with patch("kedro_datasets_experimental.opik.opik_evaluation_dataset.logger") as mock_logger:
            with patch.object(ds, "_upload_items"):
                ds._sync_local_to_remote(mock_remote_dataset)
            warning_messages = [c[0][0] for c in mock_logger.warning.call_args_list]
            assert any("without an 'id' field" in msg for msg in warning_messages)


class TestMergeItems:
    """Test the _merge_items static method."""

    def test_new_item_replaces_existing_by_id(self):
        """New item with existing ID replaces the old entry in place."""
        existing = [{"id": "a", "input": {"v": 1}}, {"id": "b", "input": {"v": 2}}]
        new = [{"id": "a", "input": {"v": 99}}]
        result = OpikEvaluationDataset._merge_items(existing, new)
        assert result[0]["input"]["v"] == 99
        assert len(result) == 2

    def test_new_item_without_id_is_appended(self):
        """New item without ID is always appended, never deduped."""
        existing = [{"id": "a", "input": {"v": 1}}]
        new = [{"input": {"v": 2}}]
        result = OpikEvaluationDataset._merge_items(existing, new)
        assert len(result) == 2
        assert result[1]["input"]["v"] == 2

    def test_new_item_with_new_id_is_appended(self):
        """New item with a novel ID is appended after existing items."""
        existing = [{"id": "a", "input": {"v": 1}}]
        new = [{"id": "b", "input": {"v": 2}}]
        result = OpikEvaluationDataset._merge_items(existing, new)
        assert len(result) == 2
        assert result[1]["id"] == "b"

    def test_empty_existing_returns_new(self):
        """Merging into empty list returns a copy of new items."""
        new = [{"id": "a", "input": {"v": 1}}]
        result = OpikEvaluationDataset._merge_items([], new)
        assert result == new

    def test_empty_new_returns_existing(self):
        """Merging empty new list returns existing unchanged."""
        existing = [{"id": "a", "input": {"v": 1}}]
        result = OpikEvaluationDataset._merge_items(existing, [])
        assert result == existing

    def test_order_preserved_with_replacement(self):
        """Replacement keeps the item at its original position."""
        existing = [{"id": "a", "input": {"v": 1}}, {"id": "b", "input": {"v": 2}}]
        new = [{"id": "b", "input": {"v": 99}}]
        result = OpikEvaluationDataset._merge_items(existing, new)
        assert result[0]["id"] == "a"
        assert result[1]["input"]["v"] == 99

    def test_duplicate_no_id_items_both_appended(self):
        """Two new items without ID are both appended (no dedup possible)."""
        existing = []
        new = [{"input": {"v": 1}}, {"input": {"v": 1}}]
        result = OpikEvaluationDataset._merge_items(existing, new)
        assert len(result) == 2


class TestLoad:
    """Test the load() method."""

    def test_load_remote_mode_returns_dataset(self, dataset_remote, mock_opik, mock_remote_dataset):
        """Remote mode fetches and returns the dataset without syncing."""
        mock_opik.get_dataset.return_value = mock_remote_dataset
        result = dataset_remote.load()
        assert result is mock_remote_dataset

    def test_load_remote_mode_does_not_sync(self, dataset_remote, mock_opik, mock_remote_dataset):
        """Remote mode does not call _sync_local_to_remote."""
        mock_opik.get_dataset.return_value = mock_remote_dataset
        with patch.object(dataset_remote, "_sync_local_to_remote") as mock_sync:
            dataset_remote.load()
            mock_sync.assert_not_called()

    def test_load_local_mode_calls_sync(self, dataset_local, mock_opik, mock_remote_dataset):
        """Local mode calls _sync_local_to_remote."""
        mock_opik.get_dataset.return_value = mock_remote_dataset
        with patch.object(dataset_local, "_sync_local_to_remote", return_value=mock_remote_dataset) as mock_sync:
            dataset_local.load()
            mock_sync.assert_called_once_with(mock_remote_dataset)

    def test_load_creates_dataset_if_missing(self, dataset_local, mock_opik, mock_remote_dataset):
        """Creates the remote dataset if it does not exist."""
        mock_opik.get_dataset.side_effect = [make_api_error(404), mock_remote_dataset]
        mock_opik.create_dataset.return_value = mock_remote_dataset

        with patch.object(dataset_local, "_sync_local_to_remote", return_value=mock_remote_dataset):
            result = dataset_local.load()

        mock_opik.create_dataset.assert_called_once()
        assert result is mock_remote_dataset

    def test_load_api_error_raises_dataset_error(self, dataset_local, mock_opik):
        """Non-404 API error from load is wrapped in DatasetError."""
        mock_opik.get_dataset.side_effect = make_api_error(503)
        with pytest.raises(DatasetError, match="Opik API error while fetching dataset"):
            dataset_local.load()


class TestSave:
    """Test the save() method."""

    def test_save_local_mode_uploads_to_remote(self, dataset_local, mock_opik, mock_remote_dataset, eval_items):
        """Local mode uploads items to the remote dataset."""
        mock_opik.get_dataset.return_value = mock_remote_dataset
        dataset_local.save(eval_items)
        mock_remote_dataset.insert.assert_called_once()

    def test_save_local_mode_merges_into_file(self, dataset_local, mock_opik, mock_remote_dataset, eval_items):
        """Local mode merges new items into the local file."""
        mock_opik.get_dataset.return_value = mock_remote_dataset
        new_item = [{"id": "item_003", "input": {"question": "What is DL?"}}]
        dataset_local.save(new_item)

        written = json.loads(dataset_local._filepath.read_text())
        ids = [i.get("id") for i in written]
        assert "item_003" in ids

    def test_save_local_mode_replaces_existing_id(self, dataset_local, mock_opik, mock_remote_dataset, eval_items):
        """Local mode replaces an existing item when IDs match."""
        mock_opik.get_dataset.return_value = mock_remote_dataset
        updated = [{"id": "item_001", "input": {"question": "Updated?"}}]
        dataset_local.save(updated)

        written = json.loads(dataset_local._filepath.read_text())
        item_001 = next(i for i in written if i.get("id") == "item_001")
        assert item_001["input"]["question"] == "Updated?"

    def test_save_local_mode_creates_file_if_missing(self, tmp_path, mock_credentials, mock_opik, mock_remote_dataset):
        """Creates the local file if it does not exist yet."""
        missing = tmp_path / "new.json"
        mock_opik.get_dataset.return_value = mock_remote_dataset

        ds = OpikEvaluationDataset(
            dataset_name="ds",
            credentials=mock_credentials,
            filepath=str(missing),
            sync_policy="local",
        )
        ds.save([{"id": "x", "input": {"q": "hello"}}])
        assert missing.exists()

    def test_save_remote_mode_uploads_to_remote(self, dataset_remote, mock_opik, mock_remote_dataset, eval_items):
        """Remote mode uploads items to the remote dataset."""
        mock_opik.get_dataset.return_value = mock_remote_dataset
        dataset_remote.save(eval_items)
        mock_remote_dataset.insert.assert_called_once()

    def test_save_remote_mode_does_not_write_local_file(self, dataset_remote, mock_opik, mock_remote_dataset, eval_items):
        """Remote mode does not create or modify a local file."""
        mock_opik.get_dataset.return_value = mock_remote_dataset
        dataset_remote.save(eval_items)
        assert dataset_remote._filepath is None

    def test_save_remote_mode_logs_warning(self, dataset_remote, mock_opik, mock_remote_dataset, eval_items):
        """Remote mode logs a warning that the local file won't be updated."""
        mock_opik.get_dataset.return_value = mock_remote_dataset
        with patch("kedro_datasets_experimental.opik.opik_evaluation_dataset.logger") as mock_logger:
            dataset_remote.save(eval_items)
            warning_messages = [c[0][0] for c in mock_logger.warning.call_args_list]
            assert any("uploads to remote only" in msg for msg in warning_messages)

    def test_save_missing_input_raises_dataset_error(self, dataset_local, mock_opik, mock_remote_dataset):
        """Item missing 'input' key raises DatasetError before any upload."""
        mock_opik.get_dataset.return_value = mock_remote_dataset
        bad_items = [{"expected_output": "no input here"}]
        with pytest.raises(DatasetError, match="missing required 'input'"):
            dataset_local.save(bad_items)
        mock_remote_dataset.insert.assert_not_called()


class TestExists:
    """Test the _exists() method."""

    def test_returns_true_when_dataset_exists(self, dataset_local, mock_opik, mock_remote_dataset):
        """Returns True when get_dataset succeeds."""
        mock_opik.get_dataset.return_value = mock_remote_dataset
        assert dataset_local._exists() is True

    def test_returns_false_on_404(self, dataset_local, mock_opik):
        """Returns False when get_dataset raises a 404 ApiError."""
        mock_opik.get_dataset.side_effect = make_api_error(404)
        assert dataset_local._exists() is False

    def test_non_404_api_error_raises_dataset_error(self, dataset_local, mock_opik):
        """Non-404 ApiError is wrapped in DatasetError."""
        mock_opik.get_dataset.side_effect = make_api_error(500)
        with pytest.raises(DatasetError, match="Opik API error while checking dataset"):
            dataset_local._exists()


class TestDescribe:
    """Test the _describe() method."""

    def test_describe_returns_all_fields(self, dataset_local):
        """_describe returns the expected keys."""
        desc = dataset_local._describe()
        assert desc["dataset_name"] == "test-dataset"
        assert desc["sync_policy"] == "local"
        assert "filepath" in desc
        assert "metadata" in desc

    def test_describe_filepath_none_when_not_set(self, dataset_remote):
        """filepath is None in _describe when not configured."""
        assert dataset_remote._describe()["filepath"] is None

    def test_describe_metadata_included(self, mock_credentials, mock_opik, mock_remote_dataset):
        """metadata dict is returned in _describe."""
        mock_opik.get_dataset.return_value = mock_remote_dataset
        ds = OpikEvaluationDataset(
            dataset_name="ds",
            credentials=mock_credentials,
            metadata={"project": "evaluation"},
        )
        assert ds._describe()["metadata"] == {"project": "evaluation"}


class TestPreview:
    """Test the preview() method."""

    def test_preview_existing_json_file(self, dataset_local, eval_items):
        """Returns a JSON-parseable preview for an existing file."""
        preview = dataset_local.preview()
        parsed = json.loads(str(preview))
        assert isinstance(parsed, list)
        assert len(parsed) == len(eval_items)

    def test_preview_nonexistent_file(self, tmp_path, mock_credentials, mock_opik, mock_remote_dataset):
        """Returns a descriptive message when the local file does not exist."""
        mock_opik.get_dataset.return_value = mock_remote_dataset
        ds = OpikEvaluationDataset(
            dataset_name="ds",
            credentials=mock_credentials,
            filepath=str(tmp_path / "missing.json"),
        )
        assert "does not exist" in str(ds.preview())

    def test_preview_no_filepath(self, dataset_remote):
        """Returns a descriptive message when no filepath is configured."""
        assert "No filepath configured" in str(dataset_remote.preview())
