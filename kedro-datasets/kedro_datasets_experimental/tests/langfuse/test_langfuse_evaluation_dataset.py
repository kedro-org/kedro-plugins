import json
from datetime import timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import yaml
from kedro.io import DatasetError
from langfuse.api import Error as LangfuseApiError
from langfuse.api import NotFoundError as LangfuseNotFoundError

from kedro_datasets_experimental.langfuse.langfuse_evaluation_dataset import (
    LangfuseEvaluationDataset,
)

MODULE = "kedro_datasets_experimental.langfuse.langfuse_evaluation_dataset"


@pytest.fixture
def mock_langfuse():
    """Mock Langfuse client for testing."""
    with patch(f"{MODULE}.Langfuse") as mock:
        langfuse_instance = Mock()
        mock.return_value = langfuse_instance
        yield langfuse_instance


@pytest.fixture
def mock_credentials():
    """Valid Langfuse credentials for testing."""
    return {
        "public_key": "pk_test_12345",
        "secret_key": "sk_test_67890",  # pragma: allowlist secret
        "host": "https://cloud.langfuse.com",
    }


@pytest.fixture
def filepath_json(tmp_path):
    """Create a temporary JSON file with evaluation items."""
    filepath = tmp_path / "eval_items.json"
    items = [
        {
            "id": "q1",
            "input": {"text": "cancel my order"},
            "expected_output": "cancel_order",
        },
        {
            "id": "q2",
            "input": {"text": "where is my package"},
            "expected_output": "track_order",
        },
    ]
    filepath.write_text(json.dumps(items, indent=2))
    return filepath.as_posix()


@pytest.fixture
def filepath_yaml(tmp_path):
    """Create a temporary YAML file with evaluation items."""
    filepath = tmp_path / "eval_items.yaml"
    items = [
        {
            "id": "q1",
            "input": {"text": "cancel my order"},
            "expected_output": "cancel_order",
        },
    ]
    filepath.write_text(yaml.dump(items))
    return filepath.as_posix()


@pytest.fixture
def mock_remote_dataset():
    """Mock DatasetClient with pre-existing items."""
    dataset = Mock()
    item1 = Mock()
    item1.id = "q1"
    item1.input = {"text": "cancel my order"}
    item1.expected_output = "cancel_order"
    item2 = Mock()
    item2.id = "q2"
    item2.input = {"text": "where is my package"}
    item2.expected_output = "track_order"
    dataset.items = [item1, item2]
    return dataset


@pytest.fixture
def empty_remote_dataset():
    """Mock DatasetClient with no items."""
    dataset = Mock()
    dataset.items = []
    return dataset


@pytest.fixture
def eval_dataset(filepath_json, mock_credentials, mock_langfuse):
    """Basic LangfuseEvaluationDataset for testing (local sync)."""
    return LangfuseEvaluationDataset(
        dataset_name="test-eval",
        credentials=mock_credentials,
        filepath=filepath_json,
        sync_policy="local",
    )


class TestInit:
    """Test LangfuseEvaluationDataset initialization."""

    def test_init_minimal_params(self, mock_credentials, mock_langfuse):
        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
        )
        assert ds._dataset_name == "test-eval"
        assert ds._filepath is None
        assert ds._sync_policy == "local"
        assert ds._metadata is None
        assert ds._version is None

    def test_init_all_params(self, filepath_json, mock_credentials, mock_langfuse):
        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
            filepath=filepath_json,
            sync_policy="remote",
            metadata={"project": "test"},
            version="2026-01-15T00:00:00Z",
        )
        assert ds._dataset_name == "test-eval"
        assert ds._filepath is not None
        assert ds._sync_policy == "remote"
        assert ds._metadata == {"project": "test"}
        assert ds._version is not None

    @pytest.mark.parametrize(
        "missing_key,credentials_dict",
        [
            ("public_key", {"secret_key": "sk_test_67890"}),  # pragma: allowlist secret
            ("secret_key", {"public_key": "pk_test_12345"}),
        ],
    )
    def test_init_missing_required_credentials(
        self, missing_key, credentials_dict, mock_langfuse
    ):
        with pytest.raises(
            DatasetError, match=f"Missing required Langfuse credential: '{missing_key}'"
        ):
            LangfuseEvaluationDataset(
                dataset_name="test-eval",
                credentials=credentials_dict,
            )

    @pytest.mark.parametrize("invalid_value", ["", "   "])
    def test_init_empty_credentials(self, invalid_value, mock_langfuse):
        creds = {
            "public_key": invalid_value,
            "secret_key": "sk_test_67890",  # pragma: allowlist secret
        }
        with pytest.raises(
            DatasetError, match="Langfuse credential 'public_key' cannot be empty"
        ):
            LangfuseEvaluationDataset(
                dataset_name="test-eval",
                credentials=creds,
            )

    def test_init_empty_host(self, mock_langfuse):
        creds = {
            "public_key": "pk_test_12345",
            "secret_key": "sk_test_67890",  # pragma: allowlist secret
            "host": "",
        }
        with pytest.raises(
            DatasetError, match="Langfuse credential 'host' cannot be empty if provided"
        ):
            LangfuseEvaluationDataset(
                dataset_name="test-eval",
                credentials=creds,
            )

    def test_init_invalid_sync_policy(self, mock_credentials, mock_langfuse):
        with pytest.raises(DatasetError, match="Invalid sync_policy 'bad'"):
            LangfuseEvaluationDataset(
                dataset_name="test-eval",
                credentials=mock_credentials,
                sync_policy="bad",
            )

    def test_init_unsupported_extension(self, tmp_path, mock_credentials, mock_langfuse):
        bad_file = tmp_path / "items.txt"
        bad_file.write_text("test")
        with pytest.raises(DatasetError, match="Unsupported file extension '.txt'"):
            LangfuseEvaluationDataset(
                dataset_name="test-eval",
                credentials=mock_credentials,
                filepath=str(bad_file),
            )

    def test_init_version_with_local_raises(self, mock_credentials, mock_langfuse):
        with pytest.raises(
            DatasetError,
            match="'version' parameter can only be used with sync_policy='remote'",
        ):
            LangfuseEvaluationDataset(
                dataset_name="test-eval",
                credentials=mock_credentials,
                sync_policy="local",
                version="2026-01-15T00:00:00Z",
            )

    def test_init_version_with_remote_accepted(self, mock_credentials, mock_langfuse):
        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
            sync_policy="remote",
            version="2026-01-15T00:00:00Z",
        )
        assert ds._version is not None

    def test_init_invalid_version_format(self, mock_credentials, mock_langfuse):
        with pytest.raises(DatasetError, match="Invalid version 'not-a-date'"):
            LangfuseEvaluationDataset(
                dataset_name="test-eval",
                credentials=mock_credentials,
                sync_policy="remote",
                version="not-a-date",
            )


class TestLoadLocal:
    """Test load() with sync_policy='local'."""

    def test_load_existing_remote_no_local_file(
        self, mock_credentials, mock_langfuse, mock_remote_dataset
    ):
        """Remote exists, no local file → returns remote as-is."""
        mock_langfuse.get_dataset.return_value = mock_remote_dataset

        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
            sync_policy="local",
        )
        result = ds.load()

        assert result is mock_remote_dataset
        mock_langfuse.create_dataset.assert_not_called()

    def test_load_existing_remote_local_file_upserts_all_items(
        self, filepath_json, mock_credentials, mock_langfuse
    ):
        """Remote exists → all local items are upserted (create or update)."""
        remote = Mock()
        remote_item = Mock()
        remote_item.id = "q1"
        remote.items = [remote_item]

        refreshed = Mock()
        refreshed.items = [remote_item, Mock()]
        mock_langfuse.get_dataset.side_effect = [remote, refreshed]

        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
            filepath=filepath_json,
            sync_policy="local",
        )
        result = ds.load()

        assert mock_langfuse.create_dataset_item.call_count == 2
        uploaded_ids = [
            call[1]["id"]
            for call in mock_langfuse.create_dataset_item.call_args_list
        ]
        assert "q1" in uploaded_ids
        assert "q2" in uploaded_ids
        assert result is refreshed

    def test_load_nonexistent_remote_creates_it(
        self, mock_credentials, mock_langfuse
    ):
        """Remote not found → creates dataset, then returns it."""
        not_found_body = Mock()
        not_found_body.message = "Not found"
        created = Mock()
        created.items = []

        mock_langfuse.get_dataset.side_effect = [
            LangfuseNotFoundError(body=not_found_body),
            created,
            created,
        ]

        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
            sync_policy="local",
        )
        result = ds.load()

        mock_langfuse.create_dataset.assert_called_once_with(
            name="test-eval", metadata={}
        )
        assert result is created

    def test_load_no_local_no_remote_creates_empty(
        self, tmp_path, mock_credentials, mock_langfuse
    ):
        """No local file, no remote → creates remote, returns empty dataset."""
        not_found_body = Mock()
        not_found_body.message = "Not found"
        empty_ds = Mock()
        empty_ds.items = []

        mock_langfuse.get_dataset.side_effect = [
            LangfuseNotFoundError(body=not_found_body),
            empty_ds,
            empty_ds,
        ]

        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
            filepath=(tmp_path / "nonexistent.json").as_posix(),
            sync_policy="local",
        )
        result = ds.load()
        assert len(result.items) == 0

    def test_load_idempotent_reload(
        self, filepath_json, mock_credentials, mock_langfuse, mock_remote_dataset
    ):
        """Each load upserts all local items to remote."""
        mock_langfuse.get_dataset.return_value = mock_remote_dataset

        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
            filepath=filepath_json,
            sync_policy="local",
        )
        ds.load()
        assert mock_langfuse.create_dataset_item.call_count == 2

        ds.load()
        assert mock_langfuse.create_dataset_item.call_count == 4


class TestLoadRemote:
    """Test load() with sync_policy='remote'."""

    def test_load_returns_dataset_client(
        self, mock_credentials, mock_langfuse, mock_remote_dataset
    ):
        mock_langfuse.get_dataset.return_value = mock_remote_dataset

        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
            sync_policy="remote",
        )
        result = ds.load()

        assert result is mock_remote_dataset

    def test_load_no_local_file_interaction(
        self, filepath_json, mock_credentials, mock_langfuse, mock_remote_dataset
    ):
        """Remote policy never reads or writes the local file."""
        mock_langfuse.get_dataset.return_value = mock_remote_dataset

        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
            filepath=filepath_json,
            sync_policy="remote",
        )
        result = ds.load()

        assert result is mock_remote_dataset
        assert ds._file_dataset is None
        mock_langfuse.create_dataset_item.assert_not_called()

    def test_load_without_version(
        self, mock_credentials, mock_langfuse, mock_remote_dataset
    ):
        mock_langfuse.get_dataset.return_value = mock_remote_dataset

        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
            sync_policy="remote",
        )
        ds.load()

        mock_langfuse.get_dataset.assert_called_with(name="test-eval")

    def test_load_with_version(
        self, mock_credentials, mock_langfuse, mock_remote_dataset
    ):
        """Versioned load passes the parsed datetime to get_dataset."""
        mock_langfuse.get_dataset.return_value = mock_remote_dataset

        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
            sync_policy="remote",
            version="2026-01-15T00:00:00Z",
        )
        ds.load()

        calls = mock_langfuse.get_dataset.call_args_list
        versioned_call = calls[-1]
        assert versioned_call[1].get("version") == ds._version


class TestSave:
    """Test save() behaviour."""

    def test_save_uploads_new_items(
        self, mock_credentials, mock_langfuse, empty_remote_dataset
    ):
        mock_langfuse.get_dataset.return_value = empty_remote_dataset

        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
            sync_policy="remote",
        )
        items = [{"id": "q1", "input": {"text": "hello"}}]
        ds.save(items)

        mock_langfuse.create_dataset_item.assert_called_once()
        call_kwargs = mock_langfuse.create_dataset_item.call_args[1]
        assert call_kwargs["id"] == "q1"
        assert call_kwargs["input"] == {"text": "hello"}

    def test_save_upserts_existing_ids(
        self, mock_credentials, mock_langfuse, mock_remote_dataset
    ):
        mock_langfuse.get_dataset.return_value = mock_remote_dataset

        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
            sync_policy="remote",
        )
        items = [{"id": "q1", "input": {"text": "cancel my order"}}]
        ds.save(items)

        mock_langfuse.create_dataset_item.assert_called_once()
        call_kwargs = mock_langfuse.create_dataset_item.call_args[1]
        assert call_kwargs["id"] == "q1"
        assert call_kwargs["input"] == {"text": "cancel my order"}

    def test_save_updates_existing_item_content(
        self, mock_credentials, mock_langfuse, mock_remote_dataset
    ):
        """Saving an item with an existing id but different input upserts it."""
        mock_langfuse.get_dataset.return_value = mock_remote_dataset

        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
            sync_policy="remote",
        )
        items = [{"id": "q1", "input": {"text": "updated input"}}]
        ds.save(items)

        mock_langfuse.create_dataset_item.assert_called_once()
        call_kwargs = mock_langfuse.create_dataset_item.call_args[1]
        assert call_kwargs["id"] == "q1"
        assert call_kwargs["input"] == {"text": "updated input"}

    def test_save_items_without_id_always_uploaded(
        self, mock_credentials, mock_langfuse, mock_remote_dataset
    ):
        mock_langfuse.get_dataset.return_value = mock_remote_dataset

        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
            sync_policy="remote",
        )
        items = [{"input": {"text": "no-id item"}}]

        with patch(f"{MODULE}.logger") as mock_logger:
            ds.save(items)
            mock_logger.warning.assert_called_once()
            assert "without an 'id' field" in mock_logger.warning.call_args[0][0]

        mock_langfuse.create_dataset_item.assert_called_once()

    def test_save_local_mode_merges_to_file(
        self, filepath_json, mock_credentials, mock_langfuse, empty_remote_dataset
    ):
        mock_langfuse.get_dataset.return_value = empty_remote_dataset

        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
            filepath=filepath_json,
            sync_policy="local",
        )
        new_items = [{"id": "q3", "input": {"text": "refund please"}}]
        ds.save(new_items)

        saved_data = json.loads(Path(filepath_json).read_text())
        ids = [item["id"] for item in saved_data]
        assert "q1" in ids
        assert "q2" in ids
        assert "q3" in ids

    def test_save_remote_mode_skips_file(
        self, filepath_json, mock_credentials, mock_langfuse, empty_remote_dataset
    ):
        """Remote policy save does not touch local file."""
        mock_langfuse.get_dataset.return_value = empty_remote_dataset

        original = Path(filepath_json).read_text()

        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
            filepath=filepath_json,
            sync_policy="remote",
        )
        ds.save([{"id": "new", "input": {"text": "new"}}])

        assert Path(filepath_json).read_text() == original

    def test_save_empty_list_no_op(
        self, mock_credentials, mock_langfuse, mock_remote_dataset
    ):
        mock_langfuse.get_dataset.return_value = mock_remote_dataset

        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
            sync_policy="remote",
        )
        ds.save([])
        mock_langfuse.create_dataset_item.assert_not_called()

    def test_save_missing_input_raises(
        self, mock_credentials, mock_langfuse, mock_remote_dataset
    ):
        mock_langfuse.get_dataset.return_value = mock_remote_dataset

        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
            sync_policy="remote",
        )
        with pytest.raises(DatasetError, match="missing required 'input' key"):
            ds.save([{"id": "q1", "expected_output": "test"}])


class TestPreview:
    """Test preview() output."""

    def test_preview_returns_json_preview_with_data(
        self, filepath_json, mock_credentials, mock_langfuse
    ):
        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
            filepath=filepath_json,
            sync_policy="local",
        )
        preview = ds.preview()
        parsed = json.loads(str(preview))
        assert isinstance(parsed, list)
        assert len(parsed) == 2
        assert parsed[0]["id"] == "q1"

    def test_preview_no_filepath(self, mock_credentials, mock_langfuse):
        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
        )
        preview = ds.preview()
        assert "No filepath configured" in str(preview)

    def test_preview_missing_file(self, tmp_path, mock_credentials, mock_langfuse):
        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
            filepath=(tmp_path / "missing.json").as_posix(),
        )
        preview = ds.preview()
        assert "does not exist" in str(preview)


class TestDescribe:
    """Test _describe() output."""

    def test_describe_returns_expected_keys(
        self, filepath_json, mock_credentials, mock_langfuse
    ):
        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
            filepath=filepath_json,
            sync_policy="local",
            metadata={"project": "test"},
        )
        desc = ds._describe()

        assert desc["dataset_name"] == "test-eval"
        assert filepath_json in desc["filepath"]
        assert desc["sync_policy"] == "local"
        assert desc["version"] is None
        assert desc["metadata"] == {"project": "test"}

    def test_describe_no_credentials_in_output(
        self, mock_credentials, mock_langfuse
    ):
        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
        )
        desc = ds._describe()
        desc_str = str(desc)
        assert "pk_test_12345" not in desc_str
        assert "sk_test_67890" not in desc_str


class TestExists:
    """Test _exists() behaviour."""

    def test_exists_true(self, mock_credentials, mock_langfuse, mock_remote_dataset):
        mock_langfuse.get_dataset.return_value = mock_remote_dataset
        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
        )
        assert ds._exists() is True

    def test_exists_false(self, mock_credentials, mock_langfuse):
        not_found_body = Mock()
        not_found_body.message = "Not found"
        mock_langfuse.get_dataset.side_effect = LangfuseNotFoundError(
            body=not_found_body
        )
        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
        )
        assert ds._exists() is False

    def test_exists_api_error_raises(self, mock_credentials, mock_langfuse):
        mock_langfuse.get_dataset.side_effect = LangfuseApiError(body="Server error")
        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
        )
        with pytest.raises(DatasetError, match="Langfuse API error"):
            ds._exists()


class TestHelpers:
    """Test static helper methods in isolation."""

    def test_validate_credentials_valid(self):
        LangfuseEvaluationDataset._validate_credentials(
            {"public_key": "pk", "secret_key": "sk"}  # pragma: allowlist secret
        )

    def test_validate_credentials_missing_key(self):
        with pytest.raises(DatasetError, match="Missing required"):
            LangfuseEvaluationDataset._validate_credentials({"public_key": "pk"})

    def test_validate_credentials_empty_value(self):
        with pytest.raises(DatasetError, match="cannot be empty"):
            LangfuseEvaluationDataset._validate_credentials(
                {"public_key": "", "secret_key": "sk"}  # pragma: allowlist secret
            )

    def test_validate_credentials_empty_host(self):
        with pytest.raises(DatasetError, match="cannot be empty if provided"):
            LangfuseEvaluationDataset._validate_credentials(
                {"public_key": "pk", "secret_key": "sk", "host": "  "}  # pragma: allowlist secret
            )

    @pytest.mark.parametrize("policy", ["local", "remote"])
    def test_validate_sync_policy_valid(self, policy):
        LangfuseEvaluationDataset._validate_sync_policy(policy)

    def test_validate_sync_policy_invalid(self):
        with pytest.raises(DatasetError, match="Invalid sync_policy"):
            LangfuseEvaluationDataset._validate_sync_policy("strict")

    @pytest.mark.parametrize("ext", [".json", ".yaml", ".yml"])
    def test_validate_filepath_valid(self, tmp_path, ext):
        LangfuseEvaluationDataset._validate_filepath(str(tmp_path / f"items{ext}"))

    def test_validate_filepath_none(self):
        LangfuseEvaluationDataset._validate_filepath(None)

    def test_validate_filepath_invalid(self, tmp_path):
        with pytest.raises(DatasetError, match="Unsupported file extension"):
            LangfuseEvaluationDataset._validate_filepath(str(tmp_path / "items.csv"))

    def test_validate_items_valid(self):
        LangfuseEvaluationDataset._validate_items(
            [{"input": "a"}, {"input": "b"}]
        )

    def test_validate_items_missing_input(self):
        with pytest.raises(DatasetError, match="index 1 is missing required 'input'"):
            LangfuseEvaluationDataset._validate_items(
                [{"input": "ok"}, {"expected_output": "bad"}]
            )

    def test_merge_items_no_overlap(self):
        existing = [{"id": "a", "input": "x"}]
        new = [{"id": "b", "input": "y"}]
        merged = LangfuseEvaluationDataset._merge_items(existing, new)
        assert len(merged) == 2
        assert [m["id"] for m in merged] == ["a", "b"]

    def test_merge_items_new_takes_precedence(self):
        existing = [{"id": "a", "input": "x"}]
        new = [{"id": "a", "input": "updated"}]
        merged = LangfuseEvaluationDataset._merge_items(existing, new)
        assert len(merged) == 1
        assert merged[0]["input"] == "updated"

    def test_merge_items_without_id_appended(self):
        existing = [{"id": "a", "input": "x"}]
        new = [{"input": "no-id"}]
        merged = LangfuseEvaluationDataset._merge_items(existing, new)
        assert len(merged) == 2

    def test_merge_items_empty_existing(self):
        merged = LangfuseEvaluationDataset._merge_items(
            [], [{"id": "a", "input": "x"}]
        )
        assert len(merged) == 1

    def test_merge_items_empty_new(self):
        existing = [{"id": "a", "input": "x"}]
        merged = LangfuseEvaluationDataset._merge_items(existing, [])
        assert merged == existing

    def test_parse_version_none(self):
        assert LangfuseEvaluationDataset._parse_version(None) is None

    def test_parse_version_valid(self):
        result = LangfuseEvaluationDataset._parse_version("2026-01-15T00:00:00Z")
        assert result is not None
        assert result.year == 2026

    def test_parse_version_naive_gets_utc(self):
        result = LangfuseEvaluationDataset._parse_version("2026-01-15T00:00:00")
        assert result.tzinfo == timezone.utc

    def test_parse_version_invalid(self):
        with pytest.raises(DatasetError, match="Invalid version"):
            LangfuseEvaluationDataset._parse_version("bad")

    @pytest.mark.parametrize(
        "filepath_fixture,expected_class",
        [
            ("filepath_json", "JSONDataset"),
            ("filepath_yaml", "YAMLDataset"),
        ],
    )
    def test_file_dataset_property(
        self, request, mock_credentials, mock_langfuse, filepath_fixture, expected_class
    ):
        filepath = request.getfixturevalue(filepath_fixture)
        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
            filepath=filepath,
        )
        assert ds.file_dataset.__class__.__name__ == expected_class

    def test_file_dataset_caching(self, filepath_json, mock_credentials, mock_langfuse):
        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
            filepath=filepath_json,
        )
        fd1 = ds.file_dataset
        fd2 = ds.file_dataset
        assert fd1 is fd2

    def test_file_dataset_no_filepath_raises(self, mock_credentials, mock_langfuse):
        ds = LangfuseEvaluationDataset(
            dataset_name="test-eval",
            credentials=mock_credentials,
        )
        with pytest.raises(DatasetError, match="filepath must be provided"):
            _ = ds.file_dataset
