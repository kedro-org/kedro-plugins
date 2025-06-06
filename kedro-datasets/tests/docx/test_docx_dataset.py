from pathlib import Path, PurePosixPath

import pytest
from docx import Document
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io.core import PROTOCOL_DELIMITER, DatasetError, Version
from s3fs.core import S3FileSystem

from kedro_datasets.docx import DocxDataset


@pytest.fixture
def filepath_docx(tmp_path):
    return (tmp_path / "test.docx").as_posix()


@pytest.fixture
def docx_dataset(filepath_docx, fs_args):
    return DocxDataset(filepath=filepath_docx, fs_args=fs_args)


@pytest.fixture
def versioned_docx_dataset(filepath_docx, load_version, save_version):
    return DocxDataset(
        filepath=filepath_docx, version=Version(load_version, save_version)
    )


@pytest.fixture
def dummy_data() -> Document:
    test_doc = Document()
    test_doc.add_paragraph("Hello, this is dummy data docx document.")
    return test_doc


class TestDocxDataset:
    def test_save_and_load(self, docx_dataset, dummy_data):
        """Test saving and reloading the dataset."""
        docx_dataset.save(dummy_data)
        reloaded = docx_dataset.load()
        assert dummy_data.paragraphs[0].text == reloaded.paragraphs[0].text
        assert docx_dataset._fs_open_args_load == {}
        assert docx_dataset._fs_open_args_save == {"mode": "wb"}

    def test_exists(self, docx_dataset, dummy_data):
        """Test `exists` method invocation for both existing and
        nonexistent dataset."""
        assert not docx_dataset.exists()
        docx_dataset.save(dummy_data)
        assert docx_dataset.exists()

    @pytest.mark.parametrize(
        "fs_args",
        [{"open_args_load": {"mode": "rb", "compression": "gzip"}}],
        indirect=True,
    )
    def test_open_extra_args(self, docx_dataset, fs_args):
        assert docx_dataset._fs_open_args_load == fs_args["open_args_load"]
        assert docx_dataset._fs_open_args_save == {"mode": "wb"}  # default unchanged

    def test_load_missing_file(self, docx_dataset):
        """Check the error when trying to load missing file."""
        pattern = r"Failed while loading data from dataset DocxDataset\(.*\)"
        with pytest.raises(DatasetError, match=pattern):
            docx_dataset.load()

    @pytest.mark.parametrize(
        "filepath, instance_type",
        [
            ("s3://bucket/file.docx", S3FileSystem),
            ("file://{tmp}/test.docx", LocalFileSystem),
            ("{tmp}/test.docx", LocalFileSystem),
            ("gcs://bucket/file.docx", GCSFileSystem),
            ("https://example.com/file.docx", HTTPFileSystem),
        ],
    )
    def test_protocol_usage(self, filepath, instance_type, tmp_path):
        filepath = filepath.format(tmp=tmp_path.as_posix())
        dataset = DocxDataset(filepath=filepath)

        assert isinstance(dataset._fs, instance_type)

        path = filepath.split(PROTOCOL_DELIMITER, 1)[-1]

        assert Path(dataset._filepath).resolve() == Path(path).resolve()
        assert isinstance(dataset._filepath, PurePosixPath)

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test.docx"
        dataset = DocxDataset(filepath=filepath)
        dataset.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)


class TestDocxDatasetVersioned:
    def test_version_str_repr(self, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = "test.docx"
        ds = DocxDataset(filepath=filepath)
        ds_versioned = DocxDataset(
            filepath=filepath, version=Version(load_version, save_version)
        )
        assert filepath in str(ds)
        assert "version" not in str(ds)

        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "DocxDataset" in str(ds_versioned)
        assert "DocxDataset" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)

    def test_save_and_load(self, versioned_docx_dataset, dummy_data):
        """Test that saved and reloaded data matches the original one for
        the versioned dataset."""
        versioned_docx_dataset.save(dummy_data)
        reloaded = versioned_docx_dataset.load()
        assert dummy_data.paragraphs[0].text == reloaded.paragraphs[0].text

    def test_no_versions(self, versioned_docx_dataset):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for DocxDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            versioned_docx_dataset.load()

    def test_exists(self, versioned_docx_dataset, dummy_data):
        """Test `exists` method invocation for versioned dataset."""
        assert not versioned_docx_dataset.exists()
        versioned_docx_dataset.save(dummy_data)
        assert versioned_docx_dataset.exists()

    def test_prevent_overwrite(self, versioned_docx_dataset, dummy_data):
        """Check the error when attempting to override the dataset if the
        corresponding docx file for a given save version already exists."""
        versioned_docx_dataset.save(dummy_data)
        pattern = (
            r"Save path \'.+\' for DocxDataset\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_docx_dataset.save(dummy_data)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_docx_dataset, load_version, save_version, dummy_data
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            rf"Save version '{save_version}' did not match load version "
            rf"'{load_version}' for DocxDataset\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_docx_dataset.save(dummy_data)

    def test_http_filesystem_no_versioning(self):
        pattern = "Versioning is not supported for HTTP protocols."

        with pytest.raises(DatasetError, match=pattern):
            DocxDataset(
                filepath="https://example.com/file.docx", version=Version(None, None)
            )

    def test_versioning_existing_dataset(
        self, docx_dataset, versioned_docx_dataset, dummy_data
    ):
        """Check the error when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset."""
        docx_dataset.save(dummy_data)
        assert docx_dataset.exists()
        assert docx_dataset._filepath == versioned_docx_dataset._filepath
        pattern = (
            f"(?=.*file with the same name already exists in the directory)"
            f"(?=.*{versioned_docx_dataset._filepath.parent.as_posix()})"
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_docx_dataset.save(dummy_data)

        # Remove non-versioned dataset and try again
        Path(docx_dataset._filepath.as_posix()).unlink()
        versioned_docx_dataset.save(dummy_data)
        assert versioned_docx_dataset.exists()
