import shutil
from pathlib import PurePosixPath

import pypdf
import pytest
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io.core import PROTOCOL_DELIMITER, DatasetError
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from s3fs.core import S3FileSystem

from kedro_datasets_experimental.pypdf import PDFDataset


@pytest.fixture
def filepath_pdf(tmp_path):
    return (tmp_path / "test.pdf").as_posix()


@pytest.fixture
def pdf_dataset(filepath_pdf, load_args, fs_args):
    return PDFDataset(filepath=filepath_pdf, load_args=load_args, fs_args=fs_args)


@pytest.fixture
def dummy_pdf_data(tmp_path):
    """Create a simple PDF file for testing."""
    filepath = tmp_path / "test_dummy.pdf"

    # Create a simple PDF with pypdf
    writer = pypdf.PdfWriter()

    # Add page 1
    page1 = pypdf.PageObject.create_blank_page(width=200, height=200)
    writer.add_page(page1)

    # Add page 2
    page2 = pypdf.PageObject.create_blank_page(width=200, height=200)
    writer.add_page(page2)

    # Write to file
    with open(filepath, "wb") as f:
        writer.write(f)

    return filepath


@pytest.fixture
def dummy_pdf_with_text(tmp_path):
    """Create a PDF with actual text content."""
    filepath = tmp_path / "test_with_text.pdf"

    # Create PDF with reportlab
    c = canvas.Canvas(str(filepath), pagesize=letter)

    # Page 1
    c.drawString(100, 750, "This is page 1")
    c.drawString(100, 730, "Hello World")
    c.showPage()

    # Page 2
    c.drawString(100, 750, "This is page 2")
    c.drawString(100, 730, "Testing PDF Dataset")
    c.showPage()

    c.save()

    return filepath


class TestPDFDataset:
    def test_save_raises_error(self, pdf_dataset):
        """Test that saving raises an error."""
        pattern = r"Saving to PDFDataset is not supported\."
        with pytest.raises(DatasetError, match=pattern):
            pdf_dataset.save(["some", "data"])

    def test_load_pdf(self, dummy_pdf_data):
        """Test loading a PDF file."""
        dataset = PDFDataset(filepath=str(dummy_pdf_data))
        pages = dataset.load()

        assert isinstance(pages, list)
        assert len(pages) == 2  # Two pages created in dummy_pdf_data
        assert all(isinstance(page, str) for page in pages)

    def test_load_pdf_with_text(self, dummy_pdf_with_text):
        """Test loading a PDF with actual text content."""
        dataset = PDFDataset(filepath=str(dummy_pdf_with_text))
        pages = dataset.load()

        assert len(pages) == 2
        assert "page 1" in pages[0].lower()
        assert "page 2" in pages[1].lower()

    def test_exists(self, pdf_dataset, dummy_pdf_data):
        """Test `exists` method invocation for both existing and
        nonexistent dataset."""
        assert not pdf_dataset.exists()

        # Copy dummy PDF to the expected filepath
        shutil.copy(dummy_pdf_data, pdf_dataset._filepath)

        assert pdf_dataset.exists()

    @pytest.mark.parametrize("load_args", [{"strict": True}], indirect=True)
    def test_load_extra_params(self, pdf_dataset, load_args):
        """Test overriding the default load arguments."""
        for key, value in load_args.items():
            assert pdf_dataset._load_args[key] == value

    @pytest.mark.parametrize(
        "fs_args",
        [{"open_args_load": {"mode": "rb", "compression": "gzip"}}],
        indirect=True,
    )
    def test_open_extra_args(self, pdf_dataset, fs_args):
        assert pdf_dataset._fs_open_args_load == fs_args["open_args_load"]

    def test_load_missing_file(self, pdf_dataset):
        """Check the error when trying to load missing file."""
        pattern = r"Failed while loading data from dataset kedro_datasets_experimental.pypdf.pdf_dataset.PDFDataset\(.*\)"
        with pytest.raises(DatasetError, match=pattern):
            pdf_dataset.load()

    @pytest.mark.parametrize(
        "filepath,instance_type",
        [
            ("s3://bucket/file.pdf", S3FileSystem),
            ("file:///tmp/test.pdf", LocalFileSystem),
            ("/tmp/test.pdf", LocalFileSystem),  # nosec
            ("gcs://bucket/file.pdf", GCSFileSystem),
            ("https://example.com/file.pdf", HTTPFileSystem),
        ],
    )
    def test_protocol_usage(self, filepath, instance_type):
        dataset = PDFDataset(filepath=filepath)
        assert isinstance(dataset._fs, instance_type)

        path = filepath.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(dataset._filepath) == path
        assert isinstance(dataset._filepath, PurePosixPath)

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test.pdf"
        dataset = PDFDataset(filepath=filepath)
        dataset.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)
