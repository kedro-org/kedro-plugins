import inspect
import json
import warnings
from pathlib import Path

import boto3
import matplotlib
import matplotlib.pyplot as plt
import pytest
from kedro.io import DatasetError, Version
from moto import mock_aws
from s3fs import S3FileSystem

from kedro_datasets import KedroDeprecationWarning
from kedro_datasets.matplotlib import MatplotlibDataset, MatplotlibWriter

BUCKET_NAME = "test_bucket"
AWS_CREDENTIALS = {"key": "testing", "secret": "testing"}  # pragma: allowlist secret
KEY_PATH = "matplotlib"
COLOUR_LIST = ["blue", "green", "red"]
FULL_PATH = f"s3://{BUCKET_NAME}/{KEY_PATH}"

matplotlib.use("Agg")  # Disable interactive mode


@pytest.fixture
def mock_single_plot():
    plt.plot([1, 2, 3], [4, 5, 6])
    plt.close("all")
    return plt


@pytest.fixture
def mock_list_plot():
    plots_list = []
    colour = "red"
    for index in range(5):
        plots_list.append(plt.figure())
        plt.plot([1, 2, 3], [4, 5, 6], color=colour)
    plt.close("all")
    return plots_list


@pytest.fixture
def mock_dict_plot():
    plots_dict = {}
    for colour in COLOUR_LIST:
        plots_dict[colour] = plt.figure()
        plt.plot([1, 2, 3], [4, 5, 6], color=colour)
    plt.close("all")
    return plots_dict


@pytest.fixture
def mocked_s3_bucket():
    """Create a bucket for testing using moto."""
    with mock_aws():
        conn = boto3.client(
            "s3",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",  # pragma: allowlist secret
        )
        conn.create_bucket(Bucket=BUCKET_NAME)
        yield conn


@pytest.fixture
def mocked_encrypted_s3_bucket():
    bucket_policy = {
        "Version": "2012-10-17",
        "Id": "PutObjPolicy",
        "Statement": [
            {
                "Sid": "DenyUnEncryptedObjectUploads",
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": f"arn:aws:s3:::{BUCKET_NAME}/*",
                "Condition": {"Null": {"s3:x-amz-server-side-encryption": "aws:kms"}},
            }
        ],
    }
    bucket_policy = json.dumps(bucket_policy)

    with mock_aws():
        conn = boto3.client(
            "s3",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",  # pragma: allowlist secret
        )
        conn.create_bucket(Bucket=BUCKET_NAME)
        conn.put_bucket_policy(Bucket=BUCKET_NAME, Policy=bucket_policy)
        yield conn


@pytest.fixture()
def s3fs_cleanup():
    # clear cache for clean mocked s3 bucket each time
    yield
    S3FileSystem.cachable = False


@pytest.fixture(params=[False])
def overwrite(request):
    return request.param


@pytest.fixture
def plot_dataset(mocked_s3_bucket, fs_args, save_args, overwrite):
    return MatplotlibDataset(
        filepath=FULL_PATH,
        credentials=AWS_CREDENTIALS,
        fs_args=fs_args,
        save_args=save_args,
        overwrite=overwrite,
    )


@pytest.fixture
def plot_writer(mocked_s3_bucket, fs_args, save_args, overwrite):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return MatplotlibWriter(
            filepath=FULL_PATH,
            credentials=AWS_CREDENTIALS,
            fs_args=fs_args,
            save_args=save_args,
            overwrite=overwrite,
        )


@pytest.fixture
def versioned_plot_dataset(tmp_path, load_version, save_version):
    filepath = (tmp_path / "matplotlib.png").as_posix()
    return MatplotlibDataset(
        filepath=filepath, version=Version(load_version, save_version)
    )


@pytest.fixture(autouse=True)
def cleanup_plt():
    yield
    plt.close("all")


class TestMatplotlibDataset:
    def test_writer_deprecation_warning(self, tmp_path):
        """Test that MatplotlibWriter raises a deprecation warning"""
        with pytest.warns(
            KedroDeprecationWarning, match="renamed to MatplotlibDataset"
        ):
            MatplotlibWriter(filepath=tmp_path / "test.png")

    def test_writer_is_dataset_subclass(self, plot_writer):
        """Test that MatplotlibWriter is a subclass of MatplotlibDataset"""
        assert isinstance(plot_writer, MatplotlibDataset)
        assert isinstance(plot_writer, MatplotlibWriter)

    @pytest.mark.parametrize("save_args", [{"format": "png"}], indirect=True)
    def test_save_data(
        self, tmp_path, mock_single_plot, plot_dataset, mocked_s3_bucket, save_args
    ):
        """Test saving single matplotlib plot to S3."""
        plot_dataset.save(mock_single_plot)

        download_path = tmp_path / "downloaded_image.png"
        actual_filepath = tmp_path / "locally_saved.png"

        mock_single_plot.savefig(str(actual_filepath))

        mocked_s3_bucket.download_file(BUCKET_NAME, KEY_PATH, str(download_path))

        assert actual_filepath.read_bytes() == download_path.read_bytes()
        assert plot_dataset._fs_open_args_save == {"mode": "wb"}
        for key, value in save_args.items():
            assert plot_dataset._save_args[key] == value

    def test_list_save(self, tmp_path, mock_list_plot, plot_dataset, mocked_s3_bucket):
        """Test saving list of plots to S3."""

        plot_dataset.save(mock_list_plot)

        for index in range(5):
            download_path = tmp_path / "downloaded_image.png"
            actual_filepath = tmp_path / "locally_saved.png"

            mock_list_plot[index].savefig(str(actual_filepath))
            _key_path = f"{KEY_PATH}/{index}.png"
            mocked_s3_bucket.download_file(BUCKET_NAME, _key_path, str(download_path))

            assert actual_filepath.read_bytes() == download_path.read_bytes()

    def test_dict_save(self, tmp_path, mock_dict_plot, plot_dataset, mocked_s3_bucket):
        """Test saving dictionary of plots to S3."""

        plot_dataset.save(mock_dict_plot)

        for colour in COLOUR_LIST:
            download_path = tmp_path / "downloaded_image.png"
            actual_filepath = tmp_path / "locally_saved.png"

            mock_dict_plot[colour].savefig(str(actual_filepath))

            _key_path = f"{KEY_PATH}/{colour}"

            mocked_s3_bucket.download_file(BUCKET_NAME, _key_path, str(download_path))

            assert actual_filepath.read_bytes() == download_path.read_bytes()

    @pytest.mark.parametrize(
        "overwrite,expected_num_plots", [(False, 8), (True, 3)], indirect=["overwrite"]
    )
    def test_overwrite(
        self,
        mock_list_plot,
        mock_dict_plot,
        plot_dataset,
        mocked_s3_bucket,
        expected_num_plots,
    ):
        """Test saving dictionary of plots after list of plots to S3."""

        plot_dataset.save(mock_list_plot)
        plot_dataset.save(mock_dict_plot)

        response = mocked_s3_bucket.list_objects(Bucket=BUCKET_NAME)
        saved_plots = {obj["Key"] for obj in response["Contents"]}

        assert {f"{KEY_PATH}/{colour}" for colour in COLOUR_LIST} <= saved_plots
        assert len(saved_plots) == expected_num_plots

    def test_fs_args(self, tmp_path, mock_single_plot, mocked_encrypted_s3_bucket):
        """Test writing to encrypted bucket."""
        encrypted_dataset = MatplotlibDataset(
            fs_args={"s3_additional_kwargs": {"ServerSideEncryption": "AES256"}},
            filepath=FULL_PATH,
            credentials=AWS_CREDENTIALS,
        )

        encrypted_dataset.save(mock_single_plot)

        download_path = tmp_path / "downloaded_image.png"
        actual_filepath = tmp_path / "locally_saved.png"

        mock_single_plot.savefig(str(actual_filepath))

        mocked_encrypted_s3_bucket.download_file(
            BUCKET_NAME, KEY_PATH, str(download_path)
        )

        assert actual_filepath.read_bytes() == download_path.read_bytes()

    @pytest.mark.parametrize(
        "fs_args",
        [{"open_args_save": {"mode": "w", "compression": "gzip"}}],
        indirect=True,
    )
    def test_open_extra_args(self, plot_dataset, fs_args):
        assert plot_dataset._fs_open_args_save == fs_args["open_args_save"]

    def test_load_fail(self, plot_dataset):
        pattern = r"Loading not supported for 'MatplotlibDataset'"
        with pytest.raises(DatasetError, match=pattern):
            plot_dataset.load()

    def test_writer_load_fail(self, plot_writer):
        """Test that the deprecated writer raises appropriate error on load."""
        pattern = r"Loading not supported for 'MatplotlibWriter'"
        with pytest.raises(DatasetError, match=pattern):
            plot_writer.load()

    @pytest.mark.usefixtures("s3fs_cleanup")
    def test_exists_single(self, mock_single_plot, plot_dataset):
        assert not plot_dataset.exists()
        plot_dataset.save(mock_single_plot)
        assert plot_dataset.exists()

    @pytest.mark.usefixtures("s3fs_cleanup")
    def test_exists_multiple(self, mock_dict_plot, plot_dataset):
        assert not plot_dataset.exists()
        plot_dataset.save(mock_dict_plot)
        assert plot_dataset.exists()

    def test_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        dataset = MatplotlibDataset(filepath=FULL_PATH)
        dataset.release()
        fs_mock.invalidate_cache.assert_called_once_with(f"{BUCKET_NAME}/{KEY_PATH}")

    def test_preview(self, mock_single_plot, plot_dataset):
        plot_dataset.save(mock_single_plot)
        # Define the expected beginning of the base64 encoded image string
        expected_beginning = "iVBORw0KGgoAAAANSUh"
        preview = plot_dataset.preview()
        assert preview.startswith(expected_beginning)
        assert (
            inspect.signature(plot_dataset.preview).return_annotation == "ImagePreview"
        )

    # Test that deprecated writer also works
    def test_writer_save_data(
        self, tmp_path, mock_single_plot, plot_writer, mocked_s3_bucket
    ):
        """Test saving with deprecated writer class still works."""
        plot_writer.save(mock_single_plot)

        download_path = tmp_path / "downloaded_image.png"
        actual_filepath = tmp_path / "locally_saved.png"

        mock_single_plot.savefig(str(actual_filepath))

        mocked_s3_bucket.download_file(BUCKET_NAME, KEY_PATH, str(download_path))

        assert actual_filepath.read_bytes() == download_path.read_bytes()


class TestMatplotlibDatasetVersioned:
    def test_version_str_repr(self, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = "chart.png"
        chart = MatplotlibDataset(filepath=filepath)
        chart_versioned = MatplotlibDataset(
            filepath=filepath, version=Version(load_version, save_version)
        )
        assert filepath in str(chart)
        assert "version" not in str(chart)

        assert filepath in str(chart_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(chart_versioned)

    def test_prevent_overwrite(self, mock_single_plot, versioned_plot_dataset):
        """Check the error when attempting to override the dataset if the
        corresponding matplotlib file for a given save version already exists."""
        versioned_plot_dataset.save(mock_single_plot)
        pattern = (
            r"Save path \'.+\' for kedro_datasets.matplotlib.matplotlib_dataset.MatplotlibDataset\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_plot_dataset.save(mock_single_plot)

    def test_ineffective_overwrite(self, load_version, save_version):
        pattern = (
            "Setting 'overwrite=True' is ineffective if versioning "
            "is enabled, since the versioned path must not already "
            "exist; overriding flag with 'overwrite=False' instead."
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_dataset = MatplotlibDataset(
                filepath="/tmp/file.txt",
                version=Version(load_version, save_version),
                overwrite=True,
            )
        assert not versioned_dataset._overwrite

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, load_version, save_version, mock_single_plot, versioned_plot_dataset
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            rf"Save version '{save_version}' did not match load version "
            rf"'{load_version}' for kedro_datasets.matplotlib.matplotlib_dataset.MatplotlibDataset\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_plot_dataset.save(mock_single_plot)

    def test_http_filesystem_no_versioning(self):
        pattern = "Versioning is not supported for HTTP protocols."

        with pytest.raises(DatasetError, match=pattern):
            MatplotlibDataset(
                filepath="https://example.com/file.png", version=Version(None, None)
            )

    def test_load_not_supported(self, versioned_plot_dataset):
        """Check the error if no versions are available for load."""
        pattern = (
            rf"Loading not supported for '{versioned_plot_dataset.__class__.__name__}'"
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_plot_dataset.load()

    def test_exists(self, versioned_plot_dataset, mock_single_plot):
        """Test `exists` method invocation for versioned dataset."""
        assert not versioned_plot_dataset.exists()
        versioned_plot_dataset.save(mock_single_plot)
        assert versioned_plot_dataset.exists()

    def test_exists_multiple(self, versioned_plot_dataset, mock_list_plot):
        """Test `exists` method invocation for versioned dataset."""
        assert not versioned_plot_dataset.exists()
        versioned_plot_dataset.save(mock_list_plot)
        assert versioned_plot_dataset.exists()

    def test_save_data(self, versioned_plot_dataset, mock_single_plot, tmp_path):
        """Test saving dictionary of plots with enabled versioning."""
        versioned_plot_dataset.save(mock_single_plot)

        test_path = tmp_path / "test_image.png"
        actual_filepath = Path(versioned_plot_dataset._get_load_path().as_posix())

        plt.savefig(str(test_path))

        assert actual_filepath.read_bytes() == test_path.read_bytes()

    def test_list_save(self, tmp_path, mock_list_plot, versioned_plot_dataset):
        """Test saving list of plots to with enabled versioning."""

        versioned_plot_dataset.save(mock_list_plot)

        for index in range(5):
            test_path = tmp_path / "test_image.png"
            versioned_filepath = str(versioned_plot_dataset._get_load_path())

            mock_list_plot[index].savefig(str(test_path))
            actual_filepath = Path(f"{versioned_filepath}/{index}.png")

            assert actual_filepath.read_bytes() == test_path.read_bytes()

    def test_dict_save(self, tmp_path, mock_dict_plot, versioned_plot_dataset):
        """Test saving dictionary of plots with enabled versioning."""

        versioned_plot_dataset.save(mock_dict_plot)

        for colour in COLOUR_LIST:
            test_path = tmp_path / "test_image.png"
            versioned_filepath = str(versioned_plot_dataset._get_load_path())

            mock_dict_plot[colour].savefig(str(test_path))
            actual_filepath = Path(f"{versioned_filepath}/{colour}")

            assert actual_filepath.read_bytes() == test_path.read_bytes()

    def test_versioning_existing_dataset_single_plot(
        self, plot_dataset, versioned_plot_dataset, mock_single_plot
    ):
        """Check the error when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset, using a single plot."""

        plot_dataset = MatplotlibDataset(
            filepath=versioned_plot_dataset._filepath.as_posix()
        )
        plot_dataset.save(mock_single_plot)
        assert plot_dataset.exists()
        pattern = (
            f"(?=.*file with the same name already exists in the directory)"
            f"(?=.*{versioned_plot_dataset._filepath.parent.as_posix()})"
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_plot_dataset.save(mock_single_plot)

        # Remove non-versioned dataset and try again
        Path(plot_dataset._filepath.as_posix()).unlink()
        versioned_plot_dataset.save(mock_single_plot)
        assert versioned_plot_dataset.exists()

    def test_versioning_existing_dataset_list_plot(
        self, plot_dataset, versioned_plot_dataset, mock_list_plot
    ):
        """Check the behavior when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset, using a list of plots. Note: because
        a list of plots saves to a directory, an error is not expected."""
        plot_dataset = MatplotlibDataset(
            filepath=versioned_plot_dataset._filepath.as_posix()
        )
        plot_dataset.save(mock_list_plot)
        assert plot_dataset.exists()
        versioned_plot_dataset.save(mock_list_plot)
        assert versioned_plot_dataset.exists()

    def test_versioning_existing_dataset_dict_plot(
        self, plot_dataset, versioned_plot_dataset, mock_dict_plot
    ):
        """Check the behavior when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset, using a dict of plots. Note: because
        a dict of plots saves to a directory, an error is not expected."""
        plot_dataset = MatplotlibDataset(
            filepath=versioned_plot_dataset._filepath.as_posix()
        )
        plot_dataset.save(mock_dict_plot)
        assert plot_dataset.exists()
        versioned_plot_dataset.save(mock_dict_plot)
        assert versioned_plot_dataset.exists()
