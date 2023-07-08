import importlib

import pytest


@pytest.fixture()
def patch_kedro_io_core_dataset_error():
    kedro_io_core = importlib.import_module("kedro.io.core")

    try:
        dataset_error_new = kedro_io_core.DatasetError
        del kedro_io_core.DatasetError
        dataset_error_old = None
    except AttributeError:
        dataset_error_new = None
        dataset_error_old = kedro_io_core.DataSetError
        del kedro_io_core.DataSetError

    yield kedro_io_core

    # Restore
    if dataset_error_new:
        kedro_io_core.DatasetError = dataset_error_new
        try:
            del kedro_io_core.DataSetError
        except AttributeError:
            pass
    else:
        try:
            del kedro_io_core.DatasetError
        except AttributeError:
            pass
        kedro_io_core.DataSetError = dataset_error_old


@pytest.fixture()
def patch_kedro_io_core_abstract_dataset():
    kedro_io_core = importlib.import_module("kedro.io.core")

    try:
        abstract_dataset_new = kedro_io_core.AbstractDataset
        abstract_versioned_dataset_new = kedro_io_core.AbstractVersionedDataset
        del kedro_io_core.AbstractDataset
        del kedro_io_core.AbstractVersionedDataset
        abstract_dataset_old = None
        abstract_versioned_dataset_old = None
    except AttributeError:
        abstract_dataset_new = None
        abstract_versioned_dataset_new = None
        abstract_dataset_old = kedro_io_core.AbstractDataSet
        abstract_versioned_dataset_old = kedro_io_core.AbstractVersionedDataSet
        del kedro_io_core.AbstractDataSet
        del kedro_io_core.AbstractVersionedDataSet

    yield kedro_io_core

    # Restore
    if abstract_dataset_new:
        kedro_io_core.AbstractDataset = abstract_dataset_new
        kedro_io_core.AbstractVersionedDataset = abstract_versioned_dataset_new
        try:
            del kedro_io_core.AbstractDataSet
            del kedro_io_core.AbstractVersionedDataSet
        except AttributeError:
            pass
    else:
        try:
            del kedro_io_core.AbstractDataset
            del kedro_io_core.AbstractVersionedDataset
        except AttributeError:
            pass
        kedro_io_core.AbstractDataSet = abstract_dataset_old
        kedro_io_core.AbstractVersionedDataSet = abstract_versioned_dataset_old


@pytest.mark.parametrize("class_name", ["DatasetError", "DataSetError"])
def test_io_dataset_error_compatible(
    class_name, mocker, patch_kedro_io_core_dataset_error
):
    mocked_name = "MockedDatasetError"

    mocked_object = mocker.MagicMock()
    mocked_object.__name__ = mocked_name

    setattr(patch_kedro_io_core_dataset_error, class_name, mocked_object)

    kedro_datasets_io = importlib.import_module("kedro_datasets._io")
    importlib.reload(kedro_datasets_io)

    assert kedro_datasets_io.DatasetError.__name__ == mocked_name


@pytest.mark.parametrize(
    "class_names",
    [
        ("AbstractDataset", "AbstractVersionedDataset"),
        ("AbstractDataSet", "AbstractVersionedDataSet"),
    ],
)
def test_io_abstract_dataset_forwards_compatible(
    class_names, mocker, patch_kedro_io_core_abstract_dataset
):
    mocked_module_name_base = "MockedAbstractDataset"
    mocked_module_name_versioned = "MockedAbstractVersionedDataset"

    # Override the *new* classes, or create them if they don't exist
    for class_name, mocked_name in zip(
        class_names, (mocked_module_name_base, mocked_module_name_versioned)
    ):
        mocked_object = mocker.MagicMock()
        mocked_object.__name__ = mocked_name
        setattr(patch_kedro_io_core_abstract_dataset, class_name, mocked_object)

    kedro_datasets_io = importlib.import_module("kedro_datasets._io")
    importlib.reload(kedro_datasets_io)

    assert kedro_datasets_io.AbstractDataset.__name__ == mocked_module_name_base
    assert (
        kedro_datasets_io.AbstractVersionedDataset.__name__
        == mocked_module_name_versioned
    )
