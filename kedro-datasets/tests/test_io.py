import importlib


def test_io_dataset_error_forwards_compatible(mocker):
    mocked_module_name = "MockedDatasetError"

    kedro_io_core = importlib.import_module("kedro.io.core")

    # Override the *new* class, or create it if it doesn't exist
    kedro_io_core.DatasetError = mocker.MagicMock()
    kedro_io_core.DatasetError.__name__ = mocked_module_name

    kedro_datasets_io = importlib.import_module("kedro_datasets._io")
    importlib.reload(kedro_datasets_io)

    assert kedro_datasets_io.DatasetError.__name__ == mocked_module_name


def test_io_dataset_error_backwards_compatible(mocker):
    mocked_module_name = "MockedDatasetError"

    kedro_io_core = importlib.import_module("kedro.io.core")

    # Delete the *new* class if it exists
    try:
        del kedro_io_core.DatasetError
    except:
        pass

    # Override the *old* class, or create it if it doesn't exist
    kedro_io_core.DataSetError = mocker.MagicMock()
    kedro_io_core.DataSetError.__name__ = mocked_module_name

    kedro_datasets_io = importlib.import_module("kedro_datasets._io")
    importlib.reload(kedro_datasets_io)

    assert kedro_datasets_io.DatasetError.__name__ == mocked_module_name


def test_io_abstract_dataset_forwards_compatible(mocker):
    mocked_module_name_base = "MockedAbstractDataset"
    mocked_module_name_versioned = "MockedAbstractVersionedDataset"

    kedro_io_core = importlib.import_module("kedro.io.core")

    # Override the *new* classes, or create them if them don't exist
    kedro_io_core.AbstractDataset = mocker.MagicMock()
    kedro_io_core.AbstractDataset.__name__ = mocked_module_name_base
    kedro_io_core.AbstractVersionedDataset = mocker.MagicMock()
    kedro_io_core.AbstractVersionedDataset.__name__ = mocked_module_name_versioned

    kedro_datasets_io = importlib.import_module("kedro_datasets._io")
    importlib.reload(kedro_datasets_io)

    assert kedro_datasets_io.AbstractDataset.__name__ == mocked_module_name_base
    assert (
        kedro_datasets_io.AbstractVersionedDataset.__name__
        == mocked_module_name_versioned
    )


def test_io_abstract_dataset_backwards_compatible(mocker):
    mocked_module_name_base = "MockedAbstractDataset"
    mocked_module_name_versioned = "MockedAbstractVersionedDataset"

    kedro_io_core = importlib.import_module("kedro.io.core")

    # Delete the *new* classes if they exist
    try:
        del kedro_io_core.AbstractDataset
        del kedro_io_core.AbstractVersionedDataset
    except:
        pass

    # Override the *old* class, or create it if it doesn't exist
    kedro_io_core.AbstractDataSet = mocker.MagicMock()
    kedro_io_core.AbstractDataSet.__name__ = mocked_module_name_base
    kedro_io_core.AbstractVersionedDataSet = mocker.MagicMock()
    kedro_io_core.AbstractVersionedDataSet.__name__ = mocked_module_name_versioned

    kedro_datasets_io = importlib.import_module("kedro_datasets._io")
    importlib.reload(kedro_datasets_io)

    assert kedro_datasets_io.AbstractDataset.__name__ == mocked_module_name_base
    assert (
        kedro_datasets_io.AbstractVersionedDataset.__name__
        == mocked_module_name_versioned
    )
