from time import sleep

import optuna
import pytest
from kedro.io.core import DatasetError, Version, generate_timestamp

from kedro_datasets_experimental.optuna import StudyDataset


@pytest.fixture
def database_name(tmp_path):
    return (tmp_path / "optuna.db").as_posix()


@pytest.fixture
def study_dataset(database_name, load_args):
    return StudyDataset(
        study_name="test",
        backend="sqlite",
        database=database_name,
        load_args=load_args,
    )

@pytest.fixture
def versioned_study_dataset(database_name, load_args, load_version, save_version):
    return StudyDataset(
        study_name="test",
        backend="sqlite",
        database=database_name,
        load_args=load_args,
        version=Version(load_version, save_version)
    )


@pytest.fixture
def dummy_study():
    study = optuna.create_study("sqlite:///:memory:")
    trial = optuna.trial.create_trial(
        params={"x": 2.0},
        distributions={"x": optuna.distributions.FloatDistribution(0, 10)},
        value=4.0,
    )
    study.add_trial(trial)
    return study


class TestStudyDataset:
    def test_save_and_load(self, study_dataset, dummy_study):
        """Test saving and reloading the dataset."""
        study_dataset.save(dummy_study)
        reloaded = study_dataset.load()

        # Verify that the reloaded study has the same trials as the original.
        assert len(reloaded.trials) == len(dummy_study.trials)
        assert reloaded.trials[0].params["x"] == dummy_study.trials[0].params["x"]
        assert reloaded.trials[0].value == dummy_study.trials[0].value

    def test_exists(self, study_dataset, dummy_study):
        """Test `exists` method invocation for both existing and nonexistent dataset."""
        assert not study_dataset.exists()
        study_dataset.save(dummy_study)
        assert study_dataset.exists()

    def test_invalid_backend(self):
        """Test invalid backend raises ValueError."""
        with pytest.raises(ValueError, match="is not registered as an SQLAlchemy dialect"):
            StudyDataset(
                study_name="test",
                backend="invalid_backend",
                database="optuna.db",
            )

    def test_invalid_database(self):
        """Test invalid database raises ValueError."""
        with pytest.raises(ValueError, match="is not a string"):
            StudyDataset(
                study_name="test",
                backend="sqlite",
                database=123,
            )

        with pytest.raises(ValueError, match="does not have an extension"):
            StudyDataset(
                study_name="test",
                backend="sqlite",
                database="optuna",
            )

    def test_invalid_study_name(self):
        """Test invalid study name raises ValueError."""
        with pytest.raises(ValueError, match="is not a string"):
            StudyDataset(
                study_name=123,
                backend="sqlite",
                database="optuna.db",
            )

    def test_invalid_credentials(self):
        """Test invalid credentials raise ValueError."""
        with pytest.raises(ValueError, match="Incorrect `credentials`"):
            StudyDataset(
                study_name="test",
                backend="postgresql",
                database="optuna_db",
                credentials={"username": "user", "pwd": "pass"}, # pragma: allowlist secret
            )

    def test_study_name_exists(self, study_dataset, dummy_study):
        """Test `_study_name_exists` method."""
        assert not study_dataset._study_name_exists("test")
        study_dataset.save(dummy_study)
        assert study_dataset._study_name_exists("test")

    def test_study_name_glob(self, study_dataset, dummy_study):
        """Test `_study_name_glob` method."""
        study_dataset.save(dummy_study)
        study_names = list(study_dataset._study_name_glob("test"))
        assert "test" in study_names

    @pytest.mark.parametrize("load_args", [{"sampler": "test"}], indirect=True)
    def test_load_extra_params(self, study_dataset, load_args):
        """Test overriding the default load arguments."""
        for key, value in load_args.items():
            assert study_dataset._load_args[key] == value

    @pytest.mark.parametrize(
        "sampler_config, expected_class",
        [
            ({"class": "TPESampler", "n_startup_trials": 10}, optuna.samplers.TPESampler),
            ({"class": "RandomSampler", "seed": 42}, optuna.samplers.RandomSampler),
            ({"class": "QMCSampler", "qmc_type": "sobol", "independent_sampler": {"class": "RandomSampler", "seed": 42}}, optuna.samplers.QMCSampler),
            ({"class": "PartialFixedSampler", "fixed_params": {"x": 2.0}, "base_sampler": {"class": "RandomSampler", "seed": 42}}, optuna.samplers.PartialFixedSampler),
        ],
    )
    def test_get_sampler(self, study_dataset, sampler_config, expected_class):
        """Test `_get_sampler` method."""
        sampler = study_dataset._get_sampler(sampler_config)
        assert isinstance(sampler, expected_class)

        with pytest.raises(ValueError, match="Optuna `sampler` 'class' should be specified"):
            study_dataset._get_sampler({})

    @pytest.mark.parametrize(
        "pruner_config, expected_class",
        [
            ({"class": "NopPruner"}, optuna.pruners.NopPruner),
            ({"class": "MedianPruner", "n_startup_trials": 5}, optuna.pruners.MedianPruner),
            ({"class": "PatientPruner", "wrapped_pruner": {"class": "NopPruner"}, "patience": 10}, optuna.pruners.PatientPruner),
        ],
    )
    def test_get_pruner(self, study_dataset, pruner_config, expected_class):
        """Test `_get_pruner` method."""
        pruner = study_dataset._get_pruner(pruner_config)
        assert isinstance(pruner, expected_class)

        with pytest.raises(ValueError, match="Optuna `pruner` 'class' should be specified"):
            study_dataset._get_pruner({})


class TestStudyDatasetVersioned:
    def test_save_and_load(self, versioned_study_dataset, dummy_study):
        """Test that saved and reloaded data matches the original one for
        the versioned dataset."""
        versioned_study_dataset.save(dummy_study)
        reloaded_study = versioned_study_dataset.load()
        assert len(reloaded_study.trials) == len(dummy_study.trials)
        assert reloaded_study.trials[0].params["x"] == dummy_study.trials[0].params["x"]
        assert reloaded_study.trials[0].value == dummy_study.trials[0].value

    def test_version_str_repr(self, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        study_name = "test"
        ds = StudyDataset(study_name=study_name, backend="sqlite", database="optuna.db")
        ds_versioned = StudyDataset(
            study_name=study_name, backend="sqlite", database="optuna.db", version=Version(load_version, save_version)
        )
        assert study_name in str(ds)
        assert "version" not in str(ds)

        assert study_name in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "StudyDataset" in str(ds_versioned)
        assert "StudyDataset" in str(ds)

    def test_multiple_loads(self, versioned_study_dataset, dummy_study, database_name):
        """Test that if a new version is created mid-run, by an
        external system, it won't be loaded in the current run."""
        versioned_study_dataset.save(dummy_study)
        versioned_study_dataset.load()
        v1 = versioned_study_dataset.resolve_load_version()

        sleep(0.5)
        # force-drop a newer version into the same location
        v_new = generate_timestamp()
        StudyDataset(study_name="test", backend="sqlite", database=database_name, version=Version(v_new, v_new)).save(
            dummy_study
        )

        versioned_study_dataset.load()
        v2 = versioned_study_dataset.resolve_load_version()

        assert v2 == v1  # v2 should not be v_new!
        ds_new = StudyDataset(study_name="test", backend="sqlite", database=database_name, version=Version(None, None))
        assert (
            ds_new.resolve_load_version() == v_new
        )  # new version is discoverable by a new instance

    def test_multiple_saves(self, dummy_study, database_name):
        """Test multiple cycles of save followed by load for the same dataset"""
        ds_versioned = StudyDataset(study_name="test", backend="sqlite", database=database_name, version=Version(None, None))

        # first save
        ds_versioned.save(dummy_study)
        first_save_version = ds_versioned.resolve_save_version()
        first_load_version = ds_versioned.resolve_load_version()
        assert first_load_version == first_save_version

        # second save
        sleep(0.5)
        ds_versioned.save(dummy_study)
        second_save_version = ds_versioned.resolve_save_version()
        second_load_version = ds_versioned.resolve_load_version()
        assert second_load_version == second_save_version
        assert second_load_version > first_load_version

        # another dataset
        ds_new = StudyDataset(study_name="test", backend="sqlite", database=database_name, version=Version(None, None))
        assert ds_new.resolve_load_version() == second_load_version

    def test_release_instance_cache(self, dummy_study, database_name):
        """Test that cache invalidation does not affect other instances"""
        ds_a = StudyDataset(study_name="test", backend="sqlite", database=database_name, version=Version(None, None))
        assert ds_a._version_cache.currsize == 0
        ds_a.save(dummy_study)  # create a version
        assert ds_a._version_cache.currsize == 2

        ds_b = StudyDataset(study_name="test", backend="sqlite", database=database_name, version=Version(None, None))
        assert ds_b._version_cache.currsize == 0
        ds_b.resolve_save_version()
        assert ds_b._version_cache.currsize == 1
        ds_b.resolve_load_version()
        assert ds_b._version_cache.currsize == 2

        ds_a.release()

        # dataset A cache is cleared
        assert ds_a._version_cache.currsize == 0

        # dataset B cache is unaffected
        assert ds_b._version_cache.currsize == 2

    def test_no_versions(self, versioned_study_dataset):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for StudyDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            versioned_study_dataset.load()

    def test_exists(self, versioned_study_dataset, dummy_study):
        """Test `exists` method invocation for versioned dataset."""
        assert not versioned_study_dataset.exists()
        versioned_study_dataset.save(dummy_study)
        assert versioned_study_dataset.exists()

    def test_prevent_overwrite(self, versioned_study_dataset, dummy_study):
        """Check the error when attempting to override the dataset if the
        corresponding study for a given save version already exists."""
        versioned_study_dataset.save(dummy_study)
        pattern = (
            r"Study name \'.+\' for StudyDataset\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_study_dataset.save(dummy_study)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_study_dataset, load_version, save_version, dummy_study
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            rf"Save version '{save_version}' did not match load version "
            rf"'{load_version}' for StudyDataset\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_study_dataset.save(dummy_study)
