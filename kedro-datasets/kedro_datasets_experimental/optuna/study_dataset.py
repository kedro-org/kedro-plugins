"""``StudyDataset`` loads/saves data from/to an optuna Study."""

from __future__ import annotations

import fnmatch
import logging
import os
from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any

import optuna
from kedro.io.core import (
    AbstractVersionedDataset,
    DatasetError,
    Version,
)
from sqlalchemy import URL
from sqlalchemy.dialects import registry

logger = logging.getLogger(__name__)


class StudyDataset(AbstractVersionedDataset[optuna.Study, optuna.Study]):
    """``StudyDataset`` loads/saves data from/to an optuna Study.

    Example usage for the
    `YAML API <https://docs.kedro.org/en/stable/data/data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

        review_prediction_study:
          type: optuna.StudyDataset
          backend: sqlite
          database: data/05_model_input/review_prediction_study.db
          load_args:
            sampler:
              class: TPESampler
              n_startup_trials: 10
              n_ei_candidates: 5
            pruner:
              class: NopPruner
          versioned: true

        price_prediction_study:
          type: optuna.StudyDataset
          backend: postgresql
          database: optuna_db
          credentials: dev_optuna_postgresql

    Example usage for the
    `Python API <https://docs.kedro.org/en/stable/data/\
    advanced_data_catalog_usage.html>`_:

    .. code-block:: pycon

        >>> from kedro_datasets.optuna import StudyDataset
        >>> from optuna.distributions import FloatDistribution
        >>> import optuna
        >>>
        >>> study = optuna.create_study()
        >>> trial = optuna.trial.create_trial(
        ...     params={"x": 2.0},
        ...     distributions={"x": FloatDistribution(0, 10)},
        ...     value=4.0,
        ... )
        >>> study.add_trial(trial)
        >>>
        >>> dataset = StudyDataset(backend="sqlite", database="optuna.db")
        >>> dataset.save(study)
        >>> reloaded = dataset.load()
        >>> assert len(reloaded.trials) == 1
        >>> assert reloaded.trials[0].params["x"] == 2.0
    """

    DEFAULT_LOAD_ARGS: dict[str, Any] = {"sampler": None, "pruner": None}

    def __init__(  # noqa: PLR0913
        self,
        *,
        backend: str,
        database: str,
        study_name: str,
        load_args: dict[str, Any] | None = None,
        version: Version = None,
        credentials: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``StudyDataset`` pointing to a concrete optuna
        Study on a specific relational database.

        Args:
            backend: Name of the database backend. This name should correspond to a module
                in ``SQLAlchemy``.
            database: Name of the database.
            study_name: Name of the optuna Study.
            load_args: Optuna options for loading studies. Accepts a `sampler` and a
                `pruner`. If either are provided, a `class` matching any Optuna `sampler`,
                respecitively `pruner` class name should be provided, optionally with
                their argyments. Here you can find all available samplers and pruners
                and their arguments:
                - https://optuna.readthedocs.io/en/stable/reference/samplers/index.html
                - https://optuna.readthedocs.io/en/stable/reference/pruners.html
                All defaults are preserved.
            version: If specified, should be an instance of
                ``kedro.io.core.Version``. If its ``load`` attribute is
                None, the latest version will be loaded. If its ``save``
                attribute is None, save version will be autogenerated.
            credentials: Credentials required to get access to the underlying RDB.
                They can include `username`, `password`, `host`, and `port`.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        """
        self._backend = self._validate_backend(backend=backend)
        self._database = self._validate_database(backend=backend, database=database)
        self._study_name = self._validate_study_name(study_name=study_name)

        credentials = self._validate_credentials(backend=backend, credentials=credentials)
        storage = URL.create(
            drivername=backend,
            database=database,
            **credentials,
        )

        self._storage = str(storage)
        self.metadata = metadata

        filepath = None
        if backend == "sqlite":
            filepath = PurePosixPath(os.path.realpath(database))

        super().__init__(
            filepath=filepath,
            version=version,
            exists_function=self._study_name_exists,
            glob_function=self._study_name_glob,
        )

        # Handle default load and save and fs arguments
        self._load_args = {**self.DEFAULT_LOAD_ARGS, **(load_args or {})}

    def _validate_backend(self, backend):
        valid_backends = list(registry.impls.keys()) + ["mssql", "mysql", "oracle", "postgresql", "sqlite"]
        if backend not in valid_backends:
            raise ValueError(
                f"Requested `backend` '{backend}' is not registered as an SQLAlchemy dialect."
            )
        return backend

    def _validate_database(self, backend, database):
        if not isinstance(database, str):
            raise ValueError(f"`database` '{database}' is not a string.")

        if backend == "sqlite":
            if database == ":memory:":
                return database

            # Check if the directory exists
            database_dir = os.path.dirname(database)
            if len(database_dir) and not os.path.isdir(database_dir):
                raise FileNotFoundError(
                    f"The directory of the sqlite DB '{database_dir}' does not exist."
                )

            # Check if the file has an extension
            _, extension = os.path.splitext(database)
            if not extension:
                raise ValueError(f"The sqlite file `database` '{database}' does not have an extension.")

        return database

    def _validate_study_name(self, study_name):
        if not isinstance(study_name, str):
            raise ValueError(f"`study_name` '{study_name}' is not a string.")
        return study_name

    def _validate_credentials(self, backend, credentials):
        if backend == "sqlite" or credentials is None:
            return {}

        if not set(credentials.keys()) <= {"username", "password", "host", "port"}:
            raise ValueError(
                "Incorrect `credentials`. Provided `credentials` should contain "
                "`'username'`, `'password'`, `'host'`, and/or `'port'`. It contains "
                f"{set(credentials.keys())}."
            )

        return deepcopy(credentials)

    def _get_versioned_path(self, version: str) -> PurePosixPath:
        study_name_posix = PurePosixPath(self._study_name)
        return study_name_posix / version / study_name_posix

    def resolve_load_version(self) -> str | None:
        """Compute the version the dataset should be loaded with."""
        if not self._version:
            return None
        if self._version.load:
            return self._version.load
        return self._fetch_latest_load_version()

    def _get_load_path(self) -> PurePosixPath:
        # Path is not affected by versioning
        return self._filepath

    def _get_load_study_name(self) -> str:
        if not self._version:
            # When versioning is disabled, load from original study name
            return self._study_name

        load_version = self.resolve_load_version()
        return str(self._get_versioned_path(load_version))

    def _get_save_path(self) -> PurePosixPath:
        # Path is not affected by versioning
        return self._filepath

    def _get_save_study_name(self) -> str:
        if not self._version:
            # When versioning is disabled, return original study name
            return self._study_name

        save_version = self.resolve_save_version()
        versioned_study_name = self._get_versioned_path(save_version)

        if self._exists_function(str(versioned_study_name)):
            raise DatasetError(
                f"Study name '{versioned_study_name}' for {self!s} must not exist if "
                f"versioning is enabled."
            )

        return str(versioned_study_name)

    def _describe(self) -> dict[str, Any]:
        return {
            "backend": self._backend,
            "database": self._database,
            "study_name": self._study_name,
            "load_args": self._load_args,
            "version": self._version,
        }

    def _get_sampler(self, sampler_config):
        if sampler_config is None:
            return None

        if "class" not in sampler_config:
            raise ValueError(
                "Optuna `sampler` 'class' should be specified when trying to load study "
                f"named '{self._study_name}' with a `sampler`."
            )

        sampler_class_name = sampler_config.pop("class")
        if sampler_class_name in ["QMCSampler", "CmaEsSampler", "GPSampler"]:
            sampler_config["independent_sampler"] = self._get_sampler(
                sampler_config.pop("independent_sampler")
            )

        if sampler_class_name == "PartialFixedSampler":
            sampler_config["base_sampler"] = self._get_sampler(
                sampler_config.pop("base_sampler")
            )

        sampler_class = getattr(optuna.samplers, sampler_class_name)

        return sampler_class(**sampler_config)

    def _get_pruner(self, pruner_config):
        if pruner_config is None:
            return None

        if "class" not in pruner_config:
            raise ValueError(
                "Optuna `pruner` 'class' should be specified when trying to load study "
                f"named '{self._study_name}' with a `pruner`."
            )

        pruner_class_name = pruner_config.pop("class")
        if pruner_class_name == "PatientPruner":
            pruner_config["wrapped_pruner"] = self._get_pruner(
                pruner_config.pop("wrapped_pruner")
            )

        pruner_class = getattr(optuna.pruners, pruner_class_name)

        return pruner_class(**pruner_config)

    def load(self) -> optuna.Study:
        """Load the optuna Study from the storage.

        Returns:
            optuna.Study: The loaded study.
        """
        load_args = deepcopy(self._load_args)
        sampler_config = load_args.pop("sampler")
        sampler = self._get_sampler(sampler_config)

        pruner_config = load_args.pop("pruner")
        pruner = self._get_pruner(pruner_config)

        study = optuna.load_study(
            storage=self._storage,
            study_name=self._get_load_study_name(),
            sampler=sampler,
            pruner=pruner,
        )

        return study

    def save(self, study: optuna.Study) -> None:
        """Save the optuna Study to the storage.

        Args:
            study: The study to save.
        """
        save_study_name = self._get_save_study_name()
        if self._backend == "sqlite":
            os.makedirs(os.path.dirname(self._filepath), exist_ok=True)

            if not os.path.isfile(self._filepath):
                optuna.create_study(
                    storage=self._storage,
                )

        # To overwrite an existing study, we need to first delete it if it exists
        if self._study_name_exists(save_study_name):
            optuna.delete_study(
                storage=self._storage,
                study_name=save_study_name,
            )

        optuna.copy_study(
            from_study_name=study.study_name,
            from_storage=study._storage,
            to_storage=self._storage,
            to_study_name=save_study_name,
        )

    def _study_name_exists(self, study_name) -> bool:
        study_names = optuna.study.get_all_study_names(storage=self._storage)
        return study_name in study_names

    def _study_name_glob(self, pattern):
        study_names = optuna.study.get_all_study_names(storage=self._storage)
        for study_name in study_names:
            if fnmatch.fnmatch(study_name, pattern):
                yield study_name

    def _exists(self) -> bool:
        try:
            load_study_name = self._get_load_study_name()
        except DatasetError:
            return False

        return self._study_name_exists(load_study_name)
