.SUFFIXES:

package:
	cd $(plugin);\
	rm -Rf dist;\
	python -m build

pypi:
	python -m pip install twine -U
	python -m twine upload $(plugin)/dist/*

install: package
	cd $(plugin) && pip install -U dist/*.whl

install-pip-setuptools:
	python -m pip install -U pip setuptools wheel

lint:
	pre-commit run -a --hook-stage manual ruff-$(plugin) && pre-commit run trailing-whitespace --all-files && pre-commit run end-of-file-fixer --all-files && pre-commit run check-yaml --all-files && pre-commit run check-added-large-files --all-files && pre-commit run check-case-conflict --all-files && pre-commit run check-merge-conflict --all-files && pre-commit run debug-statements --all-files && pre-commit run black-$(plugin) --all-files --hook-stage manual && pre-commit run secret_scan --all-files --hook-stage manual && pre-commit run bandit --all-files --hook-stage manual

test:
	cd $(plugin) && pytest tests --cov-config pyproject.toml --numprocesses 4 --dist loadfile

# Run test_tensorflow_model_dataset separately, because these tests are flaky when run as part of the full test-suite
dataset-tests: dataset-doctests
	cd kedro-datasets && pytest tests --cov-config pyproject.toml --numprocesses 4 --dist loadfile --ignore tests/databricks --ignore tests/tensorflow
	cd kedro-datasets && pytest tests/tensorflow/test_tensorflow_model_dataset.py  --no-cov

extra_pytest_args-no-spark=--ignore kedro_datasets/databricks --ignore kedro_datasets/spark
extra_pytest_args=
dataset-doctest%:
	if [ "${*}" != 's-no-spark' ] && [ "${*}" != 's' ]; then \
	  echo "make: *** No rule to make target \`${@}\`.  Stop."; \
	  exit 2; \
	fi; \
    \
	# The ignored datasets below require complicated setup with cloud/database clients which is overkill for the doctest examples.
	cd kedro-datasets && pytest kedro_datasets --doctest-modules --doctest-continue-on-failure --no-cov \
	  --ignore kedro_datasets/pandas/gbq_dataset.py \
	  --ignore kedro_datasets/partitions/partitioned_dataset.py \
	  --ignore kedro_datasets/redis/redis_dataset.py \
	  --ignore kedro_datasets/snowflake/snowpark_dataset.py \
	  --ignore kedro_datasets/spark/spark_hive_dataset.py \
	  --ignore kedro_datasets/spark/spark_jdbc_dataset.py \
	  $(extra_pytest_arg${*})

test-sequential:
	cd $(plugin) && pytest tests --cov-config pyproject.toml

e2e-tests:
	cd $(plugin) && behave

secret-scan:
	trufflehog --max_depth 1 --exclude_paths trufflehog-ignore.txt .

clean:
	cd $(plugin);\
	rm -rf build dist pip-wheel-metadata .pytest_cache;\
	find . -regex ".*/__pycache__" -exec rm -rf {} +;\
	find . -regex ".*\.egg-info" -exec rm -rf {} +;\

install-test-requirements:
	cd $(plugin) && pip install ".[test]"

install-pre-commit:
	pre-commit install --install-hooks

uninstall-pre-commit:
	pre-commit uninstall
	pre-commit uninstall --hook-type pre-push

sign-off:
	echo "git interpret-trailers --if-exists doNothing \c" >> .git/hooks/commit-msg
	echo '--trailer "Signed-off-by: $$(git config user.name) <$$(git config user.email)>" \c' >> .git/hooks/commit-msg
	echo '--in-place "$$1"' >> .git/hooks/commit-msg
	chmod +x .git/hooks/commit-msg

# kedro-datasets related only
test-no-spark: dataset-doctests-no-spark
	cd kedro-datasets && pytest tests --no-cov --ignore tests/spark --ignore tests/databricks --numprocesses 4 --dist loadfile

test-no-spark-sequential: dataset-doctests-no-spark
	cd kedro-datasets && pytest tests --no-cov --ignore tests/spark --ignore tests/databricks

# kedro-datasets/snowflake tests skipped from default scope
test-snowflake-only:
	cd kedro-datasets && pytest --no-cov --numprocesses 1 --dist loadfile -m snowflake
	cd kedro-datasets && pytest kedro_datasets/snowflake --doctest-modules --doctest-continue-on-failure --no-cov

rtd:
	cd kedro-datasets && python -m sphinx -WETan -j auto -D language=en -b linkcheck -d _build/doctrees docs/source _build/linkcheck
