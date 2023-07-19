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
	pre-commit run trailing-whitespace --all-files && pre-commit run end-of-file-fixer --all-files && pre-commit run check-yaml --all-files && pre-commit run check-added-large-files --all-files && pre-commit run check-case-conflict --all-files && pre-commit run check-merge-conflict --all-files && pre-commit run debug-statements --all-files && pre-commit run requirements-txt-fixer --all-files && pre-commit run flake8 --all-files && pre-commit run isort-$(plugin) --all-files --hook-stage manual && pre-commit run black-$(plugin) --all-files --hook-stage manual && pre-commit run secret_scan --all-files --hook-stage manual && pre-commit run bandit --all-files --hook-stage manual && pre-commit run pylint-$(plugin) --all-files --hook-stage manual && pre-commit run pylint-$(plugin)-features --all-files --hook-stage manual && pre-commit run pylint-$(plugin)-tests --all-files --hook-stage manual

test:
	cd $(plugin) && pytest tests --cov-config pyproject.toml --numprocesses 4 --dist loadfile

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
	cd $(plugin) && pip install -r test_requirements.txt

install-pre-commit: install-test-requirements
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
test-no-spark:
	cd kedro-datasets && pytest tests --no-cov --ignore tests/spark --ignore tests/databricks --numprocesses 4 --dist loadfile

test-no-spark-sequential:
	cd kedro-datasets && pytest tests --no-cov --ignore tests/spark --ignore tests/databricks

# kedro-datasets/snowflake tests skipped from default scope
test-snowflake-only:
	cd kedro-datasets && pytest tests --no-cov --numprocesses 1 --dist loadfile -m snowflake
