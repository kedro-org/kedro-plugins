package:
	cd $(plugin);\
	rm -Rf dist;\
	python setup.py sdist bdist_wheel

install: package
	cd $(plugin) && pip install -U dist/*.whl

install-pip-setuptools:
	python -m pip install -U pip setuptools wheel

lint:
	cd $(plugin) && pre-commit run -a --hook-stage manual

test:
	cd $(plugin) && pytest -vv tests

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
