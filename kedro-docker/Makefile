install:
	pip install .

install-pip-setuptools:
	python -m pip install -U "pip>=20.0" "setuptools>=38.0" wheel

test:
	pytest tests

secret-scan:
	trufflehog --max_depth 1 .

package: clean install
	python setup.py clean --all
	python setup.py sdist bdist_wheel
