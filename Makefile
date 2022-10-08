.DEFAULT_GOAL := all

black = black dyntastic tests
flake8 = flake8 dyntastic tests
isort = isort dyntastic tests
mypy = mypy dyntastic
install-pip = python -m pip install -U setuptools pip wheel
test = pytest --cov=dyntastic --cov-branch --cov-report term-missing tests/

.PHONY: install
install:
	$(install-pip)
	pip install -e .

.PHONY: install-dev
install-dev:
	$(install-pip)
	pip install -e ".[dev]" $(ARGS)

.PHONY: install-deploy
install-deploy:
	$(install-pip)
	pip install -e ".[deploy]"

.PHONY: format
format:
	$(isort)
	$(black)

.PHONY: lint
lint:
	$(isort) --check-only --df
	$(black) --check --diff
	$(flake8)
	$(mypy)

.PHONY: test
test:
	$(test)

.PHONY: coverage
coverage:
	coverage xml

.PHONY: build
build:
	python setup.py sdist bdist_wheel
	twine check dist/*

.PHONY: clean
clean:
	rm -rf `find . -name __pycache__`
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf build
	rm -rf dist
	rm -rf *.egg-info
	rm -f .coverage
	rm -f .coverage.*
	rm -f coverage.xml
