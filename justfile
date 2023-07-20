install-pip:
    python -m pip install -U setuptools pip wheel

install-dev *args: install-pip
    pip install -e ".[dev]" {{args}}

install-deploy: install-pip
    pip install -e ".[deploy]"

isort:
    isort dyntastic tests

black:
    black dyntastic tests

flake8:
    flake8 dyntastic tests

mypy:
    mypy dyntastic

pre-commit:
    pre-commit run --all-files

test +tests="tests/":
    pytest -n 10 --cov=dyntastic --cov-branch --cov-report term-missing {{tests}}

coverage:
    coverage xml

build:
    python setup.py sdist bdist_wheel
    twine check dist/*

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
