from pathlib import Path

from setuptools import find_packages, setup

ROOT_DIRECTORY = Path(__file__).resolve().parent

readme = (ROOT_DIRECTORY / "README.md").read_text()
description = next(
    line for line in readme.splitlines() if line and not line.startswith("#") and not line.startswith("[")
)
changelog = (ROOT_DIRECTORY / "CHANGELOG.md").read_text()
long_description = readme + "\n\n" + changelog


DEV_REQUIRES = [
    "black",
    "boto3-stubs[dynamodb]",
    "coverage",
    "flake8==6.0.0; python_version > '3.7'",
    "flake8<5; python_version == '3.7'",
    "flake8-bugbear",
    "isort",
    "moto==4.2.2",
    "mypy",
    "pre-commit",
    "pytest",
    "pytest-cov",
    "pytest-mock",
    "pytest-sugar",
    "pytest-xdist",
]

setup(
    name="dyntastic",
    version="0.17.0",
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
    ],
    author="Naya Verdier",
    url="https://github.com/nayaverdier/dyntastic",
    license="MIT",
    packages=find_packages(exclude=("tests",)),
    install_requires=[
        "boto3>=1.10.0; python_version < '3.12'",
        "boto3>=1.20.0; python_version >= '3.12'",
        "pydantic>=1.7.1,<3",
        "importlib-metadata>=1.0.0; python_version < '3.8'",
    ],
    python_requires=">=3.7",
    extras_require={
        "dev": DEV_REQUIRES,
        "deploy": ["twine"],
    },
    package_data={"dyntastic": ["py.typed"]},
    include_package_data=True,
)
