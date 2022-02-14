from pathlib import Path

from setuptools import find_packages, setup


ROOT_DIRECTORY = Path(__file__).resolve().parent

readme = (ROOT_DIRECTORY / "README.md").read_text()
description = readme.splitlines()[2]
changelog = (ROOT_DIRECTORY / "CHANGELOG.md").read_text()
long_description = readme + "\n\n" + changelog


DEV_REQUIRES = [
    "black==22.1.0",
    "boto3-stubs[dynamodb]==1.20.54",
    "coverage==6.3.1",
    "flake8==4.0.1",
    "flake8-bugbear==22.1.11",
    "isort==5.10.1",
    "moto==3.0.3",
    "mypy==0.931",
    "pytest==7.0.0",
    "pytest-cov==3.0.0",
    "pytest-mock==3.7.0",
    "twine==3.8.0",
]

setup(
    name="dyntastic",
    version="0.1.0",
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
        "boto3~=1.6",
        "pydantic~=1.8",
        "importlib-metadata~=4.2",
    ],
    python_requires=">=3.7",
    extras_require={
        "dev": DEV_REQUIRES,
    },
    include_package_data=True,
)
