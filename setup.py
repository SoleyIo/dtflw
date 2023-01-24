from setuptools import setup, find_packages

setup(
    name="dtflw",
    version="0.0.10",
    author="Soley GmbH",
    author_email="",
    long_description="dtflw is a Python framework for building modular data pipelines based on Databricks dbutils.notebook API.",
    description="dtflw is a Python framework for building modular data pipelines based on Databricks dbutils.notebook API.",
    license="BSD 3-Clause License",
    url="https://github.com/SoleyIo/dtflw",
    packages=find_packages(
        exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    classifiers=[
        "Programming Language :: Python :: 3.*",
        "License :: BSD 3-Clause License"
    ],
    install_requires=[
        "ddt>=1.5.0",
        "databricks-connect",
        "setuptools"
    ],
)
