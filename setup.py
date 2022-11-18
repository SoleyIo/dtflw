from setuptools import setup, find_packages

setup(
    name="dtflw",
    version="0.0.3",
    author="Soley GmbH",
    author_email="",
    long_description = "dtflw is a Python framework for building data pipelines based on Databricks notebook workflows.",
    description="dtflw is a Python framework for building data pipelines based on Databricks notebook workflows.",
    license="BSD 3-Clause License",
    url="https://github.com/SoleyIo/dtflw",
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    classifiers=[
        "Programming Language :: Python :: 3.*",
        "License :: BSD 3-Clause License"
    ],
    install_requires=[
        "ddt>=1.5.0",
        "databricks-connect>=9.1.20",
        "setuptools>=65.0.0"
    ],
)