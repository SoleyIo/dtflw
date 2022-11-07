from setuptools import setup, find_packages

setup(
    name="dtflw",
    version="0.0.1",
    author="Soley GmbH",
    author_email="",
    description="dtflw is a Python framework for building data pipelines based on Databricks notebook workflows.",
    url="",
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    package_data={'': ['static/*.html']},
    classifiers=[
        "Programming Language :: Python :: 3.*",
        "License :: BSD 3-Clause License"
    ],
    install_requires=[
        "setuptools==56.*",
        "wheel==0.*",
        "databricks-connect==9.1.*",
        "ddt==1.*",
        "sortedcontainers==2.4.0"
    ],
)