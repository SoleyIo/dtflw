from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as f:
    long_description_content = f.read()

setup(
    name="dtflw",
    version="0.5.0",
    author="Soley GmbH",
    author_email="codeofconduct@soley.io",

    description="dtflw is a Python framework for building modular data pipelines based on Databricks dbutils.notebook API.",

    long_description=long_description_content,
    long_description_content_type="text/markdown",

    license="BSD 3-Clause License",

    url="https://github.com/SoleyIo/dtflw",

    packages=find_packages("src"),

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
