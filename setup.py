from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
long_description_content = (here / "README.md").read_text(encoding="utf-8")

setup(
    name="dtflw",
    version="0.5.1",

    description="dtflw is a Python framework for building modular data pipelines based on Databricks dbutils.notebook API.",
    long_description=long_description_content,
    long_description_content_type="text/markdown",

    url="https://github.com/SoleyIo/dtflw",

    author="Soley GmbH",
    author_email="codeofconduct@soley.io",

    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10"
    ],

    keywords="databricks, data pipelines, etl, data engineering",

    package_dir={"": "src"},
    packages=find_packages(where="src"),

    python_requires=">=3.8",

    install_requires=[
        "ddt>=1.5.0",
        "databricks-connect",
        "setuptools"
    ],

    project_urls={
        "Bug Reports": "https://github.com/SoleyIo/dtflw/issues",
        "Source": "https://github.com/SoleyIo/dtflw"
    },
)
