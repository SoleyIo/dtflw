from setuptools import setup, find_packages


setup(
    name="dtflw",
    version="1.0.0",

    description="dtflw is a Python framework for building modular data pipelines based on Databricks dbutils.notebook API.",
    long_description="See [the home page](https://github.com/SoleyIo/dtflw/blob/main/README.md) of the project for details.",
    long_description_content_type="text/markdown",

    url="https://github.com/SoleyIo/dtflw",

    author="Soley GmbH",
    author_email="codeofconduct@soley.io",

    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3.11"
    ],

    keywords="databricks, data pipelines, etl, data engineering",

    package_dir={"": "src"},
    packages=find_packages(where="src"),

    python_requires=">=3.8",

    install_requires=[
        "ddt>=1.5.0",
        "setuptools"
    ],

    extras_require = {
        "local": ["pyspark"],
        "databricks": ["databricks-connect==15.4.3"] 
    },

    project_urls={
        "Bug Reports": "https://github.com/SoleyIo/dtflw/issues",
        "Source": "https://github.com/SoleyIo/dtflw"
    },
)
