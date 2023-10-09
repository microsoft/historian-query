import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="historian_query",
    version="0.0.1",
    author=["Edgars Jakobsons", "Harrington McDowelle"],
    author_email=["ejakobsons@microsoft.com", "mcdowellehar@microsoft.com"],
    project_urls={
        "Homepage": "https://github.com/microsoft/historian-query",
    },
    description="Query regularized time series from raw historian data on Spark",
    long_description=long_description,
    long_description_content_type="text/markdown",
    py_modules=["historian_query"],
    install_requires=["pyspark>=3.4.1", "dbl-tempo>=0.1.25"],
    python_requires=">=3.7",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
