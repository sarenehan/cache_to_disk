import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="cache_to_disk",
    version="0.0.9",
    author="Stewart Renehan",
    author_email="sarenehan@gmail.com",
    description="Local disk caching decorator for python function.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/sarenehan/cache_to_disk",
    packages=setuptools.find_packages(),
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ),
)
