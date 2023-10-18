################################################################################

from setuptools import find_packages, setup

################################################################################

setup(
    name="atmostream",
    version="0.1.0",
    author="Derek J Eden",
    author_email="derekjeden@gmail.com",
    description="A pythonic approach to download, stream, and process various atmopsheric forecast models",
    include_package_data=True,
    packages=find_packages(),
    install_requires=["beautifulsoup4",
                      "requests",
                      "tqdm",
                      "pandas",
                      "mikeio",
                      "xarray",
                      "numpy",
                      "more_itertools"],
    dependency_links=[])

################################################################################
