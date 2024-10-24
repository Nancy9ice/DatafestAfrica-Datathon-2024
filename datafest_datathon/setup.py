from setuptools import find_packages, setup

setup(
    name="datafest_datathon",
    packages=find_packages(include=["assets", "machine_learning", "dbt_data_baddies_datafest"], exclude=["datafest_datathon_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
    ],
    extras_require={
        "dev": ["dagster-webserver", "pytest"],
    },
)
