from setuptools import find_packages, setup

setup(
    name="spotify",
    packages=find_packages(exclude=["spotify_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
