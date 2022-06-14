from setuptools import find_packages, setup

setup(
    name="cpconfig",
    version="1.1",
    packages=find_packages(),
    install_requires=["sqlglot", "marshmallow-dataclass[union, enum]", "pyyaml"],
)
