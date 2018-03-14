from setuptools import setup

setup(
    name = "Shot-Analysis",
    version = "0.1dev",
    packages = ['util','extract','context'],
    install_requires = [
        "pyspark == 2.2.1",
        "pytest == 3.4.0",
        "Mock == 2.0.0",
        "coverage == 4.5.1",
        "pytest-cov == 2.5.1"
    ]
)