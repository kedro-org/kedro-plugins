from setuptools import setup, find_packages

setup(
    name="kedro_datasets",  # The name of your package
    version="0.1",           # Version of your package
    packages=find_packages(),  # Automatically find all subpackages
    install_requires=[        # List any external dependencies here
        "kedro>=0.18.0",     # Include Kedro dependency (replace with your version)
        "polars",             # Include Polars package (if you're using it)
    ],
)
