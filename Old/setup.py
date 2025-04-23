from setuptools import setup, find_packages

setup(
    name="financial-data-pipeline",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.9",
    install_requires=[
        "polars>=0.19.8",
        "pyarrow>=14.0.1",
        "duckdb>=0.9.0",
        "fsspec>=2023.10.0",
        "zipfile38>=0.0.3",
        "tqdm>=4.65.0",
        "loguru>=0.7.0",
    ],
    extras_require={
        "dev": [
            "black>=23.1.0",
            "isort>=5.12.0",
            "flake8>=6.0.0",
            "pytest>=7.3.1",
            "pyinstrument>=4.6.0",
        ]
    },
)