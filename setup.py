import logging
import os
import re

import setuptools

logger = logging.getLogger(__name__)

ROOT_DIR = os.path.dirname(__file__)


def find_version(*paths):
    version_file_path = os.path.join(ROOT_DIR, *paths)
    with open(version_file_path) as file_stream:
        version_match = re.search(
            r"^__version__ = ['\"]([^'\"]*)['\"]", file_stream.read(), re.M
        )
        if version_match:
            return version_match.group(1)
        raise RuntimeError(f"Failed to find version at: {version_file_path}")


def parse_requirements(filename):
    """Parse a requirements file, ignoring comments and empty lines."""
    requirements = []
    with open(os.path.join(ROOT_DIR, filename), "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            # Skip comments, empty lines, and lines starting with extras markers
            if line and not line.startswith("#") and not line.startswith("-"):
                # Skip lines that are just section markers like "# deltacat[iceberg]"
                if "deltacat[" not in line:
                    requirements.append(line)
    return requirements


with open(os.path.join(ROOT_DIR, "README.md"), "r", encoding="utf-8") as fh:
    long_description = fh.read()


# Parse base requirements from requirements.txt
base_requirements = parse_requirements("requirements.txt")

# Filter out development-only dependencies and extras that should not be in install_requires
exclude_packages = {
    "pytest",  # Development only
    "databricks-sdk",  # Not in original install_requires
    "deltalake",  # Not in original install_requires
    "msgpack",  # Not in original install_requires
    "pyiceberg",  # This is in extras_require, not install_requires
    "s3fs",  # This is in extras_require, not install_requires
}

install_requirements = [
    req for req in base_requirements 
    if not any(req.startswith(pkg) for pkg in exclude_packages)
]

setuptools.setup(
    name="deltacat",
    version=find_version("deltacat", "__init__.py"),
    author="Ray Team",
    description="A portable, scalable, fast, and Pythonic Data Lakehouse for AI.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ray-project/deltacat",
    packages=setuptools.find_packages(where=".", include="deltacat*"),
    extras_require={
        "iceberg": [
            "pyiceberg[glue] >= 0.9.0",
            "pyiceberg[hive] >= 0.9.0",
            "pyiceberg[sql-sqlite] >= 0.9.0",
        ],
        "beam": [
            "apache-beam[gcs] == 2.65.0",
        ],
        # separate s3fs from other AWS dependencies due to vastly increased
        # installation times when included (due to boto version conflicts)
        "s3fs": ["s3fs == 2025.3.2"],
    },
    install_requires=install_requirements,
    setup_requires=["wheel"],
    package_data={
        "compute/metastats": ["*.yaml"],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.9",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
)
