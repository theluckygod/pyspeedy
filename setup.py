from setuptools import find_packages, setup

setup(
    name="pyspeedy",
    python_requires=">=3.7",
    version="0.1.0",
    description="Fast and easy-to-use package for python developers.",
    author="theluckygod",
    author_email="thieu216777@gmail.com",
    url="https://github.com/theluckygod/pyspeedy",
    packages=find_packages(),  # Automatically find all packages in the directory
    install_requires=[  # List any dependencies your package requires
        "pytest",
        "loguru",
        "pandas",
        "beartype",
        "python-dateutil",
        "IPython",
    ],
)
