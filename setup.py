import setuptools
import os


NAME = "atcociftogtfs"


def read_textfile(filename=""):
    with open(filename, "r") as textfile:
        return textfile.read()


def read_version():
    with open(os.path.join(NAME, "__init__.py")) as init:
        for line in init:
            if line.startswith("__version__"):
                return eval(line.split("=")[-1])
                # nosec - setup only (ast.literal_eval doesn't process dots)
    return None


setuptools.setup(
    name=NAME,
    version=read_version(),
    author="Tim Howgego",
    author_email="timothyhowgego@gmail.com",
    description="Converts ATCO.CIF public transport schedules into GTFS",
    long_description=read_textfile(filename="README.md"),
    long_description_content_type="text/markdown",
    url="https://github.com/timhowgego/atcociftogtfs",
    license="MIT",
    packages=[NAME],
    entry_points={
        "console_scripts": [
            "{} = {}.__main__:main".format(NAME, NAME)  # Entry
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        ],
    install_requires=[
            "pyproj",
        ],
    python_requires='>=3.3',  # According to vermin
)
