"""noxfile that runs isolated testing environments using nox.

To run all sessions, run `nox` in the root of the repository.

To run a specific session, run `nox -s <session_name>`.

You can pass any arguments as needed to pytest by adding them after --, for
example:

    nox -s tests -- -k test_my_function
"""

from dataclasses import dataclass
from functools import wraps
from itertools import chain
import concurrent
import concurrent.futures
import functools
import json
import os
import re
import warnings

import nox

import warnings
from sqlalchemy import exc

# for warnings not included in regex-based filter below, just log
warnings.filterwarnings("always", category=exc.RemovedIn20Warning)

# for warnings related to execute() / scalar(), raise
for msg in [
    r"The (?:Executable|Engine)\.(?:execute|scalar)\(\) function",
    r"The current statement is being autocommitted using implicit autocommit,",
    r"The connection.execute\(\) method in SQLAlchemy 2.0 will accept "
    "parameters as a single dictionary or a single sequence of "
    "dictionaries only.",
    r"The Connection.connect\(\) function/method is considered legacy",
    r".*DefaultGenerator.execute\(\)",
]:
    warnings.filterwarnings(
        "error",
        message=msg,
        category=exc.RemovedIn20Warning,
    )

nox.options.sessions = [
    "code_tests"
]  # , "live_server_tests", "test_test_server"]
nox.options.reuse_existing_virtualenvs = True
nox.options.error_on_missing_interpreters = True
nox.options.stop_on_first_error = False


class DRAGONSChannels:
    """Channels for the DRAGONS dependencies."""

    # Any class variable that starts with '_' and ends in 'channel[s]?' will be
    # added to the `channels` class attribute. They may be a string or a list
    # of strings.
    _default_channels = ["conda-forge", "defaults"]
    _gemini_channel = "http://astroconda.gemini.edu/public"

    # Magic to collect all channels. Just selects for names starting with '_'
    # and ending in 'channel[s]'.
    channels = []

    for name, value in locals().copy().items():
        if re.match(r"_.+_channel[s]?$", name):
            assert isinstance(value, str) or isinstance(value, list)

            if isinstance(value, str):
                channels.append(value)
                continue

            channels.extend(value)


def install_dependencies(session):
    """Install the dependencies required for the test session."""
    # DRAGONS @ release3.2 branch, due to updates not matching release version.
    git_url_str = (
        "git+https://github.com/GeminiDRSoftware/DRAGONS@release/3.2.x"
    )
    session.install(git_url_str)

    session.install("-r", "requirements.txt")
    session.install("-r", "requirements_dev.txt")

    # Install the package in editable mode, with no dependencies.
    session.install("-e", ".", "--no-deps")

    # Report the installed versions.
    session.run("conda", "list")


def dependencies_decorator(func):
    """Decorator to install the dependencies before running the session."""

    @wraps(func)
    def wrapper(session):
        # Bit fragile (just looks for reduce in venv), but it works.
        # TODO: Make this more robust based on nox?
        if nox.options.reuse_existing_virtualenvs and os.path.exists(
            os.path.join(session.virtualenv.location, "bin/reduce")
        ):
            print(
                f"Reusing existing virtual environment at "
                f"{session.virtualenv.location}. Not installing dependencies."
            )

        else:
            install_dependencies(session)

        func(session)

    return wrapper


# To create a testing session, need to:
# 1. Install the dependencies. These include dragons and other libraries that
#    are needed to run the tests but cannot be installed via requirements.txt.
#    + This is captured by a decorator that installs the dependencies before
#      running the session, e.g.:
#      ```
#      @nox.session(venv_backend="conda", python=["3.10",])
#      @dependencies_decorator
#      def my_session(session):
#          ...
#      ```
#      will create a conda nox session that installs the dependencies before
#      running the session.
#    + Importantly, `venv_backend` is not strictly required. If omitted, nox
#      will use the default backend with normal pip installs.
# 2. Run the tests.
#    + This is done by running the tests in the session, e.g.:
#      ```
#      @nox.session(venv_backend="conda", python=["3.10",])
#      @dependencies_decorator
#      def my_session(session):
#          session.run("pytest", "my_tests.py")
#      ```
#      The dependencies decorator will install the dependencies before running
#      the tests.
@nox.session(
    venv_backend="conda",
    python=[
        "3.10",
    ],
)
@dependencies_decorator
def code_tests(session):
    """Run the code tests."""
    # Helper functions create empty testing databases in memory with sqlite.
    command = [
        "pytest",
        "fits_storage_tests/test_code.py",
        *session.posargs,
    ]

    # Environment variables for deprecating SQLAlchemy warnings.
    env = {
        "SQLALCHEMY_WARN_20": 1,
    }

    for key, value in env.items():
        session.env[key] = str(value)

    session.run(*command)


@nox.session(
    venv_backend="conda",
    python=[
        "3.10",
    ],
)
@dependencies_decorator
def live_server_tests(session):
    """Run the live server tests."""
    # Actually HTTP server
    # wsgi server for implementation
    # Code in wsgiapp.py will spin up a local server running on port 8000.
    command = [
        "pytest",
        "fits_storage_tests/test_liveserver.py",
        *session.posargs,
    ]

    session.run(*command)


@nox.session(
    venv_backend="conda",
    python=["3.10"],
)
@dependencies_decorator
def test_test_server(session):
    """Run the test server tests."""
    command = [
        "pytest",
        "fits_storage_tests/test_testserver.py",
        *session.posargs,
    ]

    session.run(*command)


# "Linting" for SQLAlchemy < 2 stuff.
class SQLAlchemyPatternFinder:
    """Used for search/replace."""

    # {description: (pattern, replacement),}
    patterns = {
        "declarative base in orm namespace": (
            r"sqlalchemy\.ext(?:.declarative_base)?(.*)declarative_base",
            "sqlalchemy.orm\\1declarative_base",
        ),
        "get from query": (
            r"(session\.)?query\((.*)\)\.get\((.*)\)",
            "\\1get(\\2, \\3)",
        ),
    }

    # Pre-compile the patterns.
    patterns = {
        key: (re.compile(pattern), value)
        for key, (pattern, value) in patterns.items()
    }

    @classmethod
    def replace(cls, text):
        # Search and replace using regex.
        for key, (pattern, replacement) in cls.patterns.items():
            old_text = text
            text = pattern.sub(replacement, old_text)

            if pattern.search(old_text) and text == old_text:
                raise ValueError(
                    f"Pattern {key} matched but did not replace anything."
                )

        return text

    @classmethod
    def update_file(cls, file: os.PathLike):
        """Update the file with the new patterns."""
        with open(file, "r") as f:
            lines = f.readlines()

        updated_lines = []
        for i, line in enumerate(lines, start=1):
            updated_lines.append(cls.replace(line))

            if updated_lines[-1] != lines[i - 1]:
                print(f"Updated line {i}:")
                print(f"  + {updated_lines[-1]}")
                print(f"  - {lines[i - 1]}")

        text = "".join(updated_lines)

        with open(file, "w") as f:
            f.write(text)


@nox.session
def update_sqlalchemy_patterns(session):
    """Update the SQLAlchemy patterns."""
    search_dirs = ("fits_storage", "fits_storage_tests")

    for search_dir in search_dirs:
        for root, _, files in os.walk(search_dir):
            for file in files:
                if file.endswith(".py"):
                    SQLAlchemyPatternFinder.update_file(
                        os.path.join(root, file)
                    )