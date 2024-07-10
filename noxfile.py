"""noxfile that runs isolated testing environments using nox.

To run all sessions, run `nox` in the root of the repository.

To run a specific session, run `nox -s <session_name>`.

You can pass any arguments as needed to pytest by adding them after --, for
example:

    nox -s tests -- -k test_my_function
"""

from functools import wraps
import os
import sys
import re
import warnings

import nox

from sqlalchemy import exc


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

    # Ensure SQLAlchemy 2.0 is installed. DRAGONS will try to install 1.4.
    session.install("sqlalchemy>=2.0")

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
        "-W",
        "error::DeprecationWarning",
        # "error::sqlalchemy.exc.RemovedIn20Warning",
        *session.posargs,
    ]

    # Environment variables for deprecating SQLAlchemy warnings.
    env = {
        "SQLALCHEMY_WARN_20": "1",
    }

    # Checking the sqlalchemy version used by the session.
    sqlalchemy_version_code_lines = (
    "# Check the SQLAlchemy version.",
    "import sqlalchemy",
    "print(sqlalchemy.__version__)",
    )

    sqlalchemy_version_code = "\n".join(sqlalchemy_version_code_lines)

    result = session.run("python", "-c", f"{sqlalchemy_version_code}", silent=True)
    print(f"SQLAlchemy version: {result.strip()}")
    assert result.strip()[0] == "2", "SQLAlchemy version is not 2.0."

    session.run(*command, env=env)


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
    # If replacement is None, the pattern will *not* be replaced.
    patterns = {
        # From Removedin20Warning fixes.
        "declarative base in orm namespace": (
            r"sqlalchemy\.ext(?:.declarative_base)?(.*)declarative_base",
            "sqlalchemy.orm\\1declarative_base",
        ),
        "get from query": (
            r"(session).query\((.*)\).get\((.*)\)",
            r"\1.get(\2, \3)",
        ),
        "add from query": (
            r"(session).query\((.*)\).add\((.*)\)",
            r"\1.add(\2, \3)",
        ),
        # relation -> relationship
        "import 'relation' no longer supported": (
            r"from sqlalchemy(.*)import(.*)relation(\W.*)",
            None
        ),
        "relation -> relationship": (
            r"^(.*=\s*)relation(\b.*)",
            r"\1relationship\2",
        ),
        "sessionmaker without future=True": (
            r"^[^#]*(sessionmaker\(.*)$",
            None
        )
    }

    # Pre-compile the patterns.
    patterns = {
        key: (re.compile(pattern), value)
        for key, (pattern, value) in patterns.items()
    }

    @classmethod
    def replace(cls, text) -> tuple[str, list[str]]:
        """Replace the patterns in the text.

        Raises
        ------
        ValueError
            If the text matches a pattern and is supposed to be replaced, but
            fails to replace anything in the string.
        """
        # Search and replace using regex.
        _match_found = []

        pattern_iter = cls.patterns.items()

        for i, (key, (pattern, replacement)) in enumerate(pattern_iter):
            old_text = text

            if replacement is not None:
                text = pattern.sub(replacement, old_text)

            changed = text != old_text

            if (
                replacement is not None
                and pattern.search(old_text)
                and not changed
            ):
                raise ValueError(
                    f"Pattern {key} matched but did not replace anything."
                )

            if pattern.search(text):
                _match_found.append(key)

        return (text, _match_found)

    @classmethod
    def update_file(cls, file: os.PathLike, verbose=False):
        """Update the file with the new patterns."""
        with open(file, "r") as f:
            lines = f.readlines()

        updated_lines = []
        matches_found = 0
        for i, line in enumerate(lines, start=1):
            new_line, match_found = cls.replace(line)
            updated_lines.append(new_line)

            if verbose:
                if match_found:
                    if not matches_found:
                        print(f"Match found in {file}.")

                    print(f"LINE {file}:{i}:")
                    if updated_lines[-1] != line:
                        print(f" - {line.rstrip()}")
                        print(f" + {updated_lines[-1].rstrip()}")

                    else:
                        print(f" - {line.rstrip()}")

                    print(f" Matched by |=> {', '.join(match_found)}")

            if match_found:
                matches_found += 1

        text = "".join(updated_lines)

        if any(arg in ("-n", "--dry-run") for arg in sys.argv):
            if matches_found:
                print(f"DRY-RUN: Would write to {file}")

            return

        if matches_found:
            with open(file, "w") as f:
                f.write(text)


@nox.session
def update_sqlalchemy_patterns(session):
    """Update the SQLAlchemy patterns.

    Usage
    -----

    To check for patterns in the codebase, run:
    ```terminal
        nox -s update_sqlalchemy_patterns
    ```

    For verbose output, run:
    ```terminal
        nox -s update_sqlalchemy_patterns -- -v  # or --verbose
    ```
    """
    search_dirs = ("fits_storage", "fits_storage_tests")

    verbose = any(arg in ("-v", "--verbose") for arg in session.posargs)

    for search_dir in search_dirs:
        for root, _, files in os.walk(search_dir):
            files = (f for f in files if f.endswith(".py"))
            for file in files:
                SQLAlchemyPatternFinder.update_file(
                    os.path.join(root, file), verbose=verbose
                )
