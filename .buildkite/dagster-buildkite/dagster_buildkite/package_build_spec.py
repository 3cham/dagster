from typing import Callable, List, NamedTuple, Optional

from .defines import TOX_MAP
from .step_builder import BuildkiteQueue, CommandStep, StepBuilder
from .steps.mypy import build_mypy_step
from .steps.pylint import build_pylint_step
from .utils import get_python_versions_for_branch


class PackageBuildSpec(
    NamedTuple(
        "_PackageBuildSpec",
        [
            ("directory", str),
            ("env_vars", Optional[List[str]]),
            ("supported_pythons", List[str]),
            ("extra_cmds_fn", Optional[Callable[[str], List[str]]]),
            ("depends_on_fn", Optional[Callable[[str], List[str]]]),
            ("tox_file", Optional[str]),
            ("tox_env_suffixes", Optional[List[str]]),
            ("buildkite_label", Optional[str]),
            ("retries", Optional[int]),
            ("upload_coverage", bool),
            ("timeout_in_minutes", Optional[int]),
            ("queue", Optional[BuildkiteQueue]),
            ("run_pytest", bool),
            ("run_mypy", bool),
            ("run_pylint", bool),
        ],
    )
):
    """Main spec for testing Dagster Python modules using tox.

    Args:
        directory (str): Python directory to test, relative to the repository root. Should contain a
            tox.ini file.
        env_vars (List[str], optional): Additional environment variables to pass through to the
            test. Make sure you set these in tox.ini also, using ``passenv``! Defaults to None.
        supported_pythons (List[str], optional): Optional overrides for Python versions to test.
            Tests are generated for each version of Python listed here; each test will run in an
            integration test image for that Python version, and ``tox -e <<VERSION>>`` will be
            invoked for the corresponding Python version. Defaults to None (all supported Pythons).
        extra_cmds_fn (Callable[str, List[str]], optional): Optional callable to create more
            commands to run before the main test invocation through tox. Function takes a single
            argument, which is the Python version being invoked, and returns a list of shell
            commands to execute, one invocation per list item. Defaults to None (no additional
            commands).
        depends_on_fn (Callable[str, List[str]], optional): Optional callable to create a
            Buildkite dependency (e.g. on test image build step). Function takes a single
            argument, which is the Python version being invoked, and returns a list of names of
            other Buildkite build steps this build step should depend on. Defaults to None (no
            dependencies).
        tox_file (str, optional): The tox file to use. Defaults to None (uses the default tox.ini
            file).
        tox_env_suffixes: (List[str], optional): List of additional tox env suffixes to provide
            when invoking tox. When provided, a separate test run will be invoked per
            env x env_suffix string. For example, given Python tox version py38, the
            tox_env_suffixes ["-a", "-b"] will result in running "tox -e py38-a" and "tox -e py38-b"
            as two build steps. Defaults to None.
        buildkite_label (str, optional): Optional label to override what's shown in Buildkite.
            Defaults to None (uses the package name as the label).
        retries (int, optional): Whether to retry these tests on failure
        upload_coverage (bool, optional): Whether to copy coverage artifacts. Enabled by default.
        timeout_in_minutes (int, optional): Fail after this many minutes
        queue (BuildkiteQueue, optional): Which queue to run on
        run_pytest (bool, optional): Whether to run pytest. Enabled by default.
        run_mypy (bool, optional): Whether to run mypy. Enabled by default.
    """

    def __new__(
        cls,
        directory: str,
        env_vars: Optional[List[str]] = None,
        supported_pythons: Optional[List[str]] = None,
        extra_cmds_fn: Optional[Callable[[str], List[str]]] = None,
        depends_on_fn: Optional[Callable[[str], List[str]]] = None,
        tox_file: Optional[str] = None,
        tox_env_suffixes: Optional[List[str]] = None,
        buildkite_label: Optional[str] = None,
        retries: Optional[int] = None,
        upload_coverage: bool = True,
        timeout_in_minutes: Optional[int] = None,
        queue: Optional[BuildkiteQueue] = None,
        run_pytest: bool = True,
        run_mypy: bool = True,
        run_pylint: bool = True,
    ):
        return super(PackageBuildSpec, cls).__new__(
            cls,
            directory,
            env_vars or [],
            supported_pythons or get_python_versions_for_branch(),
            extra_cmds_fn,
            depends_on_fn,
            tox_file,
            tox_env_suffixes,
            buildkite_label,
            retries,
            upload_coverage,
            timeout_in_minutes,
            queue,
            run_pytest,
            run_mypy,
            run_pylint,
        )

    def build_tox_steps(self) -> List[CommandStep]:
        base_label = self.buildkite_label or self.directory.split("/")[-1]
        steps = []

        tox_env_suffixes = self.tox_env_suffixes or [""]

        for version in self.supported_pythons:
            for tox_env_suffix in tox_env_suffixes:
                label = base_label + tox_env_suffix

                if self.run_pytest:
                    extra_cmds = self.extra_cmds_fn(version) if self.extra_cmds_fn else []

                    tox_file = "-c %s " % self.tox_file if self.tox_file else ""
                    tox_cmd = f"tox -vv {tox_file}-e {TOX_MAP[version]}{tox_env_suffix}"

                    cmds = extra_cmds + [
                        "pip install -U virtualenv",
                        f"cd {self.directory}",
                        tox_cmd,
                    ]

                    if self.upload_coverage:
                        coverage = f".coverage.{label}.{version}.$BUILDKITE_BUILD_ID"
                        cmds += [
                            f"mv .coverage {coverage}",
                            f"buildkite-agent artifact upload {coverage}",
                        ]

                    version_str = ".".join(version.split(".")[:2])
                    step = (
                        StepBuilder(
                            f":pytest: {label} {version_str}",
                            timeout_in_minutes=self.timeout_in_minutes,
                        )
                        .run(*cmds)
                        .on_integration_image(version, self.env_vars or [])
                    )

                    if self.retries:
                        step = step.with_retry(self.retries)

                    if self.depends_on_fn:
                        step = step.depends_on(self.depends_on_fn(version))

                    if self.queue:
                        step = step.on_queue(self.queue)

                    steps.append(step.build())

        if self.run_mypy:
            # Toxfile must define a mypy testenv.
            steps.append(build_mypy_step(self.directory))

        if self.run_pylint:
            # Toxfile must define a pylint testenv.
            steps.append(build_pylint_step(self.directory))

        return steps
