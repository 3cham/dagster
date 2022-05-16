import os
from glob import glob
from typing import List

from ..defines import (
    DEFAULT_PYTHON_VERSION,
    GCP_CREDS_LOCAL_FILE,
    TOX_MAP,
    ExamplePythons,
    SupportedPython,
)
from ..images.versions import COVERAGE_IMAGE_VERSION
from ..package_build_spec import PackageBuildSpec
from ..step_builder import StepBuilder
from ..utils import (
    CommandStep,
    check_for_release,
    connect_sibling_docker_container,
    get_python_versions_for_branch,
    is_release_branch,
    network_buildkite_container,
    safe_getenv,
)
from .docs import docs_steps
from .helm import helm_steps
from .test_images import core_test_image_depends_fn, publish_test_images, test_image_depends_fn

GIT_REPO_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "..")

branch_name = safe_getenv("BUILDKITE_BRANCH")


def build_dagster_steps() -> List[CommandStep]:
    steps = []
    steps += publish_test_images()

    # Other linters are run in per-package environments because they rely on the dependencies of the
    # target. `black`, `isort`, and `check-manifest` are run for the whole repo at once.
    steps += build_repo_wide_isort_steps()
    steps += build_repo_wide_black_steps()
    steps += build_repo_wide_check_manifest_steps()

    # "Package" used loosely here to mean roughly "a directory with some python modules". For
    # instances, a directory of unrelated scripts counts as a package. All packages must have a
    # toxfile that defines the tests for that package.
    for m in DAGSTER_PACKAGES_WITH_CUSTOM_TESTS + DAGSTER_PACKAGES_WITH_STANDARD_STEPS:
        steps += m.build_tox_steps()

    # https://github.com/dagster-io/dagster/issues/2785
    steps += build_pipenv_smoke_steps()
    steps += docs_steps()
    steps += helm_steps()
    steps += build_sql_schema_check_steps()
    steps += build_graphql_python_client_backcompat_steps()

    return steps


# ########################
# ##### PACKAGES WITH CUSTOM STEPS
# ########################


def airflow_extra_cmds_fn(version: str) -> List[str]:
    return [
        'export AIRFLOW_HOME="/airflow"',
        "mkdir -p $${AIRFLOW_HOME}",
        "export DAGSTER_DOCKER_IMAGE_TAG=$${BUILDKITE_BUILD_ID}-" + version,
        'export DAGSTER_DOCKER_REPOSITORY="$${AWS_ACCOUNT_ID}.dkr.ecr.us-west-2.amazonaws.com"',
        "aws ecr get-login --no-include-email --region us-west-2 | sh",
        r"aws s3 cp s3://\${BUILDKITE_SECRETS_BUCKET}/gcp-key-elementl-dev.json "
        + GCP_CREDS_LOCAL_FILE,
        "export GOOGLE_APPLICATION_CREDENTIALS=" + GCP_CREDS_LOCAL_FILE,
        "pushd python_modules/libraries/dagster-airflow/dagster_airflow_tests/",
        "docker-compose up -d --remove-orphans",
        *network_buildkite_container("postgres"),
        *connect_sibling_docker_container(
            "postgres",
            "test-postgres-db-airflow",
            "POSTGRES_TEST_DB_HOST",
        ),
        "popd",
    ]


def airline_demo_extra_cmds_fn(_) -> List[str]:
    return [
        "pushd examples/airline_demo",
        # Run the postgres db. We are in docker running docker
        # so this will be a sibling container.
        "docker-compose up -d --remove-orphans",  # clean up in hooks/pre-exit
        # Can't use host networking on buildkite and communicate via localhost
        # between these sibling containers, so pass along the ip.
        *network_buildkite_container("postgres"),
        *connect_sibling_docker_container(
            "postgres", "test-postgres-db-airline", "POSTGRES_TEST_DB_HOST"
        ),
        "popd",
    ]


def dbt_example_extra_cmds_fn(_) -> List[str]:
    return [
        "pushd examples/dbt_example",
        # Run the postgres db. We are in docker running docker
        # so this will be a sibling container.
        "docker-compose up -d --remove-orphans",  # clean up in hooks/pre-exit
        # Can't use host networking on buildkite and communicate via localhost
        # between these sibling containers, so pass along the ip.
        *network_buildkite_container("postgres"),
        *connect_sibling_docker_container(
            "postgres", "dbt_example_postgresql", "DAGSTER_DBT_EXAMPLE_PGHOST"
        ),
        "mkdir -p ~/.dbt/",
        "touch ~/.dbt/profiles.yml",
        "cat dbt_example_project/profiles.yml >> ~/.dbt/profiles.yml",
        "popd",
    ]


def docs_snippets_extra_cmds_fn(_) -> List[str]:
    return [
        "pushd examples/docs_snippets",
        # Run the postgres db. We are in docker running docker
        # so this will be a sibling container.
        "docker-compose up -d --remove-orphans",  # clean up in hooks/pre-exit
        # Can't use host networking on buildkite and communicate via localhost
        # between these sibling containers, so pass along the ip.
        *network_buildkite_container("postgres"),
        *connect_sibling_docker_container(
            "postgres", "test-postgres-db-docs-snippets", "POSTGRES_TEST_DB_HOST"
        ),
        "popd",
    ]


def deploy_docker_example_extra_cmds_fn(_) -> List[str]:
    return [
        "pushd examples/deploy_docker/from_source",
        "./build.sh",
        "docker-compose up -d --remove-orphans",  # clean up in hooks/pre-exit
        *network_buildkite_container("docker_example_network"),
        *connect_sibling_docker_container(
            "docker_example_network",
            "docker_example_dagit",
            "DEPLOY_DOCKER_DAGIT_HOST",
        ),
        "popd",
    ]


def celery_extra_cmds_fn(version: str) -> List[str]:
    return [
        "export DAGSTER_DOCKER_IMAGE_TAG=$${BUILDKITE_BUILD_ID}-" + version,
        'export DAGSTER_DOCKER_REPOSITORY="$${AWS_ACCOUNT_ID}.dkr.ecr.us-west-2.amazonaws.com"',
        "pushd python_modules/libraries/dagster-celery",
        # Run the rabbitmq db. We are in docker running docker
        # so this will be a sibling container.
        "docker-compose up -d --remove-orphans",  # clean up in hooks/pre-exit,
        # Can't use host networking on buildkite and communicate via localhost
        # between these sibling containers, so pass along the ip.
        *network_buildkite_container("rabbitmq"),
        *connect_sibling_docker_container(
            "rabbitmq", "test-rabbitmq", "DAGSTER_CELERY_BROKER_HOST"
        ),
        "popd",
    ]


def celery_docker_extra_cmds_fn(version: str) -> List[str]:
    return celery_extra_cmds_fn(version) + [
        "pushd python_modules/libraries/dagster-celery-docker/dagster_celery_docker_tests/",
        "docker-compose up -d --remove-orphans",
        *network_buildkite_container("postgres"),
        *connect_sibling_docker_container(
            "postgres",
            "test-postgres-db-celery-docker",
            "POSTGRES_TEST_DB_HOST",
        ),
        "popd",
    ]


def docker_extra_cmds_fn(version: str) -> List[str]:
    return [
        "export DAGSTER_DOCKER_IMAGE_TAG=$${BUILDKITE_BUILD_ID}-" + version,
        'export DAGSTER_DOCKER_REPOSITORY="$${AWS_ACCOUNT_ID}.dkr.ecr.us-west-2.amazonaws.com"',
        "pushd python_modules/libraries/dagster-docker/dagster_docker_tests/",
        "docker-compose up -d --remove-orphans",
        *network_buildkite_container("postgres"),
        *connect_sibling_docker_container(
            "postgres",
            "test-postgres-db-docker",
            "POSTGRES_TEST_DB_HOST",
        ),
        "popd",
    ]


def dagster_extra_cmds_fn(version: str) -> List[str]:
    return [
        "export DAGSTER_DOCKER_IMAGE_TAG=$${BUILDKITE_BUILD_ID}-" + version,
        'export DAGSTER_DOCKER_REPOSITORY="$${AWS_ACCOUNT_ID}.dkr.ecr.us-west-2.amazonaws.com"',
        "aws ecr get-login --no-include-email --region us-west-2 | sh",
        "export IMAGE_NAME=$${AWS_ACCOUNT_ID}.dkr.ecr.us-west-2.amazonaws.com/buildkite-test-image-core:$${BUILDKITE_BUILD_ID}-"
        + version,
        "pushd python_modules/dagster/dagster_tests",
        "docker-compose up -d --remove-orphans",  # clean up in hooks/pre-exit
        *network_buildkite_container("dagster"),
        *connect_sibling_docker_container("dagster", "dagster-grpc-server", "GRPC_SERVER_HOST"),
        "popd",
    ]


def dagit_extra_cmds_fn(_) -> List[str]:
    return ["make rebuild_dagit"]


def mysql_extra_cmds_fn(_) -> List[str]:
    return [
        "pushd python_modules/libraries/dagster-mysql/dagster_mysql_tests/",
        "docker-compose up -d --remove-orphans",  # clean up in hooks/pre-exit,
        "docker-compose -f docker-compose-multi.yml up -d",  # clean up in hooks/pre-exit,
        *network_buildkite_container("mysql"),
        *connect_sibling_docker_container("mysql", "test-mysql-db", "MYSQL_TEST_DB_HOST"),
        *network_buildkite_container("mysql_multi"),
        *connect_sibling_docker_container(
            "mysql_multi",
            "test-run-storage-db",
            "MYSQL_TEST_RUN_STORAGE_DB_HOST",
        ),
        *connect_sibling_docker_container(
            "mysql_multi",
            "test-event-log-storage-db",
            "MYSQL_TEST_EVENT_LOG_STORAGE_DB_HOST",
        ),
        "popd",
    ]


def dbt_extra_cmds_fn(_) -> List[str]:
    return [
        "pushd python_modules/libraries/dagster-dbt/dagster_dbt_tests",
        "docker-compose up -d --remove-orphans",  # clean up in hooks/pre-exit,
        # Can't use host networking on buildkite and communicate via localhost
        # between these sibling containers, so pass along the ip.
        *network_buildkite_container("postgres"),
        *connect_sibling_docker_container(
            "postgres", "test-postgres-db-dbt", "POSTGRES_TEST_DB_DBT_HOST"
        ),
        "popd",
    ]


def k8s_extra_cmds_fn(version) -> List[str]:
    return [
        "export DAGSTER_DOCKER_IMAGE_TAG=$${BUILDKITE_BUILD_ID}-" + version,
        'export DAGSTER_DOCKER_REPOSITORY="$${AWS_ACCOUNT_ID}.dkr.ecr.us-west-2.amazonaws.com"',
    ]


def gcp_extra_cmds_fn(_) -> List[str]:
    return [
        r"aws s3 cp s3://\${BUILDKITE_SECRETS_BUCKET}/gcp-key-elementl-dev.json "
        + GCP_CREDS_LOCAL_FILE,
        "export GOOGLE_APPLICATION_CREDENTIALS=" + GCP_CREDS_LOCAL_FILE,
    ]


def postgres_extra_cmds_fn(_) -> List[str]:
    return [
        "pushd python_modules/libraries/dagster-postgres/dagster_postgres_tests/",
        "docker-compose up -d --remove-orphans",  # clean up in hooks/pre-exit,
        "docker-compose -f docker-compose-multi.yml up -d",  # clean up in hooks/pre-exit,
        *network_buildkite_container("postgres"),
        *connect_sibling_docker_container("postgres", "test-postgres-db", "POSTGRES_TEST_DB_HOST"),
        *network_buildkite_container("postgres_multi"),
        *connect_sibling_docker_container(
            "postgres_multi",
            "test-run-storage-db",
            "POSTGRES_TEST_RUN_STORAGE_DB_HOST",
        ),
        *connect_sibling_docker_container(
            "postgres_multi",
            "test-event-log-storage-db",
            "POSTGRES_TEST_EVENT_LOG_STORAGE_DB_HOST",
        ),
        "popd",
    ]


def graphql_pg_extra_cmds_fn(_) -> List[str]:
    return [
        "pushd python_modules/dagster-graphql/dagster_graphql_tests/graphql/",
        "docker-compose up -d --remove-orphans",  # clean up in hooks/pre-exit,
        # Can't use host networking on buildkite and communicate via localhost
        # between these sibling containers, so pass along the ip.
        *network_buildkite_container("postgres"),
        *connect_sibling_docker_container(
            "postgres", "test-postgres-db-graphql", "POSTGRES_TEST_DB_HOST"
        ),
        "popd",
    ]


# Some Dagster packages have more involved test configs or support only certain Python version;
# special-case those here
DAGSTER_PACKAGES_WITH_CUSTOM_TESTS: List[PackageBuildSpec] = [
    PackageBuildSpec(
        "examples/dbt_example",
        extra_cmds_fn=dbt_example_extra_cmds_fn,
        buildkite_label="dbt_example",
        upload_coverage=False,
        supported_pythons=ExamplePythons,
    ),
    PackageBuildSpec(
        "examples/deploy_docker",
        extra_cmds_fn=deploy_docker_example_extra_cmds_fn,
        buildkite_label="deploy_docker_example",
        upload_coverage=False,
        supported_pythons=ExamplePythons,
    ),
    PackageBuildSpec(
        "examples/docs_snippets",
        extra_cmds_fn=docs_snippets_extra_cmds_fn,
        buildkite_label="docs_snippets",
        upload_coverage=False,
        supported_pythons=ExamplePythons,
        run_mypy=False,
    ),
    PackageBuildSpec(
        "examples/hacker_news_assets",
        env_vars=["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"],
        buildkite_label="hacker_news_assets",
        upload_coverage=False,
        supported_pythons=ExamplePythons,
    ),
    PackageBuildSpec(
        "examples/hacker_news",
        env_vars=["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"],
        buildkite_label="hacker_news_example",
        upload_coverage=False,
        supported_pythons=ExamplePythons,
    ),
    PackageBuildSpec("python_modules/dagit", extra_cmds_fn=dagit_extra_cmds_fn),
    PackageBuildSpec("python_modules/automation"),
    PackageBuildSpec(
        "python_modules/dagster",
        extra_cmds_fn=dagster_extra_cmds_fn,
        env_vars=["AWS_ACCOUNT_ID"],
        depends_on_fn=core_test_image_depends_fn,
        tox_env_suffixes=[
            "-api_tests",
            "-cli_tests",
            "-core_tests",
            "-core_tests_old_sqlalchemy",
            "-daemon_tests",
            "-definitions_tests_old_pendulum",
            "-general_tests",
            "-scheduler_tests",
            "-scheduler_tests_old_pendulum",
            "-execution_tests",
        ],
    ),
    PackageBuildSpec(
        "python_modules/dagster-graphql",
        tox_env_suffixes=[
            "-not_graphql_context_test_suite",
            "-in_memory_instance_multi_location",
            "-in_memory_instance_managed_grpc_env",
            "-sqlite_instance_multi_location",
            "-sqlite_instance_managed_grpc_env",
            "-sqlite_instance_deployed_grpc_env",
            "-graphql_python_client",
        ],
    ),
    PackageBuildSpec(
        "python_modules/dagster-graphql",
        extra_cmds_fn=graphql_pg_extra_cmds_fn,
        tox_file="tox_postgres.ini",
        buildkite_label="dagster-graphql-postgres",
        tox_env_suffixes=[
            "-graphql_context_variants",
            "-postgres_instance_multi_location",
            "-postgres_instance_managed_grpc_env",
            "-postgres_instance_deployed_grpc_env",
        ],
    ),
    PackageBuildSpec(
        "python_modules/dagster-test",
    ),
    PackageBuildSpec(
        "python_modules/libraries/dagster-dbt",
        extra_cmds_fn=dbt_extra_cmds_fn,
        # dbt-core no longer supports python 3.6
        supported_pythons=(
            [
                SupportedPython.V3_7,
                SupportedPython.V3_8,
                SupportedPython.V3_9,
            ]
            if (branch_name == "master" or is_release_branch(branch_name))
            else [SupportedPython.V3_9]
        ),
    ),
    PackageBuildSpec(
        "python_modules/libraries/dagster-airflow",
        # omit python 3.9 until we add support
        supported_pythons=(
            [
                SupportedPython.V3_6,
                SupportedPython.V3_7,
                SupportedPython.V3_8,
            ]
            if (branch_name == "master" or is_release_branch(branch_name))
            else [SupportedPython.V3_8]
        ),
        env_vars=[
            "AIRFLOW_HOME",
            "AWS_ACCOUNT_ID",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "BUILDKITE_SECRETS_BUCKET",
            "GOOGLE_APPLICATION_CREDENTIALS",
        ],
        extra_cmds_fn=airflow_extra_cmds_fn,
        depends_on_fn=test_image_depends_fn,
        tox_env_suffixes=["-default", "-requiresairflowdb"],
    ),
    PackageBuildSpec(
        "python_modules/libraries/dagster-aws",
        env_vars=["AWS_DEFAULT_REGION", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"],
    ),
    PackageBuildSpec(
        "python_modules/libraries/dagster-azure",
        env_vars=["AZURE_STORAGE_ACCOUNT_KEY"],
    ),
    PackageBuildSpec(
        "python_modules/libraries/dagster-celery",
        env_vars=["AWS_ACCOUNT_ID", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"],
        extra_cmds_fn=celery_extra_cmds_fn,
        depends_on_fn=test_image_depends_fn,
    ),
    PackageBuildSpec(
        "python_modules/libraries/dagster-celery-docker",
        env_vars=["AWS_ACCOUNT_ID", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"],
        extra_cmds_fn=celery_docker_extra_cmds_fn,
        depends_on_fn=test_image_depends_fn,
    ),
    PackageBuildSpec(
        "python_modules/libraries/dagster-dask",
        env_vars=["AWS_SECRET_ACCESS_KEY", "AWS_ACCESS_KEY_ID", "AWS_DEFAULT_REGION"],
    ),
    PackageBuildSpec(
        "python_modules/libraries/dagster-databricks",
        run_mypy=False,
    ),
    PackageBuildSpec(
        "python_modules/libraries/dagster-docker",
        env_vars=["AWS_ACCOUNT_ID", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"],
        extra_cmds_fn=docker_extra_cmds_fn,
        depends_on_fn=test_image_depends_fn,
        run_mypy=False,
    ),
    PackageBuildSpec(
        "python_modules/libraries/dagster-gcp",
        env_vars=[
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "BUILDKITE_SECRETS_BUCKET",
            "GCP_PROJECT_ID",
        ],
        extra_cmds_fn=gcp_extra_cmds_fn,
        # Remove once https://github.com/dagster-io/dagster/issues/2511 is resolved
        retries=2,
    ),
    PackageBuildSpec(
        "python_modules/libraries/dagster-k8s",
        env_vars=[
            "AWS_ACCOUNT_ID",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "BUILDKITE_SECRETS_BUCKET",
        ],
        extra_cmds_fn=k8s_extra_cmds_fn,
        depends_on_fn=test_image_depends_fn,
    ),
    PackageBuildSpec("python_modules/libraries/dagster-mlflow", upload_coverage=False),
    PackageBuildSpec("python_modules/libraries/dagster-mysql", extra_cmds_fn=mysql_extra_cmds_fn),
    PackageBuildSpec(
        "python_modules/libraries/dagster-pandera",
        supported_pythons=(
            [
                SupportedPython.V3_7,
                SupportedPython.V3_8,
                SupportedPython.V3_9,
            ]
            if (branch_name == "master" or is_release_branch(branch_name))
            else [SupportedPython.V3_9]
        ),
    ),
    PackageBuildSpec(
        "python_modules/libraries/dagster-snowflake",
        supported_pythons=(  # dropped python 3.6 support
            [
                SupportedPython.V3_7,
                SupportedPython.V3_8,
                SupportedPython.V3_9,
            ]
            if (branch_name == "master" or is_release_branch(branch_name))
            else [SupportedPython.V3_9]
        ),
    ),
    PackageBuildSpec(
        "python_modules/libraries/dagster-snowflake-pandas",
        supported_pythons=(  # dropped python 3.6 support
            [
                SupportedPython.V3_7,
                SupportedPython.V3_8,
                SupportedPython.V3_9,
            ]
            if (branch_name == "master" or is_release_branch(branch_name))
            else [SupportedPython.V3_9]
        ),
    ),
    PackageBuildSpec(
        "python_modules/libraries/dagster-postgres", extra_cmds_fn=postgres_extra_cmds_fn
    ),
    PackageBuildSpec(
        "python_modules/libraries/dagster-twilio",
        env_vars=["TWILIO_TEST_ACCOUNT_SID", "TWILIO_TEST_AUTH_TOKEN"],
        # Remove once https://github.com/dagster-io/dagster/issues/2511 is resolved
        retries=2,
    ),
    PackageBuildSpec(
        "python_modules/libraries/dagstermill",
        tox_env_suffixes=["-papermill1", "-papermill2"],
    ),
    PackageBuildSpec(
        "python_modules/libraries/dagster-ge",
        supported_pythons=(  # dropped python 3.6 support
            [
                SupportedPython.V3_7,
                SupportedPython.V3_8,
                SupportedPython.V3_9,
            ]
            if (branch_name == "master" or is_release_branch(branch_name))
            else [SupportedPython.V3_9]
        ),
    ),
]


_customized_packages = set([pkg.directory for pkg in DAGSTER_PACKAGES_WITH_CUSTOM_TESTS])


# Find packages under a root subdirectory that are not configured above.
def _get_uncustomized_pkgs(root):
    return (
        set(
            [
                os.path.relpath(p, GIT_REPO_ROOT)
                for p in glob(f"{GIT_REPO_ROOT}/{root}/*")
                if os.path.exists(f"{p}/tox.ini")
            ]
        )
        - _customized_packages
    )


DAGSTER_PACKAGES_WITH_STANDARD_STEPS = [
    *[
        PackageBuildSpec(pkg, upload_coverage=False)
        for pkg in _get_uncustomized_pkgs("python_modules")
    ],
    *[PackageBuildSpec(pkg) for pkg in _get_uncustomized_pkgs("python_modules/libraries")],
    *[
        PackageBuildSpec(pkg, upload_coverage=False, supported_pythons=ExamplePythons)
        for pkg in _get_uncustomized_pkgs("examples")
    ],
]


def build_pipenv_smoke_steps() -> List[CommandStep]:
    is_release = check_for_release()
    if is_release:
        return []

    # tempoarily pinned due to issue with 2021.11.5, see https://github.com/dagster-io/dagster/issues/5565
    smoke_test_steps = [
        "pushd /workdir/python_modules",
        "pip install pipenv==2021.5.29",
        "pipenv install",
    ]

    # See: https://github.com/dagster-io/dagster/issues/2079
    return [
        StepBuilder(f"pipenv smoke tests {TOX_MAP[version]}")
        .run(*smoke_test_steps)
        .on_unit_image(version)
        .build()
        for version in get_python_versions_for_branch()
    ]


def build_coverage_step() -> CommandStep:
    return (
        StepBuilder(":coverage:")
        .run(
            "mkdir -p tmp",
            'buildkite-agent artifact download ".coverage*" tmp/',
            'buildkite-agent artifact download "lcov.*" tmp/',
            "cd tmp",
            "coverage debug sys",
            "coverage debug data",
            "coverage combine",
            # coveralls-lcov is currently not working - fails with:
            # converter.rb:63:in `initialize': No such file or directory @ rb_sysopen - jest/mocks/dagre_layout.worker.ts
            # "coveralls-lcov -v -n lcov.* > coverage.js.json",
            "coveralls",  # add '--merge=coverage.js.json' to report JS coverage
        )
        .on_python_image(
            "buildkite-coverage:py3.8.7-{version}".format(version=COVERAGE_IMAGE_VERSION),
            [
                "COVERALLS_REPO_TOKEN",  # exported by /env in ManagedSecretsBucket
                "CI_NAME",
                "CI_BUILD_NUMBER",
                "CI_BUILD_URL",
                "CI_BRANCH",
                "CI_PULL_REQUEST",
            ],
        )
        .build()
    )


def build_pylint_misc_steps() -> List[CommandStep]:
    base_paths = ["docs"]
    base_paths_ext = ['"%s/**.py"' % p for p in base_paths]

    return [
        StepBuilder("pylint misc")
        .run(
            # Deps needed to pylint docs
            """pip install \
                -e python_modules/dagster[test] \
                -e python_modules/dagster-graphql \
                -e python_modules/dagit \
                -e python_modules/automation \
                -e python_modules/libraries/dagstermill \
                -e python_modules/libraries/dagster-celery \
                -e python_modules/libraries/dagster-dask \
            """,
            "pylint -j 0 --rcfile=pyproject.toml `git ls-files %s`" % " ".join(base_paths_ext),
        )
        .on_integration_image(SupportedPython.V3_7)
        .build()
    ]


def build_repo_wide_black_steps() -> List[CommandStep]:
    return [
        StepBuilder(":python-black:")
        # See: https://github.com/dagster-io/dagster/issues/1999
        .run("pip install -e python_modules/dagster[black]", "make check_black")
        .on_integration_image(SupportedPython.V3_7)
        .build(),
    ]


def build_repo_wide_isort_steps() -> List[CommandStep]:
    return [
        StepBuilder(":isort:")
        .run("pip install -e python_modules/dagster[isort]", "make check_isort")
        .on_integration_image(SupportedPython.V3_7)
        .build(),
    ]


def build_repo_wide_check_manifest_steps(version=DEFAULT_PYTHON_VERSION) -> List[CommandStep]:
    published_packages = [
        "python_modules/dagit",
        "python_modules/dagster",
        "python_modules/dagster-graphql",
        *(
            os.path.relpath(p, GIT_REPO_ROOT)
            for p in glob(f"{GIT_REPO_ROOT}/python_modules/libraries/*")
        ),
    ]

    commands = [
        "pip install check-manifest",
        *(f"check-manifest {library}" for library in published_packages),
    ]

    return [StepBuilder("check-manifest").on_integration_image(version).run(*commands).build()]


def build_sql_schema_check_steps(version=DEFAULT_PYTHON_VERSION) -> List[CommandStep]:
    return [
        StepBuilder("SQL schema checks")
        .on_integration_image(version)
        .run("pip install -e python_modules/dagster", "python scripts/check_schemas.py")
        .build()
    ]


def build_graphql_python_client_backcompat_steps(
    version=DEFAULT_PYTHON_VERSION,
) -> List[CommandStep]:
    return [
        StepBuilder("Backwards compat checks for the GraphQL Python Client")
        .on_integration_image(version)
        .run(
            "pip install -e python_modules/dagster[test] -e python_modules/dagster-graphql -e python_modules/automation",
            "dagster-graphql-client query check",
        )
        .build()
    ]
