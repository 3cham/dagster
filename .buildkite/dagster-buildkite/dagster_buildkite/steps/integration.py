import os
from typing import Callable, Dict, List, Optional

import packaging.version
import requests

from ..defines import GCP_CREDS_LOCAL_FILE
from ..package_build_spec import PackageBuildSpec
from ..utils import CommandStep, connect_sibling_docker_container, network_buildkite_container
from .test_images import build_publish_test_image_steps, test_image_depends_fn

SCRIPT_PATH = os.path.dirname(os.path.abspath(__file__))
DAGSTER_CURRENT_BRANCH = "current_branch"
EARLIEST_TESTED_RELEASE = "0.12.8"


def build_integration_steps() -> List[CommandStep]:
    steps = []
    steps += build_publish_test_image_steps()

    # Shared dependency of some test suites
    steps += PackageBuildSpec(
        os.path.join("integration_tests", "python_modules", "dagster-k8s-test-infra"),
        upload_coverage=True,
    ).build_tox_steps()

    # test suites
    steps += build_backcompat_suite_steps()
    steps += build_celery_k8s_suite_steps()
    steps += build_k8s_suite_steps()
    steps += build_daemon_suite_steps()

    return steps


# ########################
# ##### BACKCOMPAT
# ########################


def build_backcompat_suite_steps() -> List[CommandStep]:
    most_recent_dagster_release = _get_latest_dagster_release()
    tox_env_suffix_map = {
        "-dagit-latest-release": {
            "dagit": most_recent_dagster_release,
            "user_code": DAGSTER_CURRENT_BRANCH,
        },
        "-dagit-earliest-release": {
            "dagit": EARLIEST_TESTED_RELEASE,
            "user_code": DAGSTER_CURRENT_BRANCH,
        },
        "-user-code-latest-release": {
            "dagit": DAGSTER_CURRENT_BRANCH,
            "user_code": most_recent_dagster_release,
        },
        "-user-code-earliest-release": {
            "dagit": DAGSTER_CURRENT_BRANCH,
            "user_code": EARLIEST_TESTED_RELEASE,
        },
    }

    backcompat_build_steps = []
    for tox_env_suffix, release_mapping in tox_env_suffix_map.items():
        backcompat_build_steps += PackageBuildSpec(
            os.path.join("integration_tests", "test_suites", "backcompat-test-suite"),
            extra_cmds_fn=make_backcompat_extra_cmds_fn(release_mapping),
            tox_env_suffixes=[tox_env_suffix],
            buildkite_label="backcompat_tests",
            upload_coverage=False,
            retries=2,
        ).build_tox_steps()
    return backcompat_build_steps


def make_backcompat_extra_cmds_fn(release_mapping: Dict[str, str]) -> Callable[[str], List[str]]:

    dagit_version = release_mapping["dagit"]
    user_code_version = release_mapping["user_code"]

    def _extra_cmds_fn(_) -> List[str]:
        return [
            f"export EARLIEST_TESTED_RELEASE={EARLIEST_TESTED_RELEASE}",
            "pushd integration_tests/test_suites/backcompat-test-suite/dagit_service",
            f"./build.sh {dagit_version} {user_code_version}",
            "docker-compose up -d --remove-orphans",  # clean up in hooks/pre-exit
            *network_buildkite_container("dagit_service_network"),
            *connect_sibling_docker_container(
                "dagit_service_network",
                "dagit",
                "BACKCOMPAT_TESTS_DAGIT_HOST",
            ),
            "popd",
        ]

    return _extra_cmds_fn


# ########################
# ##### CELERY K8S
# ########################


def build_celery_k8s_suite_steps() -> List[CommandStep]:
    tox_env_suffixes = [
        "-default",
        "-markusercodedeploymentsubchart",
        "-markdaemon",
        "-markredis",
        "-markmonitoring",
    ]
    directory = os.path.join("integration_tests", "test_suites", "celery-k8s-test-suite")
    return build_integration_suite_steps(directory, tox_env_suffixes, upload_coverage=True)


# ########################
# ##### DAEMON
# ########################


def build_daemon_suite_steps():
    tox_env_suffixes = None
    directory = os.path.join("integration_tests", "test_suites", "daemon-test-suite")
    return build_integration_suite_steps(
        directory, tox_env_suffixes, upload_coverage=False, extra_commands_fn=daemon_extra_cmds_fn
    )


def daemon_extra_cmds_fn(version):
    return [
        "export DAGSTER_DOCKER_IMAGE_TAG=$${BUILDKITE_BUILD_ID}-" + version,
        'export DAGSTER_DOCKER_REPOSITORY="$${AWS_ACCOUNT_ID}.dkr.ecr.us-west-2.amazonaws.com"',
        "pushd integration_tests/test_suites/daemon-test-suite/monitoring_daemon_tests/",
        "docker-compose up -d --remove-orphans",
        *network_buildkite_container("postgres"),
        *connect_sibling_docker_container(
            "postgres",
            "test-postgres-db-docker",
            "POSTGRES_TEST_DB_HOST",
        ),
        "popd",
    ]


# ########################
# ##### K8S
# ########################


def build_k8s_suite_steps():
    tox_env_suffixes = ["-default", "-subchart"]
    directory = os.path.join("integration_tests", "test_suites", "k8s-test-suite")
    return build_integration_suite_steps(directory, tox_env_suffixes, upload_coverage=True)


# ########################
# ##### UTILITIES
# ########################


def build_integration_suite_steps(
    directory: str,
    tox_env_suffixes: Optional[List[str]],
    upload_coverage: bool,
    extra_commands_fn: Optional[Callable] = None,
    queue=None,
):
    extra_commands_fn = extra_commands_fn or integration_suite_extra_cmds_fn
    return PackageBuildSpec(
        directory,
        env_vars=[
            "AIRFLOW_HOME",
            "AWS_ACCOUNT_ID",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "BUILDKITE_SECRETS_BUCKET",
            "GOOGLE_APPLICATION_CREDENTIALS",
        ],
        upload_coverage=upload_coverage,
        extra_cmds_fn=extra_commands_fn,
        depends_on_fn=test_image_depends_fn,
        tox_env_suffixes=tox_env_suffixes,
        retries=2,
        timeout_in_minutes=30,
        queue=queue,
    ).build_tox_steps()


def integration_suite_extra_cmds_fn(version: str) -> List[str]:
    return [
        'export AIRFLOW_HOME="/airflow"',
        "mkdir -p $${AIRFLOW_HOME}",
        "export DAGSTER_DOCKER_IMAGE_TAG=$${BUILDKITE_BUILD_ID}-" + version,
        'export DAGSTER_DOCKER_REPOSITORY="$${AWS_ACCOUNT_ID}.dkr.ecr.us-west-2.amazonaws.com"',
        "aws ecr get-login --no-include-email --region us-west-2 | sh",
        r"aws s3 cp s3://\${BUILDKITE_SECRETS_BUCKET}/gcp-key-elementl-dev.json "
        + GCP_CREDS_LOCAL_FILE,
        "export GOOGLE_APPLICATION_CREDENTIALS=" + GCP_CREDS_LOCAL_FILE,
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


def _get_latest_dagster_release() -> str:
    res = requests.get("https://pypi.org/pypi/dagster/json")
    module_json = res.json()
    releases = module_json["releases"]
    release_versions = [packaging.version.parse(release) for release in releases.keys()]
    for release_version in reversed(sorted(release_versions)):
        if not release_version.is_prerelease:
            return str(release_version)
    assert False, "No releases found"
