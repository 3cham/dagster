import os
from typing import List

from ..defines import SupportedPython
from ..package_build_spec import PackageBuildSpec
from ..step_builder import StepBuilder
from ..utils import BuildkiteLeafStep, BuildkiteStep, CommandStep, GroupStep


def build_helm_steps() -> List[BuildkiteStep]:
    steps: List[BuildkiteLeafStep] = []
    steps += _build_lint_steps()
    schema_group = PackageBuildSpec(
        os.path.join("helm", "dagster", "schema"),
        supported_pythons=[SupportedPython.V3_8],
        buildkite_label="dagster-helm-schema",
        upload_coverage=False,
        retries=2,
    ).build_steps()[0]
    steps += schema_group["steps"]

    return [
        GroupStep(
            group=":helm: helm",
            key="helm",
            steps=steps,
        )
    ]


def _build_lint_steps() -> List[CommandStep]:
    return [
        StepBuilder(":yaml: :lint-roller:")
        .run(
            "pip install yamllint",
            "make yamllint",
        )
        .on_integration_image(SupportedPython.V3_8)
        .build(),
        StepBuilder("dagster-json-schema")
        .run(
            "pip install -e helm/dagster/schema",
            "dagster-helm schema apply",
            "git diff --exit-code",
        )
        .on_integration_image(SupportedPython.V3_8)
        .build(),
        StepBuilder(":lint-roller: dagster")
        .run(
            "helm lint helm/dagster --with-subcharts --strict",
        )
        .on_integration_image(SupportedPython.V3_8)
        .with_retry(2)
        .build(),
        StepBuilder("dagster dependency build")
        .run(
            "helm repo add bitnami https://charts.bitnami.com/bitnami",
            "helm dependency build helm/dagster",
        )
        .on_integration_image(SupportedPython.V3_8)
        .build(),
    ]
