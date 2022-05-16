import os
from typing import Optional

from ..defines import SupportedPython
from ..step_builder import CommandStep, StepBuilder


def build_mypy_step(root_dir: str, base_label: Optional[str] = None) -> CommandStep:
    base_label = base_label or os.path.basename(root_dir)
    return (
        StepBuilder(f":mypy: {base_label}")
        .run("pip install -U virtualenv", f"cd {root_dir}", "tox -vv -e mypy")
        .on_integration_image(SupportedPython.V3_8)
        .build()
    )
