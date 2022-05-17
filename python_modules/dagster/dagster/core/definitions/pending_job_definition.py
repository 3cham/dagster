from functools import update_wrapper
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Dict,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

import dagster._check as check
from dagster.core.definitions.composition import MappedInputPlaceholder
from dagster.core.definitions.dependency import (
    DependencyDefinition,
    DynamicCollectDependencyDefinition,
    IDependencyDefinition,
    MultiDependencyDefinition,
    Node,
    NodeHandle,
    NodeInvocation,
    SolidOutputHandle,
)
from dagster.core.definitions.node_definition import NodeDefinition
from dagster.core.definitions.policy import RetryPolicy
from dagster.core.errors import (
    DagsterInvalidDefinitionError,
    DagsterInvalidInvocationError,
    DagsterInvalidSubsetError,
)
from dagster.core.selector.subset_selector import (
    LeafNodeSelection,
    OpSelectionData,
    parse_op_selection,
)
from dagster.core.storage.fs_asset_io_manager import fs_asset_io_manager
from dagster.core.utils import str_format_set
from dagster.utils import merge_dicts

from .asset_layer import AssetLayer
from .config import ConfigMapping
from .executor_definition import ExecutorDefinition
from .graph_definition import GraphDefinition, SubselectedGraphDefinition
from .hook_definition import HookDefinition
from .job_definition import JobDefinition, get_subselected_graph_definition
from .logger_definition import LoggerDefinition
from .mode import ModeDefinition
from .partition import PartitionSetDefinition, PartitionedConfig, PartitionsDefinition
from .pipeline_definition import PipelineDefinition
from .preset import PresetDefinition
from .resource_definition import ResourceDefinition
from .run_request import RunRequest
from .version_strategy import VersionStrategy

if TYPE_CHECKING:
    from dagster.core.execution.execute_in_process_result import ExecuteInProcessResult
    from dagster.core.instance import DagsterInstance
    from dagster.core.snap import PipelineSnapshot


class PendingJobDefinition(NamedTuple):
    resource_defs: Optional[Mapping[str, ResourceDefinition]]
    loggers: Optional[Dict[str, LoggerDefinition]]
    executor_def: Optional[ExecutorDefinition]
    config_mapping: Optional[ConfigMapping]
    partitioned_config: Optional["PartitionedConfig"]
    graph_def: GraphDefinition
    name: Optional[str] = None
    description: Optional[str] = None
    preset_defs: Optional[List[PresetDefinition]] = None
    tags: Optional[Dict[str, Any]] = None
    hook_defs: Optional[AbstractSet[HookDefinition]] = None
    op_retry_policy: Optional[RetryPolicy] = None
    version_strategy: Optional[VersionStrategy] = None
    op_selection_data: Optional[OpSelectionData] = None
    asset_layer: Optional[AssetLayer] = None

    def coerce_to_job_def(self, resource_defs: Dict[str, ResourceDefinition]) -> JobDefinition:
        override_resource_defs = self.resource_defs

        resource_defs = merge_dicts(
            resource_defs, override_resource_defs if override_resource_defs else {}
        )

        return JobDefinition(
            resource_defs=resource_defs,
            logger_defs=self.loggers,
            executor_def=self.executor_def,
            config_mapping=self.config_mapping,
            partitioned_config=self.partitioned_config,
            graph_def=self.graph_def,
            name=self.name,
            description=self.description,
            preset_defs=self.preset_defs,
            tags=self.tags,
            hook_defs=self.hook_defs,
            op_retry_policy=self.op_retry_policy,
            version_strategy=self.version_strategy,
            _op_selection_data=self.op_selection_data,
            asset_layer=self.asset_layer,
        )

    def get_job_def_for_op_selection(
        self,
        op_selection: Optional[List[str]] = None,
    ) -> "PendingJobDefinition":
        if not op_selection:
            return self

        op_selection = check.opt_list_param(op_selection, "op_selection", str)

        resolved_op_selection_dict = parse_op_selection(self.graph_def, op_selection)

        sub_graph = get_subselected_graph_definition(self.graph_def, resolved_op_selection_dict)

        return PendingJobDefinition(
            name=self.name,
            description=self.description,
            resource_defs=self.resource_defs,
            loggers=self.loggers,
            executor_def=self.executor_def,
            config_mapping=self.config_mapping,
            partitioned_config=self.partitioned_config,
            preset_defs=self.preset_defs,
            tags=self.tags,
            hook_defs=self.hook_defs,
            op_retry_policy=self.op_retry_policy,
            graph_def=sub_graph,
            version_strategy=self.version_strategy,
            op_selection_data=OpSelectionData(
                op_selection=op_selection,
                resolved_op_selection=set(
                    resolved_op_selection_dict.keys()
                ),  # equivalent to solids_to_execute. currently only gets top level nodes.
                parent_job_def=self,  # used by pipeline snapshot lineage
            ),
        )
