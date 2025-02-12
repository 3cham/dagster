import {
  Box,
  NonIdealState,
  FirstOrSecondPanelToggle,
  SplitPanelContainer,
  ErrorBoundary,
} from '@dagster-io/ui';
import * as React from 'react';
import styled from 'styled-components/macro';

import {showCustomAlert} from '../app/CustomAlertProvider';
import {filterByQuery} from '../app/GraphQueryImpl';
import {PythonErrorInfo} from '../app/PythonErrorInfo';
import {isHiddenAssetGroupJob} from '../asset-graph/Utils';
import {GanttChart, GanttChartLoadingState, GanttChartMode, QueuedState} from '../gantt/GanttChart';
import {toGraphQueryItems} from '../gantt/toGraphQueryItems';
import {RunStatus} from '../graphql/types';
import {useDocumentTitle} from '../hooks/useDocumentTitle';
import {useFavicon} from '../hooks/useFavicon';
import {useQueryPersistedState} from '../hooks/useQueryPersistedState';
import {useSupportsCapturedLogs} from '../instance/useSupportsCapturedLogs';

import {CapturedOrExternalLogPanel} from './CapturedLogPanel';
import {ComputeLogPanel} from './ComputeLogPanel';
import {LogFilter, LogsProvider, LogsProviderLogs} from './LogsProvider';
import {LogsScrollingTable} from './LogsScrollingTable';
import {LogsToolbar, LogType} from './LogsToolbar';
import {RunActionButtons} from './RunActionButtons';
import {RunContext} from './RunContext';
import {IRunMetadataDict, RunMetadataProvider} from './RunMetadataProvider';
import {RunDagsterRunEventFragment, RunPageFragment} from './types/RunFragments.types';
import {
  useComputeLogFileKeyForSelection,
  matchingComputeLogKeyFromStepKey,
} from './useComputeLogFileKeyForSelection';
import {useJobReExecution} from './useJobReExecution';
import {useQueryPersistedLogFilter} from './useQueryPersistedLogFilter';

interface RunProps {
  runId: string;
  run?: RunPageFragment;
}

const runStatusFavicon = (status: RunStatus) => {
  switch (status) {
    case RunStatus.FAILURE:
      return '/favicon-run-failed.svg';
    case RunStatus.SUCCESS:
      return '/favicon-run-success.svg';
    case RunStatus.STARTING:
    case RunStatus.STARTED:
    case RunStatus.CANCELING:
      return '/favicon-run-pending.svg';
    default:
      return '/favicon.svg';
  }
};

export const Run: React.FC<RunProps> = (props) => {
  const {run, runId} = props;
  const [logsFilter, setLogsFilter] = useQueryPersistedLogFilter();
  const [selectionQuery, setSelectionQuery] = useQueryPersistedState<string>({
    queryKey: 'selection',
    defaults: {selection: ''},
  });

  useFavicon(run ? runStatusFavicon(run.status) : '/favicon.svg');
  useDocumentTitle(
    run
      ? `${!isHiddenAssetGroupJob(run.pipelineName) ? run.pipelineName : ''} ${runId.slice(
          0,
          8,
        )} [${run.status}]`
      : `Run: ${runId}`,
  );

  const onShowStateDetails = (stepKey: string, logs: RunDagsterRunEventFragment[]) => {
    const errorNode = logs.find(
      (node) => node.__typename === 'ExecutionStepFailureEvent' && node.stepKey === stepKey,
    );

    if (errorNode) {
      showCustomAlert({
        body: <PythonErrorInfo error={errorNode} />,
      });
    }
  };

  const onSetSelectionQuery = (query: string) => {
    setSelectionQuery(query);
    setLogsFilter({
      ...logsFilter,
      logQuery: query !== '*' ? [{token: 'query', value: query}] : [],
    });
  };

  return (
    <RunContext.Provider value={run}>
      <LogsProvider key={runId} runId={runId}>
        {(logs) => (
          <RunMetadataProvider logs={logs}>
            {(metadata) => (
              <RunWithData
                run={run}
                runId={runId}
                logs={logs}
                logsFilter={logsFilter}
                metadata={metadata}
                selectionQuery={selectionQuery}
                onSetLogsFilter={setLogsFilter}
                onSetSelectionQuery={onSetSelectionQuery}
                onShowStateDetails={onShowStateDetails}
              />
            )}
          </RunMetadataProvider>
        )}
      </LogsProvider>
    </RunContext.Provider>
  );
};

interface RunWithDataProps {
  run?: RunPageFragment;
  runId: string;
  selectionQuery: string;
  logs: LogsProviderLogs;
  logsFilter: LogFilter;
  metadata: IRunMetadataDict;
  onSetLogsFilter: (v: LogFilter) => void;
  onSetSelectionQuery: (query: string) => void;
  onShowStateDetails: (stepKey: string, logs: RunDagsterRunEventFragment[]) => void;
}

const logTypeFromQuery = (queryLogType: string) => {
  switch (queryLogType) {
    case 'stdout':
      return LogType.stdout;
    case 'stderr':
      return LogType.stderr;
    default:
      return LogType.structured;
  }
};

/**
 * Note: There are two places we keep a "step query string" in the Run view:
 * selectionQuery and logsFilter.logsQuery.
 *
 * - selectionQuery is set when you click around in the Gannt view and is the
 *   selection used for re-execution, etc. When set, we autofill logsFilter.logsQuery.
 *
 * - logsFilter.logsQuery is used for filtering the logs. It can be cleared separately
 *   from the selectionQuery, so you can select a step but navigate elsewhere in the logs.
 *
 * We could revisit this in the future but I believe we iterated quite a bit to get to this
 * solution and we should avoid locking the two filter inputs together completely.
 */
const RunWithData: React.FC<RunWithDataProps> = ({
  run,
  runId,
  logs,
  logsFilter,
  metadata,
  selectionQuery,
  onSetLogsFilter,
  onSetSelectionQuery,
}) => {
  const onLaunch = useJobReExecution(run);
  const splitPanelContainer = React.createRef<SplitPanelContainer>();

  const [queryLogType, setQueryLogType] = useQueryPersistedState<string>({
    queryKey: 'logType',
    defaults: {logType: 'structured'},
  });

  const logType = logTypeFromQuery(queryLogType);
  const setLogType = (lt: LogType) => setQueryLogType(LogType[lt]);
  const [computeLogUrl, setComputeLogUrl] = React.useState<string | null>(null);

  const stepKeysJSON = JSON.stringify(Object.keys(metadata.steps).sort());
  const stepKeys = React.useMemo(() => JSON.parse(stepKeysJSON), [stepKeysJSON]);

  const runtimeGraph = run?.executionPlan && toGraphQueryItems(run?.executionPlan, metadata.steps);

  const selectionStepKeys = React.useMemo(() => {
    return runtimeGraph && selectionQuery && selectionQuery !== '*'
      ? filterByQuery(runtimeGraph, selectionQuery).all.map((n) => n.name)
      : [];
  }, [runtimeGraph, selectionQuery]);

  const supportsCapturedLogs = useSupportsCapturedLogs();
  const {
    logCaptureInfo,
    computeLogFileKey,
    setComputeLogFileKey,
  } = useComputeLogFileKeyForSelection({
    stepKeys,
    selectionStepKeys,
    metadata,
  });

  const logsFilterStepKeys = runtimeGraph
    ? logsFilter.logQuery
        .filter((v) => v.token && v.token === 'query')
        .reduce((accum, v) => {
          return [...accum, ...filterByQuery(runtimeGraph, v.value).all.map((n) => n.name)];
        }, [] as string[])
    : [];

  const onClickStep = (stepKey: string, evt: React.MouseEvent<any>) => {
    const index = selectionStepKeys.indexOf(stepKey);
    let newSelected: string[];
    const filterForExactStep = `"${stepKey}"`;
    if (evt.shiftKey) {
      // shift-click to multi select steps, preserving quotations if present
      newSelected = [
        ...selectionStepKeys.map((k) => (selectionQuery.includes(`"${k}"`) ? `"${k}"` : k)),
      ];

      if (index !== -1) {
        // deselect the step if already selected
        newSelected.splice(index, 1);
      } else {
        // select the step otherwise
        newSelected.push(filterForExactStep);
      }
    } else {
      if (selectionStepKeys.length === 1 && index !== -1) {
        // deselect the step if already selected
        newSelected = [];
      } else {
        // select the step otherwise
        newSelected = [filterForExactStep];

        // When only one step is selected, set the compute log key as well.
        const matchingLogKey = matchingComputeLogKeyFromStepKey(metadata.logCaptureSteps, stepKey);
        matchingLogKey && setComputeLogFileKey(matchingLogKey);
      }
    }

    onSetSelectionQuery(newSelected.join(', ') || '*');
  };

  const gantt = (metadata: IRunMetadataDict) => {
    if (!run) {
      return <GanttChartLoadingState runId={runId} />;
    }

    if (run.status === 'QUEUED') {
      return <QueuedState run={run} />;
    }

    if (run.executionPlan && runtimeGraph) {
      return (
        <ErrorBoundary region="gantt chart">
          <GanttChart
            options={{
              mode: GanttChartMode.WATERFALL_TIMED,
            }}
            toolbarActions={
              <Box flex={{direction: 'row', alignItems: 'center', gap: 12}}>
                <FirstOrSecondPanelToggle axis="vertical" container={splitPanelContainer} />
                <RunActionButtons
                  run={run}
                  onLaunch={onLaunch}
                  graph={runtimeGraph}
                  metadata={metadata}
                  selection={{query: selectionQuery, keys: selectionStepKeys}}
                />
              </Box>
            }
            runId={runId}
            graph={runtimeGraph}
            metadata={metadata}
            selection={{query: selectionQuery, keys: selectionStepKeys}}
            onClickStep={onClickStep}
            onSetSelection={onSetSelectionQuery}
            focusedTime={logsFilter.focusedTime}
          />
        </ErrorBoundary>
      );
    }

    return <NonIdealState icon="error" title="Unable to build execution plan" />;
  };

  return (
    <>
      <SplitPanelContainer
        ref={splitPanelContainer}
        axis="vertical"
        identifier="run-gantt"
        firstInitialPercent={35}
        firstMinSize={56}
        first={gantt(metadata)}
        second={
          <ErrorBoundary region="logs">
            <LogsContainer>
              <LogsToolbar
                logType={logType}
                onSetLogType={setLogType}
                filter={logsFilter}
                onSetFilter={onSetLogsFilter}
                steps={stepKeys}
                metadata={metadata}
                computeLogFileKey={computeLogFileKey}
                onSetComputeLogKey={setComputeLogFileKey}
                computeLogUrl={computeLogUrl}
                counts={logs.counts}
              />
              {logType !== LogType.structured ? (
                supportsCapturedLogs ? (
                  <CapturedOrExternalLogPanel
                    logKey={computeLogFileKey ? [runId, 'compute_logs', computeLogFileKey] : []}
                    logCaptureInfo={logCaptureInfo}
                    visibleIOType={LogType[logType]}
                    onSetDownloadUrl={setComputeLogUrl}
                  />
                ) : (
                  <ComputeLogPanel
                    runId={runId}
                    computeLogFileKey={stepKeys.length ? computeLogFileKey : undefined}
                    ioType={LogType[logType]}
                    setComputeLogUrl={setComputeLogUrl}
                  />
                )
              ) : (
                <LogsScrollingTable
                  logs={logs}
                  filter={logsFilter}
                  filterStepKeys={logsFilterStepKeys}
                  filterKey={`${JSON.stringify(logsFilter)}`}
                  metadata={metadata}
                />
              )}
            </LogsContainer>
          </ErrorBoundary>
        }
      />
    </>
  );
};

const LogsContainer = styled.div`
  display: flex;
  flex-direction: column;
  height: 100%;
`;
