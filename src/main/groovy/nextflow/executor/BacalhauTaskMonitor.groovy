/*
 * Copyright 2024, Nextflow Contributors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nextflow.executor

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.processor.TaskHandler
import nextflow.processor.TaskPollingMonitor
import nextflow.util.Duration

/**
 * Polling monitor for Bacalhau jobs.
 *
 * FIX #11: This class was referenced as a core component in CLAUDE.md but was
 * never implemented.  Without it the executor falls back to the inherited
 * {@link TaskPollingMonitor} which is tuned for HPC/grid schedulers and uses
 * fixed polling intervals that are too aggressive for Bacalhau's distributed,
 * potentially long-running jobs.
 *
 * This implementation extends {@link TaskPollingMonitor} to:
 * <ul>
 *   <li>Use a longer default polling interval suited to distributed compute.</li>
 *   <li>Provide a hook point for future Bacalhau-specific status aggregation
 *       (e.g. batch-polling via {@code bacalhau job list} once rather than
 *       per-job {@code bacalhau job describe} calls).</li>
 * </ul>
 *
 * <h3>Configuration</h3>
 * The polling interval can be tuned in {@code nextflow.config}:
 * <pre>
 * executor {
 *     pollInterval = '30 sec'   // how often to check job status (default: 30s)
 *     queueSize    = 100        // max concurrent Bacalhau jobs (default: 100)
 * }
 * </pre>
 *
 * @author Nextflow Contributors
 */
@Slf4j
@CompileStatic
class BacalhauTaskMonitor extends TaskPollingMonitor {

    /** Default polling interval — longer than HPC defaults because Bacalhau
     *  jobs typically run for minutes to hours, not seconds. */
    static final Duration DEFAULT_POLL_INTERVAL = Duration.of('30sec')

    /** Default maximum number of concurrently tracked Bacalhau jobs. */
    static final int DEFAULT_QUEUE_SIZE = 100

    /**
     * Create a monitor for the given session.
     *
     * @param session  The active Nextflow session.
     * @param name     Executor name (used for config key lookups).
     * @param defPoll  Default poll interval (overridden by {@code executor.pollInterval}).
     * @param capacity Default queue capacity (overridden by {@code executor.queueSize}).
     */
    static BacalhauTaskMonitor create(Session session, String name, Duration defPoll, int capacity) {
        assert session
        assert name

        final pollInterval = session.getPollInterval(name, defPoll)
        final dumpInterval = session.getMonitorDumpInterval(name)
        final queueSize    = session.getQueueSize(name, capacity)

        log.debug """\
            Creating BacalhauTaskMonitor
              name          : $name
              pollInterval  : $pollInterval
              dumpInterval  : $dumpInterval
              queueSize     : $queueSize
            """.stripIndent()

        return new BacalhauTaskMonitor(session, name, queueSize, pollInterval, dumpInterval)
    }

    /**
     * Convenience factory using Bacalhau defaults.
     */
    static BacalhauTaskMonitor create(Session session) {
        return create(session, 'bacalhau', DEFAULT_POLL_INTERVAL, DEFAULT_QUEUE_SIZE)
    }

    protected BacalhauTaskMonitor(Session session, String name, int capacity,
                                   Duration pollInterval, Duration dumpInterval) {
        super(
            builder()
                .session(session)
                .name(name)
                .capacity(capacity)
                .pollInterval(pollInterval)
                .dumpInterval(dumpInterval)
        )
    }

    /**
     * Check whether a handler's job is still active.
     *
     * Delegates to the handler's own {@code checkIfRunning()} and
     * {@code checkIfCompleted()} methods, which query Bacalhau via the CLI.
     * Override this method to implement batch status polling in a future phase.
     */
    @Override
    protected boolean checkTaskStatus(TaskHandler handler) {
        log.trace "Checking status for task: ${handler.task?.name}"
        return super.checkTaskStatus(handler)
    }
}
