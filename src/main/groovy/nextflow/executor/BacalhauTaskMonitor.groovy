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
 * Uses a longer default polling interval than HPC defaults because Bacalhau
 * jobs typically run for minutes to hours, not seconds.
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

    static final Duration DEFAULT_POLL_INTERVAL = Duration.of('30sec')

    static final int DEFAULT_QUEUE_SIZE = 100

    /**
     * Factory with explicit configuration. Overrides are sourced from the
     * active {@link Session} so users can tune via {@code executor.pollInterval}
     * and {@code executor.queueSize}.
     */
    static BacalhauTaskMonitor create(Session session, String name, Duration defPoll, int capacity) {
        assert session
        assert name

        final Duration pollInterval = session.getPollInterval(name, defPoll)
        final Duration dumpInterval = session.getMonitorDumpInterval(name)
        final int      queueSize    = session.getQueueSize(name, capacity)

        log.debug """\
            Creating BacalhauTaskMonitor
              name          : $name
              pollInterval  : $pollInterval
              dumpInterval  : $dumpInterval
              queueSize     : $queueSize
            """.stripIndent()

        return new BacalhauTaskMonitor(
            session: session,
            name: name,
            capacity: queueSize,
            pollInterval: pollInterval,
            dumpInterval: dumpInterval)
    }

    /** Convenience factory using Bacalhau defaults. */
    static BacalhauTaskMonitor create(Session session) {
        return create(session, 'bacalhau', DEFAULT_POLL_INTERVAL, DEFAULT_QUEUE_SIZE)
    }

    protected BacalhauTaskMonitor(Map params) {
        super(params)
    }

    @Override
    protected void checkTaskStatus(TaskHandler handler) {
        if (log.isTraceEnabled())
            log.trace "Checking status for task: ${handler.task?.name}"
        super.checkTaskStatus(handler)
    }
}
