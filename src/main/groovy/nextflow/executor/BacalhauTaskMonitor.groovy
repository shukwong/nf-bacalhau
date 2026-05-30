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

    static final Duration DEFAULT_DUMP_INTERVAL = Duration.of('5min')

    /**
     * Factory with explicit configuration. Overrides are read from the active
     * {@link Session}'s config map ({@code executor.pollInterval},
     * {@code executor.queueSize}, {@code executor.dumpInterval}), with a
     * per-executor selector scope ({@code executor.$<name>.*}) taking
     * precedence over the global {@code executor} scope.
     *
     * <p>Config is read via {@link Session#getConfig()} rather than the
     * {@code Session.getPollInterval/getQueueSize/getMonitorDumpInterval}
     * helpers, which existed in Nextflow 24.10 but were removed in 25.x. Using
     * only the stable config map lets a single plugin build load on both.
     */
    static BacalhauTaskMonitor create(Session session, String name, Duration defPoll, int capacity) {
        assert session
        assert name

        final Map config = (session.getConfig() ?: [:]) as Map
        final Duration pollInterval = asDuration(scopedValue(config, name, 'pollInterval'), defPoll)
        final Duration dumpInterval = asDuration(scopedValue(config, name, 'dumpInterval'), DEFAULT_DUMP_INTERVAL)
        final int      queueSize    = asInt(scopedValue(config, name, 'queueSize'), capacity)

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

    /**
     * Resolve an executor config value from the config map. A per-executor
     * selector scope ({@code executor.$<execName>.<key>}) overrides the global
     * {@code executor.<key>}. Returns {@code null} when unset.
     */
    private static Object scopedValue(Map config, String execName, String key) {
        final Object execObj = config?.get('executor')
        if( !(execObj instanceof Map) )
            return null
        final Map exec = (Map) execObj
        final Object scoped = exec.get('$' + execName)
        if( scoped instanceof Map ) {
            final Object v = ((Map) scoped).get(key)
            if( v != null )
                return v
        }
        return exec.get(key)
    }

    /** Coerce a config value (Duration or duration-string) to a Duration. */
    private static Duration asDuration(Object value, Duration fallback) {
        if( value == null )
            return fallback
        if( value instanceof Duration )
            return (Duration) value
        try {
            return Duration.of(value.toString())
        }
        catch( Exception e ) {
            log.warn "Invalid Bacalhau executor duration '${value}'; using ${fallback}"
            return fallback
        }
    }

    /** Coerce a config value (Number or numeric-string) to an int. */
    private static int asInt(Object value, int fallback) {
        if( value == null )
            return fallback
        if( value instanceof Number )
            return ((Number) value).intValue()
        try {
            return Integer.parseInt(value.toString().trim())
        }
        catch( Exception e ) {
            log.warn "Invalid Bacalhau executor integer '${value}'; using ${fallback}"
            return fallback
        }
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
