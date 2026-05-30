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

import nextflow.Session
import nextflow.util.Duration
import spock.lang.Specification

/**
 * Unit tests for BacalhauTaskMonitor.
 */
class BacalhauTaskMonitorTest extends Specification {

    def 'should have correct default poll interval'() {
        expect:
        BacalhauTaskMonitor.DEFAULT_POLL_INTERVAL == Duration.of('30sec')
    }

    def 'should have correct default queue size'() {
        expect:
        BacalhauTaskMonitor.DEFAULT_QUEUE_SIZE == 100
    }

    def 'should read config via getConfig() and not the removed Session helpers'() {
        // Nextflow 25.x removed Session.getPollInterval(String,Duration),
        // getQueueSize(String,int) and getMonitorDumpInterval(String). The
        // monitor must rely only on the stable getConfig() Map so the plugin
        // loads on both 24.10 and 25.x. (Reproduces issue: NoSuchMethodError
        // 'Session.getPollInterval' on Nextflow 25.x.)
        given:
        def session = Mock(Session)

        when:
        def monitor = BacalhauTaskMonitor.create(session)

        then:
        1 * session.getConfig() >> [:]
        0 * session.getPollInterval(_, _)
        0 * session.getQueueSize(_, _)
        0 * session.getMonitorDumpInterval(_)
        and:
        monitor != null
        monitor.getCapacity() == BacalhauTaskMonitor.DEFAULT_QUEUE_SIZE
        monitor.getPollIntervalMillis() == BacalhauTaskMonitor.DEFAULT_POLL_INTERVAL.toMillis()
    }

    def 'should apply executor-scope overrides from config'() {
        given:
        def session = Mock(Session) {
            getConfig() >> [executor: [pollInterval: '45 sec', queueSize: 25, dumpInterval: '2 min']]
        }

        when:
        def monitor = BacalhauTaskMonitor.create(
            session, 'bacalhau',
            BacalhauTaskMonitor.DEFAULT_POLL_INTERVAL,
            BacalhauTaskMonitor.DEFAULT_QUEUE_SIZE)

        then:
        monitor.getCapacity() == 25
        monitor.getPollIntervalMillis() == Duration.of('45sec').toMillis()
    }

    def 'should let the per-executor selector scope win over the global executor scope'() {
        given:
        def session = Mock(Session) {
            getConfig() >> [executor: ['$bacalhau': [pollInterval: '90 sec'], pollInterval: '10 sec']]
        }

        when:
        def monitor = BacalhauTaskMonitor.create(session)

        then:
        monitor.getPollIntervalMillis() == Duration.of('90sec').toMillis()
    }

    def 'should fall back to defaults when config is empty'() {
        given:
        def session = Mock(Session) {
            getConfig() >> null
        }

        when:
        def monitor = BacalhauTaskMonitor.create(session, 'bacalhau', Duration.of('30sec'), 100)

        then:
        monitor != null
        monitor.getCapacity() == 100
        monitor.getPollIntervalMillis() == Duration.of('30sec').toMillis()
    }
}
