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

    def 'should create monitor with default settings'() {
        given:
        def session = Mock(Session) {
            getPollInterval('bacalhau', _) >> Duration.of('30sec')
            getMonitorDumpInterval('bacalhau') >> Duration.of('5min')
            getQueueSize('bacalhau', 100) >> 100
        }

        when:
        def monitor = BacalhauTaskMonitor.create(session)

        then:
        monitor != null
    }

    def 'should create monitor with custom settings'() {
        given:
        def customPoll = Duration.of('60sec')
        def session = Mock(Session) {
            getPollInterval('bacalhau', customPoll) >> customPoll
            getMonitorDumpInterval('bacalhau') >> Duration.of('5min')
            getQueueSize('bacalhau', 50) >> 50
        }

        when:
        def monitor = BacalhauTaskMonitor.create(session, 'bacalhau', customPoll, 50)

        then:
        monitor != null
    }
}
