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
import nextflow.processor.TaskConfig
import nextflow.processor.TaskRun
import spock.lang.Specification
import spock.lang.Subject

import java.nio.file.Paths

/**
 * Unit tests for BacalhauExecutor
 */
class BacalhauExecutorTest extends Specification {

    @Subject
    BacalhauExecutor executor

    def setup() {
        executor = new BacalhauExecutor()
        executor.session = Mock(Session) {
            getRunName() >> 'test-run'
        }
    }

    def 'should have correct service name'() {
        expect:
        BacalhauExecutor.getAnnotation(nextflow.util.ServiceName).value() == 'bacalhau'
    }

    def 'should generate correct submit command'() {
        given:
        def task = Mock(TaskRun) {
            getName() >> 'test-task'
            getContainer() >> 'ubuntu:latest'
            getConfig() >> Mock(TaskConfig) {
                getCpus() >> 2
                getMemory() >> null
                getTime() >> null
            }
            getInputFilesMap() >> [
                'file1.txt': Paths.get('/data/file1.txt'),
                'file2.csv': Paths.get('/data/file2.csv')
            ]
        }
        def scriptFile = Paths.get('/work/test-script.sh')

        when:
        def cmd = executor.getSubmitCommandLine(task, scriptFile)

        then:
        cmd == [
            'bacalhau',
            'docker', 'run',
            '--cpu', '2',
            '-i', '/work/test-script.sh:/tmp/test-script.sh',
            '-i', '/data/file1.txt:/data/file1.txt',
            '-i', '/data/file2.csv:/data/file2.csv',
            'ubuntu:latest',
            '--',
            'bash',
            '/tmp/test-script.sh'
        ]
    }

    def 'should generate submit command with memory constraint'() {
        given:
        def task = Mock(TaskRun) {
            getName() >> 'test-task'
            getContainer() >> 'alpine:latest'
            getConfig() >> Mock(TaskConfig) {
                getCpus() >> 1
                getMemory() >> { 
                    def memory = Mock()
                    memory.toString() >> '4GB'
                    return memory
                }()
                getTime() >> null
            }
        }
        def scriptFile = Paths.get('/work/script.sh')

        when:
        def cmd = executor.getSubmitCommandLine(task, scriptFile)

        then:
        cmd.contains('--memory')
        cmd.contains('4GB')
        cmd.contains('alpine:latest')
    }

    def 'should throw exception for task without container'() {
        given:
        def task = Mock(TaskRun) {
            getName() >> 'test-task'
            getContainer() >> null
        }
        def scriptFile = Paths.get('/work/script.sh')

        when:
        executor.getSubmitCommandLine(task, scriptFile)

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('requires a container image')
    }

    def 'should return correct kill command'() {
        when:
        def cmd = executor.getKillCommand()

        then:
        cmd == ['bacalhau', 'job', 'stop']
    }

    def 'should parse job status correctly'() {
        given:
        def output = '''
CREATED              MODIFIED             ID                                      STATE
2024-01-15T10:30:45Z 2024-01-15T10:31:20Z job-12345678-abcd-1234-5678-123456789012 completed
2024-01-15T10:25:10Z 2024-01-15T10:30:45Z job-87654321-dcba-4321-8765-210987654321 running
'''

        when:
        def result = executor.parseQueueStatus(output)

        then:
        result.size() == 2
        result['job-12345678-abcd-1234-5678-123456789012'] == QueueStatus.DONE
        result['job-87654321-dcba-4321-8765-210987654321'] == QueueStatus.RUNNING
    }

    def 'should handle empty queue status'() {
        when:
        def result = executor.parseQueueStatus('')

        then:
        result.isEmpty()

        when:
        result = executor.parseQueueStatus(null)

        then:
        result.isEmpty()
    }

    def 'should create task handler'() {
        given:
        def task = Mock(TaskRun)

        when:
        def handler = executor.createTaskHandler(task)

        then:
        handler instanceof BacalhauTaskHandler
        handler.task == task
        handler.executor == executor
    }

    def 'should map job statuses correctly'() {
        given:
        def executor = new BacalhauExecutor()

        expect:
        executor.parseJobStatus('queued') == QueueStatus.PENDING
        executor.parseJobStatus('pending') == QueueStatus.PENDING
        executor.parseJobStatus('running') == QueueStatus.RUNNING
        executor.parseJobStatus('executing') == QueueStatus.RUNNING
        executor.parseJobStatus('completed') == QueueStatus.DONE
        executor.parseJobStatus('finished') == QueueStatus.DONE
        executor.parseJobStatus('failed') == QueueStatus.ERROR
        executor.parseJobStatus('error') == QueueStatus.ERROR
        executor.parseJobStatus('cancelled') == QueueStatus.ERROR
        executor.parseJobStatus('unknown-status') == QueueStatus.UNKNOWN
    }

    def 'should generate submit command with GPU and Env vars'() {
        given:
        def task = Mock(TaskRun) {
            getName() >> 'test-task'
            getContainer() >> 'nvidia/cuda:latest'
            getConfig() >> Mock(TaskConfig) {
                getCpus() >> 4
                getMemory() >> null
                getTime() >> null
                getAccelerator() >> {
                     def acc = Mock(nextflow.util.AcceleratorResource)
                     acc.request >> 1
                     return acc
                }()
                getEnvironment() >> ['MY_ENV': 'my_val']
            }
        }
        def scriptFile = Paths.get('/work/script.sh')

        when:
        def cmd = executor.getSubmitCommandLine(task, scriptFile)

        then:
        cmd.contains('--gpu')
        cmd.contains('1')
        cmd.contains('-e')
        cmd.contains('MY_ENV=my_val')
    }

    def 'should generate submit command with secrets'() {
        given:
        def task = Mock(TaskRun) {
            getName() >> 'test-task'
            getContainer() >> 'ubuntu:latest'
            getConfig() >> Mock(TaskConfig) {
                getCpus() >> 1
                getMemory() >> null
                getTime() >> null
                getExt() >> ['bacalhauSecrets': ['API_KEY', 'DB_PASS']]
            }
        }
        def scriptFile = Paths.get('/work/script.sh')

        when:
        def cmd = executor.getSubmitCommandLine(task, scriptFile)

        then:
        cmd.contains('--secret')
        cmd.contains('env=API_KEY')
        cmd.contains('env=DB_PASS')
    }
}