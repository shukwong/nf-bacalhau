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
import nextflow.util.MemoryUnit
import spock.lang.Specification
import spock.lang.Subject
import spock.lang.TempDir

import java.nio.file.Path

/**
 * Unit tests for BacalhauExecutor.
 */
class BacalhauExecutorTest extends Specification {

    @TempDir
    Path tempDir

    @Subject
    BacalhauExecutor executor

    def setup() {
        executor = new BacalhauExecutor()
        executor.session = Mock(Session) {
            getRunName() >> 'test-run'
        }
    }

    // -------------------------------------------------------------------------
    // Service registration
    // -------------------------------------------------------------------------

    def 'should have correct service name'() {
        expect:
        BacalhauExecutor.getAnnotation(nextflow.util.ServiceName).value() == 'bacalhau'
    }

    // -------------------------------------------------------------------------
    // Command generation — FIX #1 + #4
    //
    // getSubmitCommandLine() now writes a YAML spec to task.workDir and
    // returns ['bacalhau', 'job', 'run', <specFilePath>].
    // We verify the command structure and the YAML content.
    // -------------------------------------------------------------------------

    def 'should generate correct submit command using bacalhau job run'() {
        given: 'a task with CPU constraints and two local input files'
        def task = Mock(TaskRun) {
            getName() >> 'test-task'
            getContainer() >> 'ubuntu:latest'
            getWorkDir() >> tempDir
            getConfig() >> Mock(TaskConfig) {
                getCpus()        >> 2
                getMemory()      >> null
                getTime()        >> null
                getDisk()        >> null
                getAccelerator() >> null
                getEnvironment() >> null
                getExt()         >> null
            }
            getInputFilesMap() >> [
                'file1.txt': tempDir.resolve('file1.txt'),
                'file2.csv': tempDir.resolve('file2.csv')
            ]
        }
        def scriptFile = tempDir.resolve('test-script.sh')

        when:
        def cmd = executor.getSubmitCommandLine(task, scriptFile)

        then: 'command uses the new bacalhau job run sub-command (not deprecated docker run)'
        cmd[0] == 'bacalhau'
        cmd[1] == 'job'
        cmd[2] == 'run'

        and: 'the last argument points to the YAML spec file written in the work dir'
        cmd.last().endsWith('.bacalhau-job.yaml')

        and: 'the YAML file was created in the task work directory'
        tempDir.resolve('.bacalhau-job.yaml').exists()
    }

    def 'should embed correct container image in the YAML spec'() {
        given:
        def task = Mock(TaskRun) {
            getName() >> 'test-task'
            getContainer() >> 'alpine:latest'
            getWorkDir() >> tempDir
            getConfig() >> Mock(TaskConfig) {
                getCpus()        >> 1
                getMemory()      >> null
                getTime()        >> null
                getDisk()        >> null
                getAccelerator() >> null
                getEnvironment() >> null
                getExt()         >> null
            }
            getInputFilesMap() >> [:]
        }
        def scriptFile = tempDir.resolve('run.sh')

        when:
        executor.getSubmitCommandLine(task, scriptFile)
        def yaml = tempDir.resolve('.bacalhau-job.yaml').text

        then:
        yaml.contains('Image: "alpine:latest"')
    }

    def 'should embed memory resource in the YAML spec in Bacalhau format'() {
        // FIX #7: memory must be "4gb" not "4 GB"
        given:
        def memory = Mock(MemoryUnit) {
            // 4 GB in bytes
            toBytes() >> 4_294_967_296L
        }
        def task = Mock(TaskRun) {
            getName() >> 'test-task'
            getContainer() >> 'alpine:latest'
            getWorkDir() >> tempDir
            getConfig() >> Mock(TaskConfig) {
                getCpus()        >> 1
                getMemory()      >> memory
                getTime()        >> null
                getDisk()        >> null
                getAccelerator() >> null
                getEnvironment() >> null
                getExt()         >> null
            }
            getInputFilesMap() >> [:]
        }
        def scriptFile = tempDir.resolve('run.sh')

        when:
        executor.getSubmitCommandLine(task, scriptFile)
        def yaml = tempDir.resolve('.bacalhau-job.yaml').text

        then:
        yaml.contains('Memory: "4gb"')
        !yaml.contains('4 GB')   // old broken format must not appear
    }

    def 'should embed input files as localDirectory InputSources in YAML'() {
        // FIX #1: destinations are /inputs/<name>, not the source path
        // FIX #9: a single consistent YAML format replaces mixed CLI flags
        given:
        def task = Mock(TaskRun) {
            getName() >> 'test-task'
            getContainer() >> 'ubuntu:latest'
            getWorkDir() >> tempDir
            getConfig() >> Mock(TaskConfig) {
                getCpus()        >> 1
                getMemory()      >> null
                getTime()        >> null
                getDisk()        >> null
                getAccelerator() >> null
                getEnvironment() >> null
                getExt()         >> null
            }
            getInputFilesMap() >> [
                'file1.txt': Path.of('/data/file1.txt'),
                'file2.csv': Path.of('/data/file2.csv')
            ]
        }
        def scriptFile = tempDir.resolve('script.sh')

        when:
        executor.getSubmitCommandLine(task, scriptFile)
        def yaml = tempDir.resolve('.bacalhau-job.yaml').text

        then: 'each file gets a localDirectory source entry with the correct /inputs/<name> target'
        yaml.contains('SourcePath: "/data/file1.txt"')
        yaml.contains('Target: "/inputs/file1.txt"')
        yaml.contains('SourcePath: "/data/file2.csv"')
        yaml.contains('Target: "/inputs/file2.csv"')

        and: 'no legacy colon-separated or src=,dst= syntax'
        !yaml.contains(':/inputs/')
        !yaml.contains('src=')
    }

    def 'should embed S3 inputs as s3 InputSources in YAML'() {
        given:
        def task = Mock(TaskRun) {
            getName() >> 'test-task'
            getContainer() >> 'ubuntu:latest'
            getWorkDir() >> tempDir
            getConfig() >> Mock(TaskConfig) {
                getCpus()        >> 1
                getMemory()      >> null
                getTime()        >> null
                getDisk()        >> null
                getAccelerator() >> null
                getEnvironment() >> null
                getExt()         >> null
            }
            getInputFilesMap() >> [
                'data.csv': Path.of('s3://my-bucket/path/to/data.csv')
            ]
        }
        def scriptFile = tempDir.resolve('script.sh')

        when:
        executor.getSubmitCommandLine(task, scriptFile)
        def yaml = tempDir.resolve('.bacalhau-job.yaml').text

        then:
        yaml.contains('Type: s3')
        yaml.contains('Bucket: "my-bucket"')
        yaml.contains('Key: "path/to/data.csv"')
        yaml.contains('Target: "/inputs/data.csv"')
    }

    def 'should throw exception for task without container'() {
        given:
        def task = Mock(TaskRun) {
            getName()     >> 'test-task'
            getContainer() >> null
            getWorkDir()  >> tempDir
        }
        def scriptFile = tempDir.resolve('script.sh')

        when:
        executor.getSubmitCommandLine(task, scriptFile)

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('requires a container image')
    }

    // -------------------------------------------------------------------------
    // Kill command
    // -------------------------------------------------------------------------

    def 'should return correct kill command'() {
        when:
        def cmd = executor.getKillCommand()

        then:
        cmd == ['bacalhau', 'job', 'stop']
    }

    // -------------------------------------------------------------------------
    // Queue status parsing — FIX #2
    //
    // parseQueueStatus() uses JsonSlurper; the test must supply JSON, not the
    // old tabular/space-separated text.
    // -------------------------------------------------------------------------

    def 'should parse job status from Bacalhau JSON output'() {
        given: 'valid JSON from bacalhau job list --output json'
        def json = '''[
            {
                "ID": "job-12345678-abcd-1234-5678-123456789012",
                "State": { "StateType": "completed" }
            },
            {
                "ID": "job-87654321-dcba-4321-8765-210987654321",
                "State": { "StateType": "running" }
            }
        ]'''

        when:
        def result = executor.parseQueueStatus(json)

        then:
        result.size() == 2
        result['job-12345678-abcd-1234-5678-123456789012'] == QueueStatus.DONE
        result['job-87654321-dcba-4321-8765-210987654321'] == QueueStatus.RUNNING
    }

    def 'should handle empty or null queue status input'() {
        when:
        def result = executor.parseQueueStatus('')
        then:
        result.isEmpty()

        when:
        result = executor.parseQueueStatus(null)
        then:
        result.isEmpty()
    }

    def 'should return empty map for invalid JSON input'() {
        when:
        def result = executor.parseQueueStatus('not-json-at-all')
        then:
        result.isEmpty()
    }

    // -------------------------------------------------------------------------
    // Task handler factory
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // Job status mapping — FIX #3
    //
    // parseJobStatus() is now protected, so this test can call it directly
    // without a compile-time access violation under @CompileStatic.
    // -------------------------------------------------------------------------

    def 'should map all Bacalhau job states to Nextflow QueueStatus'() {
        expect:
        executor.parseJobStatus('queued')    == QueueStatus.PENDING
        executor.parseJobStatus('pending')   == QueueStatus.PENDING
        executor.parseJobStatus('running')   == QueueStatus.RUNNING
        executor.parseJobStatus('executing') == QueueStatus.RUNNING
        executor.parseJobStatus('completed') == QueueStatus.DONE
        executor.parseJobStatus('finished')  == QueueStatus.DONE
        executor.parseJobStatus('failed')    == QueueStatus.ERROR
        executor.parseJobStatus('error')     == QueueStatus.ERROR
        executor.parseJobStatus('cancelled') == QueueStatus.ERROR
        executor.parseJobStatus('stopped')   == QueueStatus.ERROR
        executor.parseJobStatus('unknown-x') == QueueStatus.UNKNOWN
        executor.parseJobStatus(null)        == QueueStatus.UNKNOWN
    }

    // -------------------------------------------------------------------------
    // GPU and environment variables
    // -------------------------------------------------------------------------

    def 'should embed GPU and environment variables in YAML spec'() {
        given:
        def accelerator = Mock(nextflow.util.AcceleratorResource) {
            getRequest() >> 1
        }
        def task = Mock(TaskRun) {
            getName() >> 'gpu-task'
            getContainer() >> 'nvidia/cuda:latest'
            getWorkDir() >> tempDir
            getConfig() >> Mock(TaskConfig) {
                getCpus()        >> 4
                getMemory()      >> null
                getTime()        >> null
                getDisk()        >> null
                getAccelerator() >> accelerator
                getEnvironment() >> ['MY_ENV': 'my_val']
                getExt()         >> null
            }
            getInputFilesMap() >> [:]
        }
        def scriptFile = tempDir.resolve('script.sh')

        when:
        executor.getSubmitCommandLine(task, scriptFile)
        def yaml = tempDir.resolve('.bacalhau-job.yaml').text

        then:
        yaml.contains('GPU: "1"')
        yaml.contains('MY_ENV: "my_val"')
    }

    // -------------------------------------------------------------------------
    // Secrets
    // -------------------------------------------------------------------------

    def 'should embed secrets as env placeholders in YAML spec'() {
        given:
        def task = Mock(TaskRun) {
            getName() >> 'secret-task'
            getContainer() >> 'ubuntu:latest'
            getWorkDir() >> tempDir
            getConfig() >> Mock(TaskConfig) {
                getCpus()        >> 1
                getMemory()      >> null
                getTime()        >> null
                getDisk()        >> null
                getAccelerator() >> null
                getEnvironment() >> null
                getExt()         >> ['bacalhauSecrets': ['API_KEY', 'DB_PASS']]
            }
            getInputFilesMap() >> [:]
        }
        def scriptFile = tempDir.resolve('script.sh')

        when:
        executor.getSubmitCommandLine(task, scriptFile)
        def yaml = tempDir.resolve('.bacalhau-job.yaml').text

        then:
        yaml.contains('API_KEY')
        yaml.contains('DB_PASS')
    }

    // -------------------------------------------------------------------------
    // Memory formatter (FIX #7)
    // -------------------------------------------------------------------------

    def 'formatMemory should produce Bacalhau-compatible compact strings'() {
        expect:
        executor.formatMemory(new MemoryUnit(4_294_967_296L)) == '4gb'    // 4 GiB
        executor.formatMemory(new MemoryUnit(536_870_912L))   == '512mb'  // 512 MiB
        executor.formatMemory(new MemoryUnit(1_073_741_824L)) == '1gb'    // 1 GiB
    }
}
