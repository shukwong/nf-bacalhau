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

import nextflow.processor.TaskRun
import spock.lang.Specification
import spock.lang.Subject

import java.nio.file.Files
import java.nio.file.Path

/**
 * Unit tests for BacalhauTaskHandler
 */
class BacalhauTaskHandlerTest extends Specification {

    @Subject
    BacalhauTaskHandler handler

    BacalhauExecutor executor
    TaskRun task
    Path workDir

    def setup() {
        workDir = Files.createTempDirectory('bacalhau-test')
        
        executor = Mock(BacalhauExecutor)
        task = Mock(TaskRun) {
            getName() >> 'test-task'
            getWorkDir() >> workDir
        }
        
        handler = new BacalhauTaskHandler(task, executor)
    }

    def cleanup() {
        workDir?.deleteDir()
    }

    def 'should extract job id from simple output'() {
        when:
        def jobId = handler.extractJobId('job-12345678-abcd-1234-5678-123456789012')

        then:
        jobId == 'job-12345678-abcd-1234-5678-123456789012'
    }

    def 'should extract job id from multiline output'() {
        given:
        def output = '''
Job submitted successfully
job-87654321-dcba-4321-8765-210987654321
'''

        when:
        def jobId = handler.extractJobId(output)

        then:
        jobId == 'job-87654321-dcba-4321-8765-210987654321'
    }

    def 'should handle empty output'() {
        expect:
        handler.extractJobId('') == null
        handler.extractJobId(null) == null
        handler.extractJobId('   ') == null
    }

    def 'should return job id when available'() {
        given:
        handler.@bacalhauJobId = 'test-job-123'

        when:
        def jobId = handler.getJobId()

        then:
        jobId == 'test-job-123'
    }

    def 'should return null job id when not available'() {
        when:
        def jobId = handler.getJobId()

        then:
        jobId == null
    }

    def 'should check running status correctly'() {
        given:
        handler.@bacalhauJobId = 'test-job-123'
        handler.@jobSubmitted = true
        executor.getQueueStatus() >> ['test-job-123': QueueStatus.RUNNING]

        when:
        def isRunning = handler.checkIfRunning()

        then:
        isRunning == true
        handler.status == TaskStatus.RUNNING
    }

    def 'should check completed status correctly'() {
        given:
        handler.@bacalhauJobId = 'test-job-123'
        handler.@jobSubmitted = true
        executor.getQueueStatus() >> ['test-job-123': QueueStatus.DONE]

        when:
        def isCompleted = handler.checkIfCompleted()

        then:
        isCompleted == true
        handler.status == TaskStatus.COMPLETED
    }

    def 'should check error status correctly'() {
        given:
        handler.@bacalhauJobId = 'test-job-123'
        handler.@jobSubmitted = true
        executor.getQueueStatus() >> ['test-job-123': QueueStatus.ERROR]

        when:
        def isCompleted = handler.checkIfCompleted()

        then:
        isCompleted == true
        handler.status == TaskStatus.ERROR
        task.error instanceof RuntimeException
    }

    def 'should return false for status checks when job not submitted'() {
        expect:
        handler.checkIfRunning() == false
        handler.checkIfCompleted() == false
    }

    def 'should read exit status from file'() {
        given:
        def exitFile = workDir.resolve(TaskRun.CMD_EXIT)
        exitFile.text = '0'

        when:
        def exitStatus = handler.readExitFile()

        then:
        exitStatus == 0
    }

    def 'should handle missing exit file'() {
        when:
        def exitStatus = handler.readExitFile()

        then:
        exitStatus == null
    }
}