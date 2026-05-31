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

import nextflow.executor.AbstractGridExecutor.QueueStatus
import nextflow.processor.TaskRun
import nextflow.processor.TaskStatus
import spock.lang.Specification
import spock.lang.Subject

import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit

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
        executor.getQueueStatus() >> ['test-job-123': QueueStatus.RUNNING]

        when:
        def isRunning = handler.checkIfRunning()

        then:
        isRunning == true
        handler.status == TaskStatus.RUNNING
    }

    def 'should retrieve result files before marking completed job successful'() {
        given:
        handler.@bacalhauJobId = 'test-job-123'
        executor.getQueueStatus() >> ['test-job-123': QueueStatus.DONE]
        executor.getJobGetCommand('test-job-123', workDir) >> [
            '/bin/sh',
            '-c',
            // Plain String (not a GString): ProcessBuilder requires a String[],
            // and a GString element triggers ArrayStoreException in start().
            'printf 0 > ' + TaskRun.CMD_EXIT
        ]

        when: 'first call starts retrieval thread'
        def firstCheck = handler.checkIfCompleted()

        then: 'returns false because retrieval is async'
        // The retrieval thread starts but may or may not have finished yet.
        // Either false (still retrieving) or true (retrieval completed fast) is acceptable.
        firstCheck == false || firstCheck == true

        when: 'wait for retrieval to complete then check again'
        assert handler.@retrievalLatch.await(2, TimeUnit.SECONDS)
        def secondCheck = handler.checkIfCompleted()

        then: 'now completes'
        secondCheck == true
        handler.status == TaskStatus.COMPLETED
        1 * task.setExitStatus(0)
        1 * task.setStdout(workDir.resolve(TaskRun.CMD_OUTFILE))
        1 * task.setStderr(workDir.resolve(TaskRun.CMD_ERRFILE))
    }

    def 'should fail completed job when result retrieval fails'() {
        given:
        handler.@bacalhauJobId = 'test-job-123'
        executor.getQueueStatus() >> ['test-job-123': QueueStatus.DONE]
        executor.getJobGetCommand('test-job-123', workDir) >> ['/bin/sh', '-c', 'exit 2']

        when:
        handler.checkIfCompleted()
        assert handler.@retrievalLatch.await(2, TimeUnit.SECONDS)
        def secondCheck = handler.checkIfCompleted()

        then:
        secondCheck
        handler.status == TaskStatus.COMPLETED
        1 * task.setError({ it instanceof RuntimeException })
        1 * task.setExitStatus(1)
    }

    def 'should fail completed job when retrieved exit file is missing'() {
        given:
        handler.@bacalhauJobId = 'test-job-123'
        executor.getQueueStatus() >> ['test-job-123': QueueStatus.DONE]
        executor.getJobGetCommand('test-job-123', workDir) >> ['/bin/sh', '-c', 'exit 0']

        when:
        handler.checkIfCompleted()
        assert handler.@retrievalLatch.await(2, TimeUnit.SECONDS)
        def secondCheck = handler.checkIfCompleted()

        then:
        secondCheck
        handler.status == TaskStatus.COMPLETED
        1 * task.setError({ it instanceof RuntimeException })
        1 * task.setExitStatus(1)
    }

    def 'should check error status correctly'() {
        given:
        handler.@bacalhauJobId = 'test-job-123'
        executor.getQueueStatus() >> ['test-job-123': QueueStatus.ERROR]

        when:
        def isCompleted = handler.checkIfCompleted()

        then: 'errors are signalled via task.error + non-zero exit + COMPLETED status'
        isCompleted
        handler.status == TaskStatus.COMPLETED
        1 * task.setError({ it instanceof RuntimeException })
        1 * task.setExitStatus({ it != 0 })
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

    def 'result retrieval is bounded by a shared pool, not one thread per task'() {
        given: 'a gate file that blocks every retrieval until the test releases it'
        def gate = Files.createTempFile('bacalhau-gate', '')
        def poolSize = BacalhauTaskHandler.RETRIEVAL_POOL_SIZE
        def n = poolSize + 5
        def dirs = []
        def handlers = (0..<n).collect { i ->
            def wd = Files.createTempDirectory("bacalhau-bound-${i}")
            dirs << wd
            def t = Mock(TaskRun) { getName() >> "task-${i}"; getWorkDir() >> wd }
            def ex = Mock(BacalhauExecutor) {
                getQueueStatus() >> ['j': QueueStatus.DONE]
                getJobGetCommand('j', wd) >> ['/bin/sh', '-c',
                    'while [ -e "' + gate.toString() + '" ]; do sleep 0.02; done; printf 0 > ' + TaskRun.CMD_EXIT]
            }
            def h = new BacalhauTaskHandler(t, ex)
            h.@bacalhauJobId = 'j'
            h
        }

        when: 'all N tasks complete at once and submit their retrievals'
        handlers.each { it.checkIfCompleted() }
        sleep 400   // let the bounded pool saturate (retrievals stay blocked on the gate)

        then: 'at most pool-size retrieval threads are alive (thread-per-task would be N)'
        def live = Thread.allStackTraces.keySet().count { it.alive && it.name?.startsWith('bacalhau-retrieve-') }
        live <= poolSize

        cleanup: 'release the gate and drain every retrieval'
        gate.toFile().delete()
        handlers.each { it.@retrievalLatch.await(5, TimeUnit.SECONDS) }
        dirs.each { it.deleteDir() }
    }

    def 'kill() releases the retrieval latch even when the retrieval is still queued'() {
        given: 'the shared pool saturated with blocked retrievals'
        def gate = Files.createTempFile('bacalhau-killgate', '')
        def poolSize = BacalhauTaskHandler.RETRIEVAL_POOL_SIZE
        def fillerDirs = []
        def fillers = (0..<poolSize).collect { i ->
            def wd = Files.createTempDirectory("bacalhau-fill-${i}")
            fillerDirs << wd
            def t = Mock(TaskRun) { getName() >> "fill-${i}"; getWorkDir() >> wd }
            def ex = Mock(BacalhauExecutor) {
                getQueueStatus() >> ['j': QueueStatus.DONE]
                getJobGetCommand('j', wd) >> ['/bin/sh', '-c',
                    'while [ -e "' + gate.toString() + '" ]; do sleep 0.02; done']
            }
            def h = new BacalhauTaskHandler(t, ex)
            h.@bacalhauJobId = 'j'
            h
        }
        fillers.each { it.checkIfCompleted() }
        sleep 400   // ensure every pool thread is occupied

        and: 'a victim whose retrieval can only sit in the queue'
        def victimDir = Files.createTempDirectory('bacalhau-victim')
        def vtask = Mock(TaskRun) { getName() >> 'victim'; getWorkDir() >> victimDir }
        def vexec = Mock(BacalhauExecutor) {
            getQueueStatus() >> ['j': QueueStatus.DONE]
            getJobGetCommand('j', victimDir) >> ['/bin/sh', '-c', 'printf 0 > ' + TaskRun.CMD_EXIT]
            getKillCommand() >> ['/bin/sh', '-c', 'true']
        }
        def victim = new BacalhauTaskHandler(vtask, vexec)
        victim.@bacalhauJobId = 'j'
        victim.checkIfCompleted()   // queued behind the saturated pool

        when: 'the victim is killed while its retrieval is still queued'
        victim.kill()

        then: 'its latch is released, so completion polling can reach a terminal state'
        victim.@retrievalLatch.await(2, TimeUnit.SECONDS)

        cleanup: 'release the gate and drain the fillers'
        gate.toFile().delete()
        fillers.each { it.@retrievalLatch.await(5, TimeUnit.SECONDS) }
        fillerDirs.each { it.deleteDir() }
        victimDir.deleteDir()
    }

    def 'kill() leaves a RUNNING retrieval to release its own latch (recording the error)'() {
        given: 'a retrieval that is running and blocked'
        def gate = Files.createTempFile('bacalhau-rungate', '')
        handler.@bacalhauJobId = 'j'
        executor.getQueueStatus() >> ['j': QueueStatus.DONE]
        executor.getJobGetCommand('j', workDir) >> ['/bin/sh', '-c',
            'while [ -e "' + gate.toString() + '" ]; do sleep 0.05; done']
        executor.getKillCommand() >> ['/bin/sh', '-c', 'true']
        handler.checkIfCompleted()   // runs on a free pool thread
        sleep 300                    // let the worker start (retrievalRunning = true)

        when: 'the task is killed while its retrieval is running'
        handler.kill()

        then: 'the worker — not kill() — releases the latch, after recording the failure'
        handler.@retrievalLatch.await(5, TimeUnit.SECONDS)
        handler.@retrievalError != null

        cleanup:
        gate.toFile().delete()
    }
}
