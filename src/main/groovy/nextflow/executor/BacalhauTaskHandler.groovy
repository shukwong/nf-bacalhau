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
import nextflow.processor.TaskRun
import nextflow.processor.TaskStatus
import nextflow.executor.GridTaskHandler
import nextflow.trace.TraceRecord

import java.nio.file.Path
import java.util.concurrent.TimeUnit

/**
 * Task handler for managing individual Bacalhau job lifecycle
 *
 * @author Nextflow Contributors
 */
@Slf4j
@CompileStatic
class BacalhauTaskHandler extends GridTaskHandler {

    /**
     * Bacalhau job ID for this task
     */
    private String bacalhauJobId

    /**
     * Flag to track if job has been submitted
     */
    private volatile boolean jobSubmitted = false

    /**
     * Constructor
     */
    BacalhauTaskHandler(TaskRun task, BacalhauExecutor executor) {
        super(task, executor)
    }

    /**
     * Get the Bacalhau executor instance
     */
    protected BacalhauExecutor getBacalhauExecutor() {
        return (BacalhauExecutor) executor
    }

    /**
     * Submit the task to Bacalhau
     */
    @Override
    void submit() {
        log.debug "Submitting task ${task.name} to Bacalhau"

        Process proc = null
        try {
            // Create the task script
            final scriptFile = task.workDir.resolve(TaskRun.CMD_SCRIPT)

            // Get the submit command from executor
            final cmd = getBacalhauExecutor().getSubmitCommandLine(task, scriptFile)

            // Execute the submit command with timeout
            log.info "Submitting task ${task.name} with command: ${cmd.join(' ')}"
            proc = new ProcessBuilder(cmd)
                .directory(task.workDir.toFile())
                .redirectErrorStream(false)  // Keep stdout and stderr separate
                .start()

            // Read stdout and stderr separately with timeout
            final StringBuilder stdout = new StringBuilder()
            final StringBuilder stderr = new StringBuilder()

            // Start threads to consume streams to prevent blocking
            final Thread stdoutThread = Thread.start {
                proc.inputStream.eachLine { line -> stdout.append(line).append('\n') }
            }
            final Thread stderrThread = Thread.start {
                proc.errorStream.eachLine { line -> stderr.append(line).append('\n') }
            }

            // Wait for process with timeout
            final timeout = BacalhauExecutor.DEFAULT_SUBMIT_TIMEOUT
            boolean finished = proc.waitFor(timeout, TimeUnit.SECONDS)

            // Wait for stream readers to complete
            stdoutThread.join(5000)
            stderrThread.join(5000)

            if (!finished) {
                proc.destroyForcibly()
                throw new IllegalStateException("Bacalhau job submission timed out after ${timeout} seconds for task ${task.name}")
            }

            final exitCode = proc.exitValue()
            final output = stdout.toString().trim()
            final errorOutput = stderr.toString().trim()

            if (exitCode != 0) {
                log.error "Task ${task.name} submission failed. Exit code: ${exitCode}"
                log.error "Stdout: ${output}"
                log.error "Stderr: ${errorOutput}"
                throw new IllegalStateException("Bacalhau job submission failed with exit code ${exitCode}: ${errorOutput ?: output}")
            }

            // Extract job ID from output
            bacalhauJobId = extractJobId(output)
            if (!bacalhauJobId) {
                log.error "Failed to extract job ID from output: ${output}"
                throw new IllegalStateException("Failed to extract job ID from Bacalhau output. Output: ${output}")
            }

            log.info "Task ${task.name} submitted successfully to Bacalhau with job ID: ${bacalhauJobId}"

            // Set status atomically
            synchronized (this) {
                jobSubmitted = true
                status = TaskStatus.SUBMITTED
            }

        } catch (InterruptedException e) {
            log.error "Task ${task.name} submission interrupted", e
            status = TaskStatus.ERROR
            Thread.currentThread().interrupt()
            throw new IllegalStateException("Job submission interrupted for task ${task.name}", e)
        } catch (Exception e) {
            log.error "Failed to submit task ${task.name} to Bacalhau", e
            status = TaskStatus.ERROR
            throw e
        } finally {
            // Ensure process is cleaned up
            if (proc?.isAlive()) {
                proc.destroyForcibly()
            }
        }
    }

    /**
     * Kill the running job
     */
    @Override
    void kill() {
        if (!bacalhauJobId) {
            log.warn "Cannot kill task ${task.name}: no job ID available"
            return
        }
        
        log.debug "Killing Bacalhau job: ${bacalhauJobId}"
        
        try {
            final cmd = getBacalhauExecutor().getKillCommand() + [bacalhauJobId]
            final proc = new ProcessBuilder(cmd).start()
            final exitCode = proc.waitFor()
            
            if (exitCode == 0) {
                log.info "Successfully killed Bacalhau job: ${bacalhauJobId}"
            } else {
                log.warn "Failed to kill Bacalhau job ${bacalhauJobId}, exit code: ${exitCode}"
            }
            
        } catch (Exception e) {
            log.warn "Error killing Bacalhau job ${bacalhauJobId}: ${e.message}"
        }
    }

    /**
     * Check if the job is running
     */
    @Override
    boolean checkIfRunning() {
        synchronized (this) {
            if (!jobSubmitted || !bacalhauJobId) {
                return false
            }
        }

        try {
            final queueStatus = getBacalhauExecutor().getQueueStatus()
            final jobStatus = queueStatus.get(bacalhauJobId)

            if (jobStatus == QueueStatus.RUNNING) {
                synchronized (this) {
                    status = TaskStatus.RUNNING
                }
                log.debug "Task ${task.name} is running (job ID: ${bacalhauJobId})"
                return true
            }
        } catch (Exception e) {
            log.warn "Error checking running status for task ${task.name}: ${e.message}"
        }

        return false
    }

    /**
     * Check if the job is completed
     */
    @Override
    boolean checkIfCompleted() {
        synchronized (this) {
            if (!jobSubmitted || !bacalhauJobId) {
                return false
            }
        }

        try {
            final queueStatus = getBacalhauExecutor().getQueueStatus()
            final jobStatus = queueStatus.get(bacalhauJobId)

            if (jobStatus == QueueStatus.DONE) {
                log.info "Task ${task.name} completed, retrieving results (job ID: ${bacalhauJobId})"

                // Retrieve outputs before marking as completed
                retrieveJobResults()

                // Verify output files exist
                verifyOutputFiles()

                synchronized (this) {
                    status = TaskStatus.COMPLETED
                    task.exitStatus = readExitFile()
                    task.stdout = task.workDir.resolve(TaskRun.CMD_OUTFILE)
                    task.stderr = task.workDir.resolve(TaskRun.CMD_ERRFILE)
                }
                return true
            } else if (jobStatus == QueueStatus.ERROR) {
                log.error "Task ${task.name} failed (job ID: ${bacalhauJobId})"

                synchronized (this) {
                    status = TaskStatus.ERROR
                    task.exitStatus = readExitFile() ?: 1
                    task.error = new RuntimeException("Bacalhau job ${bacalhauJobId} failed")
                }
                return true
            }
        } catch (Exception e) {
            log.error "Error checking completion status for task ${task.name}: ${e.message}", e
            synchronized (this) {
                status = TaskStatus.ERROR
                task.error = new RuntimeException("Failed to check job status: ${e.message}", e)
            }
            return true
        }

        return false
    }

    /**
     * Retrieve job results from Bacalhau
     */
    private void retrieveJobResults() {
        log.debug "Retrieving results for job ${bacalhauJobId}"

        Process proc = null
        try {
            // Use 'bacalhau job get' to download results to the task work directory
            final cmd = [
                getBacalhauExecutor().getBacalhauCli(),
                'job',
                'get',
                bacalhauJobId,
                '--output-dir', task.workDir.toString()
            ]

            log.info "Retrieving results for task ${task.name}: ${cmd.join(' ')}"
            proc = new ProcessBuilder(cmd)
                .directory(task.workDir.toFile())
                .redirectErrorStream(false)
                .start()

            // Read streams with timeout
            final StringBuilder stdout = new StringBuilder()
            final StringBuilder stderr = new StringBuilder()

            final Thread stdoutThread = Thread.start {
                proc.inputStream.eachLine { line -> stdout.append(line).append('\n') }
            }
            final Thread stderrThread = Thread.start {
                proc.errorStream.eachLine { line -> stderr.append(line).append('\n') }
            }

            // Wait for process with timeout (5 minutes for large outputs)
            boolean finished = proc.waitFor(300, TimeUnit.SECONDS)

            stdoutThread.join(5000)
            stderrThread.join(5000)

            if (!finished) {
                proc.destroyForcibly()
                log.warn "Result retrieval timed out for job ${bacalhauJobId}"
                return
            }

            final exitCode = proc.exitValue()
            final output = stdout.toString()
            final errorOutput = stderr.toString()

            if (exitCode != 0) {
                log.warn "Failed to retrieve results for job ${bacalhauJobId}. Exit code: ${exitCode}"
                log.debug "Stdout: ${output}"
                log.debug "Stderr: ${errorOutput}"
            } else {
                log.info "Successfully retrieved results for task ${task.name} (job ID: ${bacalhauJobId})"
            }
        } catch (InterruptedException e) {
            log.warn "Result retrieval interrupted for job ${bacalhauJobId}: ${e.message}"
            Thread.currentThread().interrupt()
        } catch (Exception e) {
            log.warn "Error retrieving results for job ${bacalhauJobId}: ${e.message}", e
        } finally {
            if (proc?.isAlive()) {
                proc.destroyForcibly()
            }
        }
    }

    /**
     * Verify that expected output files exist
     */
    private void verifyOutputFiles() {
        final exitFile = task.workDir.resolve(TaskRun.CMD_EXIT)
        final outFile = task.workDir.resolve(TaskRun.CMD_OUTFILE)
        final errFile = task.workDir.resolve(TaskRun.CMD_ERRFILE)

        if (!exitFile.exists()) {
            log.warn "Task ${task.name}: Exit file not found at ${exitFile}"
        }
        if (!outFile.exists()) {
            log.debug "Task ${task.name}: Stdout file not found at ${outFile}"
        }
        if (!errFile.exists()) {
            log.debug "Task ${task.name}: Stderr file not found at ${errFile}"
        }
    }

    /**
     * Get the job ID for status tracking
     */
    String getJobId() {
        return bacalhauJobId
    }

    /**
     * Get trace record for monitoring
     */
    @Override
    TraceRecord getTraceRecord() {
        final trace = super.getTraceRecord()
        if (bacalhauJobId) {
            trace.put('native_id', bacalhauJobId)
        }
        return trace
    }

    /**
     * Extract job ID from Bacalhau command output
     * Bacalhau job IDs are typically in UUID format: j-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
     */
    private String extractJobId(String output) {
        if (!output?.trim()) {
            return null
        }

        // Bacalhau typically returns just the job ID on successful submission
        // Job ID format: j-<uuid> or just <uuid>
        // UUID pattern: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        final uuidPattern = /[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}/
        final jobIdPattern = /(?:j-)?${uuidPattern}/

        final lines = output.trim().split('\n')

        // Try to find a line matching the strict job ID pattern
        for (String line : lines) {
            final trimmed = line.trim()
            if (trimmed.matches(jobIdPattern)) {
                log.debug "Extracted job ID: ${trimmed}"
                return trimmed
            }
        }

        // If no strict match, try to extract UUID from the output
        for (String line : lines) {
            final matcher = line =~ uuidPattern
            if (matcher.find()) {
                final jobId = matcher.group(0)
                log.debug "Extracted job ID from line: ${jobId}"
                return jobId
            }
        }

        log.warn "Could not extract valid job ID from output: ${output}"
        return null
    }

    /**
     * Read the exit status file
     */
    private Integer readExitFile() {
        try {
            final exitFile = task.workDir.resolve(TaskRun.CMD_EXIT)
            if (exitFile.exists()) {
                return exitFile.text.trim().toInteger()
            }
        } catch (Exception e) {
            log.debug "Failed to read exit file for task ${task.name}: ${e.message}"
        }
        return null
    }
}