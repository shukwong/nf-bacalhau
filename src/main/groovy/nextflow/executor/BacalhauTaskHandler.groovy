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
import nextflow.trace.TraceRecord

import java.nio.file.Path

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
        
        try {
            // Create the task script
            final scriptFile = task.workDir.resolve(TaskRun.CMD_SCRIPT)
            
            // Get the submit command from executor
            final cmd = getBacalhauExecutor().getSubmitCommandLine(task, scriptFile)
            
            // Execute the submit command
            log.debug "Executing submit command: ${cmd.join(' ')}"
            final proc = new ProcessBuilder(cmd)
                .directory(task.workDir.toFile())
                .redirectErrorStream(true)
                .start()
            
            // Read the output to get job ID
            final output = proc.inputStream.text.trim()
            final exitCode = proc.waitFor()
            
            if (exitCode != 0) {
                throw new IllegalStateException("Bacalhau job submission failed with exit code ${exitCode}: ${output}")
            }
            
            // Extract job ID from output
            bacalhauJobId = extractJobId(output)
            if (!bacalhauJobId) {
                throw new IllegalStateException("Failed to extract job ID from Bacalhau output: ${output}")
            }
            
            log.info "Task ${task.name} submitted to Bacalhau with job ID: ${bacalhauJobId}"
            jobSubmitted = true
            
            // Set the batch job ID for status tracking
            status = TaskStatus.SUBMITTED
            
        } catch (Exception e) {
            log.error "Failed to submit task ${task.name} to Bacalhau", e
            status = TaskStatus.ERROR
            throw e
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
        if (!jobSubmitted || !bacalhauJobId) {
            return false
        }
        
        final queueStatus = getBacalhauExecutor().getQueueStatus()
        final jobStatus = queueStatus.get(bacalhauJobId)
        
        if (jobStatus == QueueStatus.RUNNING) {
            status = TaskStatus.RUNNING
            return true
        }
        
        return false
    }

    /**
     * Check if the job is completed
     */
    @Override
    boolean checkIfCompleted() {
        if (!jobSubmitted || !bacalhauJobId) {
            return false
        }
        
        final queueStatus = getBacalhauExecutor().getQueueStatus()
        final jobStatus = queueStatus.get(bacalhauJobId)
        
        if (jobStatus == QueueStatus.DONE) {
            status = TaskStatus.COMPLETED
            task.exitStatus = readExitFile()
            task.stdout = task.workDir.resolve(TaskRun.CMD_OUTFILE)
            task.stderr = task.workDir.resolve(TaskRun.CMD_ERRFILE)
            return true
        } else if (jobStatus == QueueStatus.ERROR) {
            status = TaskStatus.ERROR
            task.exitStatus = readExitFile() ?: 1
            task.error = new RuntimeException("Bacalhau job ${bacalhauJobId} failed")
            return true
        }
        
        return false
    }

    /**
     * Get the job ID for status tracking
     */
    @Override
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
     */
    private String extractJobId(String output) {
        if (!output?.trim()) {
            return null
        }
        
        // Bacalhau typically returns just the job ID on successful submission
        // Handle both plain job ID and formatted output
        final lines = output.trim().split('\n')
        for (String line : lines) {
            final trimmed = line.trim()
            if (trimmed && trimmed.matches(/^[a-f0-9-]{8,}$/)) {
                return trimmed
            }
        }
        
        // Fallback: return the last non-empty line
        return lines.findLast { it.trim() }?.trim()
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