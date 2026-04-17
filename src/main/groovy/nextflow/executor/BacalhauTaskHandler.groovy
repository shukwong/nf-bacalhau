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
import nextflow.executor.AbstractGridExecutor.QueueStatus
import nextflow.processor.TaskRun
import nextflow.processor.TaskStatus
import nextflow.trace.TraceRecord

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Task handler for managing individual Bacalhau job lifecycle.
 *
 * @author Nextflow Contributors
 */
@Slf4j
@CompileStatic
class BacalhauTaskHandler extends GridTaskHandler {

    /** Bacalhau job ID assigned after successful submission.
     *  Volatile because it's written by submit() and read by kill() and the
     *  polling callbacks on different threads. A non-null value implies the
     *  job was submitted successfully. */
    private volatile String bacalhauJobId

    // Result retrieval runs on a dedicated background thread so
    // checkIfCompleted() does not block the polling monitor for up to 5
    // minutes per job. The CountDownLatch gives a happens-before edge between
    // the retrieval thread's disk writes and the polling thread reading them.
    private volatile boolean retrievalStarted = false
    private volatile Thread retrievalThread
    private final CountDownLatch retrievalLatch = new CountDownLatch(1)

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    BacalhauTaskHandler(TaskRun task, BacalhauExecutor executor) {
        super(task, executor)
    }

    protected BacalhauExecutor getBacalhauExecutor() {
        return (BacalhauExecutor) executor
    }

    // -------------------------------------------------------------------------
    // TaskHandler API
    // -------------------------------------------------------------------------

    /**
     * Submit the task to Bacalhau.
     *
     * Runs {@code bacalhau job run <spec-file>}, waits up to
     * {@link BacalhauExecutor#DEFAULT_SUBMIT_TIMEOUT} seconds for it to
     * finish, then extracts the job ID from the output.
     */
    @Override
    void submit() {
        log.debug "Submitting task ${task.name} to Bacalhau"

        Process proc = null
        try {
            // Stage .command.sh and the run wrapper into task.workDir.
            // GridTaskHandler's default submit() normally does this via
            // createTaskWrapper(task).build(); since we override submit() to
            // run the Bacalhau CLI directly, we must invoke the builder
            // ourselves — otherwise the container mount contains only the
            // YAML spec and bash aborts with "No such file or directory".
            createTaskWrapper(task).build()

            final scriptFile = task.workDir.resolve(TaskRun.CMD_SCRIPT)
            final cmd        = getBacalhauExecutor().getSubmitCommandLine(task, scriptFile)

            log.info "Submitting task ${task.name}: ${cmd.join(' ')}"
            proc = new ProcessBuilder(cmd)
                .directory(task.workDir.toFile())
                .redirectErrorStream(false)
                .start()

            final StringBuilder stdout = new StringBuilder()
            final StringBuilder stderr = new StringBuilder()

            // Consume both streams in background to prevent pipe-buffer deadlock
            final Thread stdoutThread = Thread.start { proc.inputStream.eachLine { stdout.append(it).append('\n') } }
            final Thread stderrThread = Thread.start { proc.errorStream.eachLine { stderr.append(it).append('\n') } }

            final int timeout       = BacalhauExecutor.DEFAULT_SUBMIT_TIMEOUT
            final boolean finished  = proc.waitFor(timeout, TimeUnit.SECONDS)

            stdoutThread.join(5_000)
            stderrThread.join(5_000)

            if (!finished) {
                proc.destroyForcibly()
                throw new IllegalStateException(
                    "Bacalhau job submission timed out after ${timeout} s for task ${task.name}")
            }

            final int    exitCode    = proc.exitValue()
            final String output      = stdout.toString().trim()
            final String errorOutput = stderr.toString().trim()

            if (exitCode != 0) {
                log.error "Task ${task.name} submission failed (exit ${exitCode})"
                log.error "stdout: ${output}"
                log.error "stderr: ${errorOutput}"
                throw new IllegalStateException(
                    "Bacalhau job submission failed with exit code ${exitCode}: ${errorOutput ?: output}")
            }

            bacalhauJobId = extractJobId(output)
            if (!bacalhauJobId) {
                log.error "Failed to extract job ID from output: ${output}"
                throw new IllegalStateException(
                    "Failed to extract job ID from Bacalhau output. Output: ${output}")
            }

            log.info "Task ${task.name} submitted — Bacalhau job ID: ${bacalhauJobId}"

            synchronized (this) {
                status = TaskStatus.SUBMITTED
            }

        } catch (InterruptedException e) {
            log.error "Task ${task.name} submission interrupted", e
            markFailed(e, 1)
            Thread.currentThread().interrupt()
            throw new IllegalStateException("Job submission interrupted for task ${task.name}", e)

        } catch (Exception e) {
            log.error "Failed to submit task ${task.name} to Bacalhau", e
            markFailed(e, 1)
            throw e

        } finally {
            if (proc?.isAlive()) proc.destroyForcibly()
        }
    }

    /**
     * Mark the task as failed. {@link TaskStatus} has no ERROR value, so
     * failures are signalled by setting {@code task.error}, a non-zero
     * {@code exitStatus}, and transitioning to {@link TaskStatus#COMPLETED}.
     */
    private void markFailed(Throwable cause, int exitCode) {
        synchronized (this) {
            task.error      = cause
            task.exitStatus = exitCode
            status          = TaskStatus.COMPLETED
        }
    }

    /** Kill a running Bacalhau job (bounded waitFor so a hanging CLI does not
     *  block the caller indefinitely). Also interrupts the result-retrieval
     *  thread if one is currently running. */
    @Override
    void kill() {
        final Thread t = retrievalThread
        if (t != null && t.isAlive())
            t.interrupt()

        if (!bacalhauJobId) {
            log.warn "Cannot kill task ${task.name}: no job ID available"
            return
        }

        log.debug "Killing Bacalhau job: ${bacalhauJobId}"
        try {
            final cmd  = getBacalhauExecutor().getKillCommand() + [bacalhauJobId]
            final proc = new ProcessBuilder(cmd).redirectErrorStream(true).start()
            Thread.start { proc.inputStream.eachLine { } }

            final boolean finished = proc.waitFor(30, TimeUnit.SECONDS)
            if (!finished) {
                proc.destroyForcibly()
                log.warn "Kill command timed out for job ${bacalhauJobId}"
                return
            }

            if (proc.exitValue() == 0)
                log.info "Successfully killed Bacalhau job: ${bacalhauJobId}"
            else
                log.warn "Kill command exited non-zero for job ${bacalhauJobId}: ${proc.exitValue()}"

        } catch (Exception e) {
            log.warn "Error killing Bacalhau job ${bacalhauJobId}: ${e.message}"
        }
    }

    /** Return {@code true} once the job has transitioned to RUNNING. */
    @Override
    boolean checkIfRunning() {
        if (bacalhauJobId == null) return false

        try {
            final queueStatus = getBacalhauExecutor().getQueueStatus()
            if (queueStatus.get(bacalhauJobId) == QueueStatus.RUNNING) {
                synchronized (this) {
                    if (status != TaskStatus.RUNNING) {
                        status = TaskStatus.RUNNING
                        log.debug "Task ${task.name} is RUNNING (job ID: ${bacalhauJobId})"
                    }
                }
                return true
            }
        } catch (Exception e) {
            log.warn "Error checking running status for task ${task.name}: ${e.message}"
        }

        return false
    }

    /**
     * Return {@code true} once the job has finished and its results have been
     * retrieved (or retrieval has been attempted and failed). Returns
     * {@code false} while the background retrieval thread is still running so
     * the polling monitor is never blocked for more than a poll interval.
     */
    @Override
    boolean checkIfCompleted() {
        if (bacalhauJobId == null) return false

        try {
            final queueStatus = getBacalhauExecutor().getQueueStatus()
            final jobStatus   = queueStatus.get(bacalhauJobId)

            if (jobStatus == QueueStatus.DONE) {
                synchronized (this) {
                    if (!retrievalStarted) {
                        retrievalStarted = true
                        final String jobId = bacalhauJobId
                        final Thread t = new Thread({
                            try {
                                retrieveJobResults(jobId)
                            } finally {
                                retrievalLatch.countDown()
                            }
                        } as Runnable, "bacalhau-retrieve-${task.name}")
                        t.setDaemon(true)
                        retrievalThread = t
                        t.start()
                    }
                }

                // Non-blocking check: has the retrieval thread finished?
                if (retrievalLatch.getCount() > 0) {
                    log.debug "Task ${task.name}: waiting for result retrieval to complete"
                    return false
                }

                // Latch is at zero — retrieval thread has finished and all its
                // memory effects are visible (CountDownLatch guarantees happens-before).
                verifyOutputFiles()
                synchronized (this) {
                    status          = TaskStatus.COMPLETED
                    task.exitStatus = readExitFile() ?: 0
                    task.stdout     = task.workDir.resolve(TaskRun.CMD_OUTFILE)
                    task.stderr     = task.workDir.resolve(TaskRun.CMD_ERRFILE)
                }
                log.info "Task ${task.name} completed (job ID: ${bacalhauJobId})"
                return true

            } else if (jobStatus == QueueStatus.ERROR) {
                log.error "Task ${task.name} failed (job ID: ${bacalhauJobId})"
                markFailed(
                    new RuntimeException("Bacalhau job ${bacalhauJobId} failed"),
                    readExitFile() ?: 1)
                return true
            }

        } catch (Exception e) {
            log.error "Error checking completion status for task ${task.name}: ${e.message}", e
            markFailed(
                new RuntimeException("Failed to check job status: ${e.message}", e),
                1)
            return true
        }

        return false
    }

    // -------------------------------------------------------------------------
    // Monitoring / trace
    // -------------------------------------------------------------------------

    @Override
    TraceRecord getTraceRecord() {
        final trace = super.getTraceRecord()
        if (bacalhauJobId)
            trace.put('native_id', bacalhauJobId)
        return trace
    }

    /** Return the Bacalhau job ID (or {@code null} if not yet submitted). */
    String getJobId() {
        return bacalhauJobId
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    /**
     * Download job outputs to the task work directory.
     *
     * Called on a background thread (see {@link #checkIfCompleted()}).
     * Any exception is caught and logged so the retrieval thread always
     * sets {@code retrievalCompleted = true}.
     */
    private void retrieveJobResults(String jobId) {
        log.debug "Retrieving results for job ${jobId}"

        Process proc = null
        try {
            final cmd = [
                getBacalhauExecutor().getBacalhauCli(),
                'job', 'get',
                jobId,
                '--output-dir', task.workDir.toString()
            ]

            log.info "Retrieving results for task ${task.name}: ${cmd.join(' ')}"
            proc = new ProcessBuilder(cmd)
                .directory(task.workDir.toFile())
                .redirectErrorStream(false)
                .start()

            final StringBuilder stdout = new StringBuilder()
            final StringBuilder stderr = new StringBuilder()

            final Thread stdoutThread = Thread.start { proc.inputStream.eachLine { stdout.append(it).append('\n') } }
            final Thread stderrThread = Thread.start { proc.errorStream.eachLine { stderr.append(it).append('\n') } }

            final boolean finished = proc.waitFor(300, TimeUnit.SECONDS)

            stdoutThread.join(5_000)
            stderrThread.join(5_000)

            if (!finished) {
                proc.destroyForcibly()
                log.warn "Result retrieval timed out for job ${jobId}"
                return
            }

            final int exitCode = proc.exitValue()
            if (exitCode != 0) {
                log.warn "Result retrieval failed for job ${jobId} (exit ${exitCode}): ${stderr.toString().trim()}"
            } else {
                log.info "Results retrieved for task ${task.name} (job ID: ${jobId})"
            }

        } catch (InterruptedException e) {
            log.warn "Result retrieval interrupted for job ${jobId}"
            Thread.currentThread().interrupt()
        } catch (Exception e) {
            log.warn "Error retrieving results for job ${jobId}: ${e.message}", e
        } finally {
            if (proc?.isAlive()) proc.destroyForcibly()
        }
    }

    /** Warn if expected output files are absent after result retrieval. */
    private void verifyOutputFiles() {
        final exitFile = task.workDir.resolve(TaskRun.CMD_EXIT)
        final outFile  = task.workDir.resolve(TaskRun.CMD_OUTFILE)
        final errFile  = task.workDir.resolve(TaskRun.CMD_ERRFILE)

        if (!exitFile.exists()) log.warn "Task ${task.name}: exit file not found at ${exitFile}"
        if (!outFile.exists())  log.debug "Task ${task.name}: stdout file not found at ${outFile}"
        if (!errFile.exists())  log.debug "Task ${task.name}: stderr file not found at ${errFile}"
    }

    /**
     * Extract a Bacalhau job ID from CLI output.
     *
     * Bacalhau's CLI emits IDs in the form {@code <prefix>-<UUID>} where
     * {@code prefix} is typically {@code job} or {@code j} (depending on CLI
     * version). We accept either — with prefix when present on the line, or
     * falling back to the bare UUID otherwise.
     */
    private String extractJobId(String output) {
        if (!output?.trim()) return null

        final uuidPattern     = /[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}/
        final prefixedPattern = /(?:job|j)-${uuidPattern}/

        final String[] lines = output.trim().split('\n')

        for (String line : lines) {
            final matcher = line =~ prefixedPattern
            if (matcher.find()) {
                final String jobId = matcher.group(0)
                log.debug "Extracted job ID: ${jobId}"
                return jobId
            }
        }

        for (String line : lines) {
            final matcher = line =~ uuidPattern
            if (matcher.find()) {
                final String jobId = matcher.group(0)
                log.debug "Extracted bare UUID job ID: ${jobId}"
                return jobId
            }
        }

        log.warn "Could not extract valid job ID from output: ${output}"
        return null
    }

    /** Read the integer exit status written by the task script. */
    private Integer readExitFile() {
        try {
            final exitFile = task.workDir.resolve(TaskRun.CMD_EXIT)
            if (exitFile.exists())
                return exitFile.text.trim().toInteger()
        } catch (Exception e) {
            log.debug "Failed to read exit file for task ${task.name}: ${e.message}"
        }
        return null
    }
}
