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
import nextflow.processor.TaskHandler
import nextflow.processor.TaskRun
import nextflow.util.ServiceName
import org.pf4j.ExtensionPoint

import java.nio.file.Path

/**
 * Nextflow executor for Bacalhau distributed compute platform
 * 
 * @author Nextflow Contributors
 */
@Slf4j
@CompileStatic
@ServiceName('bacalhau')
class BacalhauExecutor extends AbstractGridExecutor implements ExtensionPoint {

    /**
     * Bacalhau CLI executable name or path
     */
    static final String BACALHAU_CLI = 'bacalhau'

    /**
     * Default Bacalhau API endpoint
     */
    static final String DEFAULT_API_ENDPOINT = 'https://api.bacalhau.org'

    /**
     * Initialize the executor
     */
    void initialize() {
        log.debug "Initializing Bacalhau executor with session: ${session?.runName}"
        
        // Verify Bacalhau CLI is available
        if (!isBacalhauAvailable()) {
            throw new IllegalStateException("Bacalhau CLI not found in PATH. Please install Bacalhau: https://docs.bacalhau.org/getting-started/installation")
        }
        
        log.info "Bacalhau executor initialized successfully"
    }

    /**
     * Get the submit command line for executing a task
     */
    @Override
    List<String> getSubmitCommandLine(TaskRun task, Path scriptFile) {
        log.debug "Generating submit command for task: ${task.name}"
        
        final List<String> cmd = []
        cmd << BACALHAU_CLI
        cmd << 'docker'
        cmd << 'run'
        
        // Add resource constraints
        addResourceConstraints(cmd, task)

        // Mount the script file
        // Bacalhau input format: -i source_path:destination_path
        // We mount the script to /tmp in the container
        final scriptName = scriptFile.getFileName().toString()
        cmd << '-i'
        cmd << "${scriptFile.toAbsolutePath()}:/tmp/${scriptName}"

        // Mount input files
        if (task.getInputFilesMap()) {
            task.getInputFilesMap().each { name, path ->
                cmd << '-i'
                cmd << "${path.toAbsolutePath()}:${path.toAbsolutePath()}"
            }
        }
        
        // Add container image
        final container = task.getContainer()
        if (!container) {
            throw new IllegalArgumentException("Task ${task.name} requires a container image")
        }
        cmd << container
        
        // Add execution command
        cmd << '--'
        cmd << 'bash'
        cmd << "/tmp/${scriptName}"
        
        log.debug "Submit command: ${cmd.join(' ')}"
        return cmd
    }

    /**
     * Get the kill command for terminating a job
     */
    @Override
    protected List<String> getKillCommand() {
        return [BACALHAU_CLI, 'job', 'stop']
    }

    /**
     * Get the queue status command
     */
    @Override
    protected String getHeaderToken() {
        return 'CREATED'
    }

    /**
     * Parse queue status from Bacalhau output
     */
    @Override
    protected Map<String, QueueStatus> parseQueueStatus(String text) {
        log.debug "Parsing queue status from Bacalhau output"
        
        final Map<String, QueueStatus> result = [:]
        
        if (!text?.trim()) {
            return result
        }
        
        // Parse bacalhau list output format
        // Expected format: JOB_ID   CREATED   MODIFIED   STATUS
        text.split('\n').each { line ->
            if (line && !line.startsWith('CREATED') && !line.trim().isEmpty()) {
                final parts = line.split(/\s+/)
                if (parts.length >= 4) {
                    final jobId = parts[0]
                    final status = parts[3]
                    result[jobId] = parseJobStatus(status)
                }
            }
        }
        
        return result
    }

    /**
     * Get queue status by running bacalhau list command
     */
    protected Map<String, QueueStatus> getQueueStatus() {
        log.debug "Fetching queue status from Bacalhau"
        
        try {
            final cmd = [BACALHAU_CLI, 'list', '--output', 'table', '--no-header']
            final proc = new ProcessBuilder(cmd).start()
            final output = proc.inputStream.text
            proc.waitFor()
            
            return parseQueueStatus(output)
        } catch (Exception e) {
            log.warn "Failed to get queue status: ${e.message}"
            return [:]
        }
    }

    /**
     * Create task handler for managing individual task lifecycle
     */
    @Override
    BacalhauTaskHandler createTaskHandler(TaskRun task) {
        log.debug "Creating task handler for: ${task.name}"
        return new BacalhauTaskHandler(task, this)
    }

    /**
     * Check if Bacalhau CLI is available in the system
     */
    private boolean isBacalhauAvailable() {
        try {
            final proc = new ProcessBuilder([BACALHAU_CLI, 'version']).start()
            final exitCode = proc.waitFor()
            return exitCode == 0
        } catch (Exception e) {
            log.debug "Bacalhau CLI check failed: ${e.message}"
            return false
        }
    }

    /**
     * Add resource constraints to the command
     */
    private void addResourceConstraints(List<String> cmd, TaskRun task) {
        // Add CPU constraint
        final cpus = task.config.getCpus()
        if (cpus && cpus > 0) {
            cmd << '--cpu'
            cmd << cpus.toString()
        }
        
        // Add memory constraint
        final memory = task.config.getMemory()
        if (memory) {
            cmd << '--memory'
            cmd << memory.toString()
        }
        
        // Add timeout if specified
        final time = task.config.getTime()
        if (time) {
            cmd << '--timeout'
            cmd << time.toString()
        }
        
        // Add GPU constraint
        final accelerator = task.config.getAccelerator()
        if (accelerator && accelerator.request > 0) {
             cmd << '--gpu'
             cmd << accelerator.request.toString()
        }

        // Add disk constraint
        final disk = task.config.getDisk()
        if (disk) {
            // Bacalhau doesn't natively support ephemeral disk sizing in docker run yet 
            // in the same way, but we can stub it or use environment variables if needed.
            // For now, let's log it as not fully supported or map it if a flag exists.
            // Note: Bacalhau 'docker run' does not strictly have a --disk flag in all versions, 
            // but we will add it assuming standard compute node logic or ignore if not strict.
            // Checking recent Bacalhau docs, strictly it's often handled by node selection.
            // We will add it as a resource request if the CLI supports it.
            // cmd << '--disk' 
            // cmd << disk.toString()
            log.warn "Disk directive specified but not fully supported in Bacalhau executor yet: ${disk}"
        }

        // Add environment variables
        final env = task.config.getEnvironment()
        if (env) {
            env.each { key, value ->
                cmd << '-e'
                cmd << "${key}=${value}"
            }
        }
    }

    /**
     * Parse Bacalhau job status to Nextflow QueueStatus
     */
    private QueueStatus parseJobStatus(String status) {
        switch (status?.toLowerCase()) {
            case 'queued':
            case 'pending':
                return QueueStatus.PENDING
            case 'running':
            case 'executing':
                return QueueStatus.RUNNING
            case 'completed':
            case 'finished':
                return QueueStatus.DONE
            case 'failed':
            case 'error':
                return QueueStatus.ERROR
            case 'cancelled':
            case 'stopped':
                return QueueStatus.ERROR
            default:
                log.warn "Unknown Bacalhau job status: ${status}"
                return QueueStatus.UNKNOWN
        }
    }

    /**
     * Required abstract method implementation
     */
    @Override
    List getDirectives(TaskRun task, List result) {
        return result
    }

    /**
     * Required abstract method implementation  
     */
    @Override
    def parseJobId(String text) {
        return text?.trim()
    }

    /**
     * Required abstract method implementation
     */
    @Override
    List queueStatusCommand(Object queue) {
        return [BACALHAU_CLI, 'list', '--output', 'json']
    }
}