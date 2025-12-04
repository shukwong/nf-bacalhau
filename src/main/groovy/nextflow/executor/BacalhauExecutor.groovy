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
     * Default job submission timeout in seconds
     */
    static final int DEFAULT_SUBMIT_TIMEOUT = 300

    /**
     * Configuration parameters
     */
    private String bacalhauCliPath
    private String bacalhauNode
    private Boolean waitForCompletion
    private Integer maxRetries
    private String storageEngine

    /**
     * Initialize the executor
     */
    void initialize() {
        log.debug "Initializing Bacalhau executor with session: ${session?.runName}"

        // Load configuration from process.ext or session config
        loadConfiguration()

        // Verify Bacalhau CLI is available
        if (!isBacalhauAvailable()) {
            throw new IllegalStateException("Bacalhau CLI not found in PATH. Please install Bacalhau: https://docs.bacalhau.org/getting-started/installation")
        }

        log.info "Bacalhau executor initialized successfully with node: ${bacalhauNode}"
    }

    /**
     * Load and validate configuration
     */
    private void loadConfiguration() {
        // Load from session config if available
        final config = session?.config?.process as Map ?: [:]
        final extConfig = config?.ext as Map ?: [:]

        // Bacalhau CLI path (default to 'bacalhau' in PATH)
        bacalhauCliPath = extConfig.bacalhauCliPath ?: BACALHAU_CLI

        // Bacalhau API node endpoint
        bacalhauNode = extConfig.bacalhauNode ?: DEFAULT_API_ENDPOINT

        // Wait for completion mode (default: true)
        waitForCompletion = extConfig.waitForCompletion != null ? extConfig.waitForCompletion : true

        // Max retries for failed jobs (default: 3)
        maxRetries = extConfig.maxRetries ?: 3
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be non-negative, got: ${maxRetries}")
        }

        // Storage engine (default: auto-detect)
        storageEngine = extConfig.storageEngine ?: 'ipfs'
        if (storageEngine && !['ipfs', 's3', 'local'].contains(storageEngine)) {
            log.warn "Unknown storage engine: ${storageEngine}, using default"
        }

        log.debug "Configuration loaded - CLI: ${bacalhauCliPath}, Node: ${bacalhauNode}, MaxRetries: ${maxRetries}"
    }

    /**
     * Get the configured Bacalhau CLI path
     */
    String getBacalhauCli() {
        return bacalhauCliPath ?: BACALHAU_CLI
    }

    /**
     * Get the submit command line for executing a task
     */
    @Override
    List<String> getSubmitCommandLine(TaskRun task, Path scriptFile) {
        log.debug "Generating submit command for task: ${task.name}"

        final List<String> cmd = []
        cmd << getBacalhauCli()
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
                def pathStr = path.toString()
                if (pathStr.startsWith('s3://')) {
                    // Handle S3 inputs natively
                    // Syntax: -i src=s3://bucket/key,dst=/inputs/filename
                    cmd << '-i'
                    cmd << "src=${pathStr},dst=/inputs/${name}"
                } else if (pathStr.startsWith('host://')) {
                    // Handle Host Path inputs (files existing on remote node)
                    // Syntax: host:///path/to/file -> src=file:///path/to/file,dst=/inputs/filename
                    def hostPath = pathStr.substring(7) // Remove 'host://'
                    cmd << '-i'
                    cmd << "src=file://${hostPath},dst=/inputs/${name}"
                } else {
                    // Handle local file inputs - mount to /inputs directory
                    // Syntax: local_path:/inputs/filename
                    cmd << '-i'
                    cmd << "${path.toAbsolutePath()}:/inputs/${name}"
                }
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
        return [getBacalhauCli(), 'job', 'stop']
    }

    /**
     * Get the queue status command
     */
    @Override
    protected String getHeaderToken() {
        return 'CREATED'
    }

    /**
     * Parse queue status from Bacalhau JSON output
     */
    @Override
    protected Map<String, QueueStatus> parseQueueStatus(String text) {
        log.debug "Parsing queue status from Bacalhau JSON output"

        final Map<String, QueueStatus> result = [:]

        if (!text?.trim()) {
            return result
        }

        try {
            // Parse JSON output from bacalhau list
            def jsonSlurper = new groovy.json.JsonSlurper()
            def jobs = jsonSlurper.parseText(text)

            // Handle both array and single object responses
            def jobList = jobs instanceof List ? jobs : [jobs]

            jobList.each { job ->
                if (job.ID) {
                    final jobId = job.ID as String
                    final status = job.State?.StateType as String
                    result[jobId] = parseJobStatus(status)
                }
            }
        } catch (Exception e) {
            log.warn "Failed to parse queue status JSON: ${e.message}"
            log.debug "JSON content: ${text}"
        }

        return result
    }

    /**
     * Get queue status by running bacalhau list command
     */
    protected Map<String, QueueStatus> getQueueStatus() {
        log.debug "Fetching queue status from Bacalhau"

        try {
            final cmd = [getBacalhauCli(), 'job', 'list', '--output', 'json']
            final proc = new ProcessBuilder(cmd).start()
            final output = proc.inputStream.text
            final exitCode = proc.waitFor()

            if (exitCode != 0) {
                log.warn "Failed to get queue status, exit code: ${exitCode}"
                return [:]
            }

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
            final proc = new ProcessBuilder([getBacalhauCli(), 'version']).start()
            final exitCode = proc.waitFor()
            if (exitCode == 0) {
                log.debug "Bacalhau CLI found at: ${getBacalhauCli()}"
                return true
            }
            return false
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
            log.debug "Task ${task.name}: Setting CPU constraint to ${cpus}"
        }

        // Add memory constraint
        final memory = task.config.getMemory()
        if (memory) {
            cmd << '--memory'
            cmd << memory.toString()
            log.debug "Task ${task.name}: Setting memory constraint to ${memory}"
        }

        // Add timeout if specified
        final time = task.config.getTime()
        if (time) {
            cmd << '--timeout'
            cmd << time.toString()
            log.debug "Task ${task.name}: Setting timeout to ${time}"
        }

        // Add GPU constraint
        final accelerator = task.config.getAccelerator()
        if (accelerator && accelerator.request > 0) {
            cmd << '--gpu'
            cmd << accelerator.request.toString()
            log.debug "Task ${task.name}: Setting GPU constraint to ${accelerator.request}"
        }

        // Disk constraints are not supported in Bacalhau docker run
        final disk = task.config.getDisk()
        if (disk) {
            log.warn "Task ${task.name}: Disk directive specified (${disk}) but not supported in Bacalhau executor"
        }

        // Add environment variables with validation
        final env = task.config.getEnvironment()
        if (env) {
            env.each { key, value ->
                // Basic validation to prevent command injection
                if (key && !key.toString().matches(/^[A-Za-z_][A-Za-z0-9_]*$/)) {
                    log.warn "Task ${task.name}: Skipping invalid environment variable name: ${key}"
                    return
                }
                cmd << '-e'
                cmd << "${key}=${value}"
            }
            log.debug "Task ${task.name}: Added ${env.size()} environment variables"
        }

        // Add secrets from configuration
        // We look for 'bacalhauSecrets' in the process 'ext' block
        // Format expected: ext.bacalhauSecrets = ['MY_ENV_VAR', 'ANOTHER_SECRET']
        final extConfig = task.config.getExt()
        if (extConfig && extConfig.containsKey('bacalhauSecrets')) {
            final secrets = extConfig.get('bacalhauSecrets')
            if (secrets instanceof List) {
                secrets.each { secretName ->
                    // Validate secret name format
                    if (secretName && secretName.toString().matches(/^[A-Za-z_][A-Za-z0-9_]*$/)) {
                        cmd << '--secret'
                        cmd << "env=${secretName}"
                    } else {
                        log.warn "Task ${task.name}: Skipping invalid secret name: ${secretName}"
                    }
                }
                log.debug "Task ${task.name}: Added ${secrets.size()} secrets"
            } else if (secrets instanceof String) {
                // Handle single secret definition
                if (secrets.matches(/^[A-Za-z_][A-Za-z0-9_]*$/)) {
                    cmd << '--secret'
                    cmd << "env=${secrets}"
                } else {
                    log.warn "Task ${task.name}: Skipping invalid secret name: ${secrets}"
                }
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
        return [getBacalhauCli(), 'job', 'list', '--output', 'json']
    }
}