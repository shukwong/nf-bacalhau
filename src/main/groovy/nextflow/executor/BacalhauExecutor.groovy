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
import nextflow.util.Duration
import nextflow.util.MemoryUnit
import nextflow.util.ServiceName
import org.pf4j.ExtensionPoint

import java.nio.file.Path
import java.util.concurrent.TimeUnit

/**
 * Nextflow executor for Bacalhau distributed compute platform.
 *
 * Submits jobs via the Bacalhau CLI using a YAML job specification
 * (compatible with Bacalhau v1.x — "bacalhau job run").
 *
 * @author Nextflow Contributors
 */
@Slf4j
@CompileStatic
@ServiceName('bacalhau')
class BacalhauExecutor extends AbstractGridExecutor implements ExtensionPoint {

    /** Bacalhau CLI executable name or path */
    static final String BACALHAU_CLI = 'bacalhau'

    /** Default Bacalhau API endpoint */
    static final String DEFAULT_API_ENDPOINT = 'https://api.bacalhau.org'

    /** Default job submission timeout in seconds */
    static final int DEFAULT_SUBMIT_TIMEOUT = 300

    // -------------------------------------------------------------------------
    // Configuration (loaded once in initialize())
    // -------------------------------------------------------------------------
    private String bacalhauCliPath
    private String bacalhauNode
    private Boolean waitForCompletion
    private Integer maxRetries
    private String storageEngine

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /** Initialize the executor: load config and verify the CLI is reachable. */
    void initialize() {
        log.debug "Initializing Bacalhau executor with session: ${session?.runName}"
        loadConfiguration()
        if (!isBacalhauAvailable()) {
            throw new IllegalStateException(
                "Bacalhau CLI not found in PATH. " +
                "Please install Bacalhau: https://docs.bacalhau.org/getting-started/installation")
        }
        log.info "Bacalhau executor initialized successfully with node: ${bacalhauNode}"
    }

    /**
     * Load configuration.
     *
     * Priority (highest first):
     *   1. Dedicated {@code bacalhau { }} config block
     *   2. {@code process { ext { } }} block (legacy / backward compat)
     *
     * FIX #12: executor-level settings were previously read only from
     * process.ext, which is per-task and not the right place for executor
     * globals.  We now read from a top-level {@code bacalhau { }} block and
     * fall back to process.ext for backward compatibility.
     */
    private void loadConfiguration() {
        // 1. Dedicated bacalhau {} config block (preferred)
        final Map bacalhauConfig = (session?.config?.bacalhau as Map) ?: [:]
        // 2. Legacy process.ext (fallback)
        final Map processConfig  = (session?.config?.process  as Map) ?: [:]
        final Map extConfig      = (processConfig?.ext        as Map) ?: [:]

        // Merge — bacalhauConfig wins on conflicts
        final Map config = (extConfig + bacalhauConfig) as Map

        bacalhauCliPath   = config.bacalhauCliPath?.toString() ?: BACALHAU_CLI
        bacalhauNode      = config.bacalhauNode?.toString()    ?: DEFAULT_API_ENDPOINT
        waitForCompletion = config.waitForCompletion != null ? config.waitForCompletion as Boolean : true
        maxRetries        = config.maxRetries != null ? (config.maxRetries as Integer) : 3

        if (maxRetries < 0)
            throw new IllegalArgumentException("maxRetries must be non-negative, got: ${maxRetries}")

        storageEngine = config.storageEngine?.toString() ?: 'ipfs'
        if (!['ipfs', 's3', 'local'].contains(storageEngine))
            log.warn "Unknown storage engine: ${storageEngine}"

        log.debug "Config loaded — CLI: ${bacalhauCliPath}, node: ${bacalhauNode}, maxRetries: ${maxRetries}"
    }

    /** Return the configured path to the Bacalhau binary. */
    String getBacalhauCli() {
        return bacalhauCliPath ?: BACALHAU_CLI
    }

    // -------------------------------------------------------------------------
    // Job submission — FIX #4 + #9
    // -------------------------------------------------------------------------

    /**
     * Build the CLI command to submit a task.
     *
     * FIX #4: The old {@code bacalhau docker run} sub-command was removed in
     * Bacalhau v1.x.  We now write a YAML job specification to the task work
     * directory and submit it with {@code bacalhau job run}.
     *
     * FIX #9: The original code used two different {@code -i} flag syntaxes
     * ({@code path:dest} and {@code src=…,dst=…}) in the same command.  Both
     * issues are eliminated by moving all input declarations into the typed
     * YAML {@code InputSources} block.
     */
    @Override
    List<String> getSubmitCommandLine(TaskRun task, Path scriptFile) {
        log.debug "Generating submit command for task: ${task.name}"

        final String container = task.getContainer()
        if (!container)
            throw new IllegalArgumentException("Task ${task.name} requires a container image")

        // Write the job spec into the task work directory
        final String specContent = buildJobSpec(task, scriptFile, container)
        final Path   specFile    = task.workDir.resolve('.bacalhau-job.yaml')
        specFile.text = specContent
        log.debug "Written Bacalhau job spec to: ${specFile}"

        final List<String> cmd = [getBacalhauCli(), 'job', 'run']

        // Override the API host when a non-default endpoint is configured
        if (bacalhauNode && bacalhauNode != DEFAULT_API_ENDPOINT) {
            cmd << '--api-host' << bacalhauNode
        }

        cmd << specFile.toAbsolutePath().toString()

        log.debug "Submit command: ${cmd.join(' ')}"
        return cmd
    }

    /**
     * Build a Bacalhau v1.x YAML job specification from the Nextflow task.
     *
     * FIX #7: Resource values (CPU, memory, time) are now formatted in the
     * compact notation that the Bacalhau API expects (e.g. {@code "4gb"},
     * {@code 3600}) rather than relying on Nextflow's own {@code toString()}
     * which can produce strings like {@code "4 GB"} or {@code "PT1H"}.
     */
    protected String buildJobSpec(TaskRun task, Path scriptFile, String container) {
        final String scriptName = scriptFile.getFileName().toString()
        final String scriptDir  = scriptFile.getParent().toAbsolutePath().toString()

        final StringBuilder sb = new StringBuilder()
        sb.append("Name: \"${task.name}\"\n")
        sb.append("Namespace: default\n")
        sb.append("Type: batch\n")
        sb.append("Count: 1\n")
        sb.append("Tasks:\n")
        sb.append("  - Name: main\n")
        sb.append("    Engine:\n")
        sb.append("      Type: docker\n")
        sb.append("      Params:\n")
        sb.append("        Image: \"${container}\"\n")
        sb.append("        Entrypoint:\n")
        sb.append("          - bash\n")
        sb.append("          - \"/tmp/${scriptName}\"\n")

        // --- Input sources ---
        sb.append("    InputSources:\n")

        // Script file: mount its parent directory so the script lands in /tmp
        sb.append("      - Source:\n")
        sb.append("          Type: localDirectory\n")
        sb.append("          Params:\n")
        sb.append("            SourcePath: \"${scriptDir}\"\n")
        sb.append("            ReadWrite: false\n")
        sb.append("        Target: /tmp\n")

        // Task input files
        final Map<String, Path> inputFiles = task.getInputFilesMap()
        if (inputFiles) {
            inputFiles.each { String name, Path path ->
                final String pathStr = path.toString()
                if (pathStr.startsWith('s3://')) {
                    final URI s3Uri  = new URI(pathStr)
                    final String bucket = s3Uri.host
                    final String key    = s3Uri.path?.replaceFirst('^/', '') ?: ''
                    sb.append("      - Source:\n")
                    sb.append("          Type: s3\n")
                    sb.append("          Params:\n")
                    sb.append("            Bucket: \"${bucket}\"\n")
                    sb.append("            Key: \"${key}\"\n")
                    sb.append("            Region: us-east-1\n")
                    sb.append("        Target: \"/inputs/${name}\"\n")
                } else if (pathStr.startsWith('host://')) {
                    final String hostPath = pathStr.substring(7)  // strip 'host://'
                    sb.append("      - Source:\n")
                    sb.append("          Type: localDirectory\n")
                    sb.append("          Params:\n")
                    sb.append("            SourcePath: \"${hostPath}\"\n")
                    sb.append("            ReadWrite: false\n")
                    sb.append("        Target: \"/inputs/${name}\"\n")
                } else {
                    sb.append("      - Source:\n")
                    sb.append("          Type: localDirectory\n")
                    sb.append("          Params:\n")
                    sb.append("            SourcePath: \"${path.toAbsolutePath()}\"\n")
                    sb.append("            ReadWrite: false\n")
                    sb.append("        Target: \"/inputs/${name}\"\n")
                }
            }
        }

        // --- Resource constraints ---
        final cpus        = task.config.getCpus()
        final memory      = task.config.getMemory()
        final accelerator = task.config.getAccelerator()
        final disk        = task.config.getDisk()

        final boolean hasCpu  = cpus && (cpus as int) > 0
        final boolean hasGpu  = accelerator && accelerator.request > 0
        final boolean hasRes  = hasCpu || memory || hasGpu || disk

        if (hasRes) {
            sb.append("    Resources:\n")
            if (hasCpu)
                sb.append("      CPU: \"${cpus}\"\n")
            if (memory)
                sb.append("      Memory: \"${formatMemory(memory)}\"\n")
            if (hasGpu)
                sb.append("      GPU: \"${accelerator.request}\"\n")
            if (disk)
                sb.append("      Disk: \"${formatMemory(disk)}\"\n")
        }

        // --- Execution timeout ---
        final time = task.config.getTime()
        if (time) {
            sb.append("    Timeouts:\n")
            sb.append("      ExecutionTimeout: ${formatTimeout(time)}\n")
        }

        // --- Environment variables ---
        final Map<String, String> env = task.config.getEnvironment()
        boolean envHeaderWritten = false
        if (env) {
            sb.append("    Env:\n")
            envHeaderWritten = true
            env.each { String key, String value ->
                if (key?.matches(/^[A-Za-z_][A-Za-z0-9_]*$/)) {
                    sb.append("      ${key}: \"${value}\"\n")
                } else {
                    log.warn "Task ${task.name}: skipping invalid env var name: ${key}"
                }
            }
        }

        // --- Secrets via env injection ---
        final Map extConfig = task.config.getExt() as Map
        if (extConfig && extConfig.containsKey('bacalhauSecrets')) {
            final secrets = extConfig.get('bacalhauSecrets')
            final List secretList = secrets instanceof List ? secrets as List : [secrets]
            if (!envHeaderWritten)
                sb.append("    Env:\n")
            secretList.each { secretName ->
                final String sn = secretName?.toString()
                if (sn?.matches(/^[A-Za-z_][A-Za-z0-9_]*$/)) {
                    sb.append("      ${sn}: \"\${${sn}}\"\n")
                } else {
                    log.warn "Task ${task.name}: skipping invalid secret name: ${secretName}"
                }
            }
        }

        return sb.toString()
    }

    /**
     * Format a Nextflow {@link MemoryUnit} (or any object) into the compact
     * memory string expected by Bacalhau (e.g. {@code "512mb"}, {@code "4gb"}).
     *
     * FIX #7: Nextflow's MemoryUnit.toString() returns strings like "4 GB"
     * (with a space), which the Bacalhau CLI rejects.
     */
    protected String formatMemory(Object memory) {
        if (memory instanceof MemoryUnit) {
            final long bytes = (memory as MemoryUnit).toBytes()
            if (bytes >= 1_073_741_824L)
                return "${bytes.intdiv(1_073_741_824L)}gb"
            if (bytes >= 1_048_576L)
                return "${bytes.intdiv(1_048_576L)}mb"
            return "${bytes}b"
        }
        // Fallback for unexpected types: strip spaces and lowercase
        return memory.toString().toLowerCase().replace(' ', '')
    }

    /**
     * Format a Nextflow {@link Duration} into seconds (as required by
     * Bacalhau's {@code Timeouts.ExecutionTimeout} field).
     *
     * FIX #7: Nextflow Duration.toString() can produce ISO-8601 strings like
     * "PT10M" that the Bacalhau CLI does not accept.
     */
    protected long formatTimeout(Object time) {
        if (time instanceof Duration)
            return (time as Duration).toSeconds()
        try {
            return Long.parseLong(time.toString())
        } catch (Exception ignored) {
            return DEFAULT_SUBMIT_TIMEOUT
        }
    }

    // -------------------------------------------------------------------------
    // AbstractGridExecutor abstract method implementations
    // -------------------------------------------------------------------------

    @Override
    protected List<String> getKillCommand() {
        return [getBacalhauCli(), 'job', 'stop']
    }

    /**
     * Not meaningful for Bacalhau (used by AbstractGridExecutor to build batch
     * script headers for HPC schedulers).  Return an empty string.
     */
    @Override
    protected String getHeaderToken() {
        return ''
    }

    @Override
    List getDirectives(TaskRun task, List result) {
        return result
    }

    @Override
    def parseJobId(String text) {
        return text?.trim()
    }

    @Override
    List queueStatusCommand(Object queue) {
        return [getBacalhauCli(), 'job', 'list', '--output', 'json']
    }

    // -------------------------------------------------------------------------
    // Queue / status polling — FIX #5
    // -------------------------------------------------------------------------

    /**
     * Parse the JSON output of {@code bacalhau job list} into a status map.
     *
     * Expected JSON format (array of job objects):
     * <pre>
     * [ { "ID": "j-…", "State": { "StateType": "completed" } }, … ]
     * </pre>
     */
    @Override
    protected Map<String, QueueStatus> parseQueueStatus(String text) {
        log.debug "Parsing queue status from Bacalhau JSON"

        final Map<String, QueueStatus> result = [:]
        if (!text?.trim())
            return result

        try {
            final jobs = new groovy.json.JsonSlurper().parseText(text)
            final List jobList = jobs instanceof List ? jobs as List : [jobs]
            jobList.each { job ->
                final Map jobMap = job as Map
                if (jobMap.ID) {
                    final String jobId  = jobMap.ID as String
                    final String status = (jobMap.State as Map)?.StateType as String
                    result[jobId] = parseJobStatus(status)
                }
            }
        } catch (Exception e) {
            log.warn "Failed to parse queue status JSON: ${e.message}"
            log.debug "Raw content: ${text}"
        }

        return result
    }

    /**
     * Fetch the current job list from the Bacalhau API.
     *
     * FIX #5: The original implementation read only stdout and left stderr
     * unconsumed.  If the process writes enough to stderr to fill the OS pipe
     * buffer the process blocks, creating a deadlock.  We now drain both
     * streams in background threads and add a 60-second hard timeout.
     */
    protected Map<String, QueueStatus> getQueueStatus() {
        log.debug "Fetching queue status from Bacalhau"
        try {
            final List<String> cmd = [getBacalhauCli(), 'job', 'list', '--output', 'json']
            final Process proc = new ProcessBuilder(cmd).redirectErrorStream(false).start()

            final StringBuilder stdout = new StringBuilder()
            final StringBuilder stderr = new StringBuilder()

            // Drain both streams concurrently to prevent deadlock
            final Thread stdoutThread = Thread.start { proc.inputStream.eachLine  { stdout.append(it).append('\n') } }
            final Thread stderrThread = Thread.start { proc.errorStream.eachLine  { stderr.append(it).append('\n') } }

            final boolean finished = proc.waitFor(60, TimeUnit.SECONDS)
            stdoutThread.join(3_000)
            stderrThread.join(3_000)

            if (!finished) {
                proc.destroyForcibly()
                log.warn "Queue status command timed out after 60 s"
                return [:]
            }

            final int exitCode = proc.exitValue()
            if (exitCode != 0) {
                log.warn "Queue status command exited ${exitCode}: ${stderr.toString().trim()}"
                return [:]
            }

            return parseQueueStatus(stdout.toString())
        } catch (Exception e) {
            log.warn "Failed to get queue status: ${e.message}"
            return [:]
        }
    }

    // -------------------------------------------------------------------------
    // Task handler factory
    // -------------------------------------------------------------------------

    @Override
    BacalhauTaskHandler createTaskHandler(TaskRun task) {
        log.debug "Creating task handler for: ${task.name}"
        return new BacalhauTaskHandler(task, this)
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    /**
     * Check whether the Bacalhau CLI binary is available and responsive.
     *
     * FIX #6: The original {@code proc.waitFor()} had no timeout.  If the
     * binary hangs (e.g. network wait on startup) the Nextflow thread would
     * block indefinitely.  We now enforce a 30-second timeout.
     */
    private boolean isBacalhauAvailable() {
        try {
            final Process proc = new ProcessBuilder([getBacalhauCli(), 'version'])
                .redirectErrorStream(true)
                .start()
            // Drain output so the process can exit cleanly
            Thread.start { proc.inputStream.eachLine { } }

            final boolean finished = proc.waitFor(30, TimeUnit.SECONDS)
            if (!finished) {
                proc.destroyForcibly()
                log.debug "Bacalhau CLI version check timed out"
                return false
            }
            final boolean ok = proc.exitValue() == 0
            if (ok) log.debug "Bacalhau CLI found at: ${getBacalhauCli()}"
            return ok
        } catch (Exception e) {
            log.debug "Bacalhau CLI check failed: ${e.message}"
            return false
        }
    }

    /**
     * Map a Bacalhau job state string to a Nextflow {@link QueueStatus}.
     *
     * FIX #3: Changed from {@code private} to {@code protected} so that tests
     * (and potential subclasses) can call it directly without hitting a
     * compile-time access violation under {@code @CompileStatic}.
     */
    protected QueueStatus parseJobStatus(String status) {
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
}
