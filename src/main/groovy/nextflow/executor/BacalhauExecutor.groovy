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
import nextflow.processor.TaskMonitor
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

    /** Container path where the Nextflow task script is mounted.
     *  Uses a dedicated directory so we don't overlay the container's /tmp. */
    static final String NEXTFLOW_SCRIPT_MOUNT = '/nextflow-scripts'

    /** How long (ms) a cached queue-status result remains valid within one poll cycle. */
    private static final long QUEUE_STATUS_CACHE_TTL_MS = 5_000L

    /** How long to suppress retries after a queue-status CLI failure. */
    private static final long QUEUE_STATUS_FAILURE_BACKOFF_MS = 10_000L

    /** Max jobs to fetch per queue poll. Bacalhau's default is 10, which
     *  silently truncates the result; we need all in-flight jobs visible so
     *  the TaskMonitor sees every submitted job's state transitions. */
    private static final int QUEUE_STATUS_LIMIT = 1000

    // -------------------------------------------------------------------------
    // Configuration (loaded once in initialize())
    // -------------------------------------------------------------------------
    private String bacalhauCliPath
    private String bacalhauNode
    private Boolean waitForCompletion
    private Integer maxRetries
    private String storageEngine
    private String s3Region

    // Queue-status cache. Reads and writes of the cache fields go through
    // `queueStatusLock`; the CLI fetch itself happens outside the lock so it
    // doesn't serialize every polling thread. `fetchInProgress` ensures only
    // one thread fetches at a time — the rest return the last known snapshot.
    private final Object queueStatusLock = new Object()
    private Map<String, QueueStatus> cachedQueueStatus = [:]
    private long cacheTimestamp = 0L
    private boolean fetchInProgress = false

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
     * Load configuration. A dedicated {@code bacalhau { }} block wins over
     * {@code process.ext} so that executor-wide settings live at executor
     * scope, not per-task.
     */
    private void loadConfiguration() {
        final Map bacalhauConfig = (session?.config?.bacalhau as Map) ?: [:]
        final Map processConfig  = (session?.config?.process  as Map) ?: [:]
        final Map extConfig      = (processConfig?.ext        as Map) ?: [:]

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

        s3Region = config.s3Region?.toString() ?: 'us-east-1'

        log.debug "Config loaded — CLI: ${bacalhauCliPath}, node: ${bacalhauNode}, maxRetries: ${maxRetries}, s3Region: ${s3Region}"
    }

    /** Return the configured path to the Bacalhau binary. */
    String getBacalhauCli() {
        return bacalhauCliPath ?: BACALHAU_CLI
    }

    // -------------------------------------------------------------------------
    // Job submission
    // -------------------------------------------------------------------------

    /**
     * Build the CLI command to submit a task. Writes a YAML job spec to the
     * task work directory and invokes {@code bacalhau job run <spec>}.
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

    /** Build a Bacalhau v1.x YAML job specification from the Nextflow task. */
    protected String buildJobSpec(TaskRun task, Path scriptFile, String container) {
        final String scriptName = scriptFile.getFileName().toString()
        final String scriptDir  = scriptFile.getParent().toAbsolutePath().toString()

        final StringBuilder sb = new StringBuilder()
        sb.append("Name: ${yamlQuote(task.name)}\n")
        sb.append("Namespace: default\n")
        sb.append("Type: batch\n")
        sb.append("Count: 1\n")
        sb.append("Tasks:\n")
        sb.append("  - Name: main\n")
        sb.append("    Engine:\n")
        sb.append("      Type: docker\n")
        sb.append("      Params:\n")
        sb.append("        Image: ${yamlQuote(container)}\n")
        // Wrap the user script so stdout/stderr/exit status are captured into
        // the mounted work dir (the contract Nextflow's GridTaskHandler expects).
        // Without this, `bacalhau job get` retrieves an empty workdir and
        // Nextflow fails with "Missing 'stdout' file".
        //
        // `cd` into the mounted workDir so relative paths in .command.sh
        // resolve inside it — Nextflow writes output files with relative
        // names and expects them in the workDir after the task completes.
        final String wrappedCmd =
            "cd ${NEXTFLOW_SCRIPT_MOUNT} && " +
            "bash ${NEXTFLOW_SCRIPT_MOUNT}/${scriptName} " +
            "> ${NEXTFLOW_SCRIPT_MOUNT}/.command.out " +
            "2> ${NEXTFLOW_SCRIPT_MOUNT}/.command.err; " +
            "echo \$? > ${NEXTFLOW_SCRIPT_MOUNT}/.exitcode"
        sb.append("        Entrypoint:\n")
        sb.append("          - bash\n")
        sb.append("          - \"-c\"\n")
        sb.append("          - ${yamlQuote(wrappedCmd)}\n")

        // --- Input sources ---
        sb.append("    InputSources:\n")

        // Script file: mount its parent directory at a dedicated path so we
        // don't overlay the container's /tmp (which many images rely on).
        // Must be ReadWrite so the entrypoint can write .command.out /
        // .command.err / .exitcode back to the Nextflow work directory.
        sb.append("      - Source:\n")
        sb.append("          Type: localDirectory\n")
        sb.append("          Params:\n")
        sb.append("            SourcePath: ${yamlQuote(scriptDir)}\n")
        sb.append("            ReadWrite: true\n")
        sb.append("        Target: ${NEXTFLOW_SCRIPT_MOUNT}\n")

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
                    sb.append("            Bucket: ${yamlQuote(bucket)}\n")
                    sb.append("            Key: ${yamlQuote(key)}\n")
                    sb.append("            Region: ${yamlQuote(s3Region)}\n")
                    sb.append("        Target: ${yamlQuote("/inputs/${name}")}\n")
                } else if (pathStr.startsWith('host://')) {
                    final String hostPath = validateHostPath(pathStr.substring(7))
                    sb.append("      - Source:\n")
                    sb.append("          Type: localDirectory\n")
                    sb.append("          Params:\n")
                    sb.append("            SourcePath: ${yamlQuote(hostPath)}\n")
                    sb.append("            ReadWrite: false\n")
                    sb.append("        Target: ${yamlQuote("/inputs/${name}")}\n")
                } else {
                    sb.append("      - Source:\n")
                    sb.append("          Type: localDirectory\n")
                    sb.append("          Params:\n")
                    sb.append("            SourcePath: ${yamlQuote(path.toAbsolutePath().toString())}\n")
                    sb.append("            ReadWrite: false\n")
                    sb.append("        Target: ${yamlQuote("/inputs/${name}")}\n")
                }
            }
        }

        // --- Resource constraints ---
        final cpus        = task.config.getCpus()
        final memory      = task.config.getMemory()
        final accelerator = task.config.getAccelerator()
        final disk        = task.config.getDisk()

        final Integer gpuRequest = accelerator?.request
        final boolean hasCpu  = cpus && (cpus as int) > 0
        final boolean hasGpu  = gpuRequest != null && gpuRequest > 0
        final boolean hasRes  = hasCpu || memory || hasGpu || disk

        if (hasRes) {
            sb.append("    Resources:\n")
            if (hasCpu)
                sb.append("      CPU: \"${cpus}\"\n")
            if (memory)
                sb.append("      Memory: ${yamlQuote(formatMemory(memory))}\n")
            if (hasGpu)
                sb.append("      GPU: \"${gpuRequest}\"\n")
            if (disk)
                sb.append("      Disk: ${yamlQuote(formatMemory(disk))}\n")
        }

        // --- Execution timeout ---
        final long timeoutSecs = formatTimeout(task.config.getTime())
        if (timeoutSecs > 0) {
            sb.append("    Timeouts:\n")
            sb.append("      ExecutionTimeout: ${timeoutSecs}\n")
        }

        // --- Environment variables and secrets (unified block) ---
        final Map<String, String> envEntries = collectEnvEntries(task)
        if (envEntries) {
            sb.append("    Env:\n")
            envEntries.each { String key, String value ->
                sb.append("      ${key}: ${yamlQuote(value)}\n")
            }
        }

        return sb.toString()
    }

    /** Matches a valid shell/env identifier. */
    private static final java.util.regex.Pattern ENV_NAME_PATTERN =
        java.util.regex.Pattern.compile('^[A-Za-z_][A-Za-z0-9_]*$')

    /**
     * Collect validated environment variable entries for a task, including
     * any secret names declared under {@code ext.bacalhauSecrets}.
     */
    private Map<String, String> collectEnvEntries(TaskRun task) {
        final Map<String, String> entries = new LinkedHashMap<>()

        final Map<String, String> env = task.getEnvironment()
        if (env) {
            env.each { String key, String value ->
                if (key != null && ENV_NAME_PATTERN.matcher(key).matches()) {
                    entries.put(key, value)
                } else {
                    log.warn "Task ${task.name}: skipping invalid env var name: ${key}"
                }
            }
        }

        final Object extRaw = task.config.get('ext')
        if (extRaw instanceof Map && ((Map) extRaw).containsKey('bacalhauSecrets')) {
            final Object secrets = ((Map) extRaw).get('bacalhauSecrets')
            final List secretList = secrets instanceof List ? secrets as List : [secrets]
            secretList.each { secretName ->
                final String sn = secretName?.toString()
                if (sn != null && ENV_NAME_PATTERN.matcher(sn).matches()) {
                    entries.put(sn, '${' + sn + '}')
                } else {
                    log.warn "Task ${task.name}: skipping invalid secret name: ${secretName}"
                }
            }
        }

        return entries
    }

    /**
     * Format a {@link MemoryUnit} (or any value) into the compact memory
     * string Bacalhau expects (e.g. {@code "512mb"}, {@code "4gb"}).
     * Nextflow's own {@code toString()} returns {@code "4 GB"} with a space,
     * which the Bacalhau CLI rejects.
     */
    protected String formatMemory(Object memory) {
        if (memory instanceof MemoryUnit) {
            final long bytes = (memory as MemoryUnit).toBytes()
            if (bytes >= 1_073_741_824L && bytes % 1_073_741_824L == 0)
                return "${bytes.intdiv(1_073_741_824L)}gb"
            if (bytes >= 1_048_576L && bytes % 1_048_576L == 0)
                return "${bytes.intdiv(1_048_576L)}mb"
            if (bytes >= 1_024L && bytes % 1_024L == 0)
                return "${bytes.intdiv(1_024L)}kb"
            return "${bytes}b"
        }
        // Fallback for unexpected types: strip spaces and lowercase
        return memory.toString().toLowerCase().replace(' ', '')
    }

    /**
     * Format a {@link Duration} (or numeric value) into seconds for
     * {@code Timeouts.ExecutionTimeout}. Returns 0 if unparseable — callers
     * omit the timeout block in that case, leaving the task uncapped.
     */
    protected long formatTimeout(Object time) {
        if (time == null) return 0L
        if (time instanceof Duration)
            return (time as Duration).toSeconds()
        try {
            return Long.parseLong(time.toString())
        } catch (Exception ignored) {
            log.warn "Could not parse execution timeout value: ${time}"
            return 0L
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

    /** Intentionally a no-op: Bacalhau does not use batch-script header directives. */
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
        return [getBacalhauCli(), 'job', 'list', '--output', 'json',
                '--limit', String.valueOf(QUEUE_STATUS_LIMIT)]
    }

    // -------------------------------------------------------------------------
    // Queue / status polling
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

        // Bacalhau appends a pagination hint ("\nTo fetch more records use:\n…")
        // after the JSON array when results are paginated. Strict JSON parsers
        // reject this trailing non-JSON content, so truncate at the last ']'.
        final String jsonPart = extractJsonArray(text)
        if (!jsonPart) {
            log.warn "Queue status output contained no JSON array"
            log.debug "Raw content: ${text}"
            return result
        }

        try {
            final jobs = new groovy.json.JsonSlurper().parseText(jsonPart)
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
     * Return the substring from the first '[' through the matching closing ']'.
     * Bacalhau's `job list --output json` prints a JSON array followed by a
     * plain-text pagination footer; we need only the array.
     *
     * Returns {@code null} if no balanced array is found.
     */
    private static String extractJsonArray(String text) {
        final int start = text.indexOf('[')
        if (start < 0) return null
        int depth = 0
        boolean inString = false
        boolean escape = false
        for (int i = start; i < text.length(); i++) {
            final char c = text.charAt(i)
            if (escape) { escape = false; continue }
            if (inString) {
                if (c == (char) '\\') escape = true
                else if (c == (char) '"') inString = false
                continue
            }
            if (c == (char) '"') { inString = true; continue }
            if (c == (char) '[') depth++
            else if (c == (char) ']') {
                depth--
                if (depth == 0) return text.substring(start, i + 1)
            }
        }
        return null
    }

    /**
     * Return a cached snapshot of the Bacalhau job list, refreshing it at most
     * once every {@link #QUEUE_STATUS_CACHE_TTL_MS} ms. Only one thread fetches
     * at a time; concurrent callers get the last known snapshot so the CLI
     * call does not serialize the polling monitor.
     */
    protected Map<String, QueueStatus> getQueueStatus() {
        synchronized (queueStatusLock) {
            final long now = System.currentTimeMillis()
            if (now - cacheTimestamp < QUEUE_STATUS_CACHE_TTL_MS || fetchInProgress)
                return cachedQueueStatus
            fetchInProgress = true
        }

        try {
            final Map<String, QueueStatus> result = fetchQueueStatus()
            synchronized (queueStatusLock) {
                if (result != null) {
                    cachedQueueStatus = result
                    cacheTimestamp = System.currentTimeMillis()
                    return result
                }
                // Fetch failed — suppress retries for BACKOFF ms so a broken
                // CLI doesn't get re-invoked on every poll.
                cacheTimestamp = System.currentTimeMillis() +
                    (QUEUE_STATUS_FAILURE_BACKOFF_MS - QUEUE_STATUS_CACHE_TTL_MS)
                return cachedQueueStatus
            }
        } finally {
            synchronized (queueStatusLock) {
                fetchInProgress = false
            }
        }
    }

    /**
     * Run {@code bacalhau job list --output json} and parse the result.
     * Returns {@code null} on failure so callers can distinguish "empty
     * queue" from "CLI error". Stdout and stderr are drained on background
     * threads so the child process can exit cleanly.
     */
    private Map<String, QueueStatus> fetchQueueStatus() {
        log.debug "Fetching queue status from Bacalhau"
        Process proc = null
        try {
            final List<String> cmd = [getBacalhauCli(), 'job', 'list', '--output', 'json',
                                      '--limit', String.valueOf(QUEUE_STATUS_LIMIT)]
            proc = new ProcessBuilder(cmd).redirectErrorStream(false).start()

            final StringBuilder stdout = new StringBuilder()
            final StringBuilder stderr = new StringBuilder()

            final Thread stdoutThread = Thread.start { proc.inputStream.eachLine  { stdout.append(it).append('\n') } }
            final Thread stderrThread = Thread.start { proc.errorStream.eachLine  { stderr.append(it).append('\n') } }

            final boolean finished = proc.waitFor(60, TimeUnit.SECONDS)
            stdoutThread.join(3_000)
            stderrThread.join(3_000)

            if (!finished) {
                proc.destroyForcibly()
                log.warn "Queue status command timed out after 60 s"
                return null
            }

            final int exitCode = proc.exitValue()
            if (exitCode != 0) {
                log.warn "Queue status command exited ${exitCode}: ${stderr.toString().trim()}"
                return null
            }

            return parseQueueStatus(stdout.toString())
        } catch (Exception e) {
            log.warn "Failed to get queue status: ${e.message}"
            return null
        } finally {
            if (proc?.isAlive()) proc.destroyForcibly()
        }
    }

    // -------------------------------------------------------------------------
    // Task handler & monitor factories
    // -------------------------------------------------------------------------

    @Override
    BacalhauTaskHandler createTaskHandler(TaskRun task) {
        log.debug "Creating task handler for: ${task.name}"
        return new BacalhauTaskHandler(task, this)
    }

    /**
     * Use {@link BacalhauTaskMonitor} instead of the HPC-tuned default.
     * Without this override the custom monitor is dead code — Nextflow
     * instantiates the generic {@link nextflow.processor.TaskPollingMonitor}
     * with HPC-style polling intervals.
     */
    @Override
    TaskMonitor createTaskMonitor() {
        return BacalhauTaskMonitor.create(
            session,
            name,
            BacalhauTaskMonitor.DEFAULT_POLL_INTERVAL,
            BacalhauTaskMonitor.DEFAULT_QUEUE_SIZE)
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    /**
     * Check whether the Bacalhau CLI binary is available and responsive,
     * bounded by a 30-second timeout so a hung binary doesn't stall the
     * session.
     */
    private boolean isBacalhauAvailable() {
        Process proc = null
        try {
            proc = new ProcessBuilder([getBacalhauCli(), 'version'])
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
        } finally {
            if (proc?.isAlive()) proc.destroyForcibly()
        }
    }

    /** Map a Bacalhau job state string to a Nextflow {@link QueueStatus}. */
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

    /** Escape a string for safe inclusion in a YAML double-quoted scalar.
     *  Prevents YAML injection by escaping backslashes, quotes, and
     *  whitespace control characters before wrapping in quotes. */
    protected static String yamlQuote(String value) {
        if (value == null) return '""'
        final String escaped = value
            .replace('\\', '\\\\')
            .replace('"',  '\\"')
            .replace('\n', '\\n')
            .replace('\r', '\\r')
            .replace('\t', '\\t')
        return "\"${escaped}\""
    }

    /**
     * Validate and normalize a {@code host://} path to prevent path traversal.
     *
     * Rejects paths that contain a {@code ..} path segment (which would escape
     * the intended mount scope). A plain substring match would spuriously
     * reject legitimate paths like {@code /data/foo..bar/file}.
     *
     * @throws IllegalArgumentException if the path is empty or contains a
     *         {@code ..} segment.
     */
    protected static String validateHostPath(String hostPath) {
        if (!hostPath || hostPath.trim().isEmpty())
            throw new IllegalArgumentException("host:// path must not be empty")

        final boolean hasParentSegment = hostPath.split('[/\\\\]').any { it == '..' }
        if (hasParentSegment)
            throw new IllegalArgumentException(
                "host:// path must not contain '..' traversal: ${hostPath}")

        return new File(hostPath).canonicalPath
    }
}
