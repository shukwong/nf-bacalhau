# Architecture

## Plugin topology

```
BacalhauExecutor  (extends AbstractGridExecutor)
  ├── BacalhauTaskHandler   (extends GridTaskHandler)
  ├── Queue status cache    (cached CLI poll, 5s TTL, 10s failure backoff)
  └── Job spec builder      (Nextflow directives → Bacalhau YAML)
```

`AbstractGridExecutor` is Nextflow's base class for batch-style executors —
the same layer Slurm, SGE, PBS, and LSF plug into. Reusing it means the
polling monitor, retry policy, and workDir lifecycle are all standard
Nextflow machinery; the Bacalhau-specific code is confined to job
translation and CLI invocation.

## Task lifecycle

```
Nextflow TaskProcessor
       │
       ▼
 BacalhauTaskHandler.submit()
       │   writes .bacalhau-job.yaml
       │   shells out to: bacalhau job run ...
       ▼
  Bacalhau orchestrator
       │
       ▼
  Compute node runs container:
     /nextflow-scripts  ← workDir (rw)   .command.sh, .command.out,
                                           .command.err, .exitcode
     /inputs/<name>     ← inputs (ro)    Nextflow-staged files, s3://, host://
       │
       ▼
  TaskPollingMonitor reads cached queue status
       │   bacalhau job list --output json --limit 1000
       ▼
  Completed → results copied back, task advanced
```

## Job-spec translation

Nextflow process directives map to Bacalhau job YAML as follows:

| Nextflow                      | Bacalhau                                    |
| ----------------------------- | ------------------------------------------- |
| `container`                   | `Engine.Params.Image`                       |
| script (`.command.sh`)        | workDir mounted rw at `/nextflow-scripts`   |
| `input: path x`               | `InputSources[].Type = localDirectory`      |
| `input: val 's3://...'`       | `InputSources[].Type = s3`                  |
| `input: val 'host:///...'`    | `InputSources[].Type = localDirectory` (rw) |
| `cpus`, `memory`, `disk`      | `Resources.{CPU,Memory,Disk}`               |
| `time`                        | `Timeouts.ExecutionTimeout`                 |
| `accelerator`                 | `Resources.GPU`                             |
| `ext.bacalhauSecrets`         | `Engine.Params.EnvironmentVariables`        |

The wrapped entrypoint is:

```
cd /nextflow-scripts && \
bash /nextflow-scripts/.command.sh \
  > /nextflow-scripts/.command.out \
  2> /nextflow-scripts/.command.err; \
echo $? > /nextflow-scripts/.exitcode
```

`cd` into the workDir is important — Nextflow script fragments reference
relative output filenames and expect them in the workDir after the task
completes.

## Queue polling

`bacalhau job list --output json` is:

- **Paginated** — default limit is 10 jobs. The plugin passes `--limit 1000`
  so long-running workflows don't drop in-flight jobs from view.
- **Decorated** — the CLI appends a human-readable pagination hint after the
  JSON array. The parser extracts the balanced array via a small scanner
  (`extractJsonArray`) and ignores trailing non-JSON content.

The status result is cached for `QUEUE_STATUS_CACHE_TTL_MS` (5 s) so every
`TaskPollingMonitor` tick reuses one CLI fetch. On CLI error the retry is
suppressed for `QUEUE_STATUS_FAILURE_BACKOFF_MS` (10 s).

## Why this shape

- **AbstractGridExecutor over a custom Executor** — inherits batch semantics
  (polling, retries, workDir staging) and keeps the plugin small and
  forward-compatible with Nextflow changes.
- **Shell-out CLI rather than HTTP SDK** — Bacalhau's Go SDK is not exposed
  to JVM clients, and the CLI contract is stable across v1.6–1.7.
- **Workspace mount + cd, not working-dir container setting** — Bacalhau's
  `Engine.Params` does not expose a reliable CWD knob across runtimes;
  `cd /nextflow-scripts && bash ...` is portable.
