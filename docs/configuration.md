# Configuration

The Bacalhau executor is configured through standard Nextflow configuration
files. Options can live under a dedicated `bacalhau { }` scope (preferred) or
`process.ext.*` (legacy). The dedicated scope wins when both are set.

## Minimal configuration

```groovy
plugins { id 'nf-bacalhau@0.1.0-SNAPSHOT' }

process {
    executor = 'bacalhau'
}
```

## Full configuration

```groovy
plugins { id 'nf-bacalhau@0.1.0-SNAPSHOT' }

bacalhau {
    bacalhauCliPath   = 'bacalhau'                   // CLI binary
    bacalhauNode      = 'http://localhost:1234'      // API endpoint
    waitForCompletion = true                         // Block until done
    maxRetries        = 3                            // >= 0
    storageEngine     = 'local'                      // 'ipfs' | 's3' | 'local'
    s3Region          = 'us-east-1'                  // for s3:// inputs
}

process {
    executor = 'bacalhau'
    ext.bacalhauSecrets = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
}
```

## Reference

| Option | Default | Description |
|---|---|---|
| `bacalhauCliPath` | `bacalhau` | Path to the Bacalhau CLI binary |
| `bacalhauNode` | `https://api.bacalhau.org` | API endpoint |
| `waitForCompletion` | `true` | Wait for job completion before returning |
| `maxRetries` | `3` | Retry count for failed jobs (non-negative) |
| `storageEngine` | `ipfs` | Storage backend: `ipfs`, `s3`, or `local` |
| `s3Region` | `us-east-1` | AWS region for `s3://` input sources |
| `ext.bacalhauSecrets` | `[]` | Env vars forwarded to the remote job |

## Resource directives

Standard Nextflow resource directives are translated to Bacalhau job limits:

```groovy
process heavyTask {
    container   'python:3.11'
    cpus        4
    memory      '8.GB'
    disk        '20.GB'      // Optional
    time        '1h'
    accelerator 1            // GPU count

    script: "python worker.py"
}
```

Supported directives: `cpus`, `memory`, `disk`, `time`, `accelerator`.

## Environment notes

- **`stageInMode`** — the executor honors Nextflow's default `symlink` staging;
  for Bacalhau-submitted jobs, inputs are mounted read-only at
  `/inputs/<filename>` inside the container.
- **workDir** — the executor mounts the task's Nextflow workDir read-write at
  `/nextflow-scripts` inside the container, and the wrapped command `cd`s
  into it so relative output paths resolve correctly.
- **Queue polling** — status is cached for 5 seconds (`QUEUE_STATUS_CACHE_TTL_MS`)
  and failures are suppressed for 10 seconds; up to 1000 jobs are retrieved per
  poll so long-running workflows don't lose track of in-flight jobs.
