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
| `s3Region` | `us-east-1` | AWS region for `s3://` input sources |
| `ext.bacalhauSecrets` | `[]` | Env vars forwarded to the remote job |

Prototype-era `bacalhau.waitForCompletion`, `bacalhau.maxRetries`, and
`bacalhau.storageEngine` keys are accepted only for compatibility and are
ignored. Use standard Nextflow `process.errorStrategy` and
`process.maxRetries` for retries.

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
- **local path visibility** — local `path` inputs and the task workDir are
  mounted as Bacalhau `localDirectory` sources. Compute nodes must be able to
  access those paths directly, so this is currently intended for local nodes or
  shared filesystems.
- **Queue polling** — status is cached for 5 seconds (`QUEUE_STATUS_CACHE_TTL_MS`)
  and failures are suppressed for 10 seconds; up to 1000 jobs are retrieved per
  poll so long-running workflows don't lose track of in-flight jobs.
