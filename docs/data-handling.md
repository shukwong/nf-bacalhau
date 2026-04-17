# Data Handling

The executor supports three input-source shapes. All of them land on the
compute container at `/inputs/<filename>` read-only, so the process script
can reference inputs uniformly regardless of where they came from.

## Local paths

The default Nextflow `path` input. The file is staged from the workDir and
mounted at `/inputs/<name>`.

```groovy
process analyzeLocal {
    input:  path vcf
    script: "bcftools view /inputs/${vcf.name}"
}
```

## S3 URIs

Pass an `s3://` URI as a `val`. Bacalhau's native S3 input source fetches
the object directly on the compute node — you never download it locally.

```groovy
process analyzeS3 {
    input:  val s3_path   // "s3://my-bucket/data.csv"
    script: "process_data /inputs/data.csv"
}
```

Credentials: forward the relevant env vars via `ext.bacalhauSecrets` (see
[Secrets](#secrets)). The region is taken from `bacalhau.s3Region`
(default: `us-east-1`).

## Host paths

Use the `host://` prefix to bind-mount a directory that already exists on
the compute node — useful for large reference datasets that would be slow
to re-stage on every job.

```groovy
process analyzeLocal {
    input:  val local_path   // "host:///data/reference_genome.fa"
    script: "bwa index /inputs/reference_genome.fa"
}
```

Host paths are mounted **read-write** so the process can touch sidecar
files (e.g. `bwa` index companions). Add the directory to the compute
node's `Compute.AllowListedLocalPaths` to permit the mount — the plugin
will not silently bypass the allowlist.

## Secrets

Forward environment variables from the Nextflow driver to the remote job
via `ext.bacalhauSecrets`. Names listed here must exist in the shell that
launches `nextflow`; their values are copied into the job spec.

```groovy
process {
    executor = 'bacalhau'
    ext.bacalhauSecrets = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
}
```

!!! warning "Do not hard-code secrets in nextflow.config"
    Store credentials in your shell profile or a secret manager. The
    plugin only references names — values come from the runtime
    environment at submit time.

## Output staging

The Bacalhau job runs with the task workDir mounted read-write at
`/nextflow-scripts`. Your process script writes outputs by their declared
`output:` names, and the executor retrieves `.command.out`, `.command.err`,
`.exitcode`, and the declared output files after the job reaches a
terminal state via `bacalhau job get --output-dir <workDir>`.
