# Data Handling

The executor supports three input-source shapes. All of them land on the
compute container at `/inputs/<filename>` read-only, so the process script
can reference inputs uniformly regardless of where they came from.

## Local paths

The default Nextflow `path` input. The staged file is mounted at
`/inputs/<name>` as a Bacalhau `localDirectory` source.

The Bacalhau compute node must be able to see the staged path directly. This
works for local Bacalhau nodes and shared filesystems; it is not yet a general
remote upload/staging mechanism.

```groovy
process analyzeLocal {
    input:  path vcf
    script: "bcftools view /inputs/${vcf.name}"
}
```

## S3 URIs

Pass an `s3://` URI as a `path` value recognized by Nextflow's file staging
machinery. Bacalhau's native S3 input source fetches the object directly on
the compute node.

```groovy
process analyzeS3 {
    input:  path s3_path   // "s3://my-bucket/data.csv"
    script: "process_data /inputs/data.csv"
}
```

Credentials: forward the relevant env vars via `ext.bacalhauSecrets` (see
[Secrets](#secrets)). The region is taken from `bacalhau.s3Region`
(default: `us-east-1`).

## Host paths

Use the `host://` prefix with a path input to bind-mount a directory that
already exists on the compute node — useful for large reference datasets that
would be slow to re-stage on every job.

```groovy
process analyzeLocal {
    input:  path local_path   // "host:///data/reference_genome.fa"
    script: "bwa index /inputs/reference_genome.fa"
}
```

Host paths are mounted read-only. Add the directory to the compute node's
`Compute.AllowListedLocalPaths` to permit the mount — the plugin will not
silently bypass the allowlist.

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
