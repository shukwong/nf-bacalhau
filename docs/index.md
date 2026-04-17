# Nextflow on Bacalhau

**Run Nextflow workflows on [Bacalhau](https://bacalhau.org)'s distributed compute network — bring the compute to the data instead of moving data to the compute.**

## Why this plugin

Modern bioinformatics, scientific computing, and machine-learning pipelines routinely
touch datasets that are too large, too slow, or too sensitive to move to a central
cluster:

- **Genomics shards** held by hospitals that cannot leave an institutional boundary.
- **Geographically distributed sensor data** where bandwidth dominates runtime.
- **Public S3-hosted reference data** you want to process without egress fees.

Bacalhau orchestrates containers *next to the node that holds the data*. This
plugin lets any Nextflow pipeline target Bacalhau as its executor — your
processes, directives, and channels are unchanged; only the execution layer
moves to a data-local compute network.

## What you get

- A drop-in Nextflow executor selected by `process.executor = 'bacalhau'`.
- Standard Nextflow resource directives (`cpus`, `memory`, `disk`, `time`,
  `accelerator`) translate directly to Bacalhau job specs.
- Input sources: local paths, `s3://` URIs (fetched on the compute node), and
  `host://` bind mounts for node-local reference data.
- Secret injection via `ext.bacalhauSecrets`.
- Cached queue-status polling with paginated CLI support.

## A 60-second tour

```groovy title="main.nf"
process sayHello {
    container 'ubuntu:latest'
    input:  val name
    output: stdout
    script: "echo 'Hello, ${name} from Bacalhau!'"
}

workflow {
    Channel.of('World', 'Nextflow', 'Bacalhau') | sayHello | view
}
```

```groovy title="nextflow.config"
plugins { id 'nf-bacalhau@0.1.0-SNAPSHOT' }

process {
    executor  = 'bacalhau'
    container = 'ubuntu:latest'
}

bacalhau {
    bacalhauNode      = 'http://localhost:1234'
    waitForCompletion = true
    maxRetries        = 1
    storageEngine     = 'local'
}
```

```bash
nextflow run main.nf
```

Next: [install the plugin](installation.md) or skip ahead to the
[federated allele-frequency demo](examples/federated-af.md) that exercises the
full pipeline against a local Bacalhau node.

## The privacy story

The most compelling use case for *compute-to-data* is federated genomics.
In our [federated allele-frequency demo](examples/federated-af.md), each
hospital's genotype matrix stays on-site; only integer allele counts cross
the network. The runner asserts the privacy invariant automatically.

## Status

Phases 1–3 are complete: core executor, job translation, monitoring, and
security hardening. Phase 4 (performance tuning, multi-node benchmarking,
networking features) is in progress. See [Status & Roadmap](status.md).
