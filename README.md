# nf-bacalhau

A Nextflow executor plugin for running workflows on
[Bacalhau](https://bacalhau.org), a distributed compute orchestration framework
that brings compute to data.

The plugin lets Nextflow target Bacalhau as its executor while preserving normal
process directives. It suits local or shared-filesystem Bacalhau deployments;
fully remote data staging is limited to explicitly configured input sources such
as S3.

> The federated-genomics **examples**, the **site coordinator + dashboard**, and
> the **manuscript** that use this plugin live in the companion research repo:
> [shukwong/nextflow_on_bacalhau](https://github.com/shukwong/nextflow_on_bacalhau).

## Requirements

- **Bacalhau CLI** — see the [installation guide](https://docs.bacalhau.org/getting-started/installation)
- **Nextflow 24.10.x (LTS)** — 25.x and newer are not yet supported; the plugin
  aborts on them with a clear message. Run e.g. `NXF_VER=24.10.0 nextflow run ...`
- **JDK 21** — only to build the plugin from source

## Install

**From the Nextflow Plugin Registry:**

```groovy
// nextflow.config
plugins { id 'nf-bacalhau@0.1.0' }
```

**Build and stage locally:**

```bash
make install        # or: ./gradlew install  — stages into ~/.nextflow/plugins/
```

## Configure

```groovy
plugins { id 'nf-bacalhau@0.1.0' }

process { executor = 'bacalhau' }

bacalhau {
    bacalhauCliPath = 'bacalhau'                 // Bacalhau CLI binary
    bacalhauNode    = 'https://api.bacalhau.org' // API endpoint
    s3Region        = 'us-east-1'                // region for s3:// inputs
}
```

| Option | Default | Description |
|---|---|---|
| `bacalhauCliPath` | `bacalhau` | Path to the Bacalhau CLI binary |
| `bacalhauNode` | `https://api.bacalhau.org` | Bacalhau API endpoint |
| `s3Region` | `us-east-1` | AWS region for `s3://` input sources |

Options may also be supplied under `process.ext`; the `bacalhau { }` block wins
when both are set.

## Use

Standard Nextflow directives translate to Bacalhau job specs:

```groovy
process compute {
    container 'python:3.11'
    cpus 2
    memory '4.GB'
    disk '10.GB'
    time '30m'
    accelerator 1   // GPU
    script: "..."
}
```

Supported directives: `cpus`, `memory`, `disk`, `time`, `accelerator`. Inputs may
be local paths, `s3://` URIs (fetched on the compute node), or `host://` bind
mounts; secrets are forwarded to the job via `ext.bacalhauSecrets`. See
[`docs/`](docs/) for configuration, architecture, and data-handling detail, and
[`examples/hello-world.nf`](examples/hello-world.nf) for a minimal workflow.

## Develop

```bash
make build              # or ./gradlew build   (JDK 21)
make test               # or ./gradlew test
BACALHAU_INTEGRATION=1 ./gradlew integrationTest   # opt-in live smoke test
```

Result retrieval (`bacalhau job get`, which can block up to 300 s) runs on a
shared bounded pool; size is overridable via the `bacalhau.retrievalThreads`
system property (default 10).

## Publish

1. Claim the `nf-bacalhau` provider at <https://registry.nextflow.io/claim-plugin>.
2. Add your registry token to `$HOME/.gradle/gradle.properties` (`npr.apiKey=...`)
   or export `NPR_API_KEY`.
3. `make release`  (or `./gradlew releasePlugin`)

## License

Apache License 2.0 — see [LICENSE](LICENSE).
