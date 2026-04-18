# Quickstart

This page runs a minimal "hello world" against a **local** Bacalhau node so
you can verify the plugin end-to-end without any cloud setup.

## 1. Start a local Bacalhau node

```bash
bacalhau serve \
  --orchestrator \
  --compute \
  --name=local-node \
  -c API.Port=1234 \
  -c "Compute.AllowListedLocalPaths=/tmp/**:rw,/private/tmp/**:rw"
```

The `AllowListedLocalPaths` allowlist lets the executor mount your Nextflow
workDir into the compute container. Without it, jobs cannot write
`.command.out`, `.command.err`, or `.exitcode` back to the shared workDir.

Check it's alive:

```bash
curl http://localhost:1234/api/v1/agent/alive
```

## 2. Point Nextflow at the local node

```groovy title="nextflow.local.config"
plugins { id 'nf-bacalhau@0.1.0' }

bacalhau {
    bacalhauCliPath   = 'bacalhau'
    bacalhauNode      = 'http://localhost:1234'
    waitForCompletion = true
    maxRetries        = 1
    storageEngine     = 'local'
}

process {
    executor  = 'bacalhau'
    container = 'ubuntu:latest'
    cpus      = 1
    memory    = '512.MB'
    time      = '5m'
}

docker {
    enabled    = true
    runOptions = '--rm'
}
```

## 3. Run a pipeline

```groovy title="hello-world.nf"
process greet {
    container 'ubuntu:latest'
    input:  val name
    output: stdout
    script: "echo 'Hello from Bacalhau, ${name}!'"
}

workflow {
    Channel.of('Alice', 'Bob', 'Carol') | greet | view
}
```

```bash
nextflow run hello-world.nf -c nextflow.local.config
```

You should see:

```
executor >  bacalhau (3)
[xx/xxxxxx] process > greet (2) [100%] 3 of 3 ✔
Hello from Bacalhau, Alice!
Hello from Bacalhau, Bob!
Hello from Bacalhau, Carol!
```

## 4. Next steps

- Walk through the [federated allele-frequency demo](examples/federated-af.md)
  to see a privacy-preserving genomics pipeline.
- Read about [data handling](data-handling.md) to stage inputs from S3 or
  node-local paths.
- Review the [configuration reference](configuration.md).
