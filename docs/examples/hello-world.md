# Hello World

A three-process pipeline that confirms the plugin round-trip works:
script staging, container execution, output capture, and state transitions.

## Files

Clone the repo and you'll find everything under `examples/`:

```
examples/
├── nextflow.local.config    # Plugin + local node config
└── run-hello-world.sh       # Build, start node, run pipeline, assert exit codes
```

## Run

```bash
./examples/run-hello-world.sh
```

The driver:

1. Verifies Docker, `bacalhau`, and Nextflow 24.10+ are present.
2. Auto-detects Java 17 (`openjdk@17` from Homebrew or `java_home -v 17`).
3. Builds the plugin with Gradle and stages it into `~/.nextflow/plugins/`.
4. Starts a local Bacalhau node on `:1234` with
   `Compute.AllowListedLocalPaths=/tmp/**:rw,/private/tmp/**:rw`.
5. Runs the pipeline, prints exit codes, and tails captured stdout.

`--skip-build` and `--skip-bacalhau` speed up re-runs.

## What it proves

- The plugin can be loaded by Nextflow 24.10+.
- `.command.sh` is staged into the workDir and mounted into the container.
- The wrapper writes `.command.out`, `.command.err`, and `.exitcode` back to
  the workDir so Nextflow's TaskPollingMonitor sees completion.
- `bacalhau job list --output json` is parsed correctly even when the CLI
  appends its pagination footer.

If this pipeline passes, you're ready to try the
[federated allele-frequency demo](federated-af.md).
