# Installation

## Prerequisites

| Component | Version | Notes |
|---|---|---|
| Bacalhau CLI | 1.7.x | [Install guide](https://docs.bacalhau.org/getting-started/installation) |
| Nextflow | 23.10.1 or later | |
| Java | 17 (build), 11+ (run) | OpenJDK 17 recommended for building the plugin |
| Docker | any current release | Only required if you want to run the compute node locally |

## Install from source

The plugin is not yet published to the Nextflow plugin registry, so you build
it locally and stage it into `~/.nextflow/plugins/`.

```bash
git clone https://github.com/shukwong/nextflow_on_bacalhau.git
cd nextflow_on_bacalhau

# Build the plugin (Java 17 required)
export JAVA_HOME=$(/usr/libexec/java_home -v 17)    # macOS
./gradlew assemble
```

This produces `build/libs/nf-bacalhau-0.1.0-SNAPSHOT.jar`.

## Stage the plugin

Nextflow discovers plugins in `~/.nextflow/plugins/<id>-<version>/`. Stage the
JAR and extracted classes in one step:

```bash
PLUGIN_DIR="$HOME/.nextflow/plugins/nf-bacalhau-0.1.0-SNAPSHOT"
rm -rf "$PLUGIN_DIR"
mkdir -p "$PLUGIN_DIR/classes" "$PLUGIN_DIR/lib"
cp build/libs/nf-bacalhau-0.1.0-SNAPSHOT.jar "$PLUGIN_DIR/lib/"
(cd "$PLUGIN_DIR/classes" && unzip -oq "../lib/nf-bacalhau-0.1.0-SNAPSHOT.jar")
```

The included [`examples/run-hello-world.sh`](https://github.com/shukwong/nextflow_on_bacalhau/blob/main/examples/run-hello-world.sh)
automates the build + stage step and boots a local Bacalhau node for smoke
testing.

## Verify

```bash
nextflow run 'https://github.com/shukwong/nextflow_on_bacalhau' \
  -r main -latest -profile local -c examples/nextflow.local.config
```

Or run the bundled hello-world end-to-end driver:

```bash
./examples/run-hello-world.sh
```

On success you'll see three hello messages, exit code 0 on every task, and a
local Bacalhau node reachable at `http://localhost:1234`.

## Uninstall

```bash
rm -rf "$HOME/.nextflow/plugins/nf-bacalhau-0.1.0-SNAPSHOT"
```
