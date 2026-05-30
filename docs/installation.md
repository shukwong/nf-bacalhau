# Installation

## Prerequisites

| Component | Version | Notes |
|---|---|---|
| Bacalhau CLI | 1.7.x | [Install guide](https://docs.bacalhau.org/getting-started/installation) |
| Nextflow | 24.10.0 or later | |
| Java | 21 (build only) | Required by the `io.nextflow.nextflow-plugin` Gradle plugin when building from source |
| Docker | any current release | Only required if you want to run the compute node locally |

## Install from the Nextflow Plugin Registry

Once the plugin is published, reference it directly in `nextflow.config`:

```groovy
plugins { id 'nf-bacalhau@0.1.0' }
```

Nextflow downloads and caches the plugin on first run — no manual staging
required.

## Install from source

```bash
git clone https://github.com/shukwong/nextflow_on_bacalhau.git
cd nextflow_on_bacalhau

# Build the plugin (Java 21 required)
export JAVA_HOME=$(/usr/libexec/java_home -v 21)    # macOS
make assemble            # or: ./gradlew assemble
```

This produces `build/libs/nf-bacalhau-0.1.0.jar` plus a
`build/distributions/nf-bacalhau-0.1.0.zip` package suitable for the registry.

## Stage the plugin

Use `make install` to stage the plugin into `~/.nextflow/plugins/`:

```bash
make install             # or: ./gradlew install
```

If you prefer to stage manually:

```bash
PLUGIN_DIR="$HOME/.nextflow/plugins/nf-bacalhau-0.1.0"
rm -rf "$PLUGIN_DIR"
mkdir -p "$PLUGIN_DIR/classes" "$PLUGIN_DIR/lib"
cp build/libs/nf-bacalhau-0.1.0.jar "$PLUGIN_DIR/lib/"
(cd "$PLUGIN_DIR/classes" && unzip -oq "../lib/nf-bacalhau-0.1.0.jar")
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
rm -rf "$HOME/.nextflow/plugins/nf-bacalhau-0.1.0"
```
