#!/usr/bin/env bash
#
# End-to-end test runner for the nf-bacalhau plugin.
#
# What this does:
#   1. Verifies prerequisites (Docker, bacalhau, Nextflow 24.10+).
#   2. Builds and stages the plugin into ~/.nextflow/plugins/.
#   3. Starts a local Bacalhau compute node (if one is not already running)
#      with writable AllowListedLocalPaths for /tmp and /private/tmp.
#   4. Runs the hello-world Nextflow pipeline against the local node.
#   5. Prints per-task exit codes and captured stdout.
#
# Usage:
#   ./examples/run-hello-world.sh [--skip-build] [--skip-bacalhau]
#
set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLUGIN_VERSION="0.1.0"
PLUGIN_ID="nf-bacalhau"
PLUGIN_DIR="$HOME/.nextflow/plugins/${PLUGIN_ID}-${PLUGIN_VERSION}"
PLUGIN_JAR="${PROJECT_ROOT}/build/libs/${PLUGIN_ID}-${PLUGIN_VERSION}.jar"

RUN_DIR="${RUN_DIR:-/tmp/nf-bacalhau-run}"
BACALHAU_API_PORT="${BACALHAU_API_PORT:-1234}"
BACALHAU_LOG="${RUN_DIR}/bacalhau-server.log"
BACALHAU_PID_FILE="${RUN_DIR}/bacalhau-server.pid"

NEXTFLOW_BIN="${NEXTFLOW_BIN:-nextflow23}"

SKIP_BUILD=false
SKIP_BACALHAU=false
for arg in "$@"; do
  case "$arg" in
    --skip-build)    SKIP_BUILD=true ;;
    --skip-bacalhau) SKIP_BACALHAU=true ;;
    -h|--help)
      sed -n '2,15p' "$0"; exit 0 ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done

log()  { printf '\033[1;34m[run]\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m[fail]\033[0m %s\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 1. Prerequisites
# ---------------------------------------------------------------------------
log "Checking prerequisites"
command -v docker    >/dev/null || fail "docker CLI not found"
command -v bacalhau  >/dev/null || fail "bacalhau CLI not found (install from https://docs.bacalhau.org)"
command -v "$NEXTFLOW_BIN" >/dev/null || \
  fail "Nextflow binary '$NEXTFLOW_BIN' not found. Set NEXTFLOW_BIN or install Nextflow >=24.10.0 (plugin manifest requires it)."

docker info >/dev/null 2>&1 || fail "Docker daemon is not running"

# Nextflow 23.10.x needs Java 17. Autodetect if JAVA_HOME is not already set.
if [[ -z "${JAVA_HOME:-}" ]] || [[ ! -x "${JAVA_HOME}/bin/java" ]]; then
  for candidate in \
      /opt/homebrew/opt/openjdk@17 \
      /usr/local/opt/openjdk@17 \
      "$(/usr/libexec/java_home -v 17 2>/dev/null || true)"; do
    if [[ -n "$candidate" && -x "$candidate/bin/java" ]]; then
      export JAVA_HOME="$candidate"
      export PATH="$JAVA_HOME/bin:$PATH"
      break
    fi
  done
fi
command -v java >/dev/null || fail "Java 17 not found — install openjdk@17 or export JAVA_HOME."

mkdir -p "$RUN_DIR"

# ---------------------------------------------------------------------------
# 2. Build + stage plugin
# ---------------------------------------------------------------------------
if ! $SKIP_BUILD; then
  log "Building plugin (./gradlew assemble)"
  (cd "$PROJECT_ROOT" && ./gradlew --quiet assemble)
fi

[[ -f "$PLUGIN_JAR" ]] || fail "Plugin JAR not found at $PLUGIN_JAR — run without --skip-build."

log "Staging plugin to $PLUGIN_DIR"
rm -rf "$PLUGIN_DIR"
mkdir -p "$PLUGIN_DIR/classes" "$PLUGIN_DIR/lib"
cp "$PLUGIN_JAR" "$PLUGIN_DIR/lib/"
# Expand class + resource files for PF4J so the MANIFEST + extensions.idx resolve.
# Use `unzip` rather than `jar xf` so we don't depend on a JDK being on PATH.
(cd "$PLUGIN_DIR/classes" && unzip -oq "$PLUGIN_JAR")

# ---------------------------------------------------------------------------
# 3. Local Bacalhau compute node
# ---------------------------------------------------------------------------
is_bacalhau_up() {
  curl -fsS "http://localhost:${BACALHAU_API_PORT}/api/v1/agent/alive" >/dev/null 2>&1
}

if ! $SKIP_BACALHAU; then
  if is_bacalhau_up; then
    log "Bacalhau node already running on :${BACALHAU_API_PORT}"
  else
    log "Starting local Bacalhau node (log: $BACALHAU_LOG)"
    nohup bacalhau serve \
      --orchestrator \
      --compute \
      --name=local-node \
      -c "API.Port=${BACALHAU_API_PORT}" \
      -c "Compute.AllowListedLocalPaths=/tmp/**:rw,/private/tmp/**:rw" \
      >"$BACALHAU_LOG" 2>&1 &
    echo $! > "$BACALHAU_PID_FILE"
    for _ in $(seq 1 30); do
      is_bacalhau_up && break
      sleep 1
    done
    is_bacalhau_up || { tail -40 "$BACALHAU_LOG"; fail "Bacalhau failed to come up"; }
    log "Bacalhau node ready (pid $(cat "$BACALHAU_PID_FILE"))"
  fi
fi

# ---------------------------------------------------------------------------
# 4. Run the hello-world pipeline
# ---------------------------------------------------------------------------
# Nextflow 23.10.x does not allow `plugins {}` or `process {}` blocks inside
# .nf files, so we write a minimal clean pipeline and use the shipped config.
PIPELINE_FILE="${RUN_DIR}/hello-world.nf"
cat >"$PIPELINE_FILE" <<'NF'
#!/usr/bin/env nextflow

process sayHello {
    container 'ubuntu:latest'

    input:
    val greeting

    output:
    stdout

    script:
    """
    echo "${greeting} from Bacalhau distributed compute!"
    echo "Running on node: \$(hostname)"
    echo "Current time: \$(date)"
    """
}

workflow {
    greetings = Channel.of('Hello', 'Hola', 'Bonjour')
    sayHello(greetings) | view
}
NF

CONFIG_FILE="${PROJECT_ROOT}/examples/nextflow.local.config"
# The shipped config does not declare the plugin itself. Append if missing so
# Nextflow resolves the staged snapshot.
RUN_CONFIG="${RUN_DIR}/nextflow.config"
cp "$CONFIG_FILE" "$RUN_CONFIG"
grep -q "plugins" "$RUN_CONFIG" || \
  printf "\nplugins { id '%s@%s' }\n" "$PLUGIN_ID" "$PLUGIN_VERSION" >>"$RUN_CONFIG"

log "Running pipeline in $RUN_DIR"
cd "$RUN_DIR"
rm -rf work .nextflow*
"$NEXTFLOW_BIN" run "$PIPELINE_FILE" -c "$RUN_CONFIG"

# ---------------------------------------------------------------------------
# 5. Report per-task outputs
# ---------------------------------------------------------------------------
log "Per-task results:"
shopt -s nullglob
for d in "$RUN_DIR"/work/*/*; do
  [[ -f "$d/.exitcode" ]] || continue
  printf '  %s\n' "$d"
  printf '    exitcode: %s\n' "$(cat "$d/.exitcode")"
  printf '    stdout:   %s\n' "$(head -1 "$d/.command.out" 2>/dev/null || true)"
done

log "Done."
