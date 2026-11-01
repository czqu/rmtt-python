#!/usr/bin/env bash
#
# Fetch, patch and build the reference RMTT implementations used by the
# cross-language E2E matrix, then drop the harness entry points into them.
#
#   REF_DIR=/tmp/ref ./e2e/setup_refs.sh
#
# Produces:
#   /tmp/bin/go-harness-server, /tmp/bin/go-harness-client
#   /tmp/ref/rmtt-java/... (HarnessServerMain/HarnessClientMain compiled)
#   /tmp/java-cp.txt                      (java classpath for the harnesses)
#   /tmp/ref/rmtt-rust/...                (e2e server/client release binaries)
#   /tmp/ref/rmtt-cpp/example/test_client
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
REF_DIR=${REF_DIR:-/tmp/ref}
BIN_DIR=${BIN_DIR:-/tmp/bin}
JAVA_CLASSES=${JAVA_CLASSES:-$REF_DIR/rmtt-java/rmtt-it/target/classes}
MAVEN_SETTINGS=${MAVEN_SETTINGS:-}

mkdir -p "$REF_DIR" "$BIN_DIR"

clone() {
  local repo=$1
  if [ ! -d "$REF_DIR/$repo/.git" ]; then
    git clone --depth 1 "https://github.com/czqu/$repo.git" "$REF_DIR/$repo"
  fi
}

clone rmtt-go
clone rmtt-java
clone rmtt-rust
clone rmtt-cpp

# --------------------------------------------------------------------- Go ----
mkdir -p "$REF_DIR/rmtt-go/e2e/harness/server" "$REF_DIR/rmtt-go/e2e/harness/client"
cp "$ROOT/e2e/harness/go-server/main.go" "$REF_DIR/rmtt-go/e2e/harness/server/main.go"
cp "$ROOT/e2e/harness/go-client/main.go" "$REF_DIR/rmtt-go/e2e/harness/client/main.go"
(cd "$REF_DIR/rmtt-go" && go build -o "$BIN_DIR/go-harness-server" ./e2e/harness/server \
  && go build -o "$BIN_DIR/go-harness-client" ./e2e/harness/client)
echo "go harness ready"

# ------------------------------------------------------------------- Java ----
# The parent POM registers the Central publishing plugin as a build extension;
# that is irrelevant for an E2E build and cannot always be resolved offline.
python3 - "$REF_DIR/rmtt-java/pom.xml" <<'PY'
import re, sys
path = sys.argv[1]
source = open(path).read()
stripped = re.sub(
    r'\s*<plugin>\s*<groupId>org\.sonatype\.central</groupId>.*?</plugin>', '', source, flags=re.S)
if stripped != source:
    open(path, 'w').write(stripped)
PY
mkdir -p "$REF_DIR/rmtt-java/rmtt-it/src/main/java/net/czqu/rmtt/it"
cp "$ROOT/e2e/harness/java/HarnessServerMain.java" \
   "$ROOT/e2e/harness/java/HarnessClientMain.java" \
   "$REF_DIR/rmtt-java/rmtt-it/src/main/java/net/czqu/rmtt/it/"
MVN_ARGS=(-B -q -DskipTests -Dmaven.javadoc.skip=true)
if [ -n "$MAVEN_SETTINGS" ]; then
  MVN_ARGS+=(-s "$MAVEN_SETTINGS")
fi
# `install` (not `compile`) so that rmtt-it's dependency:build-classpath can
# resolve the sibling modules from the local repository in a fresh workspace.
(cd "$REF_DIR/rmtt-java" && mvn "${MVN_ARGS[@]}" -pl rmtt-it -am install \
  && mvn "${MVN_ARGS[@]}" -pl rmtt-it dependency:build-classpath \
       -Dmdep.outputFile=/tmp/java-cp.txt >/dev/null)
# Belt and braces: put every module's freshly built classes first so the
# harness never depends on the installed jars being current.
MODULE_CLASSES=$(find "$REF_DIR/rmtt-java" -type d -path '*/target/classes' -print | paste -sd: -)
printf '%s:%s' "$MODULE_CLASSES" "$(cat /tmp/java-cp.txt)" > /tmp/java-cp.txt
echo "java harness ready ($JAVA_CLASSES)"

# ------------------------------------------------------------------- Rust ----
cp "$ROOT/e2e/harness/rust/client-main.rs" "$REF_DIR/rmtt-rust/e2e/rust-client/src/main.rs"
cp "$ROOT/e2e/harness/rust/server-main.rs" "$REF_DIR/rmtt-rust/e2e/rust-server/src/main.rs"
(cd "$REF_DIR/rmtt-rust/e2e/rust-server" && cargo build --release --quiet)
(cd "$REF_DIR/rmtt-rust/e2e/rust-client" && cargo build --release --quiet)
echo "rust harness ready"

# -------------------------------------------------------------------- C++ ----
(cd "$REF_DIR/rmtt-cpp" && make TRANSPORTS="tcp kcp tls ws" -j4 >/dev/null)
echo "cpp client ready"

echo "reference implementations ready under $REF_DIR"
