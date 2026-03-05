#!/bin/bash
#
# Deadlock / Thread Hang Test
#
# Tests for thread hang issues when cluster becomes unavailable.
#
# Usage:
#   ./run-test.sh                    # Run with defaults (1 pod, 5 min, auto cleanup)
#   NUM_PODS=4 ./run-test.sh         # Run with 4 pods
#   DURATION=600 ./run-test.sh       # Run for 10 minutes
#   VERSION=2.2.7 ./run-test.sh      # Use specific version from jars/
#   KILL_CLUSTER_AT=30 ./run-test.sh # Kill cluster at 30s to trigger error paths
#   CLEANUP=false ./run-test.sh      # Keep cluster running for debugging
#
#   python3 ../utils/cluster_manager.py stop --prefix cluster # Manual cleanup cluster
#   rm pod*.log heap_*.hprof                                  # Cleanup logs
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GLIDE_ROOT="$(dirname "$SCRIPT_DIR")"
NUM_PODS=${NUM_PODS:-1}
DURATION=${DURATION:-300}
VERSION=${VERSION:-}
KILL_CLUSTER_AT=${KILL_CLUSTER_AT:-0}
CLEANUP=${CLEANUP:-true}

echo "=== Thread Hang Test ==="
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting test"
echo "Pods: $NUM_PODS"
echo "Duration: ${DURATION}s"
if [ "$KILL_CLUSTER_AT" -gt 0 ]; then
    echo "Kill cluster at: ${KILL_CLUSTER_AT}s"
fi
echo "Cleanup on exit: $CLEANUP"
echo ""

cleanup() {
    echo ""
    echo "=== Cleaning up ==="

    # Kill tracked Java processes by PID
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "Killing pod PID $pid"
            kill -9 "$pid" 2>/dev/null || true
        fi
    done

    # Fallback: kill any remaining DeadlockTest processes
    pkill -9 -f "DeadlockTest" || true

    if [ "$CLEANUP" != "true" ]; then
        echo "Skipping cluster cleanup (CLEANUP=$CLEANUP)"
        echo "Cluster still running for debugging"
        echo "To stop manually: python3 $GLIDE_ROOT/utils/cluster_manager.py stop --prefix cluster"
        return
    fi

    # Stop cluster
    if [ -f "$GLIDE_ROOT/utils/cluster_manager.py" ]; then
        echo "Stopping Valkey cluster..."
        python3 "$GLIDE_ROOT/utils/cluster_manager.py" stop --prefix cluster || true
    fi

    echo "Cleanup complete"
}

# Register cleanup on exit
trap cleanup EXIT INT TERM

# 1. Find Glide JAR
echo "=== Finding Glide JAR ==="

# Check jars directory first (from build-glide.sh)
JARS_DIR="$SCRIPT_DIR/jars"

if [ -n "$VERSION" ]; then
    # Specific version requested
    GLIDE_JAR="$JARS_DIR/glide-${VERSION}.jar"
    if [ ! -f "$GLIDE_JAR" ]; then
        echo "ERROR: Version $VERSION not found. Build it first:"
        echo "  ./build-glide.sh $VERSION"
        exit 1
    fi
    echo "Using version $VERSION: $GLIDE_JAR"
elif [ -d "$JARS_DIR" ] && [ -n "$(ls $JARS_DIR/*.jar 2>/dev/null)" ]; then
    # Use most recently modified JAR
    GLIDE_JAR=$(ls -t $JARS_DIR/*.jar 2>/dev/null | head -1)
    echo "Using most recent JAR: $GLIDE_JAR"
else
    # Fallback: check if already built in repo
    cd "$GLIDE_ROOT/java"
    GLIDE_JAR="$GLIDE_ROOT/java/$(ls client/build/libs/client-*.jar 2>/dev/null | grep -v sources | grep -v javadoc | head -1)"

    if [ ! -f "$GLIDE_JAR" ]; then
        echo "No JAR found. Build with:"
        echo "  ./build-glide.sh           # Build from repo"
        echo "  ./build-glide.sh 2.2.7     # Download version 2.2.7"
        exit 1
    fi
    echo "Using JAR from repo build: $GLIDE_JAR"
fi

cd "$GLIDE_ROOT"

# 2. Start Valkey cluster
echo ""
echo "=== Starting Valkey Cluster ==="
CLUSTER_OUTPUT=$(python3 utils/cluster_manager.py start --cluster-mode 2>&1)
echo "$CLUSTER_OUTPUT"

# Extract cluster addresses from output
CLUSTER_ADDRESSES=$(echo "$CLUSTER_OUTPUT" | grep "CLUSTER_NODES=" | cut -d'=' -f2 | head -3 | tr ',' '\n' | head -3 | tr '\n' ',' | sed 's/,$//')

if [ -z "$CLUSTER_ADDRESSES" ]; then
    echo "ERROR: Failed to get cluster addresses"
    exit 1
fi

echo "Cluster addresses: $CLUSTER_ADDRESSES"
sleep 5

# 3. Build classpath with protobuf
echo ""
echo "=== Building classpath ==="
PROTOBUF_JAR=$(find ~/.gradle/caches/modules-2/files-2.1/com.google.protobuf/protobuf-java -name "protobuf-java-*.jar" ! -name "*javadoc*" ! -name "*sources*" 2>/dev/null | head -1)
if [ -z "$PROTOBUF_JAR" ]; then
    echo "ERROR: protobuf-java not found in gradle cache"
    exit 1
fi

# Find commons-lang3 (needed for older versions like 2.1.1)
COMMONS_LANG3_JAR=$(find ~/.gradle/caches/modules-2/files-2.1/org.apache.commons/commons-lang3 -name "commons-lang3-*.jar" ! -name "*javadoc*" ! -name "*sources*" 2>/dev/null | head -1)
if [ -n "$COMMONS_LANG3_JAR" ]; then
    FULL_CP="$GLIDE_JAR:$PROTOBUF_JAR:$COMMONS_LANG3_JAR"
    echo "Using commons-lang3: $COMMONS_LANG3_JAR"
else
    FULL_CP="$GLIDE_JAR:$PROTOBUF_JAR"
fi
echo "Using protobuf: $PROTOBUF_JAR"

# 4. Compile Java app
echo ""
echo "=== Compiling DeadlockTest ==="
cd "$SCRIPT_DIR"
javac -cp "$FULL_CP:." DeadlockTest.java

# 5. Run multiple pods
echo ""
echo "=== Starting $NUM_PODS DEL pods ==="
PIDS=()
for i in $(seq 1 $NUM_PODS); do
    LOG_FILE="pod${i}.log"
    echo "Starting pod $i (log: $LOG_FILE)"

    java -Xmx64m \
         -XX:+HeapDumpOnOutOfMemoryError \
         -XX:HeapDumpPath="$SCRIPT_DIR/heap_pod${i}.hprof" \
         -Dcluster.addresses="$CLUSTER_ADDRESSES" \
         -cp "$FULL_CP:." \
         DeadlockTest > "$LOG_FILE" 2>&1 &

    PIDS+=($!)
    sleep 2
done

echo ""
echo "=== Pods running (PIDs: ${PIDS[@]}) ==="
echo "Monitoring for $DURATION seconds..."
echo "Watch logs: tail -f $SCRIPT_DIR/pod*.log"
echo ""

if [ "$KILL_CLUSTER_AT" -gt 0 ]; then
    echo "Will kill cluster at ${KILL_CLUSTER_AT}s to test error handling"
    echo ""
fi

# 5. Kill cluster at specified time (to test error handling)
if [ "$KILL_CLUSTER_AT" -gt 0 ]; then
    (
        sleep "$KILL_CLUSTER_AT"
        echo ""
        echo "=== Killing cluster at ${KILL_CLUSTER_AT}s ==="
        python3 "$GLIDE_ROOT/utils/cluster_manager.py" stop --prefix cluster || true
        echo "Cluster killed - monitoring error handling"
        echo ""
    ) &
    KILL_PID=$!
fi

# 6. Monitor for test completion
START_TIME=$(date +%s)
ABANDONED_DETECTED=false

while [ $(($(date +%s) - START_TIME)) -lt $DURATION ]; do
    ELAPSED=$(($(date +%s) - START_TIME))

    # Check if any pod died or detected abandoned callbacks
    for i in "${!PIDS[@]}"; do
        if ! kill -0 "${PIDS[$i]}" 2>/dev/null; then
            echo "Pod $((i+1)) (PID ${PIDS[$i]}) died"

            # Check for OOM or thread hang in logs
            if grep -q "OutOfMemoryError\|THREAD HANG DETECTED" "pod$((i+1)).log"; then
                echo "THREAD HANG DETECTED in pod $((i+1))"
                ABANDONED_DETECTED=true
                break 2
            fi
        fi
    done

    sleep 5
done

# Kill cluster killer if still running
if [ -n "$KILL_PID" ]; then
    kill "$KILL_PID" 2>/dev/null || true
fi

# 7. Results
echo ""
echo "=== Test Results ==="
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Test complete"
if [ "$ABANDONED_DETECTED" = true ]; then
    echo "❌ THREAD HANG DETECTED"
    echo ""
    echo "Check logs for details:"
    grep -H "THREAD HANG\|OutOfMemoryError\|Queue:" pod*.log | tail -20
    echo ""
    echo "Thread dump captured in log when hang first detected"
else
    echo "✅ No thread hangs detected in $DURATION seconds"
    echo ""
    echo "Final queue sizes:"
    grep -H "Queue:" pod*.log | tail -$NUM_PODS
fi

echo ""
echo "Full logs available in: $SCRIPT_DIR/pod*.log"
