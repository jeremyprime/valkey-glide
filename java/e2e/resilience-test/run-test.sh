#!/bin/bash
# Run resilience e2e tests. See README.md for full usage.
# Usage: ./run-test.sh [network|memory]
set -e

TEST=${1:-network}
export TEST_DURATION=${TEST_DURATION:-600}
SCRIPT_TIMEOUT=$(( TEST_DURATION + 300 ))  # test duration + 5 min buffer for build/startup
export GLIDE_VERSION=${GLIDE_VERSION:-255.255.255}

# Always remove stale Docker image unless NO_CLEAN is set
if [ -z "$NO_CLEAN" ]; then
    docker rmi "resilience-test-${TEST}-test" 2>/dev/null || true
fi

if [ "$GLIDE_VERSION" = "255.255.255" ]; then
    if [ -z "$NO_CLEAN" ]; then
        echo "Cleaning Rust and Java builds..."
        cd ../../..
        cargo clean --manifest-path glide-core/Cargo.toml 2>/dev/null || true
        cd java && ./gradlew clean -q && cd e2e/resilience-test
    fi
    echo "Building and publishing valkey-glide locally..."
    cd ../.. && ./gradlew publishToMavenLocal -x test -x javadoc
    cd e2e/resilience-test
fi

echo "Building test JAR..."
../../gradlew shadowJar

echo "Starting test environment..."
docker compose --profile "$TEST" up -d --build

CONTAINER="resilience-${TEST}-test"
echo "Waiting for test to complete (live output)..."
if timeout "${SCRIPT_TIMEOUT}s" bash -c "docker logs -f $CONTAINER 2>&1 | while IFS= read -r line; do
    echo \"\$line\"
    if echo \"\$line\" | grep -q 'TEST PASSED\|TEST FAILED'; then
        exit 0
    fi
done"; then
    echo "Test completed"
else
    echo "Test timed out"
fi

echo ""
if docker logs "$CONTAINER" 2>&1 | grep -q "TEST PASSED"; then
    echo "Test Passed!"
    EXIT_CODE=0
else
    echo "Test Failed!"
    EXIT_CODE=1
fi

echo "Cleaning up..."
docker compose --profile "$TEST" down -v
exit $EXIT_CODE
