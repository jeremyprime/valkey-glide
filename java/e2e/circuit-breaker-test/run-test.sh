#!/bin/bash
# Run circuit breaker e2e test
set -e

echo "Building and publishing valkey-glide locally..."
cd ../.. && ./gradlew publishToMavenLocal
cd e2e/circuit-breaker-test

echo "Building test JAR..."
../../gradlew shadowJar

echo "Starting test environment..."
docker compose up -d --build

echo "Waiting for test to complete..."
if timeout 90s bash -c 'while true; do
    LOGS=$(docker compose logs test-runner 2>&1)
    if echo "$LOGS" | grep -q "Circuit Breaker Test PASSED\|Circuit Breaker Test FAILED"; then
        break
    fi
    sleep 3
done'; then
    echo "Test completed"
else
    echo "Test timed out"
fi

echo "Test output:"
docker compose logs test-runner

if docker compose logs test-runner | grep -q "Circuit Breaker Test PASSED"; then
    echo "PASSED"
    EXIT_CODE=0
else
    echo "FAILED"
    EXIT_CODE=1
fi

docker compose down -v
exit $EXIT_CODE
