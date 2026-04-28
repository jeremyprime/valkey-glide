# Resilience E2E Tests

Tests for client recovery under network partition and memory pressure. Both use Docker to simulate failure conditions and verify the client recovers.

## Tests

- **Network Partition** — pauses one node in a 3-node cluster via `docker pause` to simulate half-open TCP. Verifies healthy shards keep processing during the pause. Proves circuit breaker (#5861) + per-command timeout (#5581). ~2m.
- **Memory Pressure** — sustained 500 req/s load in a 1400MB container starves the Tokio runtime. Verifies timeouts fire and throughput doesn't collapse permanently. Proves watchdog (#5752) + per-command timeout (#5581) + Java inflight release (#5606). ~5m.

## Usage

```bash
# Run against current branch (builds GLIDE locally, clean build)
./run-test.sh network
./run-test.sh memory

# Run against a released version (no local build)
GLIDE_VERSION=2.2.9 ./run-test.sh network
GLIDE_VERSION=2.2.9 ./run-test.sh memory

# Fast iteration (skip clean, reuse existing build)
NO_CLEAN=1 ./run-test.sh memory

# Custom duration / load
TEST_DURATION=600 ./run-test.sh memory
RPS=1000 ./run-test.sh memory

# Watch live output
docker logs -f resilience-network-test
docker logs -f resilience-memory-test

# Stop test
docker compose --profile network down
docker compose --profile memory down
```
