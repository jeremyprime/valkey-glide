#!/bin/bash
#
# Build or download Glide client JAR for testing
#
# Usage:
#   ./build-glide.sh           # Build from current repo
#   ./build-glide.sh 2.2.7     # Download version 2.2.7 from Maven
#   ./build-glide.sh 2.1.1     # Download version 2.1.1 from Maven

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GLIDE_ROOT="$(dirname "$SCRIPT_DIR")"
JARS_DIR="$SCRIPT_DIR/jars"

VERSION="${1:-local}"

mkdir -p "$JARS_DIR"

if [ "$VERSION" = "local" ]; then
    echo "=== Building Glide from local repository ==="
    cd "$GLIDE_ROOT/java"
    ./gradlew clean :client:build -x test

    # Find the built JAR
    BUILT_JAR=$(ls client/build/libs/client-*.jar 2>/dev/null | grep -v sources | grep -v javadoc | head -1)
    if [ -z "$BUILT_JAR" ]; then
        echo "ERROR: No JAR found after build"
        exit 1
    fi

    # Copy to jars directory
    cp "$BUILT_JAR" "$JARS_DIR/glide-local.jar"
    echo "✅ Built and copied to: $JARS_DIR/glide-local.jar"

else
    echo "=== Downloading Glide $VERSION from Maven Central ==="

    # Detect platform
    OS=$(uname -s | tr '[:upper:]' '[:lower:]')
    ARCH=$(uname -m)
    if [ "$OS" = "linux" ] && [ "$ARCH" = "x86_64" ]; then
        CLASSIFIER="linux-x86_64"
    elif [ "$OS" = "darwin" ] && [ "$ARCH" = "arm64" ]; then
        CLASSIFIER="osx-aarch_64"
    elif [ "$OS" = "darwin" ] && [ "$ARCH" = "x86_64" ]; then
        CLASSIFIER="osx-x86_64"
    else
        echo "ERROR: Unsupported platform: $OS-$ARCH"
        exit 1
    fi

    MAVEN_URL="https://repo1.maven.org/maven2/io/valkey/valkey-glide/${VERSION}/valkey-glide-${VERSION}-${CLASSIFIER}.jar"

    OUTPUT_JAR="$JARS_DIR/glide-${VERSION}.jar"

    if [ -f "$OUTPUT_JAR" ]; then
        echo "JAR already exists: $OUTPUT_JAR"
        echo "Delete it to re-download"
    else
        echo "Downloading $CLASSIFIER JAR from: $MAVEN_URL"
        curl -f -L -o "$OUTPUT_JAR" "$MAVEN_URL" || {
            echo "ERROR: Failed to download version $VERSION for $CLASSIFIER"
            echo "Check if version exists at: https://repo1.maven.org/maven2/io/valkey/valkey-glide/${VERSION}/"
            exit 1
        }
        echo "✅ Downloaded to: $OUTPUT_JAR"
    fi
fi

echo ""
echo "Available JARs:"
ls -lh "$JARS_DIR"/*.jar 2>/dev/null || echo "No JARs found"
