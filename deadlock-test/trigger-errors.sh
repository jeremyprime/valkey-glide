#!/bin/bash
#
# Test different error scenarios in GLIDE
#

echo "=== Error Path Testing ==="
echo ""

# Auto-detect cluster ports from running Valkey processes
PORTS=($(ps aux | grep "valkey-server.*cluster" | grep -v grep | sed -n 's/.*:\([0-9]\{4,5\}\).*/\1/p' | sort -u))

if [ ${#PORTS[@]} -eq 0 ]; then
    echo "ERROR: No running Valkey cluster found"
    echo "Start cluster first with: cd .. && python3 utils/cluster_manager.py start --cluster-mode"
    exit 1
fi

echo "Detected cluster ports: ${PORTS[@]}"
echo ""

case "${1:-help}" in
    "network-partition")
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Test 1: Network partition (iptables OUTPUT DROP - half-open TCP)"
        echo "Triggers: Pipeline channel fill, Tokio starvation"

        # Save rule info for safe restoration
        RULE_FILE="/tmp/glide-test-iptables-rules.txt"
        > "$RULE_FILE"  # Clear file

        for port in "${PORTS[@]}"; do
            sudo iptables -A OUTPUT -p tcp --dport $port -j DROP
            echo "OUTPUT:$port" >> "$RULE_FILE"
            echo "  Blocked outgoing to port $port"
        done
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Applied. Remove with: $0 restore"
        echo "Rule info saved to: $RULE_FILE"
        ;;

    "packet-loss")
        echo "Test 2: Random packet loss (20%)"
        echo "Triggers: Intermittent connection errors"
        for port in "${PORTS[@]}"; do
            sudo iptables -A INPUT -p tcp --dport $port -m statistic --mode random --probability 0.2 -j DROP
            echo "  20% loss on port $port"
        done
        echo "Applied. Remove with: $0 restore"
        ;;

    "kill-node")
        echo "Test 3: Kill one cluster node"
        echo "Triggers: MOVED redirects, topology changes"
        FIRST_PORT=${PORTS[0]}
        echo "Killing node on port $FIRST_PORT..."
        pkill -f "valkey-server.*$FIRST_PORT" || true
        echo "Killed. Restart cluster with: python3 ../utils/cluster_manager.py start --cluster-mode"
        ;;

    "restore")
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Restoring normal operation..."

        RULE_FILE="/tmp/glide-test-iptables-rules.txt"

        if [ -f "$RULE_FILE" ]; then
            echo "Removing rules from: $RULE_FILE"
            while IFS=: read -r chain port; do
                if [ "$chain" = "OUTPUT" ]; then
                    sudo iptables -D OUTPUT -p tcp --dport $port -j DROP 2>/dev/null && echo "  Removed OUTPUT rule for port $port" || true
                elif [ "$chain" = "INPUT" ]; then
                    sudo iptables -D INPUT -p tcp --dport $port -j DROP 2>/dev/null && echo "  Removed INPUT rule for port $port" || true
                fi
            done < "$RULE_FILE"
            rm -f "$RULE_FILE"
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] Rules restored and cleaned up"
        else
            echo "No rule file found, attempting auto-detect..."
            if [ ${#PORTS[@]} -eq 0 ]; then
                echo "ERROR: No cluster ports detected, cannot restore safely"
                echo "Manually remove rules with: sudo iptables -L OUTPUT -n --line-numbers"
                exit 1
            fi

            echo "Removing rules for cluster ports: ${PORTS[@]}"
            for port in "${PORTS[@]}"; do
                sudo iptables -D OUTPUT -p tcp --dport $port -j DROP 2>/dev/null || true
                sudo iptables -D INPUT -p tcp --dport $port -j DROP 2>/dev/null || true
                sudo iptables -D INPUT -p tcp --dport $port -m statistic --mode random --probability 0.2 -j DROP 2>/dev/null || true
                echo "  Restored port $port"
            done
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] Cluster ports restored"
        fi
        ;;

    *)
        echo "Usage: $0 <test-type>"
        echo ""
        echo "Test types:"
        echo "  network-partition  - Block all cluster traffic"
        echo "  packet-loss        - Random 20% packet loss"
        echo "  kill-node          - Kill one node (triggers MOVED)"
        echo "  restore            - Remove all iptables rules"
        echo ""
        echo "Example:"
        echo "  Terminal 1: ./run-test.sh"
        echo "  Terminal 2: sleep 30 && ./trigger-errors.sh network-partition"
        echo "  Terminal 2: sleep 60 && ./trigger-errors.sh restore"
        ;;
esac
