#!/usr/bin/env bash
# Entrypoint for the ROS2 image (live-compiler). Same pattern as RED's
# backend/entrypoint.sh: when DISCOVERY_SERVER_IP is set, join that Fast DDS
# discovery server (how the car's DAQ is reached); otherwise fall back to
# ROS2's default multicast discovery.
#
#   DISCOVERY_SERVER_IP    IP or hostname of the discovery server (hostnames
#                          are resolved here: Fast DDS's XML wants an IP)
#   DISCOVERY_SERVER_PORT  default 11811
#   ROS_DOMAIN_ID          set by compose (the car uses 14)
set -eo pipefail

# setup.bash reads unset variables, so -u only after it is sourced.
source /opt/ros/humble/setup.bash
set -u

# rclpy writes logs under $ROS_HOME (default ~/.ros). Running as a non-root user (e.g. so
# data files are not root-owned) leaves HOME=/ unwritable and rclpy.init fails with
# "Failed to create log directory": fall back to /tmp.
if [[ -z "${ROS_LOG_DIR:-}" ]] && ! mkdir -p "${ROS_HOME:-${HOME:-/}/.ros}/log" 2>/dev/null; then
    export ROS_HOME=/tmp/ros
    export ROS_LOG_DIR=/tmp/ros/log
    mkdir -p "$ROS_LOG_DIR"
fi

if [[ -n "${DISCOVERY_SERVER_IP:-}" ]]; then
    port="${DISCOVERY_SERVER_PORT:-11811}"
    ip="$DISCOVERY_SERVER_IP"
    if [[ ! "$ip" =~ ^[0-9.]+$ ]]; then
        resolved="$(getent ahostsv4 "$ip" | awk 'NR==1{print $1}' || true)"
        if [[ -z "$resolved" ]]; then
            echo "[ros2-entrypoint] cannot resolve discovery server '$ip'" >&2
            exit 1
        fi
        ip="$resolved"
    fi
    profile=/tmp/fastdds_super_client.xml
    sed -e "s/DISCOVERY_SERVER_IP/${ip}/" -e "s/DISCOVERY_SERVER_PORT/${port}/" \
        "$(dirname "$0")/fastdds_super_client.xml" > "$profile"
    export FASTRTPS_DEFAULT_PROFILES_FILE="$profile"
    export ROS_DISCOVERY_SERVER="${ip}:${port}"
    echo "[ros2-entrypoint] discovery server ${ip}:${port}, domain ${ROS_DOMAIN_ID:-0}"
else
    echo "[ros2-entrypoint] no DISCOVERY_SERVER_IP: multicast discovery, domain ${ROS_DOMAIN_ID:-0}"
fi

exec "$@"
