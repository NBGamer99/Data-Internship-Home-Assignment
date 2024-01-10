#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#
# Run airflow command in container
#

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

set -euo pipefail

function cleanup() {
    echo -e "\033[0;32m=============================== Cleaning up ===============================\033[0m"
    docker compose down --volumes --remove-orphans
    echo -e "\033[0;32m=============================== Done cleanup ===============================\033[0m"
    }

function initdb() {
    echo -e "\033[0;32m=============================== Init airflow  ===============================\033[0m"
    docker compose up airflow-init
    runairflow
    echo -e "\033[0;32m=============================== Done init airflow ===============================\033[0m"
}

function runairflow() {
    echo -e "\033[0;32m=============================== Running airflow ===============================\033[0m"
    docker compose up
    echo -e "\033[0;32m=============================== Done running airflow ===============================\033[0m"
}

function restart() {
    echo -e "\033[0;32m=============================== Restarting All Services ===============================\033[0m"
    docker compose down
    docker compose up
    echo -e "\033[0;32m=============================== Done Restarting All Services ===============================\033[0m"
}

function rebuild() {
    echo -e "\033[0;32m=============================== Rebuilding services ===============================\033[0m"
    docker compose down
    initdb
    echo -e "\033[0;32m=============================== Done rebuilding services ===============================\033[0m"
}

# check is there a docker-compose command, if not, use "docker compose" instead.
if [ -x "$(command -v docker-compose)" ]; then
    dc=docker-compose
else
    dc="docker compose"
fi

export COMPOSE_FILE="${PROJECT_DIR}/docker-compose.yaml"
if [ $# -gt 0 ]; then
    if [ "$1" == "cleanup" ]; then
        cleanup
    elif [ "$1" == "initdb" ]; then
        initdb
    elif [ "$1" == "rebuild" ]; then
        rebuild
    elif [ "$1" == "runairflow" ]; then
        runairflow
    elif [ "$1" == "restart" ]; then
        restart
    else
        exec $dc run --rm airflow-cli "${@}"
    fi
else
    exec $dc run --rm airflow-cli
fi