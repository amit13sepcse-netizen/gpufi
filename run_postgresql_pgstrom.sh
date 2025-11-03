#!/bin/bash

###############################################################################
# Quick Start Script for PostgreSQL with PG-Strom
# This script runs PostgreSQL with PG-Strom after installation
###############################################################################

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Detect OS and PostgreSQL version
detect_postgres() {
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        OS=$ID
    fi
    
    # Find PostgreSQL version
    if [ "$OS" == "ubuntu" ] || [ "$OS" == "debian" ]; then
        POSTGRES_VERSION=$(ls /etc/postgresql/ | sort -V | tail -1)
        SERVICE_NAME="postgresql"
    else
        POSTGRES_VERSION=$(ls /usr/pgsql-*/bin/postgres 2>/dev/null | grep -oP '\d+' | head -1)
        SERVICE_NAME="postgresql-${POSTGRES_VERSION}"
    fi
}

# Start PostgreSQL service
start_service() {
    print_info "Starting PostgreSQL service..."
    sudo systemctl start ${SERVICE_NAME}
    sudo systemctl enable ${SERVICE_NAME}
    
    if systemctl is-active --quiet ${SERVICE_NAME}; then
        print_info "PostgreSQL is running"
    else
        print_warn "PostgreSQL may not be running. Check status with: systemctl status ${SERVICE_NAME}"
    fi
}

# Display connection info
show_info() {
    print_info "=========================================="
    print_info "PostgreSQL with PG-Strom"
    print_info "=========================================="
    echo ""
    print_info "Connect to PostgreSQL:"
    echo "  sudo -u postgres psql"
    echo ""
    print_info "Connect to test database:"
    echo "  sudo -u postgres psql -d pgstrom_test"
    echo ""
    print_info "Check PG-Strom status:"
    echo "  sudo -u postgres psql -d pgstrom_test -c \"SELECT * FROM pgstrom.gpu_device_info();\""
    echo ""
    print_info "Create PG-Strom extension in a database:"
    echo "  CREATE EXTENSION pg_strom;"
    echo ""
    print_info "Check service status:"
    echo "  systemctl status ${SERVICE_NAME}"
    print_info "=========================================="
}

# Main
detect_postgres
start_service
show_info
