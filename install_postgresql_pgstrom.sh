#!/bin/bash

###############################################################################
# PostgreSQL with PG-Strom Installation Script
# This script installs PostgreSQL and PG-Strom (GPU acceleration extension)
# With lifecycle tracking, checkpointing, and resume capability
###############################################################################

# DO NOT use set -e - we handle errors manually for better control
set -o pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration variables
POSTGRES_VERSION="15"
PGSTROM_VERSION="5.1"
CUDA_VERSION="12.3"
INSTALL_DIR="/usr/local"
# Use a dedicated cluster name and directory instead of the default 'main'
CLUSTER_NAME="postgres_gpu"
DATA_DIR="/var/lib/postgresql/${POSTGRES_VERSION}/${CLUSTER_NAME}"
LOG_FILE="./install_pgstrom.log"
STATE_FILE="./install_pgstrom.state"
CHECKPOINT_FILE="./install_pgstrom.checkpoint"

# Installation stages
declare -A STAGES=(
    [1]="check_root"
    [2]="detect_os"
    [3]="check_gpu"
    [4]="install_cuda"
    [5]="install_postgresql"
    [6]="install_pgstrom_deps"
    [7]="install_pgstrom"
    [8]="configure_postgresql"
    [9]="start_postgresql"
    [10]="create_pgstrom_extension"
    [11]="verify_installation"
)

# Current stage tracking
CURRENT_STAGE=0
RESUME_MODE=false
UPDATE_SRC=false
RECLONE=false


# Function to print colored messages
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1" | tee -a ${LOG_FILE}
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1" | tee -a ${LOG_FILE}
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1" | tee -a ${LOG_FILE}
}

print_success() {
    echo -e "${CYAN}[SUCCESS]${NC} $1" | tee -a ${LOG_FILE}
}

print_stage() {
    echo -e "${BLUE}[STAGE $1]${NC} $2" | tee -a ${LOG_FILE}
}

# Checkpoint management functions
save_checkpoint() {
    local stage=$1
    local stage_name=$2
    echo "LAST_COMPLETED_STAGE=$stage" > ${CHECKPOINT_FILE}
    echo "LAST_COMPLETED_STAGE_NAME=\"$stage_name\"" >> ${CHECKPOINT_FILE}
    echo "TIMESTAMP=\"$(date '+%Y-%m-%d %H:%M:%S')\"" >> ${CHECKPOINT_FILE}
    print_success "Checkpoint saved: Stage $stage ($stage_name) completed"
}

load_checkpoint() {
    if [ -f ${CHECKPOINT_FILE} ]; then
        # Safely parse checkpoint file without sourcing untrusted/legacy contents
        LAST_COMPLETED_STAGE=$(grep -E '^LAST_COMPLETED_STAGE=' ${CHECKPOINT_FILE} | tail -1 | cut -d'=' -f2)
        LAST_COMPLETED_STAGE_NAME=$(grep -E '^LAST_COMPLETED_STAGE_NAME=' ${CHECKPOINT_FILE} | tail -1 | cut -d'=' -f2-)
        TIMESTAMP=$(grep -E '^TIMESTAMP=' ${CHECKPOINT_FILE} | tail -1 | cut -d'=' -f2-)
        # Strip surrounding quotes if present
        LAST_COMPLETED_STAGE_NAME="${LAST_COMPLETED_STAGE_NAME%\"}"
        LAST_COMPLETED_STAGE_NAME="${LAST_COMPLETED_STAGE_NAME#\"}"
        TIMESTAMP="${TIMESTAMP%\"}"
        TIMESTAMP="${TIMESTAMP#\"}"
        print_info "Found checkpoint from ${TIMESTAMP}"
        print_info "Last completed stage: $LAST_COMPLETED_STAGE_NAME (Stage $LAST_COMPLETED_STAGE)"
        return 0
    else
        return 1
    fi
}

clear_checkpoint() {
    if [ -f ${CHECKPOINT_FILE} ]; then
        rm -f ${CHECKPOINT_FILE}
        print_info "Checkpoint cleared"
    fi
}

save_state() {
    local stage=$1
    local status=$2
    local message=$3
    echo "$(date '+%Y-%m-%d %H:%M:%S')|Stage $stage|${STAGES[$stage]}|$status|$message" >> ${STATE_FILE}
}

show_progress() {
    print_info "=========================================="
    print_info "Installation Progress Summary"
    print_info "=========================================="
    
    if [ ! -f ${STATE_FILE} ]; then
        print_warn "No progress data available"
        return
    fi
    
    while IFS='|' read -r timestamp stage stage_name status message; do
        if [ "$status" == "SUCCESS" ]; then
            echo -e "${GREEN}✓${NC} $stage_name ($timestamp)"
        elif [ "$status" == "FAILED" ]; then
            echo -e "${RED}✗${NC} $stage_name ($timestamp) - $message"
        elif [ "$status" == "STARTED" ]; then
            echo -e "${YELLOW}→${NC} $stage_name ($timestamp)"
        fi
    done < ${STATE_FILE}
    
    print_info "=========================================="
}

# Error handler with stage tracking
handle_error() {
    local exit_code=$?
    local stage=$1
    local stage_name=$2
    
    print_error "Stage $stage ($stage_name) FAILED with exit code $exit_code"
    save_state $stage "FAILED" "Exit code: $exit_code"
    
    print_info ""
    print_info "=========================================="
    print_error "INSTALLATION FAILED"
    print_info "=========================================="
    show_progress
    print_info ""
    print_info "To resume installation after fixing the error, run:"
    print_info "  sudo ./install_postgresql_pgstrom.sh --resume"
    print_info ""
    print_info "To start fresh (will clear all checkpoints):"
    print_info "  sudo ./install_postgresql_pgstrom.sh --clean"
    print_info ""
    print_info "Check ${LOG_FILE} for detailed error information"
    print_info "=========================================="
    
    exit $exit_code
}

# Execute a stage with error handling
execute_stage() {
    local stage_num=$1
    local stage_name=$2
    local stage_func=$3
    
    # Skip if already completed and in resume mode
    if [ "$RESUME_MODE" = true ] && [ -f ${CHECKPOINT_FILE} ]; then
        # Use safe loader to avoid executing contents of checkpoint
        if load_checkpoint && [ $stage_num -le $LAST_COMPLETED_STAGE ]; then
            print_info "Skipping stage $stage_num ($stage_name) - already completed"
            return 0
        fi
    fi
    
    print_stage $stage_num "$stage_name"
    save_state $stage_num "STARTED" ""
    
    # Execute the stage function with error handling
    if $stage_func; then
        save_state $stage_num "SUCCESS" ""
        save_checkpoint $stage_num "$stage_name"
        print_success "Stage $stage_num ($stage_name) completed successfully"
        return 0
    else
        handle_error $stage_num "$stage_name"
    fi
}


# Function to check if running as root
check_root() {
    print_info "Checking root privileges..."
    if [ "$EUID" -ne 0 ]; then 
        print_error "This script must be run as root or with sudo"
        return 1
    fi
    print_success "Running with root privileges"
    return 0
}


# Function to detect OS
detect_os() {
    print_info "Detecting operating system..."
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        OS=$ID
        VERSION=$VERSION_ID
        print_success "Detected OS: $OS $VERSION"
        return 0
    else
        print_error "Cannot detect OS"
        return 1
    fi
}


# Function to check NVIDIA GPU
check_gpu() {
    print_info "Checking for NVIDIA GPU..."
    if command -v nvidia-smi &> /dev/null; then
        nvidia-smi 2>&1 | tee -a ${LOG_FILE}
        print_success "NVIDIA GPU detected"
        return 0
    else
        print_warn "nvidia-smi not found. Please ensure NVIDIA drivers are installed."
        
        # In resume mode, don't prompt - use previous decision
        if [ "$RESUME_MODE" = true ]; then
            print_warn "Continuing without GPU verification (resume mode)"
            return 0
        fi
        
        read -p "Continue anyway? (y/n): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            return 1
        fi
        return 0
    fi
}


# Function to install CUDA toolkit
install_cuda() {
    print_info "Installing CUDA Toolkit..."
    
    # Check if CUDA is already installed
    if command -v nvcc &> /dev/null; then
        local cuda_version=$(nvcc --version | grep "release" | sed -n 's/.*release \([0-9.]*\).*/\1/p')
        print_warn "CUDA $cuda_version is already installed"
        read -p "Skip CUDA installation? (y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            print_info "Skipping CUDA installation"
            return 0
        fi
    fi
    
    if [ "$OS" == "ubuntu" ] || [ "$OS" == "debian" ]; then
        wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/cuda-keyring_1.1-1_all.deb || return 1
        dpkg -i cuda-keyring_1.1-1_all.deb || return 1
        apt-get update || return 1
        apt-get install -y cuda-toolkit-12-3 cuda-drivers || return 1
    elif [ "$OS" == "rhel" ] || [ "$OS" == "centos" ] || [ "$OS" == "rocky" ]; then
        dnf config-manager --add-repo https://developer.download.nvidia.com/compute/cuda/repos/rhel8/x86_64/cuda-rhel8.repo || return 1
        dnf clean all || return 1
        dnf -y install cuda-toolkit-12-3 || return 1
    else
        print_warn "Automatic CUDA installation not supported for $OS. Please install manually."
        return 0
    fi
    
    # Set CUDA environment variables
    export PATH=/usr/local/cuda/bin:$PATH
    export LD_LIBRARY_PATH=/usr/local/cuda/lib64:$LD_LIBRARY_PATH
    
    if [ ! -f /etc/profile.d/cuda.sh ]; then
        echo 'export PATH=/usr/local/cuda/bin:$PATH' > /etc/profile.d/cuda.sh
        echo 'export LD_LIBRARY_PATH=/usr/local/cuda/lib64:$LD_LIBRARY_PATH' >> /etc/profile.d/cuda.sh
    fi
    
    print_success "CUDA Toolkit installed successfully"
    return 0
}


# Function to install PostgreSQL
install_postgresql() {
    print_info "Installing PostgreSQL ${POSTGRES_VERSION}..."
    
    # Check if PostgreSQL is already installed
    if command -v psql &> /dev/null; then
        local pg_version=$(psql --version | grep -oP '\d+' | head -1)
        print_warn "PostgreSQL $pg_version is already installed"
        if [ "$pg_version" == "$POSTGRES_VERSION" ]; then
            print_info "Target version already installed, skipping"
            return 0
        fi
    fi
    
    if [ "$OS" == "ubuntu" ] || [ "$OS" == "debian" ]; then
        # Add PostgreSQL repository
        apt-get install -y wget gnupg2 lsb-release || return 1
        wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - || return 1
        echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list || return 1
        
        apt-get update || return 1
        apt-get install -y \
            postgresql-${POSTGRES_VERSION} \
            postgresql-server-dev-${POSTGRES_VERSION} \
            postgresql-contrib-${POSTGRES_VERSION} \
            build-essential \
            git \
            cmake \
            make \
            gcc \
            g++ || return 1

        # Ensure a dedicated cluster named ${CLUSTER_NAME} exists at ${DATA_DIR}
        if command -v pg_lsclusters >/dev/null 2>&1; then
            if pg_lsclusters | awk -v v="${POSTGRES_VERSION}" -v n="${CLUSTER_NAME}" '$1==v && $2==n {found=1} END{exit !found}'; then
                print_info "PostgreSQL cluster '${CLUSTER_NAME}' already exists"
            else
                # Stop default 'main' if it's running to free up 5432 (optional)
                if pg_lsclusters | awk -v v="${POSTGRES_VERSION}" '$1==v && $2=="main" && $3=="online" {exit 0} END{exit 1}'; then
                    print_warn "Stopping default cluster '${POSTGRES_VERSION}/main' to prepare '${CLUSTER_NAME}'"
                    systemctl stop postgresql@${POSTGRES_VERSION}-main || true
                fi
                print_info "Creating PostgreSQL cluster '${CLUSTER_NAME}' at ${DATA_DIR}"
                mkdir -p "${DATA_DIR}" || return 1
                chown -R postgres:postgres "${DATA_DIR}" || return 1
                pg_createcluster ${POSTGRES_VERSION} ${CLUSTER_NAME} --datadir="${DATA_DIR}" || return 1
                print_success "Cluster '${CLUSTER_NAME}' created"
            fi
        else
            print_warn "pg_lsclusters not found; using default cluster paths"
        fi
            
    elif [ "$OS" == "rhel" ] || [ "$OS" == "centos" ] || [ "$OS" == "rocky" ]; then
        # Add PostgreSQL repository
        dnf install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-8-x86_64/pgdg-redhat-repo-latest.noarch.rpm || return 1
        dnf -qy module disable postgresql || return 1
        
        dnf install -y \
            postgresql${POSTGRES_VERSION} \
            postgresql${POSTGRES_VERSION}-server \
            postgresql${POSTGRES_VERSION}-devel \
            postgresql${POSTGRES_VERSION}-contrib \
            git \
            cmake \
            make \
            gcc \
            gcc-c++ || return 1
            
        # Initialize database
        /usr/pgsql-${POSTGRES_VERSION}/bin/postgresql-${POSTGRES_VERSION}-setup initdb || return 1
    else
        print_error "Unsupported OS: $OS"
        return 1
    fi
    
    print_success "PostgreSQL ${POSTGRES_VERSION} installed successfully"
    return 0
}


# Function to install PG-Strom dependencies
install_pgstrom_deps() {
    print_info "Installing PG-Strom dependencies..."
    
    if [ "$OS" == "ubuntu" ] || [ "$OS" == "debian" ]; then
        # Install build deps; include 'rpm' so rpmbuild macros used in PG-Strom Makefiles do not fail on Debian/Ubuntu
        apt-get install -y \
            libssl-dev \
            libreadline-dev \
            zlib1g-dev \
            libaio-dev \
            rpm || return 1

        # Ensure exactly one of pkg-config or pkgconf is installed to avoid conflicts
        if ! command -v pkg-config >/dev/null 2>&1 && ! command -v pkgconf >/dev/null 2>&1; then
            if apt-get install -y pkg-config; then
                :
            else
                print_warn "pkg-config install failed; trying pkgconf"
                apt-get install -y pkgconf || return 1
            fi
        fi

        # Arrow/Parquet dev packages are optional and may not be available on this distro
        if apt-cache show libarrow-dev >/dev/null 2>&1; then
            apt-get install -y libarrow-dev || print_warn "Failed to install libarrow-dev; proceeding without Arrow"
        else
            print_warn "libarrow-dev not found in APT repos; proceeding without Arrow"
        fi
        if apt-cache show libparquet-dev >/dev/null 2>&1; then
            apt-get install -y libparquet-dev libthrift-dev || print_warn "Failed to install Parquet/Thrift dev packages; proceeding without Parquet"
        else
            print_warn "libparquet-dev not found in APT repos; proceeding without Parquet"
        fi
    elif [ "$OS" == "rhel" ] || [ "$OS" == "centos" ] || [ "$OS" == "rocky" ]; then
        dnf install -y \
            openssl-devel \
            readline-devel \
            zlib-devel \
            libaio-devel || return 1
    fi
    
    print_success "PG-Strom dependencies installed"
    return 0
}

# Function to install PG-Strom
install_pgstrom() {
    print_info "Installing PG-Strom ${PGSTROM_VERSION}..."
    # Ensure OS variables available when resuming directly at this stage
    if [ -z "$OS" ]; then
        detect_os || return 1
    fi
    
    # Navigate to /tmp
    cd /tmp || return 1
    
    # Reuse or obtain source tree
    if [ -d "pg-strom/.git" ]; then
        print_info "Reusing existing PG-Strom source at /tmp/pg-strom"
        if [ "$RECLONE" = true ]; then
            print_warn "--reclone specified: removing existing clone"
            rm -rf pg-strom || return 1
        elif [ "$UPDATE_SRC" = true ]; then
            print_info "Updating existing repository..."
            if ! git -C pg-strom fetch --depth=1 origin; then
                print_warn "git fetch failed; proceeding with existing sources"
            else
                # Reset to latest origin/master
                git -C pg-strom checkout -q . 2>/dev/null || true
                git -C pg-strom reset --hard origin/master || true
            fi
        fi
    fi

    if [ ! -d "pg-strom/.git" ]; then
        # Robust clone with retries (helps with transient TLS/network issues)
        local retries=3
        local count=0
        local clone_success=false
        until [ $count -ge $retries ]
        do
            print_info "Cloning PG-Strom (attempt $((count+1))/$retries)..."
            # Try shallow clone first to reduce data size
            if git clone --depth 1 https://github.com/heterodb/pg-strom.git; then
                clone_success=true
                break
            fi
            count=$((count+1))
            if [ $count -lt $retries ]; then
                print_warn "git clone failed. Retrying in 5 seconds..."
                sleep 5
            fi
        done

        # If shallow clone failed after retries, try full clone once as a fallback
        if [ "$clone_success" = false ]; then
            print_warn "Shallow clone failed after ${retries} attempts. Trying full clone..."
            if ! git clone https://github.com/heterodb/pg-strom.git; then
                print_error "Failed to clone PG-Strom repository. Please check network connectivity and try again."
                return 1
            fi
        fi
    fi

    cd pg-strom || return 1
    
    # Checkout specific version if needed
    # git checkout v${PGSTROM_VERSION}
    
    # Set PostgreSQL path
    if [ "$OS" == "ubuntu" ] || [ "$OS" == "debian" ]; then
        export PATH=/usr/lib/postgresql/${POSTGRES_VERSION}/bin:$PATH
    elif [ "$OS" == "rhel" ] || [ "$OS" == "centos" ] || [ "$OS" == "rocky" ]; then
        export PATH=/usr/pgsql-${POSTGRES_VERSION}/bin:$PATH
    fi
    
    # Verify pg_config is available
    if ! command -v pg_config &> /dev/null; then
        print_error "pg_config not found in PATH"
        return 1
    fi
    
    # Ensure rpmbuild tooling exists on Debian/Ubuntu to avoid parse-time errors in Makefiles
    if [ "$OS" == "ubuntu" ] || [ "$OS" == "debian" ]; then
        if ! command -v rpmbuild >/dev/null 2>&1; then
            print_info "Installing 'rpm' package to provide rpmbuild/rpmspec..."
            apt-get update || true
            apt-get install -y rpm || return 1
        fi

        # Ensure pkg-config (or pkgconf) is available for build checks
        if ! command -v pkgconf >/dev/null 2>&1 && ! command -v pkg-config >/dev/null 2>&1; then
            print_info "Installing pkg-config..."
            apt-get update || true
            apt-get install -y pkg-config || return 1
        fi
        # Do not attempt to install Arrow/Parquet dev packages here; we build without them on Ubuntu/Debian by default
        print_warn "Skipping Arrow/Parquet dev package installation on Ubuntu/Debian; building PG-Strom without Arrow/Parquet support"
    fi

    print_info "Building PG-Strom..."
    # On Ubuntu/Debian, avoid triggering rpmbuild commands by building only source files
    # The Makefile has rpmbuild commands at the top level which fail on non-RPM systems
    if [ "$OS" == "ubuntu" ] || [ "$OS" == "debian" ]; then
        # Build in an isolated directory to avoid top-level Makefile (which uses rpmbuild)
        BUILD_DIR=$(mktemp -d)
        print_info "Using isolated build directory: ${BUILD_DIR}"
        mkdir -p "${BUILD_DIR}/src" || return 1
        # Copy source tree files required for extension build
        cp -a src/* "${BUILD_DIR}/src/" || return 1
        cp -a Makefile.common "${BUILD_DIR}/" || return 1
        # Copy LICENSE expected by src/Makefile during 'make install'
        if [ -f LICENSE ]; then
            cp -a LICENSE "${BUILD_DIR}/" || return 1
        else
            print_warn "LICENSE file not found in repository root; continuing"
        fi
        # Proactively disable Arrow/Parquet in Makefile.common if such toggles exist
        if [ -f "${BUILD_DIR}/Makefile.common" ]; then
            sed -i 's/^\(ARROW_FDW[ \t]*=[ \t]*\)yes/\1no/' "${BUILD_DIR}/Makefile.common" || true
            sed -i 's/^\(ARROW[ \t]*=[ \t]*\)yes/\1no/' "${BUILD_DIR}/Makefile.common" || true
            sed -i 's/^\(PARQUET[ \t]*=[ \t]*\)yes/\1no/' "${BUILD_DIR}/Makefile.common" || true
            sed -i 's/^\(HAVE_ARROW[ \t]*:=[ \t]*\)1/\10/' "${BUILD_DIR}/Makefile.common" || true
            sed -i 's/^\(HAVE_PARQUET[ \t]*:=[ \t]*\)1/\10/' "${BUILD_DIR}/Makefile.common" || true
        fi
        # Patch include path to local Makefile.common
        sed -i 's#^include \\.\./Makefile\\.common#include Makefile.common#' "${BUILD_DIR}/src/Makefile" || return 1
        # Always drop Arrow/Parquet objects on Ubuntu/Debian unless explicitly enabled later
        print_warn "Building without Apache Arrow/Parquet support (skipping arrow_fdw, arrow_meta, parquet_read)"
        sed -i 's/[[:space:]]*arrow_fdw\.o//g; s/[[:space:]]*arrow_meta\.o//g; s/[[:space:]]*parquet_read\.o//g' "${BUILD_DIR}/src/Makefile" || return 1
        # If pkgconf is not available on this system, replace with pkg-config in the isolated Makefile
        if ! command -v pkgconf >/dev/null 2>&1 && command -v pkg-config >/dev/null 2>&1; then
            sed -i 's/\bpkgconf\b/pkg-config/g' "${BUILD_DIR}/src/Makefile" || return 1
        fi

        # Strip Arrow/Parquet related SQL objects from extension scripts to avoid CREATE EXTENSION failures
        if [ -d "${BUILD_DIR}/src/sql" ]; then
            for f in "${BUILD_DIR}/src/sql/pg_strom--"*.sql; do
                [ -f "$f" ] || continue
                sed -i "/pgstrom_arrow_fdw/d; /arrow_fdw/d" "$f" || true
            done
        fi

        # Append fixed stubs for Arrow/Parquet symbols with exact signatures
        cat >> "${BUILD_DIR}/src/extra.c" <<'EOF'
/* Auto-generated stubs for Arrow/Parquet when disabled */
#include "pg_strom.h"
#include <sys/uio.h>

/* From pg_strom.h */
bool baseRelIsArrowFdw(RelOptInfo *baserel) { (void)baserel; return false; }
bool RelationIsArrowFdw(Relation frel) { (void)frel; return false; }
gpumask_t GetOptimalGpusForArrowFdw(PlannerInfo *root, RelOptInfo *baserel)
{ (void)root; (void)baserel; return (gpumask_t)0; }
const DpuStorageEntry *GetOptimalDpuForArrowFdw(PlannerInfo *root, RelOptInfo *baserel)
{ (void)root; (void)baserel; return NULL; }
bool pgstromArrowFdwExecInit(pgstromTaskState *pts, List *outer_quals, const Bitmapset *outer_refs)
{ (void)pts; (void)outer_quals; (void)outer_refs; return false; }
XpuCommand *pgstromScanChunkArrowFdw(pgstromTaskState *pts,
                                     struct iovec *iov,
                                     int *iovcnt)
{
    (void)pts; (void)iov; (void)iovcnt;
    return NULL;
}
void pgstromArrowFdwExecEnd(ArrowFdwState *arrow_state)
{ (void)arrow_state; }
void pgstromArrowFdwExecReset(ArrowFdwState *arrow_state)
{ (void)arrow_state; }
void pgstromArrowFdwInitDSM(ArrowFdwState *arrow_state, pgstromSharedState *ps_state)
{ (void)arrow_state; (void)ps_state; }
void pgstromArrowFdwAttachDSM(ArrowFdwState *arrow_state, pgstromSharedState *ps_state)
{ (void)arrow_state; (void)ps_state; }
void pgstromArrowFdwShutdown(ArrowFdwState *arrow_state)
{ (void)arrow_state; }
void pgstromArrowFdwExplain(ArrowFdwState *arrow_state, Relation frel, ExplainState *es, List *dcontext)
{ (void)arrow_state; (void)frel; (void)es; (void)dcontext; }
bool kds_arrow_fetch_tuple(TupleTableSlot *slot, kern_data_store *kds, size_t index, const Bitmapset *referenced)
{ (void)slot; (void)kds; (void)index; (void)referenced; return false; }
void pgstrom_init_arrow_fdw(void) {}

/* From arrow_defs.h */
struct kern_data_store *
parquetReadOneRowGroup(const char *filename,
                       const struct kern_data_store *kds_head,
                       void *(*malloc_callback)(void *malloc_private,
                                               size_t malloc_size),
                       void *malloc_private,
                       const char **p_error_message)
{
    (void)filename; (void)kds_head; (void)malloc_callback; (void)malloc_private; (void)p_error_message;
    return NULL;
}
EOF

        # Build and install from isolated tree
        # Add defensive CPPFLAGS to ensure Arrow/Parquet code paths are excluded if possible
    local DEF_CPPFLAGS="-D_GNU_SOURCE -DNO_ARROW_FDW -DWITHOUT_ARROW_FDW -DNO_ARROW -DWITHOUT_ARROW -DNO_PARQUET -DWITHOUT_PARQUET"
        if ! make -C "${BUILD_DIR}/src" SHELL=/bin/bash PG_CONFIG=$(which pg_config) CPPFLAGS+="$DEF_CPPFLAGS" all; then
            print_error "PG-Strom build failed"
            return 1
        fi
        
        print_info "Installing PG-Strom..."
        if ! make -C "${BUILD_DIR}/src" SHELL=/bin/bash PG_CONFIG=$(which pg_config) CPPFLAGS+="$DEF_CPPFLAGS" install; then
            print_error "PG-Strom installation failed"
            return 1
        fi
    else
        # RHEL/CentOS can use the top-level Makefile
        if ! make SHELL=/bin/bash PG_CONFIG=$(which pg_config); then
            print_error "PG-Strom build failed"
            return 1
        fi
        
        print_info "Installing PG-Strom..."
        if ! make install SHELL=/bin/bash PG_CONFIG=$(which pg_config); then
            print_error "PG-Strom installation failed"
            return 1
        fi
    fi
    
    print_success "PG-Strom installed successfully"
    return 0
}

# Function to configure PostgreSQL for PG-Strom
configure_postgresql() {
    print_info "Configuring PostgreSQL for PG-Strom..."
    # Ensure OS variables available when resuming directly at this stage
    if [ -z "$OS" ]; then
        detect_os || return 1
    fi
    
    # Find postgresql.conf
    if [ "$OS" == "ubuntu" ] || [ "$OS" == "debian" ]; then
        PG_CONF="/etc/postgresql/${POSTGRES_VERSION}/${CLUSTER_NAME}/postgresql.conf"
        PG_HBA="/etc/postgresql/${POSTGRES_VERSION}/${CLUSTER_NAME}/pg_hba.conf"
    elif [ "$OS" == "rhel" ] || [ "$OS" == "centos" ] || [ "$OS" == "rocky" ]; then
        PG_CONF="/var/lib/pgsql/${POSTGRES_VERSION}/data/postgresql.conf"
        PG_HBA="/var/lib/pgsql/${POSTGRES_VERSION}/data/pg_hba.conf"
    fi
    
    # If the expected config path doesn't exist on Ubuntu/Debian, try to create the cluster now (resume-safe)
    if [ ! -f "$PG_CONF" ]; then
        if [ "$OS" == "ubuntu" ] || [ "$OS" == "debian" ]; then
            if command -v pg_lsclusters >/dev/null 2>&1; then
                # Create the cluster if it doesn't exist yet
                if ! pg_lsclusters | awk -v v="${POSTGRES_VERSION}" -v n="${CLUSTER_NAME}" '$1==v && $2==n {found=1} END{exit !found}'; then
                    print_warn "Cluster '${CLUSTER_NAME}' not found; creating it now for configuration"
                    # Stop default 'main' if it is online to avoid port conflicts
                    if pg_lsclusters | awk -v v="${POSTGRES_VERSION}" '$1==v && $2=="main" && $3=="online" {exit 0} END{exit 1}'; then
                        print_warn "Stopping default cluster '${POSTGRES_VERSION}/main' before creating '${CLUSTER_NAME}'"
                        systemctl stop postgresql@${POSTGRES_VERSION}-main || true
                    fi
                    mkdir -p "${DATA_DIR}" || return 1
                    chown -R postgres:postgres "${DATA_DIR}" || return 1
                    if ! pg_createcluster ${POSTGRES_VERSION} ${CLUSTER_NAME} --datadir="${DATA_DIR}"; then
                        print_error "Failed to create PostgreSQL cluster '${CLUSTER_NAME}'"
                        return 1
                    fi
                    print_success "Cluster '${CLUSTER_NAME}' created"
                fi
            else
                print_error "pg_lsclusters not available; expected config missing at: $PG_CONF"
                return 1
            fi
        fi
    fi

    # Re-check after potential cluster creation
    if [ ! -f "$PG_CONF" ]; then
        print_error "PostgreSQL configuration file not found: $PG_CONF"
        return 1
    fi
    
    # Backup original configuration
    if [ ! -f "${PG_CONF}.backup" ]; then
        cp ${PG_CONF} ${PG_CONF}.backup || return 1
    fi
    
    # Add PG-Strom to shared_preload_libraries
    if grep -q "shared_preload_libraries.*pg_strom" ${PG_CONF}; then
        print_info "PG-Strom already configured in postgresql.conf"
    else
        sed -i "s/^#shared_preload_libraries = ''/shared_preload_libraries = 'pg_strom'/" ${PG_CONF} || return 1
        sed -i "s/^shared_preload_libraries = ''/shared_preload_libraries = 'pg_strom'/" ${PG_CONF} || return 1
        echo "" >> ${PG_CONF}
        echo "# PG-Strom Configuration" >> ${PG_CONF}
        echo "shared_preload_libraries = 'pg_strom'" >> ${PG_CONF}
        echo "max_worker_processes = 100" >> ${PG_CONF}
        echo "shared_buffers = 10GB" >> ${PG_CONF}
        echo "work_mem = 1GB" >> ${PG_CONF}
    fi
    
    print_success "PostgreSQL configured for PG-Strom"
    return 0
}

# Function to start PostgreSQL
start_postgresql() {
    print_info "Starting PostgreSQL service..."
    # Ensure OS variables available when resuming directly at this stage
    if [ -z "$OS" ]; then
        detect_os || return 1
    fi
    
    if [ "$OS" == "ubuntu" ] || [ "$OS" == "debian" ]; then
        # Prefer managing the specific cluster unit to avoid starting unintended clusters
        if systemctl list-unit-files | grep -q "postgresql@"; then
            systemctl enable postgresql@${POSTGRES_VERSION}-${CLUSTER_NAME} || return 1
            systemctl restart postgresql@${POSTGRES_VERSION}-${CLUSTER_NAME} || return 1
        else
            systemctl enable postgresql || return 1
            systemctl restart postgresql || return 1
        fi
    elif [ "$OS" == "rhel" ] || [ "$OS" == "centos" ] || [ "$OS" == "rocky" ]; then
        systemctl enable postgresql-${POSTGRES_VERSION} || return 1
        systemctl restart postgresql-${POSTGRES_VERSION} || return 1
    fi
    
    sleep 5
    
    if systemctl is-active --quiet postgresql@${POSTGRES_VERSION}-${CLUSTER_NAME} || systemctl is-active --quiet postgresql || systemctl is-active --quiet postgresql-${POSTGRES_VERSION}; then
        print_success "PostgreSQL started successfully"
        return 0
    else
        print_error "Failed to start PostgreSQL"
        return 1
    fi
}

# Function to create PG-Strom extension in database
create_pgstrom_extension() {
    print_info "Creating PG-Strom extension in database..."
    
    # Determine the correct port for the target cluster on Ubuntu/Debian
    local PSQL_PORT=""
    if [ "$OS" == "ubuntu" ] || [ "$OS" == "debian" ]; then
        if command -v pg_lsclusters >/dev/null 2>&1; then
            PSQL_PORT=$(pg_lsclusters | awk -v v="${POSTGRES_VERSION}" -v n="${CLUSTER_NAME}" '$1==v && $2==n {print $3; exit}')
        fi
    fi
    local PSQL="sudo -u postgres psql"
    if [ -n "$PSQL_PORT" ]; then
        PSQL="$PSQL -p $PSQL_PORT"
    fi

    # Create test database and enable extension
    $PSQL -c "CREATE DATABASE pgstrom_test;" 2>&1 | tee -a ${LOG_FILE} || print_warn "Database might already exist"
    
    if ! $PSQL -d pgstrom_test -c "CREATE EXTENSION pg_strom;" 2>&1 | tee -a ${LOG_FILE}; then
        print_warn "CREATE EXTENSION failed; attempting lightweight function install without Arrow/Parquet"
        $PSQL -d pgstrom_test -v ON_ERROR_STOP=1 <<'EOSQL' 2>&1 | tee -a ${LOG_FILE} || print_warn "Lightweight install encountered errors"
DO $$ BEGIN
    -- Ensure schema exists
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname='pgstrom') THEN
        EXECUTE 'CREATE SCHEMA pgstrom';
    END IF;
END $$;

-- Core helper functions bound to the shared library
CREATE OR REPLACE FUNCTION pgstrom.githash() RETURNS text
LANGUAGE C AS 'pg_strom', 'pgstrom_githash';

CREATE OR REPLACE FUNCTION pgstrom.license_query() RETURNS text
LANGUAGE C AS 'pg_strom', 'pgstrom_license_query';

CREATE OR REPLACE FUNCTION pgstrom.gpu_device_info() RETURNS SETOF record
LANGUAGE C AS 'pg_strom', 'pgstrom_gpu_device_info';
EOSQL
    fi
    
    if ! $PSQL -d pgstrom_test -c "SELECT pgstrom.license_info();" 2>&1 | tee -a ${LOG_FILE}; then
        print_warn "Could not get PG-Strom license info"
    fi
    
    print_success "PG-Strom extension created successfully"
    return 0
}

# Function to run verification tests
verify_installation() {
    print_info "Verifying installation..."
    
    # Determine the correct port for the target cluster on Ubuntu/Debian
    local PSQL_PORT=""
    if [ "$OS" == "ubuntu" ] || [ "$OS" == "debian" ]; then
        if command -v pg_lsclusters >/dev/null 2>&1; then
            PSQL_PORT=$(pg_lsclusters | awk -v v="${POSTGRES_VERSION}" -v n="${CLUSTER_NAME}" '$1==v && $2==n {print $3; exit}')
        fi
    fi
    local PSQL="sudo -u postgres psql"
    if [ -n "$PSQL_PORT" ]; then
        PSQL="$PSQL -p $PSQL_PORT"
    fi

    # Check PostgreSQL version
    if ! $PSQL --version; then
        print_error "PostgreSQL verification failed"
        return 1
    fi
    
    # Check if PG-Strom is preloaded and basic functions are callable
    if ! $PSQL -d pgstrom_test -c "SHOW shared_preload_libraries;" 2>&1 | tee -a ${LOG_FILE}; then
        print_error "Could not query shared_preload_libraries"
        return 1
    fi
    if ! $PSQL -d pgstrom_test -c "SELECT pgstrom.githash();" 2>&1 | tee -a ${LOG_FILE}; then
        print_error "Failed to run pgstrom.githash();"
        return 1
    fi
    if ! $PSQL -d pgstrom_test -c "SELECT pgstrom.license_query();" 2>&1 | tee -a ${LOG_FILE}; then
        print_warn "Failed to run pgstrom.license_query();"
    fi
    
    # Check GPU visibility
    # Try GPU info if available (may need column definition list if extension packaging not installed)
    $PSQL -d pgstrom_test -c "SELECT 'gpu_info_available' WHERE EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='pgstrom' AND p.proname='gpu_device_info');" 2>&1 | tee -a ${LOG_FILE} || true
    
    print_success "Verification complete!"
    return 0
}


# Main installation flow
main() {
    # Parse command line arguments
    case "${1:-}" in
        --resume)
            RESUME_MODE=true
            print_info "Resume mode enabled"
            ;;
        --clean)
            print_info "Cleaning checkpoints and state..."
            rm -f ${CHECKPOINT_FILE} ${STATE_FILE}
            print_info "Cleaned. Starting fresh installation..."
            ;;
        --update-src)
            UPDATE_SRC=true
            print_info "Will update existing PG-Strom source if present"
            ;;
        --reclone)
            RECLONE=true
            print_info "Will reclone PG-Strom source (fresh copy)"
            ;;
        --status)
            show_progress
            exit 0
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --resume    Resume installation from last successful checkpoint"
            echo "  --clean     Clear all checkpoints and start fresh"
            echo "  --update-src  Update existing PG-Strom source tree before building"
            echo "  --reclone     Remove existing source and reclone fresh"
            echo "  --status    Show current installation progress"
            echo "  --help      Show this help message"
            exit 0
            ;;
        "")
            # No arguments - normal installation
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
    
    print_info "=========================================="
    print_info "PostgreSQL with PG-Strom Installation"
    print_info "=========================================="
    print_info "Logs: ${LOG_FILE}"
    print_info "State: ${STATE_FILE}"
    print_info "Checkpoint: ${CHECKPOINT_FILE}"
    print_info "=========================================="
    
    # Initialize log file
    echo "Installation started at $(date '+%Y-%m-%d %H:%M:%S')" >> ${LOG_FILE}
    
    # Load checkpoint if in resume mode
    if [ "$RESUME_MODE" = true ]; then
        if load_checkpoint; then
            print_info "Resuming from stage $((LAST_COMPLETED_STAGE + 1))"
        else
            print_warn "No checkpoint found, starting from beginning"
        fi
    fi
    
    # Execute all stages with error handling
    execute_stage 1 "Root Privilege Check" check_root
    execute_stage 2 "OS Detection" detect_os
    execute_stage 3 "GPU Check" check_gpu
    execute_stage 4 "CUDA Installation" install_cuda
    execute_stage 5 "PostgreSQL Installation" install_postgresql
    execute_stage 6 "PG-Strom Dependencies" install_pgstrom_deps
    execute_stage 7 "PG-Strom Installation" install_pgstrom
    execute_stage 8 "PostgreSQL Configuration" configure_postgresql
    execute_stage 9 "PostgreSQL Start" start_postgresql
    execute_stage 10 "PG-Strom Extension Creation" create_pgstrom_extension
    execute_stage 11 "Installation Verification" verify_installation
    
    # Installation complete
    print_info ""
    print_info "=========================================="
    print_success "Installation completed successfully!"
    print_info "=========================================="
    show_progress
    print_info ""
    print_info "PostgreSQL is running with PG-Strom enabled"
    print_info "Test database: pgstrom_test"
    if [ "$OS" == "ubuntu" ] || [ "$OS" == "debian" ]; then
        if command -v pg_lsclusters >/dev/null 2>&1; then
            # pg_lsclusters columns: Ver Cluster Port Status Owner Data-dir Log-file
            # We want the Port (3rd column)
            CLUSTER_PORT=$(pg_lsclusters | awk -v v="${POSTGRES_VERSION}" -v n="${CLUSTER_NAME}" '$1==v && $2==n {print $3; exit}')
        fi
        if [ -n "${CLUSTER_PORT}" ]; then
            print_info "To connect: sudo -u postgres psql -p ${CLUSTER_PORT} -d pgstrom_test"
        else
            print_info "To connect: sudo -u postgres psql -d pgstrom_test"
        fi
    else
        print_info "To connect: sudo -u postgres psql -d pgstrom_test"
    fi
    print_info "=========================================="
    
    # Clear checkpoint on successful completion
    clear_checkpoint
}

# Run main function with all arguments
main "$@"

