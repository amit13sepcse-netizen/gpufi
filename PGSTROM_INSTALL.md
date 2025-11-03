# PostgreSQL with PG-Strom Installation Guide

This directory contains installation and runtime scripts for PostgreSQL with PG-Strom, a GPU acceleration extension that enables PostgreSQL to utilize NVIDIA GPUs for accelerated query processing.

## 📋 Prerequisites

- **NVIDIA GPU**: CUDA-capable GPU (Compute Capability 6.0 or higher recommended)
- **NVIDIA Driver**: Version 525.xx or later
- **Operating System**: Ubuntu 20.04+, Debian 11+, RHEL 8+, CentOS 8+, or Rocky Linux 8+
- **Root Access**: sudo privileges required for installation

## 🚀 Quick Start

### 1. Install PostgreSQL with PG-Strom

Run the installation script:

```bash
sudo bash install_postgresql_pgstrom.sh
```

This script will:
- Detect your OS and GPU
- Install CUDA Toolkit 12.3
- Install PostgreSQL 15
- Build and install PG-Strom from source
- Configure PostgreSQL to load PG-Strom
- Create a test database with PG-Strom enabled

### 2. Start/Run PostgreSQL

After installation, use the run script:

```bash
bash run_postgresql_pgstrom.sh
```

## 📝 Manual Installation Steps

If you prefer manual installation:

### Step 1: Install NVIDIA Drivers and CUDA

```bash
# Ubuntu/Debian
wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/cuda-keyring_1.1-1_all.deb
sudo dpkg -i cuda-keyring_1.1-1_all.deb
sudo apt-get update
sudo apt-get install -y cuda-toolkit-12-3
```

### Step 2: Install PostgreSQL

```bash
# Ubuntu/Debian
sudo apt-get install -y wget gnupg2
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" | sudo tee /etc/apt/sources.list.d/pgdg.list
sudo apt-get update
sudo apt-get install -y postgresql-15 postgresql-server-dev-15
```

### Step 3: Build PG-Strom

```bash
sudo apt-get install -y build-essential git cmake
cd /tmp
git clone https://github.com/heterodb/pg-strom.git
cd pg-strom
make PG_CONFIG=/usr/lib/postgresql/15/bin/pg_config
sudo make install PG_CONFIG=/usr/lib/postgresql/15/bin/pg_config
```

### Step 4: Configure PostgreSQL

Edit `/etc/postgresql/15/main/postgresql.conf`:

```ini
shared_preload_libraries = 'pg_strom'
max_worker_processes = 100
shared_buffers = 10GB
work_mem = 1GB
```

### Step 5: Restart PostgreSQL

```bash
sudo systemctl restart postgresql
```

### Step 6: Enable PG-Strom Extension

```bash
sudo -u postgres psql -c "CREATE DATABASE mydb;"
sudo -u postgres psql -d mydb -c "CREATE EXTENSION pg_strom;"
```

## 🔧 Configuration

### Verify GPU Detection

```sql
SELECT * FROM pgstrom.gpu_device_info();
```

### Check PG-Strom Status

```sql
SELECT * FROM pg_extension WHERE extname = 'pg_strom';
```

### PG-Strom License Info

```sql
SELECT pgstrom.license_info();
```

## 📊 Usage Examples

### Enable GPU Acceleration for a Query

```sql
-- Create a test table
CREATE TABLE test_data (
    id SERIAL PRIMARY KEY,
    value NUMERIC
);

-- Insert sample data
INSERT INTO test_data (value)
SELECT random() * 1000000
FROM generate_series(1, 10000000);

-- Run GPU-accelerated query
SET pg_strom.enabled = on;
SELECT SUM(value), AVG(value), COUNT(*)
FROM test_data
WHERE value > 500000;
```

### Check Query Execution Plan

```sql
EXPLAIN (ANALYZE, VERBOSE)
SELECT SUM(value), AVG(value)
FROM test_data
WHERE value > 500000;
```

Look for "Custom Scan (GpuScan)" or "Custom Scan (GpuJoin)" in the output.

## 🛠️ Troubleshooting

### GPU Not Detected

```bash
# Check NVIDIA driver
nvidia-smi

# Check CUDA installation
nvcc --version

# Check PostgreSQL logs
sudo tail -f /var/log/postgresql/postgresql-15-main.log
```

### PG-Strom Not Loading

```bash
# Verify shared_preload_libraries
sudo -u postgres psql -c "SHOW shared_preload_libraries;"

# Check for errors in PostgreSQL log
sudo journalctl -u postgresql -n 100
```

### Permission Issues

```bash
# Ensure postgres user can access GPU
sudo usermod -aG video postgres
sudo systemctl restart postgresql
```

## 📚 Additional Resources

- [PG-Strom Official Documentation](https://heterodb.github.io/pg-strom/)
- [PG-Strom GitHub Repository](https://github.com/heterodb/pg-strom)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [CUDA Toolkit Documentation](https://docs.nvidia.com/cuda/)

## 🔒 Security Notes

- Default PostgreSQL configuration allows local connections only
- Modify `pg_hba.conf` carefully for remote access
- Use strong passwords for database users
- Keep NVIDIA drivers and CUDA toolkit updated

## 📄 License

PG-Strom is licensed under PostgreSQL License (similar to BSD License).

## 🤝 Support

For issues related to:
- **PG-Strom**: Open an issue on [GitHub](https://github.com/heterodb/pg-strom/issues)
- **PostgreSQL**: Consult [PostgreSQL Community](https://www.postgresql.org/community/)
- **CUDA/GPU**: Check [NVIDIA Developer Forums](https://forums.developer.nvidia.com/)
