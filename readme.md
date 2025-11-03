# GPUFI - PostgreSQL PG-Strom GPU Acceleration Project

GPU-accelerated PostgreSQL performance benchmarking and monitoring utilities.

## System Specifications

### Hardware Configuration

**CPU:**
- Model: Intel Core i7-12700 (12th Gen)
- Architecture: x86_64
- Cores: 12 physical cores (20 threads with HT)
- Base/Turbo Frequency: 800 MHz - 4900 MHz

**GPU:**
- Model: NVIDIA GeForce RTX 3070 Ti
- VRAM: 8 GB GDDR6X
- CUDA Driver Version: 580.95.05

**Memory:**
- Total RAM: 128 GB
- Type: DDR4

**Motherboard:**
- Manufacturer: HP
- Model: 894B
- Version: 10

**Operating System:**
- Distribution: Ubuntu 22.04.5 LTS
- Kernel: 6.8.0-85-generic

### Software Stack

- **PostgreSQL:** With PG-Strom extension
- **CUDA Toolkit:** For GPU acceleration
- **Python 3:** For monitoring and benchmarking scripts

## Features

- PostgreSQL with PG-Strom GPU acceleration setup
- Automated benchmark scripts for GPU-accelerated queries
- Real-time GPU monitoring utilities
- Performance comparison between CPU and GPU execution
- HTML report generation for benchmark results

## Repository Contents

- `install_postgresql_pgstrom.sh` - PostgreSQL + PG-Strom installation script
- `benchmark_pgstrom.sh` - Comprehensive benchmarking suite
- `gpu_top.py` - Real-time GPU monitoring utility
- `run_postgresql_pgstrom.sh` - Quick start script for PostgreSQL
- `sql/` - SQL test queries and examples
- `data/` - Benchmark results and reports (HTML)
- `reports/` - Performance analysis reports

## Getting Started

See `PGSTROM_INSTALL.md` for installation instructions and `README_BENCHMARK.md` for benchmarking guidelines.