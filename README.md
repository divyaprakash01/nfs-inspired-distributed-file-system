# NFS-Inspired Distributed File System

A distributed file system implemented in C, inspired by the architecture of NFS and collaborative document systems.

## Architecture

The system is composed of three components:

- **Name Server (NM):** Maintains file metadata, resolves file locations, enforces access control, handles replication logic, and maintains an LRU cache.
- **Storage Server (SS):** Stores file data, handles client file operations, manages concurrent writes with sentence-level locks, and synchronizes backups.
- **User Client (UC):** A command-line interface for file management, permissions, execution, checkpoints, and access requests.

## Features

- CREATE, DELETE, READ, WRITE, INFO, and STREAM operations
- File listing and metadata inspection
- Read/write access control
- Remote shell-script execution
- Undo support
- Hierarchical folders
- File checkpoints and rollback
- Access request and approval workflow
- Replication to a backup Storage Server
- Failover to a backup Storage Server when the primary server is unavailable
- LRU caching at the Name Server
- Thread-based concurrency with mutex protection
- Metadata persistence across Name Server restarts

## Build and Run

### Prerequisites

- GCC
- Linux/Unix environment
- POSIX threads

### Compile

```bash
gcc name_server.c -o nm -lpthread
gcc storage_server.c -o ss -lpthread
gcc client/user_client.c -o uc
```

### Run

Start the components in separate terminals:

```bash
./nm
./ss
./uc
```

A second Storage Server can be started for replication/failover testing after configuring its port.

## Project Structure

```text
.
├── name_server.c
├── storage_server.c
├── client/
│   └── user_client.c
└── README.md
```

## Course

Course Project — Operating Systems and Networks (OSN)

## Team

This was developed as a team course project.

Team members:
- Divya Prakash
- Jayesh Sutar
- Mohammed Faisal
- Divyansh Jain

