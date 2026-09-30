# NFS-Inspired Distributed File System

**Docs++** is a simplified distributed file system implemented in C, inspired by **NFS (Network File System)** and collaborative document systems.

The system enables multiple clients to access and manage files distributed across multiple storage servers through a central Name Server, with support for concurrency, replication, fault tolerance, access control, and hierarchical file organization.

## Architecture

The system consists of three main components:

* **Name Server (NM):** Maintains file metadata, resolves file locations, manages access control and replication, and uses an LRU cache for fast path lookup.
* **Storage Server (SS):** Stores file data, serves client read/write/stream operations, synchronizes backups, and manages concurrent access using sentence-level locking.
* **User Client (UC):** Provides a command-line interface for file operations, permissions, execution, folder management, checkpoints, and access requests.

## Key Features

* File creation, deletion, reading, writing, metadata inspection, and streaming
* Hierarchical folders and file movement
* Sentence-level locking for concurrent writes
* Undo, checkpoints, and rollback
* Read/write access control with access-request workflow
* Automatic file replication to a backup Storage Server
* Asynchronous backup synchronization and read failover
* LRU caching for faster path resolution
* Persistent Name Server metadata
* Cache invalidation after state and permission changes

## Build & Run

### Prerequisites

* GCC
* Linux/Unix environment
* POSIX threads (`pthread`)
* TCP sockets

### Compile

```bash
gcc name_server.c -o nm -lpthread
gcc storage_server.c -o ss -lpthread
gcc user_client.c -o uc
```

For fault-tolerance testing, configure a second Storage Server to use a backup port such as `9091` and compile it separately.

### Run

Start the components in separate terminals:

```bash
./nm
./ss
./ss_backup
./uc
```

Run one or more clients as required.

## Example Commands

```text
CREATE <filename>
READ <filename>
WRITE <filename> <sentence_idx>
DELETE <filename>
INFO <filename>
STREAM <filename>
UNDO <filename>

CREATEFOLDER <name>
MOVE <file> <folder>
VIEWFOLDER <folder>

CHECKPOINT <file> <tag>
VIEWCHECKPOINT <file> <tag>
REVERT <file> <tag>

ADDACCESS -R/-W <file> <user>
REMACCESS <file> <user>
REQACCESS <file> -R/-W
VIEWREQ
APPROVEREQ <file> <user>
REJECTREQ <file> <user>
```

## Fault-Tolerance Test

1. Start the Name Server, primary Storage Server, backup Storage Server, and client.
2. Create and modify a file so that it is replicated.
3. Stop the primary Storage Server.
4. Issue a `READ` request from the client.
5. The Name Server redirects the request to the backup Storage Server.

## Course Project

**Operating Systems and Networks (OSN)**

## Team

* **Divya Prakash**
* **Shashi**

> Individual contributions can be documented separately.
