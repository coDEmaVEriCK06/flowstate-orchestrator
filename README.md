
```markdown
# Flow-State Orchestrator

A distributed DAG (Directed Acyclic Graph) task scheduler and orchestrator built
entirely in C using POSIX OS primitives. Inspired by the architecture of Apache
Airflow and Kubernetes job schedulers, Flow-State executes complex multi-step
computing jobs where tasks are dispatched to remote worker processes in a strict
dependency order enforced by graph mathematics.

Built as a systems programming project demonstrating mastery of every major OS
concept: process management, IPC, concurrency, file locking, and network programming.

---

## Architecture

Flow-State is a two-binary distributed system.

### Master (Orchestrator)
The master daemon is the brain of the system. It parses a DAG definition file,
maintains the complete task state machine, and coordinates execution across all
connected workers. Internally it runs five concurrent threads:

- **Reactor thread** (main): A `select()` loop monitoring all TCP connections
  simultaneously. Handles new worker connections, incoming messages, and
  disconnections without blocking.
- **Dispatcher threads** (x2): Pull READY tasks from the DAG queue and dispatch
  them to idle workers over persistent TCP sockets.
- **Result processor thread**: Consumes completed task results, advances the DAG
  state machine, triggers child notifications, and handles retry/failure logic.
- **Logger thread**: Drains a System V message queue and writes formatted entries
  to `logs/execution.log` under dual locking (System V semaphore + `fcntl()`).

### Worker (Execution Node)
The worker daemon connects to the master, authenticates with a shared token, and
enters a dispatch loop. For each task received it:
1. Sends `MSG_ACK` immediately to confirm receipt
2. Calls `fork()` + `execve()` to run the shell command in a child process
3. Captures all stdout and stderr via unnamed pipes and `dup2()`
4. Enforces a hard timeout using `setitimer()` + `SIGALRM` + `killpg()`
5. Sends `MSG_RESULT` with the exit code and captured output

Workers reconnect automatically with exponential backoff if the master restarts.

### Wire Protocol
All TCP communication uses a custom TLV (Type-Length-Value) framing layer that
correctly handles partial reads — a fundamental TCP stream property. Every message
begins with an 8-byte header `[4-byte type][4-byte length]` followed by a
fixed-size payload. All multi-byte integers use network byte order via `htonl`
and `ntohl`.

### Task State Machine
Every task transitions through a strict state machine enforced by a single
`task_transition()` function. Invalid transitions are rejected at the code level.

~~~
PENDING → READY → DISPATCHED → RUNNING → COMPLETED (terminal)
                                       ↘ FAILED → READY (retry)
                                                 → DEAD (terminal)
                                                     → CASCADE_FAILED 
(descendants)
~~~

---

## OS Concepts Demonstrated

| Concept | Implementation | Lab Exercise |
|---|---|---|
| `fork()` / `exec()` | Worker spawns child process per task | Ex. 20, 25, 26 |
| Unnamed pipes | Capture child stdout and stderr | Ex. 31, 34 |
| `dup2()` | Redirect child I/O to pipe | Ex. 34 |
| `waitpid()` | Reap child, extract exit code | Ex. 24 |
| `setpgid()` / `killpg()` | Kill entire subprocess tree on timeout | Ex. 63, 64 |
| `sigaction()` / `SIGALRM` | Enforce task execution timeout | Ex. 53, 59, 61 |
| `select()` | Reactor loop and worker dispatch wait | Ex. 13, 51 |
| TCP sockets | Master-worker persistent connections | Ex. 51, 52 |
| `pthread_mutex_t` | Per-node DAG locks, per-worker write locks | Ex. 58 |
| `pthread_cond_t` | Blocking ready queue and idle worker queue | Ex. 58 |
| System V semaphores | Protect execution.log across threads | Ex. 48, 49 |
| System V message queues | Logger pipeline with chunked msgsnd | Ex. 41, 43, 44 |
| `fcntl()` F_SETLKW | Advisory write lock on log file | Ex. 16, 17, 18 |
| `signal()` / `SIGPIPE` | Suppress broken pipe on socket write | Ex. 59, 60 |

---

## Project Structure

```
flowstate-orchestrator/
├── Makefile                    # Builds master and worker as separate binaries
├── include/
│   ├── graph.h                 # DAG structs, TaskState enum, state machine API
│   ├── protocol.h              # TLV wire protocol, payload structs, framing API
│   ├── ipc_sync.h              # System V semaphore and MQ wrappers, LoggerArgs
│   ├── network.h               # WorkerConn, MasterState, dispatcher/reactor API
│   └── worker.h                # ExecResult struct, executor_run() declaration
├── src/
│   ├── master/
│   │   ├── main.c              # Startup, signal handling, thread orchestration
│   │   ├── server.c            # select() reactor, accept(), TLV deframing
│   │   └── scheduler.c         # Dispatcher pool, result processor, queue ops
│   ├── worker/
│   │   ├── main.c              # Entry point, argument parsing
│   │   ├── client.c            # TCP connect, handshake, dispatch loop, reconnect
│   │   └── executor.c          # fork/exec/pipe/dup2/SIGALRM/killpg
│   └── common/
│       ├── graph_utils.c       # DAG parser, Kahn's algorithm, state machine
│       ├── logger.c            # Logger thread, chunk reassembly, fcntl locking
│       ├── ipc_sync.c          # semget/semop/semctl, msgget/msgsnd/msgrcv
│       └── protocol.c          # pack/parse for all 5 message types, deframing
├── dags/
│   ├── sample.dag              # Diamond dependency pattern (4 tasks)
│   └── linear.dag              # Linear chain pattern (4 tasks)
├── jobs/
│   └── dummy.sh                # Simulated workload script
├── tests/
│   ├── test_dag.c              # 7 tests: parser, edges, Kahn's, state machine
│   ├── test_protocol.c         # 7 tests: pack/parse, partial reads, deframing
│   ├── test_executor.c         # 6 tests: exec, exit codes, stderr, timeout
│   └── test_ipc.c              # 5 tests: semaphore, MQ chunking, logger thread
└── logs/
    └── execution.log           # Written at runtime, not committed to git
```

---

## Build

Requirements: GCC, GNU Make, Linux (uses POSIX and System V IPC APIs)

```bash
# Build both binaries
make all

# Build and run all test suites
make tests
./test_dag && ./test_protocol && ./test_executor && ./test_ipc

# Clean all build artifacts
make clean
```

---

## Usage

### Starting the Master

```bash
./master <dag_file> <port> <auth_token>

# Example
./master dags/sample.dag 8080 mysecret
```

### Starting Workers

Open a new terminal for each worker. All workers connect to the same master.

```bash
./worker <master_ip> <port> <auth_token>

# Example
./worker 127.0.0.1 8080 mysecret
```

Multiple workers can be started simultaneously. The master distributes tasks
across all idle workers automatically.

### Viewing Execution Logs

```bash
cat logs/execution.log
```

### Monitoring IPC Objects While Running

```bash
ipcs
```

You will see the Flow-State semaphore (key `0x464c4f57`) and message queue
(key `0x464c4f58`) listed. Both are automatically removed on clean shutdown.

### Stopping

Press `Ctrl+C` in the master terminal. The master performs a graceful shutdown:
stops all dispatcher threads, flushes the result processor, drains the logger
queue, destroys all IPC objects, and exits cleanly.

---

## DAG File Format

```
# Comments begin with #
# Every task line must have all four fields in this exact order

TASK id=<int> cmd="<shell command>" deps=<none|id,id,...> retries=<int>
```

Example — diamond dependency pattern:

```
# Task 4 cannot run until both Task 2 and Task 3 complete
TASK id=1 cmd="jobs/dummy.sh 1" deps=none retries=1
TASK id=2 cmd="jobs/dummy.sh 2" deps=1    retries=1
TASK id=3 cmd="jobs/dummy.sh 3" deps=1    retries=1
TASK id=4 cmd="jobs/dummy.sh 4" deps=2,3  retries=2
```

Rules:
- Task IDs must be positive integers
- Dependencies must reference IDs defined in the same file
- Cyclic dependencies are detected at startup and rejected
- `retries=0` means fail immediately with no retry attempts

---

## Development Approach

Built bottom-up across 6 phases, each independently tested before the next began:

- **Phase 0**: Repository skeleton, Makefile with header dependency tracking
- **Phase 1**: DAG engine — parser, Kahn's cycle detection, state machine, ready queue
- **Phase 2**: Wire protocol — TLV framing, 5 message types, partial read handling
- **Phase 3**: Worker executor — fork/exec/pipe/dup2/SIGALRM/killpg pipeline
- **Phase 4**: IPC sync layer — System V semaphores, MQ chunked logger pipeline
- **Phase 5**: Master orchestrator — select() reactor, dispatcher pool, result processor
- **Phase 6**: Worker client — TCP connect, handshake, dispatch/execute/result loop

Each phase has a dedicated test binary with a combined total of 25 passing tests
covering correctness, edge cases, and failure scenarios.

---

## Key Design Decisions

**Why System V IPC instead of POSIX?**
System V semaphores and message queues are kernel-persistent objects visible via
`ipcs` during execution. This makes the IPC layer inspectable and debuggable at
runtime — a property that matters in a distributed orchestration system.

**Why per-node mutexes instead of a global DAG lock?**
A single global lock would serialize all state transitions across the entire graph.
Per-node locks allow concurrent updates to independent nodes — dispatcher thread A
can complete task 3 while dispatcher thread B completes task 7 simultaneously.

**Why TLV framing instead of newline-delimited messages?**
TCP is a byte stream. A single `write()` on the sender does not guarantee a single
`read()` on the receiver. TLV framing with an 8-byte header gives the receiver an
exact byte count to wait for before processing any message, making the protocol
correct under all network conditions.

**Why `setpgid()` before `exec()` in the worker?**
Shell scripts spawn subprocesses. Without `setpgid()`, a timeout `SIGKILL` sent
to the child process leaves grandchild processes running as orphans. `setpgid()`
puts the child in its own process group so `killpg()` terminates the entire
subprocess tree atomically.
```

---