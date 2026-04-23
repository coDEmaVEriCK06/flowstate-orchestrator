# Flow-State Orchestrator

A distributed DAG task scheduler built in C using POSIX OS primitives.
Modeled after Apache Airflow's architecture. Built as a systems programming project.

## Architecture
- **Master**: Parses DAGs, manages task state machine, dispatches work via TCP
- **Worker**: Receives tasks, executes via fork/exec, returns output over persistent socket

## Build
`make all`        # builds master and worker binaries
`make tests`      # builds all test binaries
`make clean`      # removes all build artifacts

## Run
# Terminal 1 — start master
./master dags/sample.dag

# Terminal 2 — connect a worker
./worker 127.0.0.1 8080

## OS Concepts Demonstrated
fork/exec, unnamed pipes, select(), fcntl() locking, System V semaphores, System V message queues, pthreads, TCP sockets