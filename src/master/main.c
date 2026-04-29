#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/msg.h>
#include <errno.h>

#include "graph.h"
#include "protocol.h"
#include "ipc_sync.h"
#include "network.h"
#include "worker.h"

/* Global shutdown flag and selfpipe write end.
 * These are global rather than inside MasterState because the
 * signal handler cannot safely receive a pointer argument —
 * it only receives the signal number. The signal handler must
 * have access to these through global scope.
 * volatile sig_atomic_t: guaranteed atomic read/write from
 * both normal code and signal handlers. */

 volatile sig_atomic_t g_shutdown = 0;
 static int g_selfpipe_wr_end = -1;

/* signal_handler — called on SIGINT or SIGTERM.
 * Sets the shutdown flag and writes one byte to the selfpipe
 * write end. This wakes the reactor from select() so it can
 * check the shutdown flag and exit cleanly.
 * write() is async-signal-safe — safe to call from a handler.
 * printf() is NOT async-signal-safe — never call it here. */

static void signal_handler(int sig) 
{
    (void)sig;
    g_shutdown = 1;
    if(g_selfpipe_wr_end >= 0) 
    {
        /* Single byte wake signal. The value does not matter. */
        char byte = 'S';
        write(g_selfpipe_wr_end, &byte, 1);
    }
}

static void print_usage(const char *prog) 
{
    fprintf(stderr,
        "Usage: %s <dag_file> <port> <auth_token>\n"
        "  dag_file:   path to .dag definition file\n"
        "  port:       TCP port to listen on (e.g. 8080)\n"
        "  auth_token: shared secret workers must present\n"
        "\nExample:\n"
        "  %s dags/sample.dag 8080 mysecret\n",
        prog, prog);
}

int main(int argc, char *argv[]) 
{
    /* ---- Argument parsing ---- */
    if(argc != 4) 
    {
        print_usage(argv[0]);
        return 1;
    }

    const char *dag_file   = argv[1];
    int         port       = atoi(argv[2]);
    const char *auth_token = argv[3];

    if(port <= 0 || port > 65535) 
    {
        fprintf(stderr, "[master] Invalid port: %s\n", argv[2]);
        return 1;
    }

    /* ---- SIGPIPE suppression — MUST be the first IPC setup step.
     *
     * Without this, writing to a socket whose remote end has closed
     * raises SIGPIPE. The default handler terminates the process.
     * This would kill the entire master if a single worker crashes
     * at the exact moment a dispatcher is writing to it.
     *
     * After SIG_IGN, write() returns -1 with errno=EPIPE instead,
     * which we handle gracefully in the dispatcher. ---- */
    signal(SIGPIPE, SIG_IGN);

    /* ---- Install SIGINT and SIGTERM handlers for clean shutdown ---- */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* ---- Allocate and zero-initialize MasterState ----
     *
     * MasterState is large (~30KB due to the embedded DAGGraph).
     * We allocate it on the heap rather than the stack to avoid
     * stack overflow — the default stack size on Linux is 8MB
     * and our struct comfortably fits on heap. */
    MasterState *ms = calloc(1, sizeof(MasterState));
    if(!ms) 
    {
        perror("[master] calloc MasterState");
        return 1;
    }
    ms->shutdown = 0;
    strncpy(ms->auth_token, auth_token, AUTH_TOKEN_LEN - 1);
    snprintf(ms->log_filepath, sizeof(ms->log_filepath), "logs/execution.log"); // snprintf used to format and store a string in buffer. Safer version of sprintf

    /* ---- Phase 1: Parse and validate the DAG ---- */
    printf("[master] Parsing DAG: %s\n", dag_file);

    if(dag_parse(&ms->graph, dag_file) < 0) 
    {
        fprintf(stderr, "[master] DAG parse failed\n");
        free(ms);
        return 1;
    }
    if(dag_build_edges(&ms->graph) < 0) 
    {
        fprintf(stderr, "[master] DAG edge build failed\n");
        free(ms);
        return 1;
    }
    if(dag_validate(&ms->graph) < 0) 
    {
        fprintf(stderr, "[master] DAG validation failed (cycle detected)\n");
        free(ms);
        return 1;
    }

    dag_init_ready_queue(&ms->graph);
    printf("[master] DAG loaded: %d tasks, %d initially READY\n", ms->graph.num_nodes, ms->graph.ready_queue.count);
    dag_print(&ms->graph);

    /* ---- Phase 2: Create the self-pipe ----
     *
     * Must happen before starting any threads or installing
     * the signal handler's write end. */
    if(pipe(ms->selfpipe) < 0) 
    {
        perror("[master] pipe (selfpipe)");
        free(ms);
        return 1;
    }
    g_selfpipe_wr_end = ms->selfpipe[1]; /* expose to signal handler */

    /* ---- Phase 3: Initialize System V IPC objects ---- */
    ms->semid = ipc_sem_create(SEM_LOG_KEY, 1);
    if(ms->semid < 0) 
    {
        fprintf(stderr, "[master] Failed to create log semaphore\n");
        free(ms);
        return 1;
    }

    ms->msqid = logger_mq_create(MQ_LOG_KEY);
    if(ms->msqid < 0) 
    {
        fprintf(stderr, "[master] Failed to create logger MQ\n");
        ipc_sem_destroy(ms->semid);
        free(ms);
        return 1;
    }

    printf("[master] IPC: semid=%d msqid=%d\n", ms->semid, ms->msqid);

    /* ---- Phase 4: Initialize scheduler queues ---- */
    idle_queue_init(&ms->idle_queue);
    work_queue_init(&ms->work_queue);

    /* ---- Phase 5: Initialize worker registry ---- */
    pthread_mutex_init(&ms->registry.lock, NULL);
    ms->registry.count = 0;

    /* ---- Phase 6: Create the listening TCP socket ---- */
    ms->listen_fd = server_setup_listen_socket(port);
    if(ms->listen_fd < 0) 
    {
        fprintf(stderr, "[master] Failed to create listen socket on port %d\n", port);
        ipc_sem_destroy(ms->semid);
        logger_mq_destroy(ms->msqid);
        free(ms);
        return 1;
    }
    printf("[master] Listening on port %d\n", port);

    /* ---- Phase 7: Start the logger thread ---- */
    static LoggerArgs logger_args;
    logger_args.msqid        = ms->msqid;
    logger_args.semid        = ms->semid;
    logger_args.log_filepath = ms->log_filepath;

    if(pthread_create(&ms->logger_tid, NULL, logger_thread_fn, &logger_args) != 0) 
    {
        perror("[master] pthread_create logger");
        close(ms->listen_fd);
        ipc_sem_destroy(ms->semid);
        logger_mq_destroy(ms->msqid);
        free(ms);
        return 1;
    }
    printf("[master] Logger thread started\n");

    /* ---- Phase 8: Start the result processor thread ---- */
    if(pthread_create(&ms->result_processor_tid, NULL, result_processor_thread_fn, ms) != 0) 
    {
        perror("[master] pthread_create result_processor");
        goto shutdown;
    }
    printf("[master] Result processor thread started\n");

    /* ---- Phase 9: Start dispatcher threads ---- */
    for(int i = 0; i < NUM_DISPATCHER_THREADS; i++) 
    {
        if(pthread_create(&ms->dispatcher_tids[i], NULL, dispatcher_thread_fn, ms) != 0) 
        {
            perror("[master] pthread_create dispatcher");
            goto shutdown;
        }
    }
    printf("[master] %d dispatcher thread(s) started\n", NUM_DISPATCHER_THREADS);

    /* ---- Phase 10: Enter the select() reactor ----
     *
     * The main thread now becomes the reactor. It does not
     * return from server_run() until ms->shutdown is set
     * by the signal handler. ---- */
    printf("[master] Entering reactor loop. Press Ctrl+C to stop.\n");
    ms->shutdown = 0;
    /* Allow signal handler to update ms->shutdown via the pointer.
     * We bridge the gap: signal handler sets g_shutdown, reactor
     * checks g_shutdown at the top of each loop and copies it. */

    server_run(ms);
    printf("\n[master] Reactor exited. Shutting down...\n");

shutdown:
    /* ---- Graceful shutdown sequence ---- */

    /* 1. Cancel dispatcher threads — they may be blocked in
     *    dag_dequeue_ready() or idle_queue_pop(). pthread_cancel
     *    delivers a cancellation to the next cancellation point
     *    (pthread_cond_wait qualifies). */
    for(int i = 0; i < NUM_DISPATCHER_THREADS; i++) 
    {
        pthread_cancel(ms->dispatcher_tids[i]);
        pthread_join(ms->dispatcher_tids[i], NULL);
    }
    printf("[master] Dispatcher threads stopped\n");

    /* 2. Stop result processor with NULL sentinel */
    work_queue_push(&ms->work_queue, NULL);
    pthread_join(ms->result_processor_tid, NULL);
    printf("[master] Result processor stopped\n");

    /* 3. Stop logger thread with shutdown message */
    LogMsg shutdown_msg;
    memset(&shutdown_msg, 0, sizeof(LogMsg));
    shutdown_msg.mtype = LOG_MTYPE_SHUTDOWN;
    msgsnd(ms->msqid, &shutdown_msg, LOG_MSG_PAYLOAD_SIZE, 0);
    pthread_join(ms->logger_tid, NULL);
    printf("[master] Logger thread stopped\n");

    /* 4. Print final DAG state */
    printf("\n[master] Final DAG state:\n");
    dag_print(&ms->graph);

    /* 5. Destroy IPC objects */
    ipc_sem_destroy(ms->semid);
    logger_mq_destroy(ms->msqid);

    /* 6. Close sockets */
    close(ms->listen_fd);
    close(ms->selfpipe[0]);
    close(ms->selfpipe[1]);

    /* 7. Destroy synchronization primitives */
    dag_destroy(&ms->graph);
    pthread_mutex_destroy(&ms->registry.lock);

    free(ms);
    printf("[master] Clean shutdown complete.\n");
    return 0;
}