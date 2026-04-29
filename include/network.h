#ifndef NETWORK_H
#define NETWORK_H

#include <pthread.h>
#include <stdint.h>
#include <time.h>

#include "graph.h"
#include "protocol.h"
#include "ipc_sync.h"

/* Maximum number of concurrent worker connections */
#define MAX_WORKERS            16

/* Number of dispatcher threads in the thread pool */
#define NUM_DISPATCHER_THREADS 2 // 2 used for safety, 1 was also sufficent as it is mostly a sleeping thread

/* TCP listen backlog — how many pending connections the
 * kernel queues before refusing new ones */
#define LISTEN_BACKLOG         10

/* Default TCP port the master listens on */
#define DEFAULT_PORT           8080

typedef enum 
{
    WORKER_CONNECTED = 0,  /* TCP connected, handshake not yet done  */
    WORKER_IDLE      = 1,  /* handshake complete, awaiting dispatch   */
    WORKER_BUSY      = 2,  /* currently executing a task             */
    WORKER_DEAD      = 3,  /* disconnected or errored — slot reusable */
} WorkerState;

typedef struct 
{
    int             socket_fd;
    WorkerState     state;
    uint32_t        current_task_id;
    uint32_t        current_generation;
    pthread_mutex_t write_lock;   /* protects write() + state fields */
    ConnRecvBuffer  recv_buf;     /* TLV receive accumulator          */
} WorkerConn;

/* Protected by its own mutex for add/remove operations.
 * The reactor also holds this lock briefly when snapshotting
 * active fds before each select() call. */
typedef struct 
{
    WorkerConn      workers[MAX_WORKERS];
    int             count;
    pthread_mutex_t lock;
} WorkerRegistry;

/* Dispatcher threads block here when no worker is available.
 * The reactor pushes workers here after a successful handshake.
 * The result processor pushes workers here after result handling. */
typedef struct 
{
    WorkerConn     *slots[MAX_WORKERS];
    int             head;
    int             tail;
    int             count;
    pthread_mutex_t lock;
    pthread_cond_t  not_empty;
} IdleWorkerQueue;

typedef struct WorkItem 
{
    DAGNode         *node;       /* direct pointer — found by reactor  */
    uint32_t         generation; /* echoed from MSG_RESULT             */
    int32_t          exit_code;
    uint32_t         output_len;
    char             output[MAX_OUTPUT_LEN + 1];
    WorkerConn      *worker;     /* which worker to return to idle     */
    struct WorkItem *next;
} WorkItem;

/* WorkQueue — thread-safe linked-list queue of WorkItems.
 * Reactor pushes, result processor pops (blocking).
 * A NULL item is the shutdown sentinel. */
typedef struct 
{
    WorkItem        *head;
    WorkItem        *tail;
    int              count;
    pthread_mutex_t  lock;
    pthread_cond_t   not_empty;
} WorkQueue;

/* MasterState — the single top-level struct holding ALL shared
 * state for the master process. Passed by pointer to every
 * thread and every function that needs shared access.
 *
 * Having one struct prevents global variables and makes
 * dependencies explicit — if a function needs the graph, it
 * takes a MasterState *, not a separate DAGGraph *. */
typedef struct 
{
    /* DAG engine */
    DAGGraph         graph;

    /* Network layer */
    int              listen_fd;
    int              selfpipe[2];  /* [0]=read end, [1]=write end    */
    WorkerRegistry   registry;

    /* Scheduler layer */
    IdleWorkerQueue  idle_queue;
    WorkQueue        work_queue;

    /* Logger layer */
    int              msqid;
    int              semid;
    char             log_filepath[256];

    /* Auth token — master and worker must present the same string */
    char             auth_token[AUTH_TOKEN_LEN];

    /* Shutdown flag — set by signal handler, checked by reactor */
    volatile int     shutdown;

    /* Dispatcher and result processor thread handles */
    pthread_t        dispatcher_tids[NUM_DISPATCHER_THREADS];
    pthread_t        result_processor_tid;
    pthread_t        logger_tid;
} MasterState;

int server_setup_listen_socket(int port); // Create, bind and listen on the given port

/*
 * server_run — the main select() reactor loop.
 * Runs in the main thread. Exits when ms->shutdown is set.
 * Monitors: listen_fd, selfpipe[0], all active worker fds.
 */
void server_run(MasterState *ms);

/* Idle worker queue operations */
void idle_queue_init(IdleWorkerQueue *q);
void idle_queue_push(IdleWorkerQueue *q, WorkerConn *w);
WorkerConn *idle_queue_pop(IdleWorkerQueue *q);

/* Work queue operations */
void work_queue_init(WorkQueue *q);
void work_queue_push(WorkQueue *q, WorkItem *item);
WorkItem *work_queue_pop(WorkQueue *q);

/*
 * dispatcher_thread_fn — thread entry point.
 * Argument: pointer to MasterState.
 * Loops: dequeue READY task → pop idle worker → dispatch.
 */
void *dispatcher_thread_fn(void *arg);

/*
 * result_processor_thread_fn — thread entry point.
 * Argument: pointer to MasterState.
 * Loops: pop WorkItem → update DAG → enqueue log → return worker.
 */
void *result_processor_thread_fn(void *arg);

#endif