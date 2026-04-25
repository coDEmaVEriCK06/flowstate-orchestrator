#ifndef GRAPH_H
#define GRAPH_H

#include <pthread.h>
#include <stdint.h>
#include <time.h>

#define MAX_TASKS 64
#define MAX_DEPS 16
#define MAX_CMD_LEN 256
#define MAX_LINE_LEN 512

typedef enum
{
    PENDING = 0, // Waiting for all parents to complete
    READY = 1, // All parents done, ready to be dispatched to a worker
    DISPATCHED = 2, // Sent to a worker, waitiing for ACK by worker
    RUNNING = 3, // Worker has sent ACK, execution has started
    COMPLETED = 4, // Success, exit code 0 on terminal
    FAILED = 5, // Non-zero exit code or worker crashed
    TIMED_OUT = 6, // Execution exceeded deadline 
    DEAD = 7, // Terminal failure, max retries exhausted
    CASCADE_FAILED = 8 // An ancestor node reached DEAD
} TaskState;

typedef struct DAGNode
{
    int task_id;
    uint32_t generation; //Updated on every retry to reject stale MSG_RESULT
    TaskState state;
    pthread_mutex_t lock; // Per node lock instead of global due to multiple threads
    int parents_completed;
    int num_parents;
    int retry_count;
    int max_retries;
    time_t dispatch_time;
    char command[MAX_CMD_LEN];
    struct DAGNode *children[MAX_TASKS]; // Nodes that depend on this node
    int num_children;
    int dep_ids[MAX_DEPS]; //dep_ids and num_deps read only once while parsing and building graph
    int num_deps;
} DAGNode;

typedef struct
{
    DAGNode *slots[MAX_TASKS];
    int head; // Next element to be removed from the queue 
    int tail; // Next element to be added to the queue
    int count; // Total elements
    pthread_mutex_t lock;
    pthread_cond_t not_empty; // Condition for waking up a worker and avoid busy loop
} ReadyQueue;

typedef struct
{
    DAGNode nodes [MAX_TASKS];
    int num_nodes;
    ReadyQueue ready_queue;
} DAGGraph;

int dag_parse(DAGGraph *graph, const char *filepath);

int dag_build_edges(DAGGraph *graph);

int dag_validate(DAGGraph *graph);

void dag_init_ready_queue(DAGGraph *graph);

void dag_enqueue_ready(DAGGraph *graph, DAGNode *node);

DAGNode *dag_dequeue_ready(DAGGraph *graph);

int task_transition(DAGNode *node, TaskState expected, TaskState next);

void dag_notify_children(DAGGraph *graph, DAGNode *completed_node);

void dag_propagate_dead(DAGGraph *graph, DAGNode *dead_node);

const char *task_state_name(TaskState state);

void dag_print(const DAGGraph *graph);

void dag_destroy(DAGGraph *graph);

#endif