#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <errno.h>

#include "graph.h"
#include "protocol.h"
#include "ipc_sync.h"
#include "network.h"
#include "worker.h"


void idle_queue_init(IdleWorkerQueue *q) 
{
    memset(q, 0, sizeof(IdleWorkerQueue));
    pthread_mutex_init(&q->lock, NULL);
    pthread_cond_init(&q->not_empty, NULL);
}

void idle_queue_push(IdleWorkerQueue *q, WorkerConn *w) 
{
    pthread_mutex_lock(&q->lock);
    if(q->count >= MAX_WORKERS) 
    {
        /* Should never happen — MAX_WORKERS bounds both */
        fprintf(stderr, "[idle_queue] Queue full — dropping worker\n");
        pthread_mutex_unlock(&q->lock);
        return;
    }

    q->slots[q->tail] = w;
    q->tail = (q->tail + 1) % MAX_WORKERS;
    q->count++;

    /* Signal one waiting dispatcher thread */
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
}

WorkerConn *idle_queue_pop(IdleWorkerQueue *q) 
{
    pthread_mutex_lock(&q->lock);

    /* while loop — mandatory to handle spurious wakeups */
    while(q->count == 0) 
    {
        pthread_cond_wait(&q->not_empty, &q->lock);
    }

    WorkerConn *w = q->slots[q->head];
    q->head = (q->head + 1) % MAX_WORKERS;
    q->count--;

    pthread_mutex_unlock(&q->lock);
    return w;
}

void work_queue_init(WorkQueue *q) 
{
    memset(q, 0, sizeof(WorkQueue));
    pthread_mutex_init(&q->lock, NULL);
    pthread_cond_init(&q->not_empty, NULL);
}

void work_queue_push(WorkQueue *q, WorkItem *item) 
{
    pthread_mutex_lock(&q->lock);

    /* Append to tail of linked list */
    if(q->tail) 
    {
        q->tail->next = item;
    } 
    else 
    {
        q->head = item;
    }
    q->tail = item;
    if(item) item->next = NULL;
    q->count++;

    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
}

WorkItem *work_queue_pop(WorkQueue *q) 
{
    pthread_mutex_lock(&q->lock);

    while(q->count == 0) 
    {
        pthread_cond_wait(&q->not_empty, &q->lock);
    }

    WorkItem *item = q->head;
    if(item) 
    {
        q->head = item->next;
        if(!q->head) q->tail = NULL;
        q->count--;
    }

    pthread_mutex_unlock(&q->lock);
    return item; /* NULL = shutdown sentinel */
}

/* dispatcher_thread_fn
 *
 * This thread's entire job:
 *   1. Get a READY task from the DAG ready queue (blocking)
 *   2. Get an IDLE worker from the idle queue (blocking)
 *   3. Transition task READY → DISPATCHED
 *   4. Write MSG_DISPATCH to the worker socket
 *
 * pthread_cancel() is used for shutdown. The cancellation points
 * are inside pthread_cond_wait() in dag_dequeue_ready() and
 * idle_queue_pop(). No cleanup handler is needed — the master
 * shuts down immediately after cancellation. */
void *dispatcher_thread_fn(void *arg) 
{
    MasterState *ms = (MasterState *)arg;
    while(1) 
    {
        /* Block until a READY task is available.
         * This is a pthread cancellation point. */
        DAGNode *node = dag_dequeue_ready(&ms->graph);

        /* Block until an IDLE worker is available.
         * Also a cancellation point. */
        WorkerConn *worker = idle_queue_pop(&ms->idle_queue);

        /* Attempt READY → DISPATCHED transition.
         * Can fail if the node became CASCADE_FAILED between
         * dag_dequeue_ready() and here (another node died).
         * In that case return the worker and try again. */
        if(task_transition(node, READY, DISPATCHED) < 0) 
        {
            printf("[dispatcher] Task %d no longer READY — returning worker\n", node->task_id);
            idle_queue_push(&ms->idle_queue, worker);
            continue;
        }

        /* Record the dispatch timestamp for timeout tracking
         * in a future sweeper thread (not implemented in this phase) */
        node->dispatch_time = time(NULL);

        /* Pack the dispatch payload */
        uint8_t buf[512];
        int n = proto_pack_dispatch(buf, sizeof(buf), (uint32_t)node->task_id, node->generation, node->command);
        if(n < 0) 
        {
            fprintf(stderr, "[dispatcher] proto_pack_dispatch failed for task %d\n", node->task_id);
            task_transition(node, DISPATCHED, FAILED);
            idle_queue_push(&ms->idle_queue, worker);
            continue;
        }

        /* Acquire per-worker write lock.
         * No other thread may write to this socket concurrently. */
        pthread_mutex_lock(&worker->write_lock);
        worker->current_task_id    = (uint32_t)node->task_id;
        worker->current_generation = node->generation;
        worker->state              = WORKER_BUSY;

        ssize_t written = write(worker->socket_fd, buf, (size_t)n);
        pthread_mutex_unlock(&worker->write_lock);

        if(written != (ssize_t)n) 
        {
            /* Write failed — worker disconnected while we were writing.
             * SIGPIPE is suppressed so write() returns -1 with EPIPE.
             * The reactor will detect the disconnect on the next
             * select() iteration via read() returning 0 or error. */
            fprintf(stderr,
                "[dispatcher] Write failed for task %d (worker fd=%d): %s\n",
                node->task_id, worker->socket_fd, strerror(errno));
            /* Roll back the task state — reactor will handle worker */
            task_transition(node, DISPATCHED, FAILED);
        } 
        else 
        {
            printf("[dispatcher] Task %d (gen %u) dispatched to fd=%d\n", node->task_id, node->generation, worker->socket_fd);
        }
    }
    return NULL;
}

/* Consumes WorkItems from the work queue. Each item was pushed
 * by the reactor after parsing a complete MSG_RESULT message.
 *
 * Responsibilities:
 *   - Validate generation (double-check after reactor's first check)
 *   - RUNNING → COMPLETED or RUNNING → FAILED transition
 *   - On COMPLETED: notify children, log result
 *   - On FAILED: retry (FAILED→READY) or give up (FAILED→DEAD)
 *   - On DEAD: cascade failures to descendants
 *   - Return worker to idle queue
 *   - Check for full DAG completion
 *
 * Shutdown: work_queue_push(NULL) sends the sentinel. */
void *result_processor_thread_fn(void *arg) 
{
    MasterState *ms = (MasterState *)arg;
    while(1) 
    {
        WorkItem *item = work_queue_pop(&ms->work_queue);

        /* NULL sentinel: shutdown requested */
        if(!item) break;

        DAGNode *node = item->node;

        /* Double-check generation under lock.
         * Between reactor's push and our pop, another thread could
         * have already processed a result for this task (extremely
         * unlikely but theoretically possible). */
        pthread_mutex_lock(&node->lock);
        int stale = (node->generation != item->generation);
        pthread_mutex_unlock(&node->lock);

        if(stale) 
        {
            printf("[result_proc] Stale WorkItem for task %d — discarding\n", node->task_id);
            idle_queue_push(&ms->idle_queue, item->worker);
            free(item);
            continue;
        }

        /* ---- Success path ---- */
        if(item->exit_code == 0) 
        {
            if(task_transition(node, RUNNING, COMPLETED) == 0) 
            {
                printf("[result_proc] Task %d COMPLETED\n", node->task_id);

                /* Notify children — may promote multiple nodes to READY
                 * and enqueue them, waking blocked dispatcher threads */
                dag_notify_children(&ms->graph, node);

                /* Enqueue log entry */
                logger_mq_send(ms->msqid, node->task_id, 0,
                               item->output, item->output_len);

                /* Check if entire DAG is done */
                if(dag_is_complete(&ms->graph)) 
                {
                    printf("\n[result_proc] *** ALL TASKS COMPLETE ***\n");
                    dag_print(&ms->graph);
                }
            }

        /* ---- Failure path ---- */
        } 
        else 
        {
            if(task_transition(node, RUNNING, FAILED) == 0) 
            {
                printf("[result_proc] Task %d FAILED (exit code %d)\n", node->task_id, item->exit_code);

                /* Log the failure with its output (may be an error message) */
                logger_mq_send(ms->msqid, node->task_id, item->exit_code, item->output, item->output_len);

                /* retry_count was already incremented inside
                 * task_transition() when we later call FAILED→READY.
                 * Check: can we retry? */
                if(node->retry_count < node->max_retries) 
                {
                    /* Retry: FAILED → READY, re-enqueue */
                    if(task_transition(node, FAILED, READY) == 0) 
                    {
                        printf("[result_proc] Task %d retrying "
                               "(attempt %d of %d)\n",
                               node->task_id,
                               node->retry_count,
                               node->max_retries);
                        dag_enqueue_ready(&ms->graph, node);
                    }
                } 
                else 
                {
                    /* No retries left: FAILED → DEAD → cascade */
                    if(task_transition(node, FAILED, DEAD) == 0) 
                    {
                        printf("[result_proc] Task %d is DEAD — " "cascading failure to descendants\n", node->task_id);
                        dag_propagate_dead(&ms->graph, node);

                        /* Check if the cascade completed the DAG */
                        if(dag_is_complete(&ms->graph)) 
                        {
                            printf("\n[result_proc] *** DAG COMPLETE " "(with failures) ***\n");
                            dag_print(&ms->graph);
                        }
                    }
                }
            }
        }

        /* Return the worker to the idle queue so another task
         * can be dispatched to it. This is the last thing we do
         * so the worker is not available until all DAG state
         * updates are complete. */
        pthread_mutex_lock(&item->worker->write_lock);
        if(item->worker->state != WORKER_DEAD) 
        {
            item->worker->state              = WORKER_IDLE;
            item->worker->current_task_id    = 0;
            item->worker->current_generation = 0;
        }
        pthread_mutex_unlock(&item->worker->write_lock);

        if(item->worker->state == WORKER_IDLE) 
        {
            idle_queue_push(&ms->idle_queue, item->worker);
        }

        free(item);
    }
    return NULL;
}