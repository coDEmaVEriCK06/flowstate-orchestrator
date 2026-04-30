#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>   /* socket, bind, listen, accept, setsockopt */
#include <netinet/in.h>   /* sockaddr_in, INADDR_ANY                  */
#include <arpa/inet.h>    /* inet_ntoa, htonl, htons                  */

#include "graph.h"
#include "protocol.h"
#include "ipc_sync.h"
#include "network.h"

static void handle_new_connection(MasterState *ms);
static void handle_worker_data(MasterState *ms, WorkerConn *w);
static void handle_worker_disconnect(MasterState *ms, WorkerConn *w);
static void process_msg_connect(MasterState *ms, WorkerConn *w,
                                const uint8_t *payload, uint32_t len);
static void process_msg_ack(MasterState *ms, WorkerConn *w,
                            const uint8_t *payload, uint32_t len);
static void process_msg_result(MasterState *ms, WorkerConn *w,
                               const uint8_t *payload, uint32_t len);
static WorkerConn *find_worker_by_fd(MasterState *ms, int fd);
static void process_msg_status_request(MasterState *ms, WorkerConn *w,
                                       const uint8_t *payload,
                                       uint32_t len);

int server_setup_listen_socket(int port) 
{
    /* AF_INET = IPv4, SOCK_STREAM = TCP */
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if(fd < 0) 
    {
        perror("[server] socket");
        return -1;
    }

    /* SO_REUSEADDR: allows re-binding to a port that is still in
     * TIME_WAIT state from a previous run. Without this, restarting
     * the master within ~60 seconds of a crash fails with EADDRINUSE.
     * Essential during development. */
    int opt = 1;
    if(setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) 
    {
        perror("[server] setsockopt SO_REUSEADDR");
        close(fd);
        return -1;
    }

    /* Bind to INADDR_ANY (all interfaces) on the given port.
     * htonl/htons convert to network byte order. */
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons((uint16_t)port);

    if(bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) 
    {
        perror("[server] bind");
        close(fd);
        return -1;
    }

    /* listen() marks the socket as passive — it now accepts
     * connections rather than initiating them.
     * LISTEN_BACKLOG: how many pending connections the kernel
     * queues before refusing new ones. */
    if(listen(fd, LISTEN_BACKLOG) < 0) 
    {
        perror("[server] listen");
        close(fd);
        return -1;
    }

    return fd;
}

/* server_run — the select() reactor loop
 * This function runs in the main thread for the entire lifetime
 * of the master. It exits only when ms->shutdown is set.
 *
 * Each iteration:
 *   1. Build fd_set from listen_fd, selfpipe[0], active workers
 *   2. Call select() — blocks until any fd is readable
 *   3. Drain selfpipe if signalled (wake or shutdown)
 *   4. Handle new connections on listen_fd
 *   5. Handle incoming data on each active worker fd */
void server_run(MasterState *ms) 
{
    /* g_shutdown is the file-scope signal flag in main.c.
     * We access it via extern — it must be declared there as
     * static, so we re-declare it here with extern to share it. */
    extern volatile sig_atomic_t g_shutdown;

    fd_set   read_fds;
    int      snapshotted_fds[MAX_WORKERS];
    int      snapshot_count;

    while(!g_shutdown) 
    {
        /* ---- Build the fd_set ----
         *
         * We take a brief snapshot of active worker fds under the
         * registry lock, then release the lock before calling select().
         * This ensures:
         *   - The fd_set is consistent at the moment select() is called
         *   - We do not hold the registry lock while blocking in select()
         *     (which would prevent new workers from being registered)
         */
        FD_ZERO(&read_fds);
        int maxfd = -1;

        /* Always monitor the listening socket */
        FD_SET(ms->listen_fd, &read_fds);
        if(ms->listen_fd > maxfd) maxfd = ms->listen_fd;

        /* Always monitor the selfpipe read end */
        FD_SET(ms->selfpipe[0], &read_fds);
        if(ms->selfpipe[0] > maxfd) maxfd = ms->selfpipe[0];

        /* Snapshot active worker fds */
        snapshot_count = 0;
        pthread_mutex_lock(&ms->registry.lock);
        for(int i = 0; i < ms->registry.count; i++) 
        {
            WorkerConn *w = &ms->registry.workers[i];
            if(w->state != WORKER_DEAD) 
            {
                int fd = w->socket_fd;
                FD_SET(fd, &read_fds);
                if (fd > maxfd) maxfd = fd;
                snapshotted_fds[snapshot_count++] = fd;
            }
        }
        pthread_mutex_unlock(&ms->registry.lock);

        /* ---- select() ----
         *
         * Timeout of 2 seconds so the loop re-checks g_shutdown
         * even if no activity occurs. This prevents the master from
         * hanging forever if Ctrl+C is pressed while select() is
         * blocking on a system that does not interrupt select() on
         * signal delivery (rare but possible). */
        struct timeval tv = { 2, 0 };
        int ready = select(maxfd + 1, &read_fds, NULL, NULL, &tv);

        if(ready < 0) 
        {
            if(errno == EINTR) continue;  /* signal — loop and re-check */
            perror("[server] select");
            break;
        }
        if(ready == 0) continue;  /* timeout — loop */

        
        /* Any thread can write to selfpipe[1] to wake us. We read
         * and discard all pending bytes. The byte value is irrelevant —
         * it is only a wake mechanism. */
        if(FD_ISSET(ms->selfpipe[0], &read_fds)) 
        {
            char drain[64];
            read(ms->selfpipe[0], drain, sizeof(drain));
            /* Now check g_shutdown immediately */
            if(g_shutdown) break;
        }

        /* ---- New connection on listening socket ---- */
        if(FD_ISSET(ms->listen_fd, &read_fds)) 
        {
            handle_new_connection(ms);
        }

        /* ---- Incoming data on worker sockets ----
         *
         * We iterate over our snapshot, not the live registry,
         * because handle_worker_data() may mark workers DEAD
         * which modifies the registry. Working from a snapshot
         * prevents iterator invalidation. */
        for(int i = 0; i < snapshot_count; i++) 
        {
            if(FD_ISSET(snapshotted_fds[i], &read_fds)) 
            {
                WorkerConn *w = find_worker_by_fd(ms, snapshotted_fds[i]);
                if(w && w->state != WORKER_DEAD) {
                    handle_worker_data(ms, w);
                }
            }
        }
    }
}

static void handle_new_connection(MasterState *ms) 
{
    struct sockaddr_in client_addr;
    socklen_t addrlen = sizeof(client_addr);

    int client_fd = accept(ms->listen_fd, (struct sockaddr *)&client_addr, &addrlen);
    if(client_fd < 0) 
    {
        if(errno != EAGAIN && errno != EWOULDBLOCK) perror("[server] accept");
        return;
    }

    printf("[server] New connection from %s:%d (fd=%d)\n",
           inet_ntoa(client_addr.sin_addr),
           ntohs(client_addr.sin_port),
           client_fd);

    /* Find a free slot in the registry */
    pthread_mutex_lock(&ms->registry.lock);

    if(ms->registry.count >= MAX_WORKERS) 
    {
        pthread_mutex_unlock(&ms->registry.lock);
        fprintf(stderr, "[server] Max workers reached — rejecting fd=%d\n", client_fd);
        close(client_fd);
        return;
    }

    /* Initialize the WorkerConn slot */
    WorkerConn *w = &ms->registry.workers[ms->registry.count];
    memset(w, 0, sizeof(WorkerConn));
    w->socket_fd = client_fd;
    w->state     = WORKER_CONNECTED;
    pthread_mutex_init(&w->write_lock, NULL);
    conn_recv_init(&w->recv_buf);
    ms->registry.count++;

    pthread_mutex_unlock(&ms->registry.lock);

    /* Wake the reactor to rebuild its fd_set on the next iteration.
     * This is necessary because we just modified the registry —
     * the reactor's snapshot from before the accept() does not
     * include the new fd. Writing to selfpipe[1] causes select()
     * to return early on the next iteration. */
    char byte = 'C';
    write(ms->selfpipe[1], &byte, 1);
}

static void handle_worker_data(MasterState *ms, WorkerConn *w) 
{
    uint8_t  tmp[1024];
    ssize_t  n = read(w->socket_fd, tmp, sizeof(tmp));

    if(n <= 0) 
    {
        /* n == 0: clean disconnect (worker closed its socket)
         * n <  0: error (ECONNRESET, ETIMEDOUT, etc.)
         * Either way, treat as disconnect. */
        if(n < 0 && errno != ECONNRESET) perror("[server] read");
        handle_worker_disconnect(ms, w);
        return;
    }

    /* Accumulate bytes into the per-connection receive buffer */
    if(conn_recv_push(&w->recv_buf, tmp, (uint32_t)n) < 0) 
    {
        fprintf(stderr, "[server] Receive buffer overflow on fd=%d\n", w->socket_fd);
        handle_worker_disconnect(ms, w);
        return;
    }

    /* Deframe loop — one call may have deposited multiple complete
     * messages. We process all of them before returning to select(). */
    uint8_t  payload[RECV_BUF_SIZE];
    uint32_t msg_type, payload_len;
    int ret;

    while((ret = proto_try_deframe(&w->recv_buf, &msg_type, payload, &payload_len)) == DEFRAME_COMPLETE) 
    {
        switch(msg_type) 
        {
            case MSG_CONNECT:
                process_msg_connect(ms, w, payload, payload_len);
                break;
            case MSG_ACK:
                process_msg_ack(ms, w, payload, payload_len);
                break;
            case MSG_RESULT:
                process_msg_result(ms, w, payload, payload_len);
                break;
            case MSG_STATUS_REQUEST:
                process_msg_status_request(ms, w, payload, payload_len);
                break;
            default:
                fprintf(stderr, "[server] Unknown message type 0x%02x from fd=%d\n", msg_type, w->socket_fd);
                break;
        }
    }

    if(ret == DEFRAME_ERROR) 
    {
        fprintf(stderr, "[server] Framing error on fd=%d — disconnecting\n", w->socket_fd);
        handle_worker_disconnect(ms, w);
    }
}

static void handle_worker_disconnect(MasterState *ms, WorkerConn *w) 
{
    printf("[server] Worker disconnected (fd=%d)\n", w->socket_fd);

    /* If the worker was BUSY when it disconnected, its task
     * must be re-queued. Transition DISPATCHED or RUNNING → FAILED
     * and let the retry logic in the result processor handle it.
     *
     * We handle this inline here since it involves the DAG state
     * machine, not a result payload. */
    pthread_mutex_lock(&w->write_lock);
    WorkerState old_state  = w->state;
    uint32_t    task_id    = w->current_task_id;
    uint32_t    generation = w->current_generation;
    w->state               = WORKER_DEAD;
    pthread_mutex_unlock(&w->write_lock);

    if(old_state == WORKER_BUSY && task_id > 0) 
    {
        DAGNode *node = dag_find_node(&ms->graph, (int)task_id);
        if(node) 
        {
            /* Attempt transitions from either DISPATCHED or RUNNING.
             * Only one will succeed — the other returns -1. */
            int transitioned = 0;
            if(task_transition(node, DISPATCHED, FAILED) == 0) transitioned = 1;
            else if(task_transition(node, RUNNING, FAILED) == 0) transitioned = 1;

            if(transitioned) 
            {
                /* Check generation — stale disconnect for an old task */
                pthread_mutex_lock(&node->lock);
                int stale = (node->generation != generation);
                pthread_mutex_unlock(&node->lock);

                if(!stale) 
                {
                    /* Retry or mark DEAD */
                    if(node->retry_count < node->max_retries) 
                    {
                        if(task_transition(node, FAILED, READY) == 0) 
                        {
                            printf("[server] Task %d requeued after worker disconnect\n", task_id);
                            dag_enqueue_ready(&ms->graph, node);
                        }
                    } 
                    else 
                    {
                        if(task_transition(node, FAILED, DEAD) == 0) 
                        {
                            printf("[server] Task %d DEAD after worker disconnect\n", task_id);
                            dag_propagate_dead(&ms->graph, node);
                        }
                    }
                }
            }
        }
    }

    close(w->socket_fd);
    w->socket_fd = -1;
}

static void process_msg_connect(MasterState *ms, WorkerConn *w, const uint8_t *payload, uint32_t len) 
{
    uint32_t role;
    char     token[AUTH_TOKEN_LEN];

    if(proto_parse_connect(payload, len, &role, token) < 0) 
    {
        fprintf(stderr, "[server] Malformed MSG_CONNECT on fd=%d\n", w->socket_fd);
        handle_worker_disconnect(ms, w);
        return;
    }

    /* Validate the auth token — strncmp is safe since both
     * buffers are AUTH_TOKEN_LEN bytes with guaranteed null term */
    int token_ok = (strncmp(token, ms->auth_token, AUTH_TOKEN_LEN) == 0);

    /* Pack the response */
    uint8_t buf[64];
    uint32_t status = token_ok ? CONNECT_STATUS_OK : CONNECT_STATUS_REJECT;
    int n = proto_pack_connect_ack(buf, sizeof(buf), status);

    pthread_mutex_lock(&w->write_lock);
    write(w->socket_fd, buf, n);
    pthread_mutex_unlock(&w->write_lock);

    if(!token_ok) 
    {
        fprintf(stderr, "[server] Worker fd=%d rejected: bad token\n", w->socket_fd);
        handle_worker_disconnect(ms, w);
        return;
    }

    /* Handshake complete — mark as IDLE and add to idle queue */
    pthread_mutex_lock(&w->write_lock);
    w->state = WORKER_IDLE;
    pthread_mutex_unlock(&w->write_lock);

    idle_queue_push(&ms->idle_queue, w);
    printf("[server] Worker fd=%d authenticated (role=%u) — now IDLE\n", w->socket_fd, role);
}

static void process_msg_ack(MasterState *ms, WorkerConn *w, const uint8_t *payload, uint32_t len) 
{
    uint32_t task_id, generation;

    if(proto_parse_ack(payload, len, &task_id, &generation) < 0) 
    {
        fprintf(stderr, "[server] Malformed MSG_ACK on fd=%d\n", w->socket_fd);
        return;
    }

    DAGNode *node = dag_find_node(&ms->graph, (int)task_id);
    if(!node) 
    {
        fprintf(stderr, "[server] MSG_ACK for unknown task %u\n", task_id);
        return;
    }

    /* Validate generation to reject stale ACKs */
    pthread_mutex_lock(&node->lock);
    int stale = (node->generation != generation);
    pthread_mutex_unlock(&node->lock);

    if(stale) 
    {
        fprintf(stderr, "[server] Stale ACK for task %u — dropping\n", task_id);
        return;
    }

    /* DISPATCHED → RUNNING transition.
     * This is lightweight enough to do in the reactor thread. */
    if(task_transition(node, DISPATCHED, RUNNING) == 0) 
    {
        printf("[server] Task %u is now RUNNING on fd=%d\n", task_id, w->socket_fd);
    }
}

static void process_msg_result(MasterState *ms, WorkerConn *w, const uint8_t *payload, uint32_t len) 
{
    uint32_t task_id, generation;
    int32_t  exit_code;
    char     output[MAX_OUTPUT_LEN + 1];
    uint32_t output_len;

    if(proto_parse_result(payload, len, &task_id, &generation, &exit_code, output, &output_len) < 0) 
    {
        fprintf(stderr, "[server] Malformed MSG_RESULT on fd=%d\n", w->socket_fd);
        return;
    }

    DAGNode *node = dag_find_node(&ms->graph, (int)task_id);
    if(!node) 
    {
        fprintf(stderr, "[server] MSG_RESULT for unknown task %u\n", task_id);
        idle_queue_push(&ms->idle_queue, w);
        return;
    }

    /* Generation check — stale result from a previous dispatch attempt */
    pthread_mutex_lock(&node->lock);
    int stale = (node->generation != generation);
    pthread_mutex_unlock(&node->lock);

    if(stale) 
    {
        fprintf(stderr,
            "[server] Stale result for task %u (expected gen %u, got %u) — dropping\n",
            task_id, node->generation, generation);
        /* Return worker to idle even for stale results */
        idle_queue_push(&ms->idle_queue, w);
        return;
    }

    /* Allocate a WorkItem and push to the work queue.
     * The reactor never does heavy DAG work — it delegates to
     * the result processor thread to keep itself non-blocking. */
    WorkItem *item = malloc(sizeof(WorkItem));
    if(!item) 
    {
        perror("[server] malloc WorkItem");
        return;
    }

    item->node       = node;
    item->generation = generation;
    item->exit_code  = exit_code;
    item->output_len = output_len;
    memcpy(item->output, output, output_len);
    item->output[output_len] = '\0';
    item->worker     = w;
    item->next       = NULL;

    work_queue_push(&ms->work_queue, item);
}

static WorkerConn *find_worker_by_fd(MasterState *ms, int fd) 
{
    pthread_mutex_lock(&ms->registry.lock);
    for(int i = 0; i < ms->registry.count; i++) 
    {
        if(ms->registry.workers[i].socket_fd == fd) 
        {
            WorkerConn *w = &ms->registry.workers[i];
            pthread_mutex_unlock(&ms->registry.lock);
            return w;
        }
    }
    pthread_mutex_unlock(&ms->registry.lock);
    return NULL;
}

/* ============================================================
 * process_msg_status_request — observer queries DAG state
 *
 * Reads current state of all tasks (no lock needed for state
 * reads since TaskState is a single int — atomic on all
 * modern architectures for read-only access).
 * Packs a MSG_STATUS_RESPONSE and sends it back.
 * ============================================================ */
static void process_msg_status_request(MasterState *ms, WorkerConn *w,
                                       const uint8_t *payload,
                                       uint32_t len) {
    (void)payload;
    (void)len;

    /* Build the entries array from the current DAG state */
    TaskStatusEntry entries[MAX_STATUS_ENTRIES];
    uint32_t num_tasks = (uint32_t)ms->graph.num_nodes;

    for(uint32_t i = 0; i < num_tasks; i++) {
        DAGNode *node = &ms->graph.nodes[i];
        entries[i].task_id   = (uint32_t)node->task_id;
        entries[i].state     = (uint32_t)node->state;
        entries[i].exit_code = 0; /* exit code not stored in DAGNode */
    }

    /* Pack and send the response */
    uint8_t buf[RECV_BUF_SIZE];
    int n = proto_pack_status_response(buf, sizeof(buf),
                                       entries, num_tasks);
    if(n < 0) {
        fprintf(stderr,
            "[server] Failed to pack status response for fd=%d\n",
            w->socket_fd);
        return;
    }

    pthread_mutex_lock(&w->write_lock);
    write(w->socket_fd, buf, n);
    pthread_mutex_unlock(&w->write_lock);

    printf("[server] Sent status response to observer fd=%d "
           "(%u tasks)\n", w->socket_fd, num_tasks);
}