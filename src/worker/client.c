#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>   /* socket, connect, send, recv        */
#include <sys/select.h>   /* select, fd_set, FD_SET, FD_ZERO    */
#include <netinet/in.h>   /* sockaddr_in, INADDR_ANY            */
#include <arpa/inet.h>    /* inet_addr, htons                   */
#include <signal.h>       /* signal, SIGPIPE                    */

#include "protocol.h"
#include "worker.h"
#include "ipc_sync.h"


/* How long select() waits before checking for shutdown.
 * Shorter = more responsive to Ctrl+C but more CPU wake-ups. */
#define SELECT_TIMEOUT_SEC    5

/* Initial reconnect wait in seconds. Doubles on each failure
 * up to RECONNECT_MAX_SEC. */
#define RECONNECT_INIT_SEC    2
#define RECONNECT_MAX_SEC     30

static volatile sig_atomic_t g_shutdown = 0;

static void signal_handler(int sig) 
{
    (void)sig;
    g_shutdown = 1;
}

static int tcp_connect(const char *host, int port) 
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if(fd < 0) 
    {
        perror("[client] socket");
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons((uint16_t)port);

    /* inet_addr converts a dotted-decimal IP string to binary.
     * We accept IP addresses only — no DNS resolution needed
     * for a local development system. */
    addr.sin_addr.s_addr = inet_addr(host);
    if(addr.sin_addr.s_addr == INADDR_NONE) 
    {
        fprintf(stderr, "[client] Invalid IP address: %s\n", host);
        close(fd);
        return -1;
    }

    if(connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) 
    {
        perror("[client] connect");
        close(fd);
        return -1;
    }
    return fd;
}

// do_handshake — send MSG_CONNECT and wait for MSG_CONNECT_ACK.
static int do_handshake(int fd, const char *auth_token, ConnRecvBuffer *rbuf) 
{
    /* Pack and send MSG_CONNECT */
    uint8_t buf[256];
    int n = proto_pack_connect(buf, sizeof(buf), ROLE_WORKER, auth_token);
    if(n < 0 || write(fd, buf, n) != n) 
    {
        fprintf(stderr, "[client] Failed to send MSG_CONNECT\n");
        return -1;
    }
    printf("[client] Sent MSG_CONNECT — awaiting ACK...\n");

    /* Wait for MSG_CONNECT_ACK with a 10-second timeout.
     * If the master does not respond in 10 seconds, something
     * is wrong — treat as connection failure. */
    fd_set rfds;
    struct timeval tv = { 10, 0 };
    FD_ZERO(&rfds);
    FD_SET(fd, &rfds);

    int ready = select(fd + 1, &rfds, NULL, NULL, &tv);
    if(ready <= 0) 
    {
        fprintf(stderr, "[client] Timeout waiting for MSG_CONNECT_ACK\n");
        return -1;
    }

    /* Read into receive buffer */
    uint8_t tmp[512];
    ssize_t nr = read(fd, tmp, sizeof(tmp));
    if(nr <= 0) 
    {
        fprintf(stderr, "[client] Connection closed during handshake\n");
        return -1;
    }

    conn_recv_push(rbuf, tmp, (uint32_t)nr);

    /* Deframe the ACK */
    uint8_t  payload[RECV_BUF_SIZE];
    uint32_t msg_type, payload_len;
    if(proto_try_deframe(rbuf, &msg_type, payload, &payload_len) != DEFRAME_COMPLETE) 
    {
        fprintf(stderr, "[client] Incomplete MSG_CONNECT_ACK\n");
        return -1;
    }

    if(msg_type != MSG_CONNECT_ACK) 
    {
        fprintf(stderr, "[client] Expected MSG_CONNECT_ACK, got 0x%02x\n", msg_type);
        return -1;
    }

    /* Parse the ACK status */
    uint32_t status;
    if(proto_parse_connect_ack(payload, payload_len, &status) < 0) 
    {
        fprintf(stderr, "[client] Failed to parse MSG_CONNECT_ACK\n");
        return -1;
    }

    if(status != CONNECT_STATUS_OK) 
    {
        fprintf(stderr, "[client] Master rejected connection " "(bad token or max workers reached)\n");
        return -1;
    }
    printf("[client] Handshake complete — connected and IDLE\n");
    return 0;
}

/* run_worker_loop — the main dispatch/execute/result loop.
 *
 * Runs until the master disconnects, a fatal error occurs,
 * or g_shutdown is set.
 *
 * Returns 0 for clean exit, -1 for connection error
 * (signals the caller to reconnect). */
static int run_worker_loop(int fd, ConnRecvBuffer *rbuf) 
{
    uint8_t  wire_buf[RECV_BUF_SIZE * 2];
    uint8_t  payload[RECV_BUF_SIZE];
    uint32_t msg_type, payload_len;

    while(!g_shutdown) 
    {
        /* ---- Wait for data on the socket using select() ----
         *
         * We use select() with a timeout rather than blocking
         * directly on read(). This lets us:
         *   1. Check g_shutdown periodically even with no traffic
         *   2. Detect a dead connection via timeout
         *
         * SELECT_TIMEOUT_SEC seconds is generous — the master
         * should send a dispatch within seconds of a task
         * becoming READY. */
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(fd, &rfds);
        struct timeval tv = { SELECT_TIMEOUT_SEC, 0 };

        int ready = select(fd + 1, &rfds, NULL, NULL, &tv);

        if(ready < 0) 
        {
            if(errno == EINTR) continue;  /* signal — re-check g_shutdown */
            perror("[client] select");
            return -1;
        }

        if(ready == 0) 
        {
            /* Timeout — no data from master.
             * This is normal when the DAG has no READY tasks.
             * Loop and wait again. */
            continue;
        }

        /* ---- Read data from socket ---- */
        uint8_t tmp[1024];
        ssize_t nr = read(fd, tmp, sizeof(tmp));

        if(nr == 0) 
        {
            printf("[client] Master closed connection\n");
            return -1;  /* trigger reconnect */
        }
        if(nr < 0) 
        {
            if(errno == EINTR) continue;
            perror("[client] read");
            return -1;
        }

        /* Accumulate into receive buffer */
        if(conn_recv_push(rbuf, tmp, (uint32_t)nr) < 0) 
        {
            fprintf(stderr, "[client] Receive buffer overflow\n");
            return -1;
        }

        /* ---- Deframe loop ----
         * One read() may contain multiple complete messages.
         * Process all of them before going back to select(). */
        while(proto_try_deframe(rbuf, &msg_type, payload, &payload_len) == DEFRAME_COMPLETE) 
        {
            if(msg_type != MSG_DISPATCH) 
            {
                fprintf(stderr, "[client] Unexpected message type 0x%02x\n", msg_type);
                continue;
            }

            /* ---- Parse MSG_DISPATCH ---- */
            uint32_t task_id, generation;
            char     command[256];

            if(proto_parse_dispatch(payload, payload_len, &task_id, &generation, command) < 0) 
            {
                fprintf(stderr, "[client] Failed to parse MSG_DISPATCH\n");
                continue;
            }

            printf("[client] Received task %u (gen %u): %s\n", task_id, generation, command);

            /* ---- Send MSG_ACK immediately ----
             *
             * ACK before executing — tells the master we received
             * the dispatch and are starting execution. This triggers
             * DISPATCHED → RUNNING in the master's state machine.
             * If we ACK after execution, a crash during execution
             * would look like a dispatch timeout to the master. */
            int n = proto_pack_ack(wire_buf, sizeof(wire_buf), task_id, generation);
            if(n < 0 || write(fd, wire_buf, n) != n) 
            {
                fprintf(stderr, "[client] Failed to send MSG_ACK\n");
                return -1;
            }
            printf("[client] Sent MSG_ACK for task %u\n", task_id);

            
            /* executor_run() blocks until the command finishes or
             * times out. This is the Phase 3 executor doing its job.
             * The worker is busy here — it cannot receive another
             * dispatch until this returns. This is by design:
             * one task at a time per worker. */
            printf("[client] Executing: %s\n", command);
            ExecResult result = executor_run(command, TASK_TIMEOUT_SEC);

            if(result.timed_out) 
            {
                printf("[client] Task %u TIMED OUT\n", task_id);
            } 
            else 
            {
                printf("[client] Task %u finished (exit code %d)\n", task_id, result.exit_code);
            }

            /* ---- Send MSG_RESULT ---- */
            n = proto_pack_result(wire_buf, sizeof(wire_buf),
                                  task_id,
                                  generation,
                                  result.timed_out ? -1 : result.exit_code,
                                  result.output,
                                  result.output_len);

            if(n < 0 || write(fd, wire_buf, n) != n) 
            {
                fprintf(stderr, "[client] Failed to send MSG_RESULT for task %u\n", task_id);
                return -1;
            }

            printf("[client] Sent MSG_RESULT for task %u " "(output: %u bytes)\n", task_id, result.output_len);
        }
    }
    return 0;
}

/* client_run — top-level entry point called by worker/main.c.
 *
 * Implements the reconnection loop with exponential backoff.
 * Exits only when g_shutdown is set or max retries reached. */
void client_run(const char *host, int port, const char *auth_token) 
{
    int reconnect_delay = RECONNECT_INIT_SEC;
    while(!g_shutdown) 
    {
        printf("[client] Connecting to %s:%d...\n", host, port);
        int fd = tcp_connect(host, port);
        if(fd < 0) 
        {
            printf("[client] Connection failed. Retrying in %d seconds...\n", reconnect_delay);
            sleep(reconnect_delay);
            reconnect_delay = (reconnect_delay * 2 > RECONNECT_MAX_SEC) ? RECONNECT_MAX_SEC : reconnect_delay * 2;
            continue;
        }
        printf("[client] TCP connection established (fd=%d)\n", fd);
        reconnect_delay = RECONNECT_INIT_SEC;  /* reset backoff on success */

        /* Initialize the receive buffer for this connection */
        ConnRecvBuffer rbuf;
        conn_recv_init(&rbuf);

        /* Perform the auth handshake */
        if(do_handshake(fd, auth_token, &rbuf) < 0) 
        {
            close(fd);
            printf("[client] Handshake failed. Retrying in %d seconds...\n", reconnect_delay);
            sleep(reconnect_delay);
            reconnect_delay = (reconnect_delay * 2 > RECONNECT_MAX_SEC) ? RECONNECT_MAX_SEC : reconnect_delay * 2;
            continue;
        }

        /* Enter the main dispatch loop */
        int ret = run_worker_loop(fd, &rbuf);
        close(fd);

        if(ret < 0 && !g_shutdown) 
        {
            printf("[client] Disconnected. Retrying in %d seconds...\n", reconnect_delay);
            sleep(reconnect_delay);
            reconnect_delay = (reconnect_delay * 2 > RECONNECT_MAX_SEC) ? RECONNECT_MAX_SEC : reconnect_delay * 2;
        }
    }
    printf("[client] Shutdown complete\n");
}

/* client_install_signals — install signal handlers.
 * Called by worker/main.c before client_run(). */
void client_install_signals(void) 
{
    /* Suppress SIGPIPE — writing to a closed master socket
     * must return EPIPE not terminate the worker */
    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
}