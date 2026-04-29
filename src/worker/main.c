#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "protocol.h"
#include "worker.h"

void client_run(const char *host, int port, const char *auth_token);
void client_install_signals(void);

static void print_usage(const char *prog) 
{
    fprintf(stderr,
        "Usage: %s <master_ip> <port> <auth_token>\n"
        "  master_ip:  IP address of the master (e.g. 127.0.0.1)\n"
        "  port:       TCP port the master is listening on\n"
        "  auth_token: shared secret matching the master's token\n"
        "\nExample:\n"
        "  %s 127.0.0.1 8080 mysecret\n",
        prog, prog);
}

int main(int argc, char *argv[]) 
{
    if(argc != 4) 
    {
        print_usage(argv[0]);
        return 1;
    }

    const char *host       = argv[1];
    int         port       = atoi(argv[2]);
    const char *auth_token = argv[3];

    if(port <= 0 || port > 65535) 
    {
        fprintf(stderr, "[worker] Invalid port: %s\n", argv[2]);
        return 1;
    }
    if(strlen(auth_token) == 0) 
    {
        fprintf(stderr, "[worker] Auth token cannot be empty\n");
        return 1;
    }

    printf("[worker] Starting — master=%s port=%d\n", host, port);

    /* Install signal handlers before any blocking calls */
    client_install_signals();

    /* Enter the connection and execution loop.
     * This function does not return until g_shutdown is set
     * by SIGINT or SIGTERM. */
    client_run(host, port, auth_token);

    printf("[worker] Exiting cleanly\n");
    return 0;
}