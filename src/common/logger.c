#define _GNU_SOURCE // Used for dprintf
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>    /* write(), close(), open()       */
#include <fcntl.h>     /* open(), fcntl(), struct flock  */
#include <errno.h>

#include "ipc_sync.h"
#include "graph.h"     /* MAX_TASKS — for reassembly table size */
#include "worker.h"    /* EXEC_OUTPUT_MAX                       */

/* ReassemblyEntry — one slot in the reassembly table.
 *
 * Tasks are identified by integer IDs starting at 1.
 * We use (task_id - 1) as the array index.
 *
 * When a task's first chunk arrives, its slot becomes active.
 * Subsequent chunks append to buffer[]. When is_last=1,
 * the complete output is written to the log file and the
 * slot is cleared for reuse by a future task with the same ID. */
typedef struct 
{
    int      active;                      /* 1 if slot is in use      */
    int      task_id;
    int      exit_code;
    char     buffer[EXEC_OUTPUT_MAX + 1]; /* assembled output         */
    uint32_t bytes_filled;                /* valid bytes so far       */
} ReassemblyEntry;

/* Static table — lives for the lifetime of the logger thread.
 * static means it is zero-initialized at program start.
 * We also explicitly zero it at thread start for safety. */
static ReassemblyEntry g_reassembly[MAX_TASKS];

/* Protection strategy: two independent locks.
 *
 * 1. System V semaphore (ipc_sem_wait/signal):
 *    Serializes all writes within our system. Even if we had
 *    multiple logger threads or multiple master processes, only
 *    one could write at a time.
 *
 * 2. fcntl() F_SETLKW advisory write lock:
 *    Protects against any external process that opens the same
 *    log file. Advisory means it only works if ALL writers use
 *    fcntl() — but since we control all writers, this holds.
 *    F_SETLKW = "set lock, wait" — blocks until lock is granted.
 *
 * The two locks are acquired in order: semaphore first, then
 * fcntl. Released in reverse order: fcntl first, then semaphore.
 * This consistent ordering prevents deadlock. */

 static void write_log_entry(const char *filepath, int semid, int task_id, int exit_code, const char *output, uint32_t output_len) 
 {
    ipc_sem_wait(semid);

    /* Open log file: write-only, create if absent, append mode.
     * O_APPEND guarantees each write goes to the end atomically
     * even without the fcntl lock — together they provide
     * double protection.
     * 0644: owner read/write, group/others read-only. */
    int fd = open(filepath, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if(fd < 0) 
    {
        perror("[write_log_entry] open");
        ipc_sem_signal(semid);
        return;
    }
    /* Acquire fcntl() advisory write lock on the entire file.
     *
     * struct flock fields:
     *   l_type:   F_WRLCK = exclusive write lock
     *             (F_RDLCK would be a shared read lock)
     *             (F_UNLCK releases the lock)
     *   l_whence: SEEK_SET = offset is from beginning of file
     *   l_start:  0 = start from byte 0
     *   l_len:    0 = lock the entire file (to end of file)
     *
     * F_SETLKW command: set lock and wait (blocking).
     * F_SETLK command: set lock non-blocking (we use for unlock).
     *
     * The lock is automatically released when fd is closed,
     * but we release it explicitly before close() for clarity. */
    struct flock fl;
    memset(&fl, 0, sizeof(fl));
    fl.l_type   = F_WRLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start  = 0;
    fl.l_len    = 0;

    if(fcntl(fd, F_SETLKW, &fl) < 0) 
    {
        perror("[write_log_entry] fcntl F_SETLKW");
        close(fd);
        ipc_sem_signal(semid);
        return;
    }

    /* Write the formatted log entry.
     * dprintf() writes formatted output to a file descriptor
     * (like fprintf but to fd, not FILE*). Available with _GNU_SOURCE. */
    dprintf(fd, "=== Task %d | Exit: %d ===\n", task_id, exit_code);
    if(output_len > 0) 
    {
        write(fd, output, output_len);
        /* Guarantee the output ends with a newline. */
        if(output[output_len - 1] != '\n') write(fd, "\n", 1);
    }
    dprintf(fd, "=========================\n\n");

    /* Release fcntl() lock before close().
     * fl.l_type = F_UNLCK releases the lock.
     * F_SETLK (not F_SETLKW) is used for unlock — it never blocks. */
    fl.l_type = F_UNLCK;
    fcntl(fd, F_SETLK, &fl);
    close(fd);

    /* Release the System V semaphore — another logger can now write */
    ipc_sem_signal(semid);
}

void *logger_thread_fn(void *arg) 
{
    LoggerArgs *args = (LoggerArgs *)arg;

    /* Zero the reassembly table at thread start.
     * Even though static variables are zero-initialized at
     * program load, this protects against a thread restart. */
    memset(g_reassembly, 0, sizeof(g_reassembly));
    LogMsg msg;

    while(1) 
    {
        /* Block until a message chunk arrives.
         * Returns -1 if the queue is deleted (master shutdown). */
        if(logger_mq_recv(args->msqid, &msg) < 0) 
        {
            /* Queue was destroyed — exit cleanly */
            if(errno == EIDRM || errno == EINVAL) break;
            /* Any other error — continue trying */
            continue;
        }

        /* Shutdown message: master sends this before calling
         * pthread_join() on the logger thread. This gives the
         * logger a chance to flush any in-progress reassembly before exiting. */
        if(msg.mtype == LOG_MTYPE_SHUTDOWN) break;

        /* Validate task_id to prevent out-of-bounds array access */
        if(msg.task_id < 1 || msg.task_id > MAX_TASKS) 
        {
            fprintf(stderr, "[logger_thread] Invalid task_id: %d\n", msg.task_id);
            continue;
        }

        /* Look up or initialize the reassembly slot for this task.
         * task_id is 1-based, array is 0-based. */
        int idx = msg.task_id - 1;
        ReassemblyEntry *entry = &g_reassembly[idx];

        if(!entry->active) 
        {
            /* First chunk for this task — initialize the slot */
            entry->active       = 1;
            entry->task_id      = msg.task_id;
            entry->exit_code    = msg.exit_code;
            entry->bytes_filled = 0;
            memset(entry->buffer, 0, sizeof(entry->buffer));
        }

        /* Append this chunk's data to the reassembly buffer.
         * Cap at EXEC_OUTPUT_MAX to prevent overflow.
         * space = how many bytes are still available in the buffer. */
        if(msg.data_len > 0) 
        {
            uint32_t space   = EXEC_OUTPUT_MAX - entry->bytes_filled;
            uint32_t to_copy = (msg.data_len < space) ? msg.data_len : space;
            memcpy(entry->buffer + entry->bytes_filled, msg.data, to_copy);
            entry->bytes_filled += to_copy;
        }

        /* The last chunk carries the authoritative exit_code.
         * Update it here so we get the correct final value
         * even if earlier chunks had a stale copy. */
        if(msg.is_last) 
        {
            entry->exit_code = msg.exit_code;
        }

        /* When all chunks have arrived, write to the log file
         * and clear the slot for reuse. */
        if(msg.is_last) 
        {
            entry->buffer[entry->bytes_filled] = '\0';
            write_log_entry(args->log_filepath,
                            args->semid,
                            entry->task_id,
                            entry->exit_code,
                            entry->buffer,
                            entry->bytes_filled);

            /* Clear the slot — task_id can be reused in a future run */
            memset(entry, 0, sizeof(ReassemblyEntry));
        }
    }
    return NULL;
}