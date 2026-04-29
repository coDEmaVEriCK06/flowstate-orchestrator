#define _GNU_SOURCE 
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/ipc.h>  /* IPC_CREAT, IPC_RMID, key_t      */
#include <sys/sem.h>  /* semget, semop, semctl, sembuf    */
#include <sys/msg.h>  /* msgget, msgsnd, msgrcv, msgctl  */

#include "ipc_sync.h"

/* union semun — required argument type for semctl().
 *
 * This union must be defined by the application on Linux.
 * If you include <sys/sem.h> without this definition and
 * call semctl() with SETVAL, you get a type error or
 * silent stack corruption on 64-bit systems. */

union semun 
{
    int              val;    /* value for SETVAL command         */
    struct semid_ds *buf;    /* buffer for IPC_STAT command      */
    unsigned short  *array;  /* array for GETALL/SETALL commands */
};

int ipc_sem_create(key_t key, int initial_value) 
{
    /* semget() creates or retrieves a semaphore set.
     *
     * key:    our application-defined identifier
     * nsems:  1 — we only need one semaphore in the set
     * semflg: IPC_CREAT = create if not existing, return if it does
     *         0666      = read/write permission for owner/group/others
     *
     * The difference between IPC_CREAT and IPC_CREAT|IPC_EXCL:
     *   IPC_CREAT alone: return existing or create new (our choice)
     *   IPC_CREAT|IPC_EXCL: fail if already exists (like O_EXCL for files)
     *
     * We use IPC_CREAT alone so master restart reuses the same object. */
    int semid = semget(key, 1, IPC_CREAT | 0666);
    if(semid < 0) 
    {
        perror("[ipc_sem_create] semget");
        return -1;
    }

    /* Set the initial value using semctl() SETVAL command.
     *
     * semctl(semid, semnum, cmd, ...):
     *   semid:  the semaphore set identifier
     *   semnum: which semaphore in the set (0 = the first and only one)
     *   cmd:    SETVAL — set the value of semaphore semnum
     *   arg:    union semun with .val = desired initial value
     *
     * For a binary semaphore used as a mutex: initial_value = 1
     *   value=1 means "resource available" (one thread can acquire)
     *   value=0 means "resource taken" (all waiters block)
     *
     * For a counting semaphore: initial_value = N
     *   allows N concurrent holders before blocking */
    union semun arg;
    arg.val = initial_value;
    if(semctl(semid, 0, SETVAL, arg) < 0) 
    {
        perror("[ipc_sem_create] semctl SETVAL");
        return -1;
    }
    return semid;
}

int ipc_sem_wait(int semid) 
{
    /* struct sembuf describes one operation on one semaphore.
     *
     * sem_num: which semaphore in the set to operate on (0 = ours)
     * sem_op:  the operation value:
     *   positive: add to the semaphore value (V/signal)
     *   negative: subtract from value (P/wait) — block if result < 0
     *   zero:     block until value reaches zero (rarely used)
     * sem_flg: flags:
     *   0:        default — block if operation cannot proceed
     *   IPC_NOWAIT: return EAGAIN instead of blocking */
    struct sembuf op;
    op.sem_num = 0;
    op.sem_op  = -1;   /* decrement: P operation */
    op.sem_flg = 0;    /* block if value would go below 0 */

    /* semop() executes the operation atomically.
     *
     * The while loop retries on EINTR. This happens when a signal
     * (like SIGALRM from our executor) interrupts the blocking wait.
     * Without the retry, a signal firing while the logger thread is
     * waiting for the semaphore would cause a spurious failure.
     *
     * Third argument is 1 — number of operations in the array.
     * semop() can perform multiple operations atomically; we only need one. */
    while(semop(semid, &op, 1) < 0) 
    {
        if(errno == EINTR) continue;  /* signal interrupted — retry */
        perror("[ipc_sem_wait] semop");
        return -1;
    }
    return 0;
}

int ipc_sem_signal(int semid) 
{
    struct sembuf op;
    op.sem_num = 0;
    op.sem_op  = 1;   /* increment: V operation — always succeeds immediately */
    op.sem_flg = 0;

    /* V operations never block (incrementing can never fail due to
     * value constraints). We still retry on EINTR for consistency. */
    while(semop(semid, &op, 1) < 0) 
    {
        if(errno == EINTR) continue;
        perror("[ipc_sem_signal] semop");
        return -1;
    }
    return 0;
}

int ipc_sem_destroy(int semid) 
{
    /* IPC_RMID removes the semaphore set from the kernel immediately.
     * Any processes currently blocked in semop() (ipc_sem_wait)
     * are woken and receive EIDRM error.
     *
     * The third argument to semctl() with IPC_RMID is ignored
     * but must be provided — we pass a zeroed union. */
    union semun arg;
    arg.val = 0;
    if(semctl(semid, 0, IPC_RMID, arg) < 0) 
    {
        perror("[ipc_sem_destroy] semctl IPC_RMID");
        return -1;
    }
    return 0;
}

int logger_mq_create(key_t key) 
{
    /* msgget() creates or retrieves a System V message queue.
     * Same IPC_CREAT | 0666 pattern as semget(). */
    int msqid = msgget(key, IPC_CREAT | 0666);
    if(msqid < 0) 
    {
        perror("[logger_mq_create] msgget");
        return -1;
    }
    return msqid;
}

int logger_mq_send(int msqid, int task_id, int exit_code, const char *output, uint32_t output_len) 
{
    LogMsg  msg;
    int     chunk_id = 0;
    uint32_t offset  = 0;

    /* Special case: empty output — send one empty final chunk.
     * The logger must receive at least one chunk with is_last=1
     * to know the task is complete and write the log entry. */
    if(output_len == 0) 
    {
        memset(&msg, 0, sizeof(LogMsg));
        msg.mtype     = LOG_MTYPE_WORK;
        msg.task_id   = task_id;
        msg.exit_code = exit_code;
        msg.chunk_id  = 0;
        msg.is_last   = 1;
        msg.data_len  = 0;

        if(msgsnd(msqid, &msg, LOG_MSG_PAYLOAD_SIZE, 0) < 0) 
        {
            perror("[logger_mq_send] msgsnd (empty)");
            return -1;
        }
        return 0;
    }

    /* Split output into LOG_CHUNK_SIZE byte chunks.
     * Each iteration sends one chunk.
     *
     * Trace through example:
     *   output_len=600, LOG_CHUNK_SIZE=512
     *   Iteration 1: offset=0, remaining=600, chunk_bytes=512, is_last=0
     *   Iteration 2: offset=512, remaining=88, chunk_bytes=88, is_last=1
     */
    while(offset < output_len) 
    {
        memset(&msg, 0, sizeof(LogMsg));
        uint32_t remaining   = output_len - offset;
        uint32_t chunk_bytes = (remaining > LOG_CHUNK_SIZE) ? LOG_CHUNK_SIZE : remaining;

        msg.mtype     = LOG_MTYPE_WORK;
        msg.task_id   = task_id;
        msg.exit_code = exit_code;
        msg.chunk_id  = chunk_id;
        msg.data_len  = chunk_bytes;

        /* is_last: true when this chunk contains the final byte */
        msg.is_last = (offset + chunk_bytes >= output_len) ? 1 : 0;

        /* Copy exactly chunk_bytes from output into msg.data */
        memcpy(msg.data, output + offset, chunk_bytes);

        /* msgsnd(msqid, msgp, msgsz, msgflg):
         *   msgp:   pointer to our LogMsg struct
         *   msgsz:  LOG_MSG_PAYLOAD_SIZE — everything except mtype
         *           THIS IS CRITICAL: do not pass sizeof(LogMsg)
         *   msgflg: 0 — block if queue is full (do not drop logs) */
        if(msgsnd(msqid, &msg, LOG_MSG_PAYLOAD_SIZE, 0) < 0) 
        {
            perror("[logger_mq_send] msgsnd");
            return -1;
        }
        offset   += chunk_bytes;
        chunk_id++;
    }
    return 0;
}

int logger_mq_recv(int msqid, LogMsg *msg) 
{
    ssize_t ret;

    /* msgrcv(msqid, msgp, msgsz, msgtyp, msgflg):
     *   msgsz:   LOG_MSG_PAYLOAD_SIZE — must match what msgsnd used
     *   msgtyp:  0 — receive the FIRST message in the queue
     *            regardless of mtype (FIFO order). This means both
     *            LOG_MTYPE_WORK and LOG_MTYPE_SHUTDOWN are received.
     *            Positive msgtyp would filter to a specific type.
     *            Negative msgtyp would receive the lowest type first.
     *   msgflg:  0 — block if queue is empty
     *
     * Returns the number of bytes copied into msgp on success.
     * Returns -1 on error:
     *   EINTR:  interrupted by signal — retry
     *   EIDRM:  queue was deleted while we were waiting — exit
     *   EINVAL: invalid msqid (queue was deleted) — exit */
    do 
    {
        ret = msgrcv(msqid, msg, LOG_MSG_PAYLOAD_SIZE, 0, 0);
    } 
    while(ret < 0 && errno == EINTR);

    if(ret < 0) 
    {
        if(errno != EIDRM && errno != EINVAL) perror("[logger_mq_recv] msgrcv");
        return -1;
    }
    return 0;
}

int logger_mq_destroy(int msqid) 
{
    /* IPC_RMID deletes the message queue. Any messages still
     * in the queue are discarded. Any blocked senders or receivers
     * wake with EIDRM error. */
    if(msgctl(msqid, IPC_RMID, NULL) < 0) 
    {
        perror("[logger_mq_destroy] msgctl IPC_RMID");
        return -1;
    }
    return 0;
}