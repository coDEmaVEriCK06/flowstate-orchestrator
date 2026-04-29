#ifndef IPC_SYNC_H
#define IPC_SYNC_H

#include <sys/types.h>
#include <stdint.h>

/* System V objects are identified using integer keys and not
 * memory addresses or file paths so, any process knowing the key
 * can access the same semaphore or message queue */

/* Keys are application-defined constants. We use hex values
 * encoding ASCII text to make them recognizable in ipcs output:
 *   SEM_LOG_KEY = 0x464C4F57 = 'F','L','O','W'
 *   MQ_LOG_KEY  = 0x464C4F58 = 'F','L','O','X'
 *
 * The cast to (key_t) is explicit for type safety — key_t is
 * typically a 32-bit signed integer on Linux. */

#define SEM_LOG_KEY  ((key_t)0x464C4F57) // FLOW chosen for recognizing project key when ipcs is used on terminal
#define MQ_LOG_KEY   ((key_t)0x464C4F58) // FLOX is just to keep it different from FLOW and relevant to the project name

/* All the following functions are prefixed with ipc_sem_ 
 * to avoid clashes with POSIX functions like sem_wait, sem_post */

int ipc_sem_create(key_t key, int initial_value);

int ipc_sem_wait(int semid);

int ipc_sem_signal(int semid);

int ipc_sem_destroy(int semid);

/* MSGMAX allows a max value of 8192 bytes
 *   Using LOG_CHUNK_SIZE = 512 keeps each LogMsg
 *   payload at 532 bytes — safe on any configuration.
 *
 *   A task output of MAX_OUTPUT_LEN (4096) bytes requires
 *   at most ceil(4096 / 512) = 8 chunks.
 *
 * Message type values:
 *   LOG_MTYPE_WORK:     a normal log chunk to process
 *   LOG_MTYPE_SHUTDOWN: tells the logger thread to exit
 *
 * These are long values because mtype MUST be a long — it is
 * a hard requirement of the System V MQ API. */

#define LOG_CHUNK_SIZE      512
#define LOG_MTYPE_WORK      1L
#define LOG_MTYPE_SHUTDOWN  2L

/* CRITICAL LAYOUT RULE: mtype must be the first field and must
 * be of type long. This is not a convention — it is a hard
 * requirement of msgsnd() and msgrcv(). 
 * mtype field is not counted in the payload size */

typedef struct 
{
    long     mtype;                /* LOG_MTYPE_WORK or _SHUTDOWN   */
    int      task_id;              /* which task this chunk belongs  */
    int      exit_code;            /* stored in every chunk for simplicity */
    int      chunk_id;             /* 0-based sequence number        */
    int      is_last;              /* 1 if this is the final chunk   */
    uint32_t data_len;             /* valid bytes in data[]          */
    char     data[LOG_CHUNK_SIZE]; /* chunk of captured output       */
} LogMsg;

#define LOG_MSG_PAYLOAD_SIZE  (sizeof(LogMsg) - sizeof(long)) // Passing mtype also here would cause E2BIG or truncate errors

int logger_mq_create(key_t key);

int logger_mq_send(int msqid, int task_id, int exit_code, const char *output, uint32_t output_len); 

int logger_mq_recv(int msqid, LogMsg *msg);

int logger_mq_destroy(int msqid);

typedef struct // Declared here so that both, master binary and test_ipc can use it
{
    int         msqid;           /* logger message queue id           */
    int         semid;           /* binary semaphore for log file     */
    const char *log_filepath;    /* path to execution.log             */
} LoggerArgs;

void *logger_thread_fn(void *arg); // Logger thread entry point

#endif