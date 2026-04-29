/*
 * tests/test_ipc.c — Phase 4 validation for the IPC sync layer.
 *
 * Run: make test_ipc && ./test_ipc
 * Expected: 5 tests PASS, exit code 0.
 *
 * NOTE: System V IPC objects persist if the test crashes.
 * Run 'ipcs' to see orphaned objects, 'ipcrm' to remove them.
 * Each test pre-cleans its keys to handle this gracefully.
 *
 * Test 2 uses pthreads to verify semaphore blocking behavior.
 * Test 5 starts the actual logger thread and verifies file output.
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/msg.h>

#include "ipc_sync.h"
#include "worker.h"

/* ============================================================
 * Test framework
 * ============================================================ */
static int g_passed = 0;
static int g_failed = 0;

static void run_test(const char *name, int (*fn)(void)) {
    printf("\n[TEST] %s\n", name);
    if (fn() == 0) {
        printf("  --> PASS\n");
        g_passed++;
    } else {
        printf("  --> FAIL\n");
        g_failed++;
    }
}

#define ASSERT(cond, msg)                                               \
    do {                                                                \
        if (!(cond)) {                                                  \
            printf("  ASSERT FAILED: %s  (line %d)\n", msg, __LINE__); \
            return -1;                                                  \
        } else {                                                        \
            printf("  ok: %s\n", msg);                                  \
        }                                                               \
    } while (0)

/* Test-specific IPC keys — different from master keys to avoid
 * conflicts if a master process happens to be running. */
#define TEST_SEM_KEY  ((key_t)0x54455354)   /* 'TEST' */
#define TEST_MQ_KEY   ((key_t)0x54455355)   /* 'TESU' */

/* ============================================================
 * pre_clean — remove any leftover IPC objects from a previous
 * crashed test run. Silently ignores errors (object may not exist).
 * ============================================================ */
static void pre_clean(void) {
    int id;
    id = semget(TEST_SEM_KEY, 1, 0666);
    if (id >= 0) semctl(id, 0, IPC_RMID);

    id = msgget(TEST_MQ_KEY, 0666);
    if (id >= 0) msgctl(id, IPC_RMID, NULL);
}

/* ============================================================
 * Test 1: Semaphore lifecycle
 *
 * Creates a semaphore with initial value 1.
 * Performs one wait (should not block — value goes 1→0).
 * Performs one signal (value goes 0→1).
 * Destroys it and verifies the semid is gone.
 * ============================================================ */
static int test_sem_lifecycle(void) {
    pre_clean();

    int semid = ipc_sem_create(TEST_SEM_KEY, 1);
    ASSERT(semid >= 0, "ipc_sem_create returns valid semid");

    /* Wait should not block because value is 1 */
    ASSERT(ipc_sem_wait(semid) == 0,
           "ipc_sem_wait succeeds immediately when value=1");

    /* Signal restores the value to 1 */
    ASSERT(ipc_sem_signal(semid) == 0,
           "ipc_sem_signal increments from 0 to 1");

    ASSERT(ipc_sem_destroy(semid) == 0,
           "ipc_sem_destroy removes the semaphore from the kernel");

    /* Verify it is gone: semget without IPC_CREAT should fail */
    int check = semget(TEST_SEM_KEY, 1, 0666);
    ASSERT(check < 0,
           "semget after destroy returns error (object is gone)");

    return 0;
}

/* ============================================================
 * Test 2: Binary semaphore blocking behavior
 *
 * Verifies that ipc_sem_wait() actually blocks when value=0.
 * Uses a pthread to be the blocked waiter. Main thread signals
 * after 50ms and verifies the waiter unblocked.
 * ============================================================ */
typedef struct {
    int semid;
    int unblocked;
} SemWaiterArg;

static void *sem_waiter_fn(void *arg) {
    SemWaiterArg *a = (SemWaiterArg *)arg;
    /* This call blocks until main thread signals */
    ipc_sem_wait(a->semid);
    a->unblocked = 1;
    return NULL;
}

static int test_sem_blocking(void) {
    pre_clean();

    /* Initial value 0 — waiter will block */
    int semid = ipc_sem_create(TEST_SEM_KEY, 0);
    ASSERT(semid >= 0, "Semaphore created with initial value 0");

    SemWaiterArg arg = { .semid = semid, .unblocked = 0 };

    pthread_t tid;
    pthread_create(&tid, NULL, sem_waiter_fn, &arg);

    /* 50ms: enough time for the thread to start and block */
    usleep(50000);
    ASSERT(arg.unblocked == 0,
           "Thread is blocked on semaphore (value=0)");

    /* Signal the semaphore — waiter should unblock */
    ipc_sem_signal(semid);
    pthread_join(tid, NULL);

    ASSERT(arg.unblocked == 1,
           "Thread unblocked after ipc_sem_signal");

    ipc_sem_destroy(semid);
    return 0;
}

/* ============================================================
 * Test 3: MQ single message round-trip
 *
 * Sends output that fits in one chunk.
 * Verifies all fields survive msgsnd/msgrcv intact.
 * ============================================================ */
static int test_mq_single(void) {
    pre_clean();

    int msqid = logger_mq_create(TEST_MQ_KEY);
    ASSERT(msqid >= 0, "logger_mq_create returns valid msqid");

    const char *output = "Task completed successfully\n";
    uint32_t    len    = (uint32_t)strlen(output);

    ASSERT(logger_mq_send(msqid, 3, 0, output, len) == 0,
           "logger_mq_send succeeds for single-chunk output");

    LogMsg msg;
    ASSERT(logger_mq_recv(msqid, &msg) == 0,
           "logger_mq_recv receives the message");

    ASSERT(msg.mtype    == LOG_MTYPE_WORK, "mtype is LOG_MTYPE_WORK");
    ASSERT(msg.task_id  == 3,              "task_id=3 round-trips");
    ASSERT(msg.exit_code == 0,             "exit_code=0 round-trips");
    ASSERT(msg.chunk_id == 0,              "chunk_id=0 for first chunk");
    ASSERT(msg.is_last  == 1,              "is_last=1 (fits in one chunk)");
    ASSERT(msg.data_len == len,            "data_len matches output length");
    ASSERT(memcmp(msg.data, output, len) == 0,
           "data[] contains original output bytes exactly");

    logger_mq_destroy(msqid);
    return 0;
}

/* ============================================================
 * Test 4: MQ chunked send and receive
 *
 * Constructs output requiring exactly 3 chunks:
 *   chunk 0: LOG_CHUNK_SIZE bytes, is_last=0
 *   chunk 1: LOG_CHUNK_SIZE bytes, is_last=0
 *   chunk 2: 100 bytes, is_last=1
 *
 * Verifies chunk_id sequence, is_last flag, and data_len.
 * ============================================================ */
static int test_mq_chunked(void) {
    pre_clean();

    int msqid = logger_mq_create(TEST_MQ_KEY);
    ASSERT(msqid >= 0, "MQ created for chunked test");

    /* Build output: 2 full chunks + 100 bytes = 3 chunks total */
    uint32_t big_len = LOG_CHUNK_SIZE * 2 + 100;
    char *big_output = malloc(big_len + 1);
    ASSERT(big_output != NULL, "malloc for large output buffer");
    /* Fill with 'B' so we can verify data integrity */
    memset(big_output, 'B', big_len);
    big_output[big_len] = '\0';

    ASSERT(logger_mq_send(msqid, 4, 1, big_output, big_len) == 0,
           "logger_mq_send for chunked output (3 chunks expected)");

    LogMsg msg;

    /* Receive chunk 0 */
    ASSERT(logger_mq_recv(msqid, &msg) == 0,   "Chunk 0 received");
    ASSERT(msg.chunk_id  == 0,                 "chunk_id=0");
    ASSERT(msg.is_last   == 0,                 "is_last=0 (more chunks follow)");
    ASSERT(msg.data_len  == LOG_CHUNK_SIZE,    "chunk 0 is exactly LOG_CHUNK_SIZE");
    ASSERT(msg.data[0]   == 'B',               "chunk 0 data is correct");

    /* Receive chunk 1 */
    ASSERT(logger_mq_recv(msqid, &msg) == 0,   "Chunk 1 received");
    ASSERT(msg.chunk_id  == 1,                 "chunk_id=1");
    ASSERT(msg.is_last   == 0,                 "is_last=0");
    ASSERT(msg.data_len  == LOG_CHUNK_SIZE,    "chunk 1 is exactly LOG_CHUNK_SIZE");

    /* Receive chunk 2 (last, partial) */
    ASSERT(logger_mq_recv(msqid, &msg) == 0,   "Chunk 2 received");
    ASSERT(msg.chunk_id  == 2,                 "chunk_id=2");
    ASSERT(msg.is_last   == 1,                 "is_last=1 (final chunk)");
    ASSERT(msg.data_len  == 100,               "final chunk has 100 bytes");
    ASSERT(msg.exit_code == 1,                 "exit_code=1 in final chunk");
    ASSERT(msg.task_id   == 4,                 "task_id=4 in final chunk");

    free(big_output);
    logger_mq_destroy(msqid);
    return 0;
}

/* ============================================================
 * Test 5: Logger thread end-to-end
 *
 * Starts the real logger thread, sends a log entry via the MQ,
 * waits for the thread to process it, reads the log file,
 * and verifies the expected content is present.
 *
 * This test exercises the complete pipeline:
 *   logger_mq_send → MQ → logger_thread_fn → reassembly →
 *   ipc_sem_wait → fcntl F_SETLKW → write → fcntl F_UNLCK →
 *   ipc_sem_signal → log file
 * ============================================================ */
static int test_logger_thread(void) {
    pre_clean();

    const char *log_path = "/tmp/flowstate_test_phase4.log";
    /* Remove any file left from a previous run */
    unlink(log_path);

    int msqid = logger_mq_create(TEST_MQ_KEY);
    ASSERT(msqid >= 0, "MQ created for logger thread test");

    /* Binary semaphore protecting the log file — initial value 1 */
    int semid = ipc_sem_create(TEST_SEM_KEY, 1);
    ASSERT(semid >= 0, "Semaphore created for log file protection");

    LoggerArgs args = {
        .msqid        = msqid,
        .semid        = semid,
        .log_filepath = log_path
    };

    /* Start the logger thread — it blocks in logger_mq_recv() */
    pthread_t logger_tid;
    pthread_create(&logger_tid, NULL, logger_thread_fn, &args);

    /* Send a log entry for task 2, exit code 0 */
    const char *output = "Executing Task 2...\nTask 2 Complete!\n";
    ASSERT(logger_mq_send(msqid, 2, 0, output,
                          (uint32_t)strlen(output)) == 0,
           "logger_mq_send for task 2 succeeds");

    /* 200ms: generous for a file write on any system */
    usleep(200000);

    /* Send shutdown message — logger thread will break its loop */
    LogMsg shutdown_msg;
    memset(&shutdown_msg, 0, sizeof(LogMsg));
    shutdown_msg.mtype = LOG_MTYPE_SHUTDOWN;
    msgsnd(msqid, &shutdown_msg, LOG_MSG_PAYLOAD_SIZE, 0);

    /* Wait for logger thread to finish */
    pthread_join(logger_tid, NULL);

    /* Read the log file and verify its contents */
    FILE *fp = fopen(log_path, "r");
    ASSERT(fp != NULL, "Log file was created by the logger thread");

    char contents[2048];
    memset(contents, 0, sizeof(contents));
    fread(contents, 1, sizeof(contents) - 1, fp);
    fclose(fp);

    ASSERT(strstr(contents, "Task 2") != NULL,
           "Log file contains 'Task 2' identifier");
    ASSERT(strstr(contents, "Exit: 0") != NULL,
           "Log file contains 'Exit: 0'");
    ASSERT(strstr(contents, "Executing Task 2") != NULL,
           "Log file contains captured task output");
    ASSERT(strstr(contents, "Task 2 Complete!") != NULL,
           "Log file contains second line of task output");

    /* Cleanup */
    ipc_sem_destroy(semid);
    logger_mq_destroy(msqid);
    unlink(log_path);

    return 0;
}

/* ============================================================
 * main
 * ============================================================ */
int main(void) {
    printf("=========================================\n");
    printf("  Phase 4: IPC Sync Layer Test Suite\n");
    printf("=========================================\n");

    run_test("Semaphore lifecycle",           test_sem_lifecycle);
    run_test("Binary semaphore blocking",     test_sem_blocking);
    run_test("MQ single message round-trip",  test_mq_single);
    run_test("MQ chunked message (3 chunks)", test_mq_chunked);
    run_test("Logger thread end-to-end",      test_logger_thread);

    printf("\n=========================================\n");
    printf("  Results: %d passed, %d failed\n", g_passed, g_failed);
    printf("=========================================\n");

    return (g_failed == 0) ? 0 : 1;
}