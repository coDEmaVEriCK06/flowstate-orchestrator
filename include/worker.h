#ifndef WORKER_H
#define WORKER_H

#include <stdint.h>
#define TASK_TIMEOUT_SEC    30

/* Maximum bytes of stdout+stderr we capture from a task.
 * Output beyond this is read and discarded (to prevent pipe
 * buffer deadlock) but not stored in ExecResult.
 * Must be <= MAX_OUTPUT_LEN in protocol.h so the full captured
 * output fits in a MSG_RESULT payload. */
#define EXEC_OUTPUT_MAX     4096

typedef struct 
{
    int      exit_code;                   /* process exit status; 0 = success  */
    int      timed_out;                   /* 1 if killed by timeout, 0 otherwise */
    uint32_t output_len;                  /* valid bytes in output[]            */
    char     output[EXEC_OUTPUT_MAX + 1]; /* captured stdout+stderr, null-term  */
} ExecResult;

/*   - Creates an unnamed pipe to capture stdout and stderr
 *   - fork()s a child process
 *   - Child: setpgid(0,0), dup2() pipe to stdout+stderr, execve()
 *   - Parent: installs SIGALRM handler, sets setitimer(), reads
 *     from pipe until EOF or timeout, waitpid()s the child
 *   - On timeout: killpg() kills entire child process group */
ExecResult executor_run(const char *command, int timeout_sec);
#endif