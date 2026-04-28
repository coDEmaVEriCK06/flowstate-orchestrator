#define _GNU_SOURCE // For struct sigaction and environ

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>      /* fork, pipe, dup2, close, read, getpgid, _exit */
#include <signal.h>      /* sigaction, SIGALRM, SIGKILL                   */
#include <sys/types.h>   /* pid_t                                          */
#include <sys/time.h>    /* setitimer, struct itimerval                   */
#include <sys/wait.h>    /* waitpid, WIFEXITED, WEXITSTATUS, WIFSIGNALED  */
#include <errno.h>       /* errno, EINTR                                   */

#include "worker.h"

/* volatile: tells the compiler this variable can change at
 * any time (from a signal handler), so it must re-read from
 * memory every time instead of caching it in a register.
 *
 * sig_atomic_t: the C standard guarantees that reads and writes
 * to this type are atomic with respect to signal delivery.
 * Using plain int would be technically undefined behavior here. */
static volatile sig_atomic_t g_timeout_fired = 0;

/* The (void)sig cast suppresses the unused-parameter warning
 * from -Wextra without removing the parameter (which must exist
 * to match the signal handler signature). */
static void sigalrm_handler(int sig) // Functions like malloc and printf cannot be called from within the handler since they may lead to deadlcok 
{
    (void)sig;
    g_timeout_fired = 1; // Signal handlers should do as little work as possible like setting a flag
}

ExecResult executor_run(const char *command, int timeout_sec) 
{
    ExecResult result;
    memset(&result, 0, sizeof(ExecResult));

    /* Use the default timeout if caller passed 0 */
    if(timeout_sec <= 0) timeout_sec = TASK_TIMEOUT_SEC;

    /* This pipe is the IPC channel between parent and child.
     * It is "unnamed" because it has no filesystem path —
     * it exists only as file descriptors in memory. */
    int pipefd[2];
    if(pipe(pipefd) < 0) 
    {
        perror("[executor_run] pipe");
        result.exit_code = -1;
        return result;
    }


    /* We use sigaction() rather than signal() because:
     * - sigaction() is POSIX-standard and portable
     * Also, it blocks on interruption like read() instead of restarting
     * sigemptyset(&sa.sa_mask): no signals are blocked during
     * the handler's execution (besides SIGALRM itself).
     *
     * We install this before fork() so the child also inherits
     * the handler — but the child resets it implicitly when
     * execve() is called (exec resets all handlers to defaults). */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigalrm_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;  /* no SA_RESTART — we want EINTR on read() */
    if(sigaction(SIGALRM, &sa, NULL) < 0) 
    {
        perror("[executor_run] sigaction");
        close(pipefd[0]);
        close(pipefd[1]);
        result.exit_code = -1;
        return result;
    }

    /* Reset the flag — must happen before fork() and before
     * setitimer() so there is no window where a stale flag
     * from a previous call causes a false timeout. */
    g_timeout_fired = 0;

    /* After fork():
     *   parent: child_pid > 0
     *   child:  child_pid == 0
     *   error:  child_pid < 0
     * Both processes now have copies of pipefd[0] and pipefd[1]. */
    pid_t child_pid = fork();

    if(child_pid < 0) 
    {
        perror("[executor_run] fork");
        close(pipefd[0]);
        close(pipefd[1]);
        result.exit_code = -1;
        return result;
    }
    if(child_pid == 0) 
    {

        /* ---- setpgid(0, 0): create a new process group ----
         *
         * The first 0 means "use my own PID as the process to
         * change." The second 0 means "use my own PID as the
         * new process group ID." So this child becomes the
         * leader of a new process group containing only itself
         * (and any processes it later spawns).
         *
         * Why this matters: killpg(getpgid(child_pid), SIGKILL)
         * sends SIGKILL to every process in the child's group.
         * Without setpgid, the child belongs to the master's
         * process group — killpg would kill the master too. */
        if(setpgid(0, 0) < 0) 
        {
            perror("[child] setpgid");
            _exit(127);
        }
        close(pipefd[0]);
        if(dup2(pipefd[1], STDOUT_FILENO) < 0) // Write operations directed towards pipe instead of the terminal
        {
            perror("[child] dup2 stdout");
            _exit(127);
        }

        if(dup2(pipefd[1], STDERR_FILENO) < 0) // Both stdout and stderr now send their output to the pipe instead of the terminal
        {
            perror("[child] dup2 stderr");
            _exit(127);
        }
        close(pipefd[1]); // pipefd[1] closed because it is redundant in the presence of dup2

        /* ---- execve(): replace this process with /bin/sh ----
         *
         * execve() replaces the current process image with a new
         * program. The three arguments are:
         *   argv[0] = "/bin/sh"  (the shell itself)
         *   argv[1] = "-c"       (run the next argument as a command)
         *   argv[2] = command    (our task's shell command string)
         *   argv[3] = NULL       (argv must be null-terminated)
         *
         * The (char * const []) cast is required because execve
         * takes char * const argv[] but our literal array has
         * const char * elements.
         *
         * environ is the global variable holding the current
         * environment variables. Declared in <unistd.h>.
         * Passing it through preserves PATH, HOME, etc.
         *
         * File descriptor inheritance: STDOUT_FILENO and
         * STDERR_FILENO survive execve() because they are not
         * marked close-on-exec. The shell inherits them and all
         * its output flows into our pipe. */
        execve("/bin/sh", (char * const []){"/bin/sh", "-c", (char *)command, NULL}, environ);

        /* _exit() terminates immediately without any cleanup.
         * 127 is the standard exit code for "command not found." */
        perror("[child] execve");
        _exit(127);
    }

    close(pipefd[1]); // Closing parent's write end otherwise read would be forever blocked waiting for data. This way parent can reach End of File.

    
    /* setitimer(ITIMER_REAL, ...) counts in real wall-clock time.
     * it_value:    time until first signal (our timeout duration)
     * it_interval: time between repeated signals (0 = fire once) */
    struct itimerval timer;
    memset(&timer, 0, sizeof(timer));
    timer.it_value.tv_sec  = timeout_sec;
    timer.it_value.tv_usec = 0;
    timer.it_interval.tv_sec  = 0;
    timer.it_interval.tv_usec = 0;
    setitimer(ITIMER_REAL, &timer, NULL);

    /*
     * We read in a loop because a single read() may return fewer
     * bytes than available — the same partial-read issue as TCP.
     * We stop when:
     *   a) read() returns 0 (EOF — child exited and closed pipe)
     *   b) g_timeout_fired is set (SIGALRM fired)
     *   c) read() returns a real error (not EINTR)
     *
     * After EXEC_OUTPUT_MAX bytes, we switch to a discard buffer
     * to drain the pipe without storing more data. This prevents
     * the deadlock where the child blocks on write() because the
     * pipe buffer is full and the parent stopped reading. */
    uint32_t total = 0;
    char discard[256];

    while(1) 
    {
        void    *dest;
        size_t   to_read;

        if(total < EXEC_OUTPUT_MAX) 
        {
            /* Buffer has room — read into result output */
            dest    = result.output + total;
            to_read = EXEC_OUTPUT_MAX - total;
        }
        else 
        {
            /* Buffer full — drain pipe into discard buffer */
            dest    = discard;
            to_read = sizeof(discard);
        }

        ssize_t n = read(pipefd[0], dest, to_read); // size_t is unsigned (cannot reperesent negative values) and ssize_t is signed

        if(n > 0) 
        {
            /* Only count bytes we actually stored */
            if(total < EXEC_OUTPUT_MAX) total += (uint32_t)n;
        } 
        else if(n == 0) 
        {
            /* EOF: child has exited and closed all write ends.
             * This is the normal successful termination path. */
            break;

        } 
        else 
        {
            /* n < 0: read() was interrupted or encountered error */
            if(errno == EINTR) 
            {
                /* A signal interrupted this read() call.
                 * Check if it was our SIGALRM timeout. */
                if(g_timeout_fired) 
                {
                    /* ---- Timeout path ----
                     *
                     * killpg(pgid, SIGKILL) sends SIGKILL to every
                     * process in the child's process group. Because
                     * the child called setpgid(0,0), this kills the
                     * child AND any grandchildren it spawned (bash
                     * subshells, background processes, etc).
                     *
                     * SIGKILL cannot be caught or ignored — it
                     * always terminates the target immediately. */
                    killpg(getpgid(child_pid), SIGKILL);
                    result.timed_out = 1;
                    break;
                }
                continue; // In case the signal was some other signal
            }
            /* Real read error — stop */
            perror("[executor_run] read");
            break;
        }
    }

    /* ---- Cancel the timer ----
     *
     * If the child finished before the timeout, we must cancel
     * the pending SIGALRM. Passing a zeroed itimerval disarms it.
     * Without this, SIGALRM would fire after executor_run()
     * returns and interrupt an unrelated read() call */
    struct itimerval cancel;
    memset(&cancel, 0, sizeof(cancel));
    setitimer(ITIMER_REAL, &cancel, NULL);
    close(pipefd[0]); // Read end closed as all work is done

    /* ---- waitpid(): reap the child process ----
     *
     * waitpid() waits for the child to change state.
     * We pass 0 as flags — blocking wait. By this point the
     * child is either already dead (normal exit or we killed it)
     * or about to die momentarily after SIGKILL.
     * 
     * WIFEXITED(status): true if child exited via exit() or _exit()
     * WEXITSTATUS(status): extracts the exit code (low 8 bits)
     * WIFSIGNALED(status): true if child was killed by a signal
     * WTERMSIG(status): which signal killed it */
    int status;
    waitpid(child_pid, &status, 0);
    if(!result.timed_out) 
    {
        if(WIFEXITED(status)) 
        {
            result.exit_code = WEXITSTATUS(status);
        } 
        else if(WIFSIGNALED(status)) 
        {
            result.exit_code = -WTERMSIG(status); // Child was killed by a signal so represented by a negative numnber for proper distinction. A child killed by us would have the termination status 1.
        }
    } 
    else 
    {
        result.exit_code = -1;
    }
    
    result.output_len = total;
    result.output[total] = '\0';
    return result;
}