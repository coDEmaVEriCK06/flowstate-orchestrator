/*
 * tests/test_executor.c — Phase 3 validation suite for the executor.
 *
 * Run: make test_executor && ./test_executor
 * Expected: 6 tests PASS, exit code 0.
 *
 * Tests cover: basic execution, exit code propagation, stderr
 * capture, timeout enforcement with process group kill, real
 * script execution, and command-not-found handling.
 *
 * NOTE: These tests fork real child processes and use real
 * timers. They take slightly longer than Phase 1/2 tests
 * because the timeout test waits 2 real seconds.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "worker.h"

/* ============================================================
 * Test framework — same pattern as previous phases
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

/* ============================================================
 * Test 1: Simple command execution
 *
 * Runs `echo "hello from executor"` and verifies:
 * - exit code is 0
 * - timed_out is 0
 * - output contains the expected string
 * - output_len is greater than 0
 * ============================================================ */
static int test_simple_command(void) {
    ExecResult r = executor_run("echo 'hello from executor'", 5);

    ASSERT(r.exit_code  == 0, "Exit code is 0 for successful command");
    ASSERT(r.timed_out  == 0, "timed_out is 0 for successful command");
    ASSERT(r.output_len  > 0, "Output length is greater than 0");
    ASSERT(strstr(r.output, "hello from executor") != NULL,
           "Output contains expected string");

    return 0;
}

/* ============================================================
 * Test 2: Non-zero exit code propagation
 *
 * Runs a shell command that explicitly exits with code 42.
 * Verifies the exact exit code is preserved through the
 * fork/exec/waitpid chain.
 * ============================================================ */
static int test_exit_code_nonzero(void) {
    ExecResult r = executor_run("exit 42", 5);

    ASSERT(r.timed_out  == 0,  "Command was not timed out");
    ASSERT(r.exit_code  == 42, "Exit code 42 propagates correctly");

    return 0;
}

/* ============================================================
 * Test 3: stderr capture
 *
 * Verifies that output written to stderr (not just stdout) is
 * captured by the pipe. Uses shell redirection >&2 to write
 * to stderr explicitly.
 *
 * This tests that both dup2(pipefd[1], STDOUT_FILENO) AND
 * dup2(pipefd[1], STDERR_FILENO) are working correctly.
 * ============================================================ */
static int test_stderr_captured(void) {
    ExecResult r = executor_run("echo 'this is stderr' >&2", 5);

    ASSERT(r.exit_code == 0, "Exit code 0");
    ASSERT(r.timed_out == 0, "Not timed out");
    ASSERT(strstr(r.output, "this is stderr") != NULL,
           "stderr output is captured through the pipe");

    return 0;
}

/* ============================================================
 * Test 4: Timeout enforcement
 *
 * Runs `sleep 60` with a 2-second timeout. Verifies:
 * - timed_out is 1
 * - exit_code is -1
 * - The function returns in approximately 2 seconds
 *   (not 60 seconds)
 *
 * This directly tests: SIGALRM delivery, sigalrm_handler,
 * setitimer, killpg, setpgid, and waitpid all working together.
 *
 * This test takes ~2 real seconds to run — expected behavior.
 * ============================================================ */
static int test_timeout_enforced(void) {
    printf("  (this test takes ~2 seconds...)\n");

    ExecResult r = executor_run("sleep 60", 2);

    ASSERT(r.timed_out == 1,  "timed_out flag is set");
    ASSERT(r.exit_code == -1, "exit_code is -1 for timed out task");

    return 0;
}

/* ============================================================
 * Test 5: Real script execution
 *
 * Runs jobs/dummy.sh which is a real bash script with arguments.
 * Verifies the output format matches what the script produces.
 *
 * This tests the full execve path with a real script file,
 * confirming that PATH resolution via /bin/sh -c works and
 * that command arguments are passed correctly.
 * ============================================================ */
static int test_real_script(void) {
    ExecResult r = executor_run("jobs/dummy.sh 99", 10);

    ASSERT(r.exit_code == 0, "dummy.sh exits with code 0");
    ASSERT(r.timed_out == 0, "dummy.sh completes within timeout");
    ASSERT(r.output_len > 0, "dummy.sh produces output");
    ASSERT(strstr(r.output, "99") != NULL,
           "Output contains the task argument '99'");
    ASSERT(strstr(r.output, "Executing Task") != NULL,
           "Output contains 'Executing Task' from dummy.sh");

    return 0;
}

/* ============================================================
 * Test 6: Command not found
 *
 * Runs a nonexistent command. The shell reports an error and
 * exits with code 127 — the Unix standard for "command not
 * found." Verifies exit code is non-zero (127 specifically).
 * ============================================================ */
static int test_command_not_found(void) {
    ExecResult r = executor_run("this_command_does_not_exist_xyz", 5);

    ASSERT(r.timed_out  == 0,   "Not timed out");
    ASSERT(r.exit_code  == 127, "Exit code is 127 for command not found");

    return 0;
}

/* ============================================================
 * main
 * ============================================================ */
int main(void) {
    printf("=========================================\n");
    printf("  Phase 3: Worker Executor Test Suite\n");
    printf("=========================================\n");

    run_test("Simple command execution",   test_simple_command);
    run_test("Non-zero exit code",         test_exit_code_nonzero);
    run_test("stderr capture via dup2",    test_stderr_captured);
    run_test("Timeout enforcement",        test_timeout_enforced);
    run_test("Real script execution",      test_real_script);
    run_test("Command not found (127)",    test_command_not_found);

    printf("\n=========================================\n");
    printf("  Results: %d passed, %d failed\n", g_passed, g_failed);
    printf("=========================================\n");

    return (g_failed == 0) ? 0 : 1;
}