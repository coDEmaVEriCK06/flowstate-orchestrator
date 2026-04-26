/*
 * tests/test_protocol.c — Phase 2 validation suite for the wire protocol.
 *
 * Run: make test_protocol && ./test_protocol
 * Expected: 7 tests PASS, exit code 0.
 *
 * Tests cover pack/parse round-trips for all 5 message types,
 * partial read simulation (the core TCP problem), back-to-back
 * message deframing, and oversized payload rejection.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "protocol.h"
#include <arpa/inet.h>

/* ============================================================
 * Test framework — identical pattern to test_dag.c
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
 * Shared wire buffer — large enough for any single message.
 * Declared at file scope so every test function can use it
 * without stack-allocating a large array each time.
 * ============================================================ */
static uint8_t wire_buf[RECV_BUF_SIZE * 2];

/* ============================================================
 * Test 1: MSG_CONNECT round-trip
 *
 * Pack a connect message with ROLE_WORKER and a token string.
 * Push into a receive buffer. Deframe it. Parse the payload.
 * Verify role and token are identical to what was packed.
 * ============================================================ */
static int test_connect_roundtrip(void) {
    const char *token = "secret_auth_token_123";
    uint32_t    role  = ROLE_WORKER;

    int n = proto_pack_connect(wire_buf, sizeof(wire_buf), role, token);
    ASSERT(n > 0, "proto_pack_connect returns positive byte count");
    ASSERT(n == WIRE_HEADER_SIZE + 4 + AUTH_TOKEN_LEN,
           "MSG_CONNECT wire size is WIRE_HEADER_SIZE + 36");

    ConnRecvBuffer rbuf;
    conn_recv_init(&rbuf);
    ASSERT(conn_recv_push(&rbuf, wire_buf, (uint32_t)n) == 0,
           "conn_recv_push succeeds");

    uint8_t  payload[RECV_BUF_SIZE];
    uint32_t out_type, out_payload_len;
    int ret = proto_try_deframe(&rbuf, &out_type, payload, &out_payload_len);
    ASSERT(ret == DEFRAME_COMPLETE,   "proto_try_deframe returns DEFRAME_COMPLETE");
    ASSERT(out_type == MSG_CONNECT,   "Deframed type is MSG_CONNECT");
    ASSERT(rbuf.bytes_filled == 0,    "Buffer empty after deframe");

    uint32_t parsed_role;
    char     parsed_token[AUTH_TOKEN_LEN];
    ASSERT(proto_parse_connect(payload, out_payload_len,
                               &parsed_role, parsed_token) == 0,
           "proto_parse_connect returns 0");
    ASSERT(parsed_role == ROLE_WORKER,
           "Parsed role matches ROLE_WORKER");
    ASSERT(strcmp(parsed_token, token) == 0,
           "Parsed auth token matches original");

    return 0;
}

/* ============================================================
 * Test 2: MSG_DISPATCH round-trip — command with spaces
 *
 * Verifies that a command containing embedded spaces
 * ("jobs/dummy.sh 4") survives the pack/parse cycle intact.
 * This is the same concern as Phase 1's quoted string parsing.
 * ============================================================ */
static int test_dispatch_roundtrip(void) {
    uint32_t    task_id    = 4;
    uint32_t    generation = 2;
    const char *command    = "jobs/dummy.sh 4";

    int n = proto_pack_dispatch(wire_buf, sizeof(wire_buf),
                                task_id, generation, command);
    ASSERT(n > 0, "proto_pack_dispatch returns positive byte count");
    ASSERT(n == WIRE_HEADER_SIZE + DISPATCH_PAYLOAD_SIZE,
           "MSG_DISPATCH wire size is WIRE_HEADER_SIZE + 264");

    ConnRecvBuffer rbuf;
    conn_recv_init(&rbuf);
    conn_recv_push(&rbuf, wire_buf, (uint32_t)n);

    uint8_t  payload[RECV_BUF_SIZE];
    uint32_t out_type, out_payload_len;
    ASSERT(proto_try_deframe(&rbuf, &out_type, payload, &out_payload_len)
               == DEFRAME_COMPLETE,
           "MSG_DISPATCH deframes completely");
    ASSERT(out_type == MSG_DISPATCH, "Type is MSG_DISPATCH");

    uint32_t parsed_task_id, parsed_generation;
    char     parsed_command[256];
    ASSERT(proto_parse_dispatch(payload, out_payload_len,
                                &parsed_task_id, &parsed_generation,
                                parsed_command) == 0,
           "proto_parse_dispatch returns 0");
    ASSERT(parsed_task_id    == task_id,    "task_id round-trips correctly");
    ASSERT(parsed_generation == generation, "generation round-trips correctly");
    ASSERT(strcmp(parsed_command, command) == 0,
           "Command with embedded space survives round-trip");

    return 0;
}

/* ============================================================
 * Test 3: MSG_RESULT round-trip — negative exit code
 *
 * Packs a result with exit_code = -1 (common error sentinel),
 * and verifies it is correctly recovered as -1 after the
 * signed/unsigned conversion through htonl/ntohl.
 * ============================================================ */
static int test_result_roundtrip(void) {
    uint32_t    task_id    = 3;
    uint32_t    generation = 0;
    int32_t     exit_code  = -1;
    const char *output     = "Error: file not found\n";
    uint32_t    output_len = (uint32_t)strlen(output);

    int n = proto_pack_result(wire_buf, sizeof(wire_buf),
                              task_id, generation,
                              exit_code, output, output_len);
    ASSERT(n > 0, "proto_pack_result returns positive byte count");

    ConnRecvBuffer rbuf;
    conn_recv_init(&rbuf);
    conn_recv_push(&rbuf, wire_buf, (uint32_t)n);

    uint8_t  payload[RECV_BUF_SIZE];
    uint32_t out_type, out_payload_len;
    ASSERT(proto_try_deframe(&rbuf, &out_type, payload, &out_payload_len)
               == DEFRAME_COMPLETE,
           "MSG_RESULT deframes completely");
    ASSERT(out_type == MSG_RESULT, "Type is MSG_RESULT");

    uint32_t parsed_task_id, parsed_gen, parsed_outlen;
    int32_t  parsed_exit;
    char     parsed_output[MAX_OUTPUT_LEN + 1];
    ASSERT(proto_parse_result(payload, out_payload_len,
                              &parsed_task_id, &parsed_gen,
                              &parsed_exit,
                              parsed_output, &parsed_outlen) == 0,
           "proto_parse_result returns 0");
    ASSERT(parsed_exit == -1,
           "Negative exit code -1 round-trips correctly through htonl/ntohl");
    ASSERT(parsed_outlen == output_len,
           "output_len round-trips correctly");
    ASSERT(strcmp(parsed_output, output) == 0,
           "Output string round-trips correctly");

    return 0;
}

/* ============================================================
 * Test 4: MSG_ACK and MSG_CONNECT_ACK round-trips
 *
 * Two small messages verified together since they share the
 * same simple structure (one or two uint32_t fields).
 * ============================================================ */
static int test_ack_roundtrip(void) {
    /* MSG_ACK */
    int n = proto_pack_ack(wire_buf, sizeof(wire_buf), 7, 3);
    ASSERT(n == WIRE_HEADER_SIZE + ACK_PAYLOAD_SIZE,
           "MSG_ACK wire size is WIRE_HEADER_SIZE + 8");

    ConnRecvBuffer rbuf;
    conn_recv_init(&rbuf);
    conn_recv_push(&rbuf, wire_buf, (uint32_t)n);

    uint8_t  payload[RECV_BUF_SIZE];
    uint32_t out_type, out_len;
    proto_try_deframe(&rbuf, &out_type, payload, &out_len);

    uint32_t tid, gen;
    ASSERT(proto_parse_ack(payload, out_len, &tid, &gen) == 0,
           "proto_parse_ack returns 0");
    ASSERT(tid == 7 && gen == 3,
           "task_id=7 and generation=3 round-trip through MSG_ACK");

    /* MSG_CONNECT_ACK */
    n = proto_pack_connect_ack(wire_buf, sizeof(wire_buf),
                               CONNECT_STATUS_OK);
    conn_recv_init(&rbuf);
    conn_recv_push(&rbuf, wire_buf, (uint32_t)n);
    proto_try_deframe(&rbuf, &out_type, payload, &out_len);
    ASSERT(out_type == MSG_CONNECT_ACK, "Type is MSG_CONNECT_ACK");

    uint32_t status;
    proto_parse_connect_ack(payload, out_len, &status);
    ASSERT(status == CONNECT_STATUS_OK,
           "CONNECT_STATUS_OK round-trips correctly");

    return 0;
}

/* ============================================================
 * Test 5: Partial read simulation
 *
 * This test directly exercises the TCP stream problem.
 * A MSG_DISPATCH message is split into 3 arbitrary chunks
 * and pushed one chunk at a time. proto_try_deframe() must
 * return DEFRAME_INCOMPLETE for the first two pushes and
 * DEFRAME_COMPLETE only after the final chunk.
 * ============================================================ */
static int test_partial_read(void) {
    int total = proto_pack_dispatch(wire_buf, sizeof(wire_buf),
                                    2, 0, "jobs/dummy.sh 2");
    ASSERT(total > 12, "MSG_DISPATCH is at least 12 bytes");

    /* Deliberately split into 3 unequal chunks.
     * Chunk 1: first 4 bytes — not even a full header (need 8) */
    uint8_t  payload[RECV_BUF_SIZE];
    uint32_t out_type, out_len;
    ConnRecvBuffer rbuf;
    conn_recv_init(&rbuf);

    conn_recv_push(&rbuf, wire_buf, 4);
    int ret = proto_try_deframe(&rbuf, &out_type, payload, &out_len);
    ASSERT(ret == DEFRAME_INCOMPLETE,
           "4 bytes: DEFRAME_INCOMPLETE (header not complete)");

    /* Chunk 2: next 4 bytes — header is now complete but payload is not */
    conn_recv_push(&rbuf, wire_buf + 4, 4);
    ret = proto_try_deframe(&rbuf, &out_type, payload, &out_len);
    ASSERT(ret == DEFRAME_INCOMPLETE,
           "8 bytes: DEFRAME_INCOMPLETE (header done, payload missing)");

    /* Chunk 3: all remaining bytes */
    conn_recv_push(&rbuf, wire_buf + 8, (uint32_t)(total - 8));
    ret = proto_try_deframe(&rbuf, &out_type, payload, &out_len);
    ASSERT(ret == DEFRAME_COMPLETE,
           "All bytes: DEFRAME_COMPLETE");
    ASSERT(rbuf.bytes_filled == 0,
           "Buffer is empty after consuming the complete message");

    uint32_t tid, gen;
    char cmd[256];
    proto_parse_dispatch(payload, out_len, &tid, &gen, cmd);
    ASSERT(tid == 2 && gen == 0,
           "Partially received message parses correctly");

    return 0;
}

/* ============================================================
 * Test 6: Two back-to-back messages in the buffer
 *
 * Pushes two complete messages at once into one receive buffer.
 * Verifies proto_try_deframe() correctly extracts them one at a
 * time, leaving the second message intact in the buffer after
 * the first is extracted.
 * ============================================================ */
static int test_back_to_back(void) {
    /* Pack message 1: MSG_ACK for task 1 */
    uint8_t buf1[256];
    int n1 = proto_pack_ack(buf1, sizeof(buf1), 1, 0);

    /* Pack message 2: MSG_ACK for task 2 */
    uint8_t buf2[256];
    int n2 = proto_pack_ack(buf2, sizeof(buf2), 2, 0);

    ConnRecvBuffer rbuf;
    conn_recv_init(&rbuf);

    /* Push both messages in one call */
    conn_recv_push(&rbuf, buf1, (uint32_t)n1);
    conn_recv_push(&rbuf, buf2, (uint32_t)n2);
    ASSERT(rbuf.bytes_filled == (uint32_t)(n1 + n2),
           "Buffer contains bytes for both messages");

    uint8_t  payload[RECV_BUF_SIZE];
    uint32_t out_type, out_len;

    /* First deframe extracts message 1 */
    int ret = proto_try_deframe(&rbuf, &out_type, payload, &out_len);
    ASSERT(ret == DEFRAME_COMPLETE,        "First deframe: COMPLETE");
    ASSERT(out_type == MSG_ACK,            "First message is MSG_ACK");
    ASSERT(rbuf.bytes_filled == (uint32_t)n2,
           "Second message still in buffer after first deframe");

    uint32_t tid, gen;
    proto_parse_ack(payload, out_len, &tid, &gen);
    ASSERT(tid == 1, "First message: task_id=1");

    /* Second deframe extracts message 2 */
    ret = proto_try_deframe(&rbuf, &out_type, payload, &out_len);
    ASSERT(ret == DEFRAME_COMPLETE,        "Second deframe: COMPLETE");
    proto_parse_ack(payload, out_len, &tid, &gen);
    ASSERT(tid == 2, "Second message: task_id=2");
    ASSERT(rbuf.bytes_filled == 0,         "Buffer empty after both deframes");

    return 0;
}

/* ============================================================
 * Test 7: Oversized payload rejection
 *
 * Manually craft a wire message with a length field that claims
 * a payload larger than MAX_PAYLOAD_SIZE. proto_try_deframe()
 * must return DEFRAME_ERROR and not attempt to read that many
 * bytes. This tests the security/sanity check.
 * ============================================================ */
static int test_oversized_payload(void) {
    /* Write a fake header with type=MSG_DISPATCH and a
     * payload_length that exceeds MAX_PAYLOAD_SIZE */
    uint8_t  fake_msg[8];
    uint32_t net_type = htonl(MSG_DISPATCH);
    /* MAX_PAYLOAD_SIZE + 1 should be rejected */
    uint32_t net_len  = htonl((uint32_t)(RESULT_PAYLOAD_SIZE + 1));
    memcpy(fake_msg,     &net_type, 4);
    memcpy(fake_msg + 4, &net_len,  4);

    ConnRecvBuffer rbuf;
    conn_recv_init(&rbuf);
    conn_recv_push(&rbuf, fake_msg, 8);

    uint8_t  payload[RECV_BUF_SIZE];
    uint32_t out_type, out_len;
    int ret = proto_try_deframe(&rbuf, &out_type, payload, &out_len);
    ASSERT(ret == DEFRAME_ERROR,
           "Oversized payload length returns DEFRAME_ERROR");

    return 0;
}

/* ============================================================
 * main
 * ============================================================ */
int main(void) {
    printf("=========================================\n");
    printf("  Phase 2: Wire Protocol Test Suite\n");
    printf("=========================================\n");

    run_test("MSG_CONNECT round-trip",        test_connect_roundtrip);
    run_test("MSG_DISPATCH round-trip",       test_dispatch_roundtrip);
    run_test("MSG_RESULT round-trip",         test_result_roundtrip);
    run_test("MSG_ACK and CONNECT_ACK",       test_ack_roundtrip);
    run_test("Partial read simulation",       test_partial_read);
    run_test("Back-to-back messages",         test_back_to_back);
    run_test("Oversized payload rejection",   test_oversized_payload);

    printf("\n=========================================\n");
    printf("  Results: %d passed, %d failed\n", g_passed, g_failed);
    printf("=========================================\n");

    return (g_failed == 0) ? 0 : 1;
}