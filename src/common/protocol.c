#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>   /* htonl(), ntohl() for little endian and big endian problems across architecture*/

#include "protocol.h"

/* Largest payload we will ever accept from the wire. */
#define MAX_PAYLOAD_SIZE          RESULT_PAYLOAD_SIZE

static void write_header(uint8_t *buf, uint32_t msg_type, uint32_t payload_len) 
{
    /* Convert both fields to network byte order before writing.
     * memcpy avoids alignment faults on strict-alignment CPUs
     * (e.g., ARM) that crash on unaligned 32-bit reads/writes. */
    uint32_t net_type = htonl(msg_type);
    uint32_t net_len  = htonl(payload_len);
    memcpy(buf, &net_type, 4);
    memcpy(buf + 4, &net_len,  4);
}

void conn_recv_init(ConnRecvBuffer *rbuf) 
{
    memset(rbuf->data, 0, RECV_BUF_SIZE);
    rbuf->bytes_filled = 0;
}

int conn_recv_push(ConnRecvBuffer *rbuf, const uint8_t *data, uint32_t len) 
{
    /* Check if there is room in the buffer.
     * bytes_filled + len must not exceed RECV_BUF_SIZE. */
    if(rbuf->bytes_filled + len > RECV_BUF_SIZE) 
    {
        fprintf(stderr, "[conn_recv_push] Buffer overflow: " "filled=%u + incoming=%u > capacity=%d\n", rbuf->bytes_filled, len, RECV_BUF_SIZE);
        return -1;
    }
    /* Append the new bytes after the existing ones */
    memcpy(rbuf->data + rbuf->bytes_filled, data, len);
    rbuf->bytes_filled += len;
    return 0;
}

int proto_try_deframe(ConnRecvBuffer *rbuf, uint32_t *out_type, uint8_t  *out_payload, uint32_t *out_payload_len) 
{
    /* Step 1: need at least a full header */
    if(rbuf->bytes_filled < WIRE_HEADER_SIZE) return DEFRAME_INCOMPLETE;

    /* Step 2: read type and length from the header.
     * We use memcpy to read the 4-byte values because the buffer
     * is a uint8_t array and may not be 4-byte aligned. */
    uint32_t net_type, net_len;
    memcpy(&net_type, rbuf->data, 4);
    memcpy(&net_len, rbuf->data + 4, 4);
    uint32_t msg_type = ntohl(net_type);
    uint32_t payload_len = ntohl(net_len);

    /* Step 3: sanity check the payload length.
     * A legitimate peer never sends a payload larger than
     * MAX_PAYLOAD_SIZE. If we see one, the connection is either
     * corrupt or being attacked — close it. */
    if(payload_len > (uint32_t)MAX_PAYLOAD_SIZE) 
    {
        fprintf(stderr, "[proto_try_deframe] Oversized payload: %u bytes " "(max %d). Closing connection.\n", payload_len, MAX_PAYLOAD_SIZE);
        return DEFRAME_ERROR;
    }

    /* Validate message type is one we recognize */
    if(msg_type < MSG_CONNECT || msg_type > MSG_RESULT) 
    {
        fprintf(stderr, "[proto_try_deframe] Unknown message type: 0x%02x\n", msg_type);
        return DEFRAME_ERROR;
    }

    /* Step 4: check if the full payload has arrived */
    uint32_t total_needed = WIRE_HEADER_SIZE + payload_len;
    if(rbuf->bytes_filled < total_needed) return DEFRAME_INCOMPLETE;

    /* Step 5: copy payload into caller's buffer */
    *out_type = msg_type;
    *out_payload_len = payload_len;
    if(payload_len > 0) 
    {
        memcpy(out_payload, rbuf->data + WIRE_HEADER_SIZE, payload_len);
    }

    /* Step 6: consume the message bytes from the buffer.
     * memmove handles overlapping regions correctly, which happens
     * when total_needed < bytes_filled (bytes remain after this msg).
     * After the shift, update bytes_filled. */
    uint32_t remaining = rbuf->bytes_filled - total_needed;
    if(remaining > 0) 
    {
        memmove(rbuf->data, rbuf->data + total_needed, remaining);
    }
    rbuf->bytes_filled = remaining;
    return DEFRAME_COMPLETE;
}

int proto_pack_connect(uint8_t *buf, size_t buf_size, uint32_t role, const char *auth_token) 
{
    uint32_t payload_len = CONNECT_PAYLOAD_SIZE;
    uint32_t total = WIRE_HEADER_SIZE + payload_len;
    if(buf_size < total) return -1;
    write_header(buf, MSG_CONNECT, payload_len);
    size_t offset = WIRE_HEADER_SIZE;

    /* Pack role as a 4-byte network-order integer */
    uint32_t net_role = htonl(role);
    memcpy(buf + offset, &net_role, 4);
    offset += 4;

    /* Pack auth_token: copy up to AUTH_TOKEN_LEN bytes.
     * strncpy pads with null bytes if the token is shorter —
     * exactly what we want for a fixed-size wire field. */
    memset(buf + offset, 0, AUTH_TOKEN_LEN);
    strncpy((char *)(buf + offset), auth_token, AUTH_TOKEN_LEN - 1);
    offset += AUTH_TOKEN_LEN;
    return (int)total;
}

int proto_pack_connect_ack(uint8_t *buf, size_t buf_size, uint32_t status) 
{
    uint32_t payload_len = CONNECT_ACK_PAYLOAD_SIZE;
    uint32_t total = WIRE_HEADER_SIZE + payload_len;
    if(buf_size < total) return -1;
    write_header(buf, MSG_CONNECT_ACK, payload_len);
    uint32_t net_status = htonl(status);
    memcpy(buf + WIRE_HEADER_SIZE, &net_status, 4);
    return (int)total;
}

int proto_pack_dispatch(uint8_t *buf, size_t buf_size, uint32_t task_id, uint32_t generation, const char *command) 
{
    uint32_t payload_len = DISPATCH_PAYLOAD_SIZE;
    uint32_t total = WIRE_HEADER_SIZE + payload_len;
    if(buf_size < total) return -1;
    write_header(buf, MSG_DISPATCH, payload_len);
    size_t offset = WIRE_HEADER_SIZE;
    uint32_t net_task_id = htonl(task_id);
    uint32_t net_generation = htonl(generation);
    memcpy(buf + offset, &net_task_id, 4); 
    offset += 4;
    memcpy(buf + offset, &net_generation, 4); 
    offset += 4;

    /* Copy command string into a fixed 256-byte field.
     * Zero the field first so unused bytes are null-padded,
     * not filled with garbage from the stack. */
    memset(buf + offset, 0, 256);
    strncpy((char *)(buf + offset), command, 255);
    /* strncpy does not guarantee null termination if command
     * is exactly 255 chars. Force it. */
    buf[offset + 255] = '\0';
    return (int)total;
}

int proto_pack_ack(uint8_t *buf, size_t buf_size, uint32_t task_id, uint32_t generation) 
{
    uint32_t payload_len = ACK_PAYLOAD_SIZE;
    uint32_t total = WIRE_HEADER_SIZE + payload_len;
    if(buf_size < total) return -1;
    write_header(buf, MSG_ACK, payload_len);
    size_t offset = WIRE_HEADER_SIZE;
    uint32_t net_task_id = htonl(task_id);
    uint32_t net_generation = htonl(generation);
    memcpy(buf + offset, &net_task_id, 4); 
    offset += 4;
    memcpy(buf + offset, &net_generation, 4);
    return (int)total;
}

int proto_pack_result(uint8_t *buf, size_t buf_size, uint32_t task_id, uint32_t generation, int32_t exit_code, const char *output, uint32_t output_len) 
{
    uint32_t payload_len = RESULT_PAYLOAD_SIZE;
    uint32_t total = WIRE_HEADER_SIZE + payload_len;
    if(buf_size < total) return -1;

    /* Clamp output_len to MAX_OUTPUT_LEN.
     * If the task produced more output than our buffer can hold,
     * we silently truncate. */
    if(output_len > MAX_OUTPUT_LEN) output_len = MAX_OUTPUT_LEN;
    write_header(buf, MSG_RESULT, payload_len);
    size_t offset = WIRE_HEADER_SIZE;
    uint32_t net_task_id = htonl(task_id);
    uint32_t net_generation = htonl(generation);
    /* Cast exit_code to uint32_t before htonl().
     * On two's complement systems (all modern hardware, and
     * mandatory in C23), this cast is well-defined and round-trips
     * correctly through ntohl() and back to int32_t. */
    uint32_t net_exit = htonl((uint32_t)exit_code);
    uint32_t net_outlen = htonl(output_len);
    memcpy(buf + offset, &net_task_id, 4); 
    offset += 4;
    memcpy(buf + offset, &net_generation, 4); 
    offset += 4;
    memcpy(buf + offset, &net_exit, 4); 
    offset += 4;
    memcpy(buf + offset, &net_outlen, 4); 
    offset += 4;

    /* Zero the output field first, then copy valid bytes */
    memset(buf + offset, 0, MAX_OUTPUT_LEN);
    if(output_len > 0 && output != NULL) memcpy(buf + offset, output, output_len);
    return (int)total;
}

int proto_parse_connect(const uint8_t *payload, uint32_t payload_len, uint32_t *role, char *auth_token) 
{
    if(payload_len != CONNECT_PAYLOAD_SIZE) return -1;
    size_t offset = 0;
    uint32_t net_role;
    memcpy(&net_role, payload + offset, 4); 
    offset += 4;
    *role = ntohl(net_role);

    /* Copy the fixed-size token field into the caller's buffer.
     * We always copy exactly AUTH_TOKEN_LEN bytes. The caller's
     * buffer must be at least AUTH_TOKEN_LEN bytes. */
    memcpy(auth_token, payload + offset, AUTH_TOKEN_LEN);
    auth_token[AUTH_TOKEN_LEN - 1] = '\0'; /* guarantee null termination */
    return 0;
}

int proto_parse_connect_ack(const uint8_t *payload, uint32_t payload_len, uint32_t *status) 
{
    if(payload_len != CONNECT_ACK_PAYLOAD_SIZE) return -1;
    uint32_t net_status;
    memcpy(&net_status, payload, 4);
    *status = ntohl(net_status);
    return 0;
}

int proto_parse_dispatch(const uint8_t *payload, uint32_t payload_len, uint32_t *task_id, uint32_t *generation, char *command) 
{
    if(payload_len != DISPATCH_PAYLOAD_SIZE) return -1;
    size_t offset = 0;
    uint32_t net_task_id, net_generation;
    memcpy(&net_task_id, payload + offset, 4); 
    offset += 4;
    memcpy(&net_generation, payload + offset, 4); 
    offset += 4;
    *task_id = ntohl(net_task_id);
    *generation = ntohl(net_generation);

    /* Copy the 256-byte command field.
     * The caller's command buffer must be at least 256 bytes.
     * We force null termination at position 255 as a safety net
     * in case a malformed sender omitted the null terminator. */
    memcpy(command, payload + offset, 256);
    command[255] = '\0';
    return 0;
}

int proto_parse_ack(const uint8_t *payload, uint32_t payload_len, uint32_t *task_id, uint32_t *generation) 
{
    if(payload_len != ACK_PAYLOAD_SIZE) return -1;
    size_t offset = 0;
    uint32_t net_task_id, net_generation;
    memcpy(&net_task_id, payload + offset, 4); 
    offset += 4;
    memcpy(&net_generation, payload + offset, 4);
    *task_id = ntohl(net_task_id);
    *generation = ntohl(net_generation);
    return 0;
}

int proto_parse_result(const uint8_t *payload, uint32_t payload_len, uint32_t *task_id, uint32_t *generation, int32_t *exit_code, char *output, uint32_t *output_len) 
{
    if(payload_len != RESULT_PAYLOAD_SIZE) return -1;
    size_t offset = 0;
    uint32_t net_task_id, net_generation, net_exit, net_outlen;
    memcpy(&net_task_id, payload + offset, 4); 
    offset += 4;
    memcpy(&net_generation, payload + offset, 4); 
    offset += 4;
    memcpy(&net_exit, payload + offset, 4); 
    offset += 4;
    memcpy(&net_outlen, payload + offset, 4); 
    offset += 4;
    *task_id = ntohl(net_task_id);
    *generation = ntohl(net_generation);
    /* Cast back to int32_t after ntohl().
     * This correctly recovers negative exit codes. */
     *exit_code = (int32_t)ntohl(net_exit);
     *output_len = ntohl(net_outlen);

    /* Clamp output_len defensively in case of a malformed sender */
    if(*output_len > MAX_OUTPUT_LEN) *output_len = MAX_OUTPUT_LEN;
    memcpy(output, payload + offset, *output_len);
    /* Null-terminate so callers can safely use output as a string */
    output[*output_len] = '\0';
    return 0;
}