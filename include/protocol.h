#ifndef PROTOCOL_H
#define PROTOCOL_H
#include <stddef.h> // For size_t
#include <stdint.h>
#define WIRE_HEADER_SIZE   8 // 4 bytes for message type and other 4 for message length
#define MAX_OUTPUT_LEN 4096 // Result payload should fit in our buffer
#define RECV_BUF_SIZE      8192 // Max allowed size for System V message queues
#define AUTH_TOKEN_LEN     32 // Shared secret between master and worker
#define CONNECT_PAYLOAD_SIZE      (4 + AUTH_TOKEN_LEN)  /* 36  bytes */
#define CONNECT_ACK_PAYLOAD_SIZE  (4)                   /* 4   bytes */
#define DISPATCH_PAYLOAD_SIZE     (4 + 4 + 256)         /* 264 bytes */
#define ACK_PAYLOAD_SIZE          (4 + 4)               /* 8   bytes */
#define RESULT_PAYLOAD_SIZE       (4 + 4 + 4 + 4 + MAX_OUTPUT_LEN) /* 4112 bytes */

/* Return values for proto_try_deframe() */
#define DEFRAME_COMPLETE   1   /* a full message was extracted    */
#define DEFRAME_INCOMPLETE 0   /* not enough bytes yet — wait     */
#define DEFRAME_ERROR     -1   /* malformed data — close connection */


typedef enum 
{
    MSG_CONNECT     = 0x01,  /* worker to master: initial role handshake  */
    MSG_CONNECT_ACK = 0x02,  /* master to worker: accept or reject        */
    MSG_DISPATCH    = 0x03,  /* master to worker: task payload            */
    MSG_ACK         = 0x04,  /* worker to master: task receipt confirmed  */
    MSG_RESULT      = 0x05,  /* worker to master: task output + exit code */
} MessageType;

// Role-based authorization
typedef enum 
{
    ROLE_WORKER   = 1,  /* can receive and execute tasks       */
    ROLE_OBSERVER = 2,  /* read-only: can query execution status */
} WorkerRole;

#define CONNECT_STATUS_OK     0
#define CONNECT_STATUS_REJECT 1

// This struct is only for documentation clarity, actual packing is done using memcpy and not direct struct casts for byte processing safety
typedef struct 
{
    uint32_t role;/* Value for WorkerRole */
    char auth_token[AUTH_TOKEN_LEN]; /* shared secret, null-padded */
} ConnectPayload;

typedef struct 
{
    uint32_t status; /* CONNECT_STATUS_OK or _REJECT */
} ConnectAckPayload;

typedef struct 
{
    uint32_t task_id; /* which task to run */
    uint32_t generation; /* current retry generation */
    char command[256]; /* shell command, null-terminated */
} DispatchPayload;

typedef struct 
{
    uint32_t task_id;
    uint32_t generation;
} AckPayload;

typedef struct //Total size 4+4+4+4+4096 = 4112 bytes
{
    uint32_t task_id;
    uint32_t generation;
    int32_t  exit_code; /* process exit code; 0 = success  */
    uint32_t output_len; /* valid bytes in output[] the rest is padding */
    char output[MAX_OUTPUT_LEN]; /* captured stdout + stderr */
} ResultPayload;

typedef struct // Per-connection receive buffer
{
    uint8_t data[RECV_BUF_SIZE]; /* raw byte accumulator */
    uint32_t bytes_filled; /* how many bytes are valid so far  */
} ConnRecvBuffer;

void conn_recv_init(ConnRecvBuffer *rbuf); // Must be called before the first conn_recv_push()

int proto_pack_connect(uint8_t *buf, size_t buf_size, uint32_t role, const char *auth_token);

int proto_pack_connect_ack(uint8_t *buf, size_t buf_size, uint32_t status);

int proto_pack_dispatch(uint8_t *buf, size_t buf_size, uint32_t task_id, uint32_t generation, const char *command);

int proto_pack_ack(uint8_t *buf, size_t buf_size, uint32_t task_id, uint32_t generation);

int proto_pack_result(uint8_t *buf, size_t buf_size, uint32_t task_id, uint32_t generation, int32_t exit_code, const char *output, uint32_t output_len);

int proto_try_deframe(ConnRecvBuffer *rbuf, uint32_t *out_type, uint8_t  *out_payload, uint32_t *out_payload_len);

int conn_recv_push(ConnRecvBuffer *rbuf, const uint8_t *data, uint32_t len); // Append len bytes from data[] to rbuf. Returns 0 on success and -1 on buffer overflow

int proto_parse_connect(const uint8_t *payload, uint32_t payload_len, uint32_t *role, char *auth_token);

int proto_parse_connect_ack(const uint8_t *payload, uint32_t payload_len, uint32_t *status);

int proto_parse_dispatch(const uint8_t *payload, uint32_t payload_len, uint32_t *task_id, uint32_t *generation, char *command);

int proto_parse_ack(const uint8_t *payload, uint32_t payload_len, uint32_t *task_id, uint32_t *generation);

int proto_parse_result(const uint8_t *payload, uint32_t payload_len, uint32_t *task_id, uint32_t *generation, int32_t *exit_code, char *output, uint32_t *output_len);

#endif