CC = gcc
CFLAGS = -Wall -Wextra -pthread -g -MMD -MP
INCLUDES = -I./include

.PHONY: all clean tests

MASTER_SRCS = src/master/main.c src/master/scheduler.c src/master/server.c \
              src/common/graph_utils.c src/common/logger.c \
              src/common/ipc_sync.c src/common/protocol.c
MASTER_OBJS = $(MASTER_SRCS:.c=.o)

WORKER_SRCS = src/worker/main.c src/worker/executor.c src/worker/client.c \
              src/common/protocol.c src/common/ipc_sync.c
WORKER_OBJS = $(WORKER_SRCS:.c=.o)

all: master worker | logs

logs:
	mkdir -p logs

master: $(MASTER_OBJS)
	$(CC) $(CFLAGS) -o master $(MASTER_OBJS)

worker: $(WORKER_OBJS)
	$(CC) $(CFLAGS) -o worker $(WORKER_OBJS)

test_dag: tests/test_dag.c src/common/graph_utils.c
	$(CC) $(CFLAGS) $(INCLUDES) -o test_dag $^

test_protocol: tests/test_protocol.c src/common/protocol.c
	$(CC) $(CFLAGS) $(INCLUDES) -o test_protocol $^

test_executor: tests/test_executor.c src/worker/executor.c
	$(CC) $(CFLAGS) $(INCLUDES) -o test_executor $^

test_ipc: tests/test_ipc.c src/common/ipc_sync.c
	$(CC) $(CFLAGS) $(INCLUDES) -o test_ipc $^

tests: test_dag test_protocol test_executor test_ipc

%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

clean:
	rm -f master worker test_dag test_protocol test_executor test_ipc
	rm -f src/master/*.o src/master/*.d \
	      src/worker/*.o src/worker/*.d \
	      src/common/*.o src/common/*.d \
	      tests/*.d

-include $(MASTER_OBJS:.o=.d) $(WORKER_OBJS:.o=.d)
-include $(wildcard tests/*.d)