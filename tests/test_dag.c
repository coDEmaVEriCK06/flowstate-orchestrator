#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "graph.h"

static int g_passed = 0;
static int g_failed = 0;

static void run_test(const char *name, int (*fn)(void)) 
{
    printf("\n[TEST] %s\n", name);
    if(fn() == 0) 
    {
        printf("  --> PASS\n");
        g_passed++;
    } 
    else 
    {
        printf("  --> FAIL\n");
        g_failed++;
    }
}

/* ASSERT: print the message and return -1 (fail) if cond is false. */
#define ASSERT(cond, msg)                                          \
    do {                                                           \
        if (!(cond)) {                                             \
            printf("  ASSERT FAILED: %s  (line %d)\n", msg, __LINE__); \
            return -1;                                             \
        } else {                                                   \
            printf("  ok: %s\n", msg);                            \
        }                                                          \
    } while (0)

// Test 1: dag_parse — basic parsing of sample.dag
static int test_parse(void) 
{
    DAGGraph graph;
    ASSERT(dag_parse(&graph, "dags/sample.dag") == 0,
           "dag_parse returns 0 for valid file");
    ASSERT(graph.num_nodes == 4,
           "Parsed exactly 4 nodes");
    ASSERT(graph.nodes[0].task_id == 1,
           "First node has task_id=1");
    ASSERT(graph.nodes[3].task_id == 4,
           "Fourth node has task_id=4");
    ASSERT(graph.nodes[0].max_retries == 1,
           "Task 1 has max_retries=1");
    ASSERT(graph.nodes[3].max_retries == 2,
           "Task 4 has max_retries=2");
    ASSERT(strcmp(graph.nodes[0].command, "jobs/dummy.sh 1") == 0,
           "Task 1 command string parsed correctly including space");
    ASSERT(strcmp(graph.nodes[3].command, "jobs/dummy.sh 4") == 0,
           "Task 4 command string parsed correctly");
    ASSERT(graph.nodes[0].state == PENDING,
           "All nodes start in PENDING state");
    ASSERT(graph.nodes[3].state == PENDING,
           "Task 4 starts in PENDING state");
    dag_destroy(&graph);
    return 0;
}

// Test 2: dag_build_edges — diamond dependency structure
static int test_build_edges(void) 
{
    DAGGraph graph;
    dag_parse(&graph, "dags/sample.dag");

    ASSERT(dag_build_edges(&graph) == 0,
           "dag_build_edges returns 0 for valid deps");

    /* Task 1: root node, no parents, 2 children */
    ASSERT(graph.nodes[0].num_parents == 0,
           "Task 1 has 0 parents");
    ASSERT(graph.nodes[0].num_children == 2,
           "Task 1 has 2 children (tasks 2 and 3)");

    /* Task 4: diamond tip, 2 parents, 0 children */
    ASSERT(graph.nodes[3].num_parents == 2,
           "Task 4 has 2 parents (tasks 2 and 3)");
    ASSERT(graph.nodes[3].num_children == 0,
           "Task 4 has no children");

    /* Tasks 2 and 3: middle of diamond */
    ASSERT(graph.nodes[1].num_parents == 1,
           "Task 2 has 1 parent");
    ASSERT(graph.nodes[1].num_children == 1,
           "Task 2 has 1 child");
    ASSERT(graph.nodes[2].num_parents == 1,
           "Task 3 has 1 parent");
    ASSERT(graph.nodes[2].num_children == 1,
           "Task 3 has 1 child");

    dag_destroy(&graph);
    return 0;
}

// Test 3: dag_validate — Kahn's algorithm on a valid DAG
static int test_validate_acyclic(void) 
{
    DAGGraph graph;
    dag_parse(&graph, "dags/sample.dag");
    dag_build_edges(&graph);

    ASSERT(dag_validate(&graph) == 0,
           "dag_validate returns 0 for an acyclic graph");

    dag_destroy(&graph);
    return 0;
}

// Test 4: task_transition — full state machine coverage
static int test_task_transition(void) 
{
    DAGGraph graph;
    dag_parse(&graph, "dags/sample.dag");
    dag_build_edges(&graph);

    DAGNode *node = &graph.nodes[0]; /* Task 1 */

    /* --- Valid transitions along the happy path --- */
    ASSERT(task_transition(node, PENDING, READY) == 0,
           "PENDING -> READY succeeds");
    ASSERT(node->state == READY,
           "State is READY after transition");

    ASSERT(task_transition(node, READY, DISPATCHED) == 0,
           "READY -> DISPATCHED succeeds");

    ASSERT(task_transition(node, DISPATCHED, RUNNING) == 0,
           "DISPATCHED -> RUNNING succeeds");

    ASSERT(task_transition(node, RUNNING, FAILED) == 0,
           "RUNNING -> FAILED succeeds");

    /* --- Retry path: generation must increment --- */
    uint32_t gen_before = node->generation;
    ASSERT(task_transition(node, FAILED, READY) == 0,
           "FAILED -> READY (retry) succeeds");
    ASSERT(node->generation == gen_before + 1,
           "Generation incremented by 1 on retry");
    ASSERT(node->retry_count == 1,
           "retry_count incremented to 1");

    /* --- READY -> DISPATCHED -> RUNNING -> TIMED_OUT -> READY --- */
    ASSERT(task_transition(node, READY, DISPATCHED) == 0,
           "READY -> DISPATCHED (second attempt)");
    ASSERT(task_transition(node, DISPATCHED, RUNNING) == 0,
           "DISPATCHED -> RUNNING (second attempt)");
    ASSERT(task_transition(node, RUNNING, TIMED_OUT) == 0,
           "RUNNING -> TIMED_OUT succeeds");

    uint32_t gen_before2 = node->generation;
    ASSERT(task_transition(node, TIMED_OUT, READY) == 0,
           "TIMED_OUT -> READY (retry) succeeds");
    ASSERT(node->generation == gen_before2 + 1,
           "Generation incremented on TIMED_OUT retry path too");

    /* --- Max retries: FAILED -> DEAD --- */
    ASSERT(task_transition(node, READY, DISPATCHED) == 0, "setup");
    ASSERT(task_transition(node, DISPATCHED, RUNNING) == 0, "setup");
    ASSERT(task_transition(node, RUNNING, FAILED) == 0, "setup");
    ASSERT(task_transition(node, FAILED, DEAD) == 0,
           "FAILED -> DEAD (max retries exhausted) succeeds");
    ASSERT(node->state == DEAD,
           "Node is now in terminal DEAD state");

    /* --- Invalid transitions: stale state mismatch --- */
    ASSERT(task_transition(node, PENDING, READY) == -1,
           "PENDING->READY rejected when node is DEAD (stale)");

    /* --- Invalid transitions: terminal states block all outgoing --- */
    ASSERT(task_transition(node, DEAD, PENDING) == -1,
           "DEAD -> PENDING rejected (DEAD is terminal)");
    ASSERT(task_transition(node, DEAD, READY) == -1,
           "DEAD -> READY rejected (DEAD is terminal)");

    /* --- Test COMPLETED terminal state --- */
    DAGNode *node2 = &graph.nodes[1]; /* Task 2 */
    task_transition(node2, PENDING, READY);
    task_transition(node2, READY, DISPATCHED);
    task_transition(node2, DISPATCHED, RUNNING);
    task_transition(node2, RUNNING, COMPLETED);
    ASSERT(task_transition(node2, COMPLETED, PENDING) == -1,
           "COMPLETED -> PENDING rejected (COMPLETED is terminal)");
    ASSERT(task_transition(node2, COMPLETED, FAILED) == -1,
           "COMPLETED -> FAILED rejected (COMPLETED is terminal)");

    dag_destroy(&graph);
    return 0;
}

// Test 5: dag_init_ready_queue — only task 1 starts READY
static int test_ready_queue_init(void) 
{
    DAGGraph graph;
    dag_parse(&graph, "dags/sample.dag");
    dag_build_edges(&graph);
    dag_validate(&graph);
    dag_init_ready_queue(&graph);

    ASSERT(graph.ready_queue.count == 1,
           "Exactly 1 task is initially READY");

    DAGNode *first = dag_dequeue_ready(&graph);
    ASSERT(first != NULL,
           "Dequeue returns a non-NULL pointer");
    ASSERT(first->task_id == 1,
           "Task 1 is the initial READY task");
    ASSERT(graph.ready_queue.count == 0,
           "Queue is empty after dequeue");

    dag_destroy(&graph);
    return 0;
}

// Test 6: dag_notify_children — full diamond execution simulation
static int test_notify_children(void) 
{
    DAGGraph graph;
    dag_parse(&graph, "dags/sample.dag");
    dag_build_edges(&graph);
    dag_validate(&graph);
    dag_init_ready_queue(&graph);

    /* Execute task 1 */
    DAGNode *t1 = dag_dequeue_ready(&graph);
    ASSERT(t1->task_id == 1, "Dequeued task 1");
    task_transition(t1, READY, DISPATCHED);
    task_transition(t1, DISPATCHED, RUNNING);
    task_transition(t1, RUNNING, COMPLETED);
    dag_notify_children(&graph, t1);

    ASSERT(graph.ready_queue.count == 2,
           "Tasks 2 and 3 both READY after task 1 completes");

    /* Execute task 2 */
    DAGNode *t2 = dag_dequeue_ready(&graph);
    ASSERT(t2->task_id == 2, "Dequeued task 2 (FIFO order)");
    task_transition(t2, READY, DISPATCHED);
    task_transition(t2, DISPATCHED, RUNNING);
    task_transition(t2, RUNNING, COMPLETED);
    dag_notify_children(&graph, t2);

    /* Task 4 has 2 parents. Only 1 done. Must NOT be READY yet. */
    ASSERT(graph.ready_queue.count == 1,
           "Task 3 still in queue; task 4 NOT yet READY");

    /* Execute task 3 */
    DAGNode *t3 = dag_dequeue_ready(&graph);
    ASSERT(t3->task_id == 3, "Dequeued task 3");
    task_transition(t3, READY, DISPATCHED);
    task_transition(t3, DISPATCHED, RUNNING);
    task_transition(t3, RUNNING, COMPLETED);
    dag_notify_children(&graph, t3);

    /* Now both parents of task 4 are done — it must be READY */
    ASSERT(graph.ready_queue.count == 1,
           "Task 4 is now READY after both parents complete");

    DAGNode *t4 = dag_dequeue_ready(&graph);
    ASSERT(t4->task_id == 4, "Task 4 is the final dequeued node");
    ASSERT(t4->state == READY, "Task 4 is in READY state");
    dag_destroy(&graph);
    return 0;
}

// Test 7: dag_propagate_dead — cascade failure from root
static int test_cascade_fail(void) 
{
    DAGGraph graph;
    dag_parse(&graph, "dags/sample.dag");
    dag_build_edges(&graph);

    /* Drive task 1 to DEAD through the full retry path */
    DAGNode *t1 = &graph.nodes[0];
    task_transition(t1, PENDING, READY);
    task_transition(t1, READY, DISPATCHED);
    task_transition(t1, DISPATCHED, RUNNING);
    task_transition(t1, RUNNING, FAILED);
    task_transition(t1, FAILED, DEAD);

    ASSERT(t1->state == DEAD, "Task 1 is in DEAD state");

    dag_propagate_dead(&graph, t1);

    ASSERT(graph.nodes[1].state == CASCADE_FAILED,
           "Task 2 is CASCADE_FAILED");
    ASSERT(graph.nodes[2].state == CASCADE_FAILED,
           "Task 3 is CASCADE_FAILED");
    ASSERT(graph.nodes[3].state == CASCADE_FAILED,
           "Task 4 is CASCADE_FAILED");

    dag_destroy(&graph);
    return 0;
}

int main(void) 
{
    printf("=========================================\n");
    printf("  Phase 1: DAG Engine Test Suite\n");
    printf("=========================================\n");

    run_test("Parse sample.dag",            test_parse);
    run_test("Build diamond edges",         test_build_edges);
    run_test("Validate acyclic graph",      test_validate_acyclic);
    run_test("State machine transitions",   test_task_transition);
    run_test("Ready queue initialization",  test_ready_queue_init);
    run_test("notify_children simulation",  test_notify_children);
    run_test("CASCADE_FAILED propagation",  test_cascade_fail);

    printf("\n=========================================\n");
    printf("  Results: %d passed, %d failed\n", g_passed, g_failed);
    printf("=========================================\n");

    /* Return 0 only if every test passed — lets CI tools detect failure */
    return (g_failed == 0) ? 0 : 1;
}