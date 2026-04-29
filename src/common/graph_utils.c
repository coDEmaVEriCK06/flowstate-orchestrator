#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "graph.h"

DAGNode *dag_find_node(DAGGraph *graph, int task_id)
{
    for(int i=0; i<graph->num_nodes; i++)
    {
        if(graph->nodes[i].task_id == task_id)
        {
            return &graph->nodes[i];
        }
    }
    return NULL;
}

int dag_is_complete(DAGGraph *graph)
{
    for(int i = 0; i < graph->num_nodes; i++) 
    {
        TaskState s = graph->nodes[i].state;
        if(s != COMPLETED && s != DEAD && s != CASCADE_FAILED) return 0;
    }
    return 1;
}

static int extract_quoted(const char *src, char *dest, size_t dest_size)
{
    const char *open = strchr(src, '"'); // strchr returns a pointer to the first occurrence of a character
    if(!open) return -1;
    open++; // We need to look at the character right after the quotation mark
    const char *close = strchr(open, '"'); // Here the search starts from the pointer open rather than the whole string
    if(!close) return -1;
    size_t len = (size_t)(close-open);
    if(len >= dest_size) return -1;
    memcpy(dest, open, len); // Copies exactly len bytes from open (source pointer) to dest (destination pointer). Used instead of strcpy since we need an exact number of bytes to copy
    dest[len] = '\0'; // memcpy doesn't include the null terminating character while copying
    return 0;
}

static int parse_deps(const char *line, int *deps, int *nums_deps)
{
    *nums_deps = 0;
    const char *ptr = strstr(line, "deps="); // strstr locates the first occurrence of a substring in a larger string
    if(!ptr) return -1;
    ptr += 5; // To skip the 5 characters of deps=
    if(strncmp(ptr, "none", 4) == 0) return 0; // strncmp compares 4 positions starting from ptr to the string "none"
    char buf[128];
    int i = 0;
    while(ptr[i] && ptr[i] != ' ' && ptr[i] != '\t' && ptr[i] != '\n' && ptr[i] != '\r')
    {
        if(i >= 127) return -1;
        buf[i] = ptr[i];
        i++;
    }
    buf[i] = '\0';
    // Buffer array is used because strtok destroys the original array while tokanization and hence we can't work with the main array
    char *token = strtok(buf, ","); // Splits the array on a specific delimiter
    while(token)
    {
        if(*nums_deps >= MAX_DEPS) return -1;
        int id = atoi(token); // atoi converts something like "42" to the integer 42
        if(id <= 0) return -1;
        deps[(*nums_deps)++] = id;
        token = strtok(NULL, ",");
    }
    return 0;
}

int dag_parse(DAGGraph *graph, const char *filepath)
{
    memset(graph, 0, sizeof(DAGGraph)); // Initializes all values of graph to 0
    pthread_mutex_init(&graph->ready_queue.lock, NULL);
    pthread_cond_init(&graph->ready_queue.not_empty, NULL);
    graph->ready_queue.head = 0;
    graph->ready_queue.tail = 0;
    graph->ready_queue.count = 0;
    FILE *fp = fopen(filepath, "r");
    if(!fp)
    {
        fprintf(stderr, "[dag_parse] Cannot open '%s': %s\n", filepath, strerror(errno));
        return -1;
    }
    char line[MAX_LINE_LEN];
    int line_num = 0;
    while(fgets(line, sizeof(line), fp))
    {
        line_num++;
        line[strcspn(line, "\n")] = '\n'; // Replaces newline character by null character for clean error messages
        if(line[0] == '\0' || line[0] == '#') continue; // Skips blank lines and commented lines
        if(strncmp(line, "TASK", 4) != 0) 
        {
            fprintf(stderr, "[dag_parse] Line %d: expected 'TASK', got: %s\n", line_num, line);
            fclose(fp);
            return -1;
        }
        if(graph->num_nodes >= MAX_TASKS) 
        {
            fprintf(stderr, "[dag_parse] Line %d: exceeded MAX_TASKS (%d)\n", line_num, MAX_TASKS);
            fclose(fp);
            return -1;
        }
        DAGNode *node = &graph->nodes[graph->num_nodes];
        memset(node, 0, sizeof(DAGNode)); // Redundant since everything is set to zero by default but defensive
        const char *id_ptr = strstr(line, "id="); // The substring check operation on line
        if(!id_ptr) 
        {
            fprintf(stderr, "[dag_parse] Line %d: missing 'id='\n", line_num);
            fclose(fp);
            return -1;
        }
        node->task_id = atoi(id_ptr + 3);
        if(node->task_id <= 0) 
        {
            fprintf(stderr, "[dag_parse] Line %d: task id must be > 0\n", line_num);
            fclose(fp);
            return -1;
        }
        const char *cmd_ptr = strstr(line, "cmd=");
        if(!cmd_ptr || extract_quoted(cmd_ptr, node->command, MAX_CMD_LEN) != 0) 
        {
            fprintf(stderr, "[dag_parse] Line %d: missing or malformed 'cmd=\"...\"'\n", line_num);
            fclose(fp);
            return -1;
        }
        const char *retry_ptr = strstr(line, "retries=");
        if(!retry_ptr) 
        {
            fprintf(stderr, "[dag_parse] Line %d: missing 'retries='\n", line_num);
            fclose(fp);
            return -1;
        }
        node->max_retries = atoi(retry_ptr + 8);
        if(parse_deps(line, node->dep_ids, &node->num_deps) != 0) 
        {
            fprintf(stderr, "[dag_parse] Line %d: malformed 'deps='\n", line_num);
            fclose(fp);
            return -1;
        }
        pthread_mutex_init(&node->lock, NULL); // This is a per-node mutex initialized here so that this happens before any thread touches the node
        node->state = PENDING;
        node->generation = 0;
        node->retry_count = 0;
        node->parents_completed = 0;
        node->num_children = 0;
        graph->num_nodes++;
    }
    fclose(fp);
    if(graph->num_nodes == 0) 
    {
        fprintf(stderr, "[dag_parse] No tasks found in '%s'\n", filepath);
        return -1;
    }
    return 0;
}

int dag_build_edges(DAGGraph *graph) 
{
    for(int i = 0; i < graph->num_nodes; i++) 
    {
        DAGNode *child = &graph->nodes[i];
        /* num_parents equals the number of declared deps */
        child->num_parents = child->num_deps; // num_deps is result from parser which is equal to num_parents
        for(int d = 0; d < child->num_deps; d++) 
        {
            int dep_id = child->dep_ids[d];
            DAGNode *parent = dag_find_node(graph, dep_id);
            if(!parent) 
            {
                fprintf(stderr, "[dag_build_edges] Task %d depends on unknown task id=%d\n", child->task_id, dep_id);
                return -1;
            }
            if(parent == child) 
            {
                fprintf(stderr, "[dag_build_edges] Task %d declares a self-dependency\n", child->task_id);
                return -1;
            }
            if(parent->num_children >= MAX_TASKS) 
            {
                fprintf(stderr, "[dag_build_edges] Task %d has too many children\n", parent->task_id);
                return -1;
            }
            /* Register child in parent's children array */
            parent->children[parent->num_children++] = child;
        }
    }
    return 0;
}

int dag_validate(DAGGraph *graph) 
{
    int in_degree[MAX_TASKS] = {0};
    for(int i = 0; i < graph->num_nodes; i++) 
    {
        in_degree[i] = graph->nodes[i].num_parents;
    }
    /* Local queue — just an array with head/tail */
    int queue[MAX_TASKS];
    int head = 0, tail = 0;
    for(int i = 0; i < graph->num_nodes; i++) 
    {
        if(in_degree[i] == 0) queue[tail++] = i;
    }
    int processed = 0;
    while(head < tail) 
    {
        int idx = queue[head++];
        processed++;
        DAGNode *node = &graph->nodes[idx];
        for(int c = 0; c < node->num_children; c++) 
        {
            /* Find the index of this child in graph->nodes[] */
            for(int j = 0; j < graph->num_nodes; j++) 
            {
                if(&graph->nodes[j] == node->children[c]) {
                    in_degree[j]--; // BFS on children of the deleted parent reducing their in-degree by 1
                    if(in_degree[j] == 0) queue[tail++] = j; // If in the decrementation process, a child has in_degree 0, it becomes eligible to be part of the queue
                    break;
                }
            }
        }
    }
    if(processed != graph->num_nodes) 
    {
        fprintf(stderr, "[dag_validate] Cycle detected: only %d of %d nodes are reachable.\n", processed, graph->num_nodes);
        return -1;
    }
    return 0;
}

void dag_init_ready_queue(DAGGraph *graph) 
{
    for(int i = 0; i < graph->num_nodes; i++) 
    {
        DAGNode *node = &graph->nodes[i];
        if(node->num_parents == 0) 
        {
            /* Direct write is safe here — single-threaded init path.
             * We bypass task_transition() intentionally: at this point
             * no threads exist yet to race with us. */
            node->state = READY;
            dag_enqueue_ready(graph, node);
        }
    }
}

void dag_enqueue_ready(DAGGraph *graph, DAGNode *node) 
{
    ReadyQueue *q = &graph->ready_queue;
    pthread_mutex_lock(&q->lock);
    if(q->count >= MAX_TASKS) 
    {
        fprintf(stderr, "[dag_enqueue_ready] Queue overflow — task %d dropped!\n", node->task_id);
        pthread_mutex_unlock(&q->lock);
        return;
    }
    q->slots[q->tail] = node;
    q->tail = (q->tail + 1) % MAX_TASKS; /* wrap around for circular buffer */
    q->count++;
    /* Wake one waiting dispatcher thread */
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->lock);
}

DAGNode *dag_dequeue_ready(DAGGraph *graph) 
{
    ReadyQueue *q = &graph->ready_queue;
    pthread_mutex_lock(&q->lock);
    /* MUST be a while loop, not if.
     * pthread_cond_wait() can return spuriously — i.e., it can
     * wake up without pthread_cond_signal() being called. The
     * while loop re-checks the actual condition (count == 0)
     * after every wakeup. An if() would proceed on a spurious
     * wakeup and dereference garbage memory. */
    while(q->count == 0) 
    {
        pthread_cond_wait(&q->not_empty, &q->lock);
    }
    DAGNode *node = q->slots[q->head];
    q->head = (q->head + 1) % MAX_TASKS;
    q->count--;
    pthread_mutex_unlock(&q->lock);
    return node;
}

int task_transition(DAGNode *node, TaskState expected, TaskState next) 
{
    pthread_mutex_lock(&node->lock);
    /* Check 1: current state must match what the caller expects.
     * If not, this is a stale message — drop it silently. */
    if(node->state != expected) 
    {
        pthread_mutex_unlock(&node->lock);
        return -1;
    }
    /* Check 2: validate that (expected -> next) is a legal pair.
     * This makes the function future-proof against coding mistakes
     * in the calling code. */
    int valid = 0;
    switch(expected) 
    {
        case PENDING:
            valid = (next == READY || next == CASCADE_FAILED);
            break;
        case READY:
            valid = (next == DISPATCHED || next == CASCADE_FAILED);
            break;
        case DISPATCHED:
            /* Can go to RUNNING (ACK received), FAILED (crash before ACK),
             * or TIMED_OUT (no ACK within deadline) */
            valid = (next == RUNNING || next == FAILED || next == TIMED_OUT);
            break;
        case RUNNING:
            valid = (next == COMPLETED || next == FAILED || next == TIMED_OUT);
            break;
        case FAILED:
            valid = (next == READY || next == DEAD);
            break;
        case TIMED_OUT:
            valid = (next == READY || next == DEAD);
            break;
        /* Terminal states have no valid outgoing transitions */
        case COMPLETED:
        case DEAD:
        case CASCADE_FAILED:
        default:
            valid = 0;
            break;
    }
    if(!valid) 
    {
        pthread_mutex_unlock(&node->lock);
        return -1;
    }
    node->state = next;
    /* Increment generation on every retry re-queue.
     * The master embeds (task_id, generation) in every dispatch message. When a
     * MSG_RESULT arrives, the master compares its generation
     * against the node's current generation. A mismatch means
     * the result is from a previous (timed-out) dispatch so we drop it. */
    if(next == READY && (expected == FAILED || expected == TIMED_OUT)) 
    {
        node->generation++;
        node->retry_count++;
    }
    pthread_mutex_unlock(&node->lock);
    return 0;
}

void dag_notify_children(DAGGraph *graph, DAGNode *completed_node) 
{
    for(int c = 0; c < completed_node->num_children; c++) 
    {
        DAGNode *child = completed_node->children[c];
        /* The increment and threshold check must be one critical section.
         * The reason being, two parent-completion events might race. If they both
         * read parents_completed == N-1, both compute N, and both try to
         * enqueue the child — the child runs twice. Holding child->lock
         * across both operations prevents this: only one thread can be
         * the one that flips the counter from N-1 to N. */
        pthread_mutex_lock(&child->lock);
        child->parents_completed++;
        int all_done = (child->parents_completed == child->num_parents);
        pthread_mutex_unlock(&child->lock);
        if(all_done) 
        {
            if(task_transition(child, PENDING, READY) == 0) 
            {
                dag_enqueue_ready(graph, child);
            }
        }
    }
}

void dag_propagate_dead(DAGGraph *graph, DAGNode *dead_node) 
{
    /* graph parameter kept for API consistency and future use */
    (void)graph;
    /* BFS using a local pointer queue */
    DAGNode *queue[MAX_TASKS];
    int head = 0, tail = 0;
    // Queue is started from children of dead node and not the node itself since it already has a DEAD state and should not be overwritten by any other state
    for(int c = 0; c < dead_node->num_children; c++) 
    {
        queue[tail++] = dead_node->children[c];
    }
    while(head < tail) 
    {
        DAGNode *cur = queue[head++];
        pthread_mutex_lock(&cur->lock);
        if(cur->state == PENDING || cur->state == READY) 
        {
            cur->state = CASCADE_FAILED;
            /* Enqueue children for further propagation */
            for(int c = 0; c < cur->num_children; c++) 
            {
                if(tail < MAX_TASKS) queue[tail++] = cur->children[c];
            }
        }
        /* COMPLETED children are left alone — they already succeeded */
        pthread_mutex_unlock(&cur->lock);
    }
}

const char *task_state_name(TaskState state) 
{
    switch(state) 
    {
        case PENDING:        return "PENDING";
        case READY:          return "READY";
        case DISPATCHED:     return "DISPATCHED";
        case RUNNING:        return "RUNNING";
        case COMPLETED:      return "COMPLETED";
        case FAILED:         return "FAILED";
        case TIMED_OUT:      return "TIMED_OUT";
        case DEAD:           return "DEAD";
        case CASCADE_FAILED: return "CASCADE_FAILED";
        default:             return "UNKNOWN";
    }
}

void dag_print(const DAGGraph *graph) 
{
    printf("=== DAG Graph (%d nodes) ===\n", graph->num_nodes);
    for(int i = 0; i < graph->num_nodes; i++) 
    {
        const DAGNode *n = &graph->nodes[i];
        printf("  Task %2d | %-14s | gen=%-2u | parents=%d/%d | "
               "retries=%d/%d | cmd=%s\n",
               n->task_id,
               task_state_name(n->state),
               n->generation,
               n->parents_completed,
               n->num_parents,
               n->retry_count,
               n->max_retries,
               n->command);
        if(n->num_children > 0) 
        {
            printf("          -> children: ");
            for(int c = 0; c < n->num_children; c++) printf("%d ", n->children[c]->task_id);
            printf("\n");
        }
    }
    printf("  Ready queue depth: %d\n", graph->ready_queue.count);
    printf("=========================\n");
}

void dag_destroy(DAGGraph *graph) 
{
    for(int i = 0; i < graph->num_nodes; i++) 
    {
        pthread_mutex_destroy(&graph->nodes[i].lock);
    }
    pthread_mutex_destroy(&graph->ready_queue.lock);
    pthread_cond_destroy(&graph->ready_queue.not_empty);
}