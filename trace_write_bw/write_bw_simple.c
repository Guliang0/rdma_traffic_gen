#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <stdint.h>
#include <time.h>


#define MIN_RANKS_SUPPORTED 2
#define MAX_RANKS_SUPPORTED 3
#define TRACE_LANES_PER_CHANNEL 20
#define PORT_BASE 18515
#define TRACE_RESULT_SUCCESS 0

static const char *rank_cx_ip[MAX_RANKS_SUPPORTED] = {
    "192.168.2.10",
    "192.168.2.12",
    "192.168.2.11"
};

struct trace_run_result {
    uint64_t elapsed_ns;
    uint64_t iters;
    int32_t status;
    int32_t reserved;
};

static int read_full(int fd, void *buffer, size_t length)
{
    char *cursor = buffer;
    size_t total = 0;

    while (total < length) {
        ssize_t rc = read(fd, cursor + total, length - total);

        if (rc > 0) {
            total += (size_t)rc;
            continue;
        }
        if (rc < 0 && errno == EINTR)
            continue;
        return -1;
    }

    return 0;
}

static int port_for_conn_lane(int nranks, int src, int dst, int lane_id,
                              char *buf, size_t len)
{
    long port;

    if (nranks < MIN_RANKS_SUPPORTED || nranks > MAX_RANKS_SUPPORTED ||
        src < 0 || src >= nranks ||
        dst < 0 || dst >= nranks ||
        src == dst ||
        lane_id < 0 || lane_id >= TRACE_LANES_PER_CHANNEL) {
        fprintf(stderr,
                "Invalid connection parameters nranks=%d src=%d dst=%d lane=%d\n",
                nranks, src, dst, lane_id);
        return -1;
    }

    /*
     * Use the runtime nranks in the mapping.
     * For -np 2:
     *   0->1 uses PORT_BASE + (0*2+1)*20 + lane
     *   1->0 uses PORT_BASE + (1*2+0)*20 + lane
     * For -np 3 this remains a full-mesh directed-channel mapping.
     */
    port = PORT_BASE +
        (src * nranks + dst) * TRACE_LANES_PER_CHANNEL +
        lane_id;

    if (port > 65535) {
        fprintf(stderr,
                "Invalid connection port nranks=%d src=%d dst=%d lane=%d port=%ld\n",
                nranks, src, dst, lane_id, port);
        return -1;
    }

    snprintf(buf, len, "%ld", port);
    return 0;
}

static uint64_t wall_time_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

static void setenv_int(const char *name, int value)
{
    char buf[32];
    snprintf(buf, sizeof(buf), "%d", value);
    setenv(name, buf, 1);
}

static void setenv_u64(const char *name, uint64_t value)
{
    char buf[64];
    snprintf(buf, sizeof(buf), "%lu", (unsigned long)value);
    setenv(name, buf, 1);
}

static pid_t spawn_write_bw(int rank,
                            const char *bin,
                            const char *port,
                            const char *server_ip,
                            int src_rank,
                            int dst_rank,
                            int lane_id,
                            int lane_count,
                            int result_read_fd,
                            int result_write_fd,
                            uint64_t global_start_ns,
                            int argc,
                            char **argv)
{
    int common_argc = argc - 2;
    int extra = server_ip ? 5 : 4;
    char **cmd = calloc(common_argc + extra, sizeof(char *));
    int k = 0;

    if (!cmd) {
        perror("calloc");
        return -1;
    }

    cmd[k++] = (char *)bin;
    cmd[k++] = "-p";
    cmd[k++] = (char *)port;

    for (int i = 2; i < argc; i++)
        cmd[k++] = argv[i];

    if (server_ip)
        cmd[k++] = (char *)server_ip;

    cmd[k] = NULL;

    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        free(cmd);
        return -1;
    }

    if (pid == 0) {
        if (result_read_fd >= 0)
            close(result_read_fd);

        setenv("TRACE_ROLE", server_ip ? "client" : "server", 1);
        setenv_int("TRACE_MY_RANK", rank);
        setenv_int("TRACE_SRC_RANK", src_rank);
        setenv_int("TRACE_DST_RANK", dst_rank);
        setenv_int("TRACE_PEER_RANK", server_ip ? dst_rank : src_rank);
        setenv_int("TRACE_LANE_ID", lane_id);
        setenv_int("TRACE_LANE_COUNT", lane_count);
        setenv_int("INDEX", lane_id);
        setenv_u64("TRACE_GLOBAL_START_NS", global_start_ns);

        if (result_write_fd >= 0)
            setenv_int("TRACE_RESULT_FD", result_write_fd);
        else
            unsetenv("TRACE_RESULT_FD");

        if (server_ip)
            setenv("TRACE_SERVER_IP", server_ip, 1);

        fprintf(stderr,
                "[rank %d] %s channel=%d->%d lane=%d/%d port=%s%s%s exec:",
                rank, server_ip ? "client" : "server", src_rank, dst_rank,
                lane_id, lane_count, port,
                server_ip ? " server_ip=" : "",
                server_ip ? server_ip : "");
        for (int i = 0; cmd[i]; i++)
            fprintf(stderr, " %s", cmd[i]);
        fprintf(stderr, "\n");

        /*
         * Keep the original launcher behavior: child waits before exec.
         * If trace_write_bw already waits internally on TRACE_GLOBAL_START_NS,
         * remove this block to avoid delaying QP setup until the start time.
         */
        if (global_start_ns > 0) {
            struct timespec target;
            target.tv_sec = global_start_ns / 1000000000ULL;
            target.tv_nsec = global_start_ns % 1000000000ULL;
            fprintf(stderr, "[rank %d] waiting until %lu.%09lu\n",
                    rank, (unsigned long)target.tv_sec, (unsigned long)target.tv_nsec);
            fflush(stderr);
            clock_nanosleep(CLOCK_REALTIME, TIMER_ABSTIME, &target, NULL);
        }

        execvp(bin, cmd);
        perror("execvp");
        _exit(127);
    }

    free(cmd);
    return pid;
}

int main(int argc, char **argv)
{
    int rank, nranks;
    int status = 0;
    int global_failed = 0;
    int local_failed = 0;
    uint64_t local_max_elapsed_ns = 0;
    uint64_t global_max_elapsed_ns = 0;
    pid_t servers[MAX_RANKS_SUPPORTED][TRACE_LANES_PER_CHANNEL];
    pid_t clients[MAX_RANKS_SUPPORTED][TRACE_LANES_PER_CHANNEL];
    int client_result_fds[MAX_RANKS_SUPPORTED][TRACE_LANES_PER_CHANNEL];
    uint64_t global_start_ns = 0;

    for (int i = 0; i < MAX_RANKS_SUPPORTED; i++) {
        for (int lane_id = 0; lane_id < TRACE_LANES_PER_CHANNEL; lane_id++) {
            servers[i][lane_id] = -1;
            clients[i][lane_id] = -1;
            client_result_fds[i][lane_id] = -1;
        }
    }

    fprintf(stderr, "before MPI_Init\n");
    fflush(stderr);

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nranks);

    if (nranks < MIN_RANKS_SUPPORTED || nranks > MAX_RANKS_SUPPORTED) {
        if (rank == 0) {
            fprintf(stderr,
                    "This launcher supports %d to %d MPI ranks; got %d.\n",
                    MIN_RANKS_SUPPORTED, MAX_RANKS_SUPPORTED, nranks);
        }
        MPI_Finalize();
        return 1;
    }

    if (rank >= MAX_RANKS_SUPPORTED || rank_cx_ip[rank] == NULL) {
        fprintf(stderr, "[rank %d] missing rank_cx_ip entry\n", rank);
        MPI_Finalize();
        return 1;
    }

    if (argc < 2) {
        if (rank == 0)
            fprintf(stderr,
                    "Usage: %s <ib_write_bw> [ib_write_bw options]\n",
                    argv[0]);

        MPI_Finalize();
        return 1;
    }

    const char *bin = argv[1];
    int peers_per_rank = nranks - 1;
    int children_per_rank = peers_per_rank * TRACE_LANES_PER_CHANNEL * 2;

    /*
     * Step 1: each rank starts one server process per lane for each incoming
     * connection. Server on rank dst listens at
     * port_for_conn_lane(nranks, src, dst, lane_id).
     */
    for (int src = 0; src < nranks; src++) {
        if (src == rank)
            continue;

        for (int lane_id = 0; lane_id < TRACE_LANES_PER_CHANNEL; lane_id++) {
            char port[16];

            if (port_for_conn_lane(nranks, src, rank, lane_id,
                                   port, sizeof(port)) != 0)
                MPI_Abort(MPI_COMM_WORLD, 1);

            fprintf(stderr,
                    "[rank %d] server channel=%d->%d lane=%d/%d listens on %s:%s\n",
                    rank, src, rank, lane_id, TRACE_LANES_PER_CHANNEL,
                    rank_cx_ip[rank], port);
            fflush(stderr);

            servers[src][lane_id] = spawn_write_bw(
                rank, bin, port, NULL, src, rank,
                lane_id, TRACE_LANES_PER_CHANNEL,
                -1, -1,
                global_start_ns, argc, argv);
            if (servers[src][lane_id] < 0) {
                fprintf(stderr,
                        "[rank %d] failed to start server process channel=%d->%d lane=%d/%d\n",
                        rank, src, rank, lane_id, TRACE_LANES_PER_CHANNEL);
                MPI_Abort(MPI_COMM_WORLD, 1);
            }
        }
    }

    sleep(2);
    MPI_Barrier(MPI_COMM_WORLD);

    if (rank == 0) {
        global_start_ns = wall_time_ns() + 10ULL * 1000 * 1000 * 1000;
    }

    MPI_Bcast(&global_start_ns, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);

    /*
     * Step 2: each rank starts one client process per lane for each outgoing
     * connection. Client on rank src connects to rank dst at
     * port_for_conn_lane(nranks, src, dst, lane_id).
     */
    for (int dst = 0; dst < nranks; dst++) {
        if (dst == rank)
            continue;

        for (int lane_id = 0; lane_id < TRACE_LANES_PER_CHANNEL; lane_id++) {
            char port[16];
            int result_pipe[2];

            if (port_for_conn_lane(nranks, rank, dst, lane_id,
                                   port, sizeof(port)) != 0)
                MPI_Abort(MPI_COMM_WORLD, 1);

            if (pipe(result_pipe) < 0) {
                fprintf(stderr,
                        "[rank %d] failed to create result pipe channel=%d->%d lane=%d/%d: %s\n",
                        rank, rank, dst, lane_id, TRACE_LANES_PER_CHANNEL,
                        strerror(errno));
                MPI_Abort(MPI_COMM_WORLD, 1);
            }

            if (fcntl(result_pipe[0], F_SETFD, FD_CLOEXEC) < 0) {
                fprintf(stderr,
                        "[rank %d] failed to configure result pipe channel=%d->%d lane=%d/%d: %s\n",
                        rank, rank, dst, lane_id, TRACE_LANES_PER_CHANNEL,
                        strerror(errno));
                close(result_pipe[0]);
                close(result_pipe[1]);
                MPI_Abort(MPI_COMM_WORLD, 1);
            }

            fprintf(stderr,
                    "[rank %d] client channel=%d->%d lane=%d/%d connects to %s:%s\n",
                    rank, rank, dst, lane_id, TRACE_LANES_PER_CHANNEL,
                    rank_cx_ip[dst], port);
            fflush(stderr);

            clients[dst][lane_id] = spawn_write_bw(
                rank, bin, port, rank_cx_ip[dst], rank, dst,
                lane_id, TRACE_LANES_PER_CHANNEL,
                result_pipe[0], result_pipe[1],
                global_start_ns, argc, argv);
            close(result_pipe[1]);
            if (clients[dst][lane_id] < 0) {
                close(result_pipe[0]);
                fprintf(stderr,
                        "[rank %d] failed to start client process channel=%d->%d lane=%d/%d\n",
                        rank, rank, dst, lane_id, TRACE_LANES_PER_CHANNEL);
                MPI_Abort(MPI_COMM_WORLD, 1);
            }
            client_result_fds[dst][lane_id] = result_pipe[0];
        }
    }

    fprintf(stderr,
            "[rank %d] nranks=%d started %d server processes and %d client processes (%d ib_write_bw child processes total)\n",
            rank, nranks,
            peers_per_rank * TRACE_LANES_PER_CHANNEL,
            peers_per_rank * TRACE_LANES_PER_CHANNEL,
            children_per_rank);
    fflush(stderr);

    /*
     * Step 3: wait clients first, then servers.
     */
    for (int dst = 0; dst < nranks; dst++) {
        if (dst == rank)
            continue;

        for (int lane_id = 0; lane_id < TRACE_LANES_PER_CHANNEL; lane_id++) {
            if (clients[dst][lane_id] > 0) {
                struct trace_run_result result;
                int child_ok;

                status = 0;
                child_ok = waitpid(clients[dst][lane_id], &status, 0) >= 0 &&
                           WIFEXITED(status) && WEXITSTATUS(status) == 0;

                if (!child_ok) {
                    fprintf(stderr,
                            "[rank %d] client channel=%d->%d lane=%d/%d failed status=%d\n",
                            rank, rank, dst, lane_id,
                            TRACE_LANES_PER_CHANNEL, status);

                    local_failed = 1;
                } else {
                    memset(&result, 0, sizeof(result));
                    if (client_result_fds[dst][lane_id] < 0 ||
                        read_full(client_result_fds[dst][lane_id],
                                &result, sizeof(result)) != 0 ||
                        result.status != TRACE_RESULT_SUCCESS) {

                        fprintf(stderr,
                                "[rank %d] invalid client result channel=%d->%d lane=%d/%d status=%d elapsed_ns=%lu iters=%lu\n",
                                rank, rank, dst, lane_id,
                                TRACE_LANES_PER_CHANNEL,
                                result.status,
                                (unsigned long)result.elapsed_ns,
                                (unsigned long)result.iters);

                        local_failed = 1;
                    } else {
                        if (result.iters > 0 && result.elapsed_ns > local_max_elapsed_ns)
                            local_max_elapsed_ns = result.elapsed_ns;

                        fprintf(stderr,
                                "[rank %d] client channel=%d->%d lane=%d/%d finished elapsed_ns=%lu iters=%lu%s\n",
                                rank, rank, dst, lane_id,
                                TRACE_LANES_PER_CHANNEL,
                                (unsigned long)result.elapsed_ns,
                                (unsigned long)result.iters,
                                result.iters == 0 ? " empty-lane" : "");
                    }
                }

                if (client_result_fds[dst][lane_id] >= 0) {
                    close(client_result_fds[dst][lane_id]);
                    client_result_fds[dst][lane_id] = -1;
                }
            }
        }
    }

    for (int src = 0; src < nranks; src++) {
        if (src == rank)
            continue;

        for (int lane_id = 0; lane_id < TRACE_LANES_PER_CHANNEL; lane_id++) {
            if (servers[src][lane_id] > 0) {
                status = 0;
                if (waitpid(servers[src][lane_id], &status, 0) < 0 ||
                    !WIFEXITED(status) ||
                    WEXITSTATUS(status) != 0) {

                    fprintf(stderr,
                            "[rank %d] server channel=%d->%d lane=%d/%d failed status=%d\n",
                            rank, src, rank, lane_id,
                            TRACE_LANES_PER_CHANNEL, status);

                    local_failed = 1;
                } else {
                    fprintf(stderr,
                            "[rank %d] server channel=%d->%d lane=%d/%d finished\n",
                            rank, src, rank, lane_id,
                            TRACE_LANES_PER_CHANNEL);
                }
            }
        }
    }

    MPI_Allreduce(&local_max_elapsed_ns,
                  &global_max_elapsed_ns,
                  1,
                  MPI_UINT64_T,
                  MPI_MAX,
                  MPI_COMM_WORLD);

    MPI_Allreduce(&local_failed,
                  &global_failed,
                  1,
                  MPI_INT,
                  MPI_MAX,
                  MPI_COMM_WORLD);

    if (rank == 0) {
        if (global_failed)
            fprintf(stderr,
                    "Sharded full-mesh ib_write_bw process test failed.\n");
        else {
            fprintf(stderr,
                    "Global trace FCT: %lu ns (%.3f ms)\n",
                    (unsigned long)global_max_elapsed_ns,
                    (double)global_max_elapsed_ns / 1000000.0);
            fprintf(stderr,
                    "All sharded full-mesh ib_write_bw processes finished successfully.\n");
        }
    }

    MPI_Finalize();
    return global_failed ? 1 : 0;
}