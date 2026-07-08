#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <stdint.h>
#include <time.h>

#define NRANKS_EXPECTED 3
#define PORT_BASE 18515


static const char *rank_cx_ip[NRANKS_EXPECTED] = {
    "192.168.2.10",
    "192.168.2.12",
    "192.168.2.11"
};

static void port_for_rank(int rank, char *buf, size_t len)
{
    snprintf(buf, len, "%d", PORT_BASE + rank);
}

static void port_for_conn(int src, int dst, char *buf, size_t len)
{
    snprintf(buf, len, "%d", PORT_BASE + src * NRANKS_EXPECTED + dst);
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
        setenv("TRACE_ROLE", server_ip ? "client" : "server", 1);
        setenv_int("TRACE_MY_RANK", rank);
        setenv_int("TRACE_SRC_RANK", src_rank);
        setenv_int("TRACE_DST_RANK", dst_rank);
        setenv_int("TRACE_PEER_RANK", server_ip ? dst_rank : src_rank);
        setenv_u64("TRACE_GLOBAL_START_NS", global_start_ns);

        if (server_ip)
            setenv("TRACE_SERVER_IP", server_ip, 1);

        fprintf(stderr, "[rank %d] exec:", rank);
        for (int i = 0; cmd[i]; i++)
            fprintf(stderr, " %s", cmd[i]);
        fprintf(stderr, "\n");

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
    pid_t servers[NRANKS_EXPECTED];
    pid_t clients[NRANKS_EXPECTED];

    for (int i = 0; i < NRANKS_EXPECTED; i++) {
        servers[i] = -1;
        clients[i] = -1;
    }
    uint64_t global_start_ns = 0;

    fprintf(stderr, "before MPI_Init\n");
    fflush(stderr);

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nranks);

    if (nranks != NRANKS_EXPECTED) {
        if (rank == 0)
            fprintf(stderr, "This launcher expects exactly 3 MPI ranks.\n");
        MPI_Finalize();
        return 1;
    }

    const char *bin = "/home/rdma_traffic_gen/perftest/ib_write_bw";

    /*
     * Step 1: each rank starts one server for each incoming connection.
     * Server on rank dst listens at port_for_conn(src, dst).
     */
    for (int src = 0; src < nranks; src++) {
        char port[16];

        fprintf(stderr, "[rank %d] server loop src=%d rank=%d\n",
                rank, src, rank);
        fflush(stderr);

        if (src == rank) {
            fprintf(stderr, "[rank %d] skip self src=%d\n", rank, src);
            fflush(stderr);
            continue;
        }

        port_for_conn(src, rank, port, sizeof(port));

        fprintf(stderr,
                "[rank %d] server for src rank %d listens on %s:%s\n",
                rank, src, rank_cx_ip[rank], port);
        fflush(stderr);

        servers[src] = spawn_write_bw(rank, bin, port, NULL, src, rank, global_start_ns, argc, argv);    
    }
    sleep(2);
    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == 0) {
        global_start_ns = wall_time_ns() + 10ULL * 1000 * 1000 * 1000;
    }

    MPI_Bcast(&global_start_ns, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);
    /*
     * Step 2: each rank starts one client for each outgoing connection.
     * Client on rank src connects to rank dst at port_for_conn(src, dst).
     */
    for (int dst = 0; dst < nranks; dst++) {
        char port[16];

        if (dst == rank)
            continue;

        port_for_conn(rank, dst, port, sizeof(port));

        fprintf(stderr,
                "[rank %d] client connects to rank %d %s:%s\n",
                rank, dst, rank_cx_ip[dst], port);
        fflush(stderr);

        clients[dst] = spawn_write_bw(rank, bin, port,
                                      rank_cx_ip[dst],
                                      rank, dst, global_start_ns,
                                      argc, argv);
        if (clients[dst] < 0) {
            fprintf(stderr, "[rank %d] failed to start client for dst %d\n",
                    rank, dst);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    /*
     * Step 3: wait clients first, then servers.
     */
    for (int dst = 0; dst < nranks; dst++) {
        if (clients[dst] > 0) {
            waitpid(clients[dst], &status, 0);
            fprintf(stderr, "[rank %d] client to rank %d finished\n",
                    rank, dst);
            fflush(stderr);
        }
    }

    for (int src = 0; src < nranks; src++) {
        if (servers[src] > 0) {
            waitpid(servers[src], &status, 0);
            fprintf(stderr, "[rank %d] server for src rank %d finished\n",
                    rank, src);
            fflush(stderr);
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);

    if (rank == 0)
        fprintf(stderr, "All full-mesh ib_write_bw processes finished.\n");

    MPI_Finalize();
    return 0;
}