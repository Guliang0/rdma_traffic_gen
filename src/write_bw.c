
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <ctype.h>
#include <limits.h>
#include <stdint.h>
#include <time.h>

#include "perftest_parameters.h"
#include "perftest_resources.h"
#include "perftest_communication.h"

#define TRACE_RANK_AUTO (-1)
#define TRACE_PAIR_AUTO (-1)
#define TRACE_DEBUG_RECORDS 8

struct trace_pair {
	int src;
	int dst;
};

struct trace_record {
	struct trace_pair pair;
	int nranks;
	uint32_t size;
	double timestamp;
	double delta;
};

struct trace_bw_trace {
	char *path;
	int rank;
	int pair_src;
	int pair_dst;
	struct trace_pair active_pair;
	struct trace_record *records;
	size_t record_count;
	uint64_t total_bytes;
};

struct trace_bw_runtime {
    struct pingpong_context ctx;
    struct perftest_parameters user_param;
    struct perftest_comm user_comm;

    struct pingpong_dest *my_dest;
    struct pingpong_dest *rem_dest;

    struct bw_report_data my_bw_rep;
    struct bw_report_data rem_bw_rep;
	struct trace_bw_trace trace;
	int rdma_cm_flow_destroyed;
};

static int trace_parse_int_arg(const char *opt, const char *value, int *out)
{
	char *end = NULL;
	long parsed;

	errno = 0;
	parsed = strtol(value, &end, 10);
	if (errno || end == value || *end != '\0' ||
		parsed < 0 || parsed > INT_MAX) {
		fprintf(stderr, "%s requires a non-negative integer\n", opt);
		return FAILURE;
	}

	*out = (int)parsed;
	return SUCCESS;
}

static int trace_strip_args(int argc, char **argv, struct trace_bw_runtime *rt)
{
	int read_idx, write_idx;

	for (read_idx = 1, write_idx = 1; read_idx < argc; read_idx++) {
		if (!strcmp(argv[read_idx], "--trace")) {
			if (read_idx + 1 >= argc) {
				fprintf(stderr, "--trace requires a file path\n");
				return -1;
			}
			rt->trace.path = argv[++read_idx];
			continue;
		}

		if (!strcmp(argv[read_idx], "--rank")) {
			if (read_idx + 1 >= argc) {
				fprintf(stderr, "--rank requires a rank\n");
				return -1;
			}
			if (trace_parse_int_arg("--rank", argv[++read_idx],
								&rt->trace.rank))
				return -1;
			continue;
		}

		if (!strcmp(argv[read_idx], "--src")) {
			if (read_idx + 1 >= argc) {
				fprintf(stderr, "--src requires a rank\n");
				return -1;
			}
			if (trace_parse_int_arg("--src", argv[++read_idx],
								&rt->trace.pair_src))
				return -1;
			continue;
		}

		if (!strcmp(argv[read_idx], "--dst")) {
			if (read_idx + 1 >= argc) {
				fprintf(stderr, "--dst requires a rank\n");
				return -1;
			}
			if (trace_parse_int_arg("--dst", argv[++read_idx],
								&rt->trace.pair_dst))
				return -1;
			continue;
		}

		argv[write_idx++] = argv[read_idx];
	}

	argv[write_idx] = NULL;
	return write_idx;
}

static int trace_resolve_runtime(struct trace_bw_runtime *rt)
{
	if (!rt->trace.path)
		return SUCCESS;

	if (rt->trace.rank == TRACE_RANK_AUTO) {
		fprintf(stderr, "--rank is required when --trace is specified\n");
		return FAILURE;
	}

	if ((rt->trace.pair_src == TRACE_PAIR_AUTO) !=
		(rt->trace.pair_dst == TRACE_PAIR_AUTO)) {
		fprintf(stderr, "--src and --dst must be specified together\n");
		return FAILURE;
	}

	if (rt->trace.pair_src != TRACE_PAIR_AUTO) {
		rt->trace.active_pair.src = rt->trace.pair_src;
		rt->trace.active_pair.dst = rt->trace.pair_dst;
	} else {
		rt->trace.active_pair.src = 1;
		rt->trace.active_pair.dst = 0;
	}

	return SUCCESS;
}

static int trace_load_file(struct trace_bw_runtime *rt)
{
	FILE *fp;
	char line[512];
	size_t line_no = 0;
	size_t cap = 128;
	size_t count = 0;
	struct trace_record *records;
	double prev_ts = -1.0;
	double first_ts = 0.0;

	if (!rt->trace.path)
		return SUCCESS;

	fp = fopen(rt->trace.path, "r");
	if (!fp) {
		fprintf(stderr, "Failed to open trace file %s: %s\n",
				rt->trace.path, strerror(errno));
		return FAILURE;
	}

	records = calloc(cap, sizeof(*records));
	if (!records) {
		fprintf(stderr, "Failed to allocate trace records\n");
		fclose(fp);
		return FAILURE;
	}

	while (fgets(line, sizeof(line), fp)) {
		struct trace_record rec;
		char *p = line;
		int parsed;

		line_no++;
		while (isspace((unsigned char)*p))
			p++;
		if (*p == '\0' || *p == '\n' || *p == '#')
			continue;

		parsed = sscanf(p, "%d %d %d %u %lf",
						&rec.pair.src, &rec.pair.dst, &rec.nranks,
						&rec.size, &rec.timestamp);
		if (parsed != 5) {
			fprintf(stderr, "%s:%zu: malformed trace record\n",
					rt->trace.path, line_no);
			goto error;
		}

		if (rec.pair.src != rt->trace.active_pair.src ||
			rec.pair.dst != rt->trace.active_pair.dst)
			continue;

		if (rec.size == 0 || (uint64_t)rec.size > rt->user_param.size) {
			fprintf(stderr,
					"%s:%zu: invalid trace size %u, configured size is %lu\n",
					rt->trace.path, line_no, rec.size,
					(unsigned long)rt->user_param.size);
			goto error;
		}

		if (prev_ts > rec.timestamp) {
			fprintf(stderr, "%s:%zu: timestamp is not non-decreasing\n",
					rt->trace.path, line_no);
			goto error;
		}
		prev_ts = rec.timestamp;
		if (count == 0) {
			first_ts = rec.timestamp;
			rec.delta = 0.0;
		} else {
			rec.delta = rec.timestamp - first_ts;
		}

		if (count == cap) {
			struct trace_record *tmp;
			cap *= 2;
			tmp = realloc(records, cap * sizeof(*records));
			if (!tmp) {
				fprintf(stderr, "Failed to grow trace records\n");
				goto error;
			}
			records = tmp;
		}

		records[count++] = rec;
		rt->trace.total_bytes += rec.size;
	}

	if (ferror(fp)) {
		fprintf(stderr, "Failed while reading trace file %s\n", rt->trace.path);
		goto error;
	}

	fclose(fp);

	if (count == 0) {
		fprintf(stderr, "No trace records matched pair %d->%d in %s\n",
				rt->trace.active_pair.src, rt->trace.active_pair.dst,
				rt->trace.path);
		free(records);
		return FAILURE;
	}

	rt->trace.records = records;
	rt->trace.record_count = count;

	fprintf(stderr,
			"[trace] path=%s rank=%d pair=%d->%d records=%zu bytes=%llu first_ts=%.9f last_ts=%.9f last_delta=%.9f\n",
			rt->trace.path, rt->trace.rank,
			rt->trace.active_pair.src, rt->trace.active_pair.dst,
			rt->trace.record_count,
			(unsigned long long)rt->trace.total_bytes,
			rt->trace.records[0].timestamp,
			rt->trace.records[rt->trace.record_count - 1].timestamp,
			rt->trace.records[rt->trace.record_count - 1].delta);

	{
		size_t i;
		size_t dump = rt->trace.record_count < TRACE_DEBUG_RECORDS ?
			rt->trace.record_count : TRACE_DEBUG_RECORDS;

		for (i = 0; i < dump; i++) {
			fprintf(stderr,
					"[trace record] idx=%zu src=%d dst=%d nranks=%d size=%u timestamp=%.9f delta=%.9f\n",
					i, rt->trace.records[i].pair.src,
					rt->trace.records[i].pair.dst,
					rt->trace.records[i].nranks,
					rt->trace.records[i].size,
					rt->trace.records[i].timestamp,
					rt->trace.records[i].delta);
		}
	}

	return SUCCESS;

error:
	free(records);
	fclose(fp);
	return FAILURE;
}

static void trace_free(struct trace_bw_runtime *rt)
{
	free(rt->trace.records);
	rt->trace.records = NULL;
	rt->trace.record_count = 0;
}

static void trace_add_seconds(struct timespec *ts, double seconds)
{
	time_t sec = (time_t)seconds;
	long nsec = (long)((seconds - (double)sec) * 1000000000.0);

	ts->tv_sec += sec;
	ts->tv_nsec += nsec;
	while (ts->tv_nsec >= 1000000000L) {
		ts->tv_sec++;
		ts->tv_nsec -= 1000000000L;
	}
}

static int trace_sleep_until(const struct timespec *deadline)
{
	struct timespec now, req;

	while (1) {
		if (clock_gettime(CLOCK_MONOTONIC, &now)) {
			fprintf(stderr, "clock_gettime failed: %s\n", strerror(errno));
			return FAILURE;
		}
		if (now.tv_sec > deadline->tv_sec ||
			(now.tv_sec == deadline->tv_sec &&
			 now.tv_nsec >= deadline->tv_nsec))
			return SUCCESS;

		req.tv_sec = deadline->tv_sec - now.tv_sec;
		req.tv_nsec = deadline->tv_nsec - now.tv_nsec;
		if (req.tv_nsec < 0) {
			req.tv_sec--;
			req.tv_nsec += 1000000000L;
		}
		if (nanosleep(&req, NULL) && errno != EINTR) {
			fprintf(stderr, "nanosleep failed: %s\n", strerror(errno));
			return FAILURE;
		}
	}
}

static int trace_poll_send_cq(struct pingpong_context *ctx,
							  unsigned int *outstanding)
{
	struct ibv_wc wc;
	int ne;

	do {
		ne = ibv_poll_cq(ctx->send_cq, 1, &wc);
		if (ne < 0) {
			fprintf(stderr, "ibv_poll_cq failed on trace send CQ\n");
			return FAILURE;
		}
	} while (ne == 0);

	if (wc.status != IBV_WC_SUCCESS) {
		fprintf(stderr,
				"Trace send completion failed: status=%d vendor_err=%u qp_num=%u wr_id=%llu\n",
				wc.status, wc.vendor_err, wc.qp_num,
				(unsigned long long)wc.wr_id);
		return FAILURE;
	}

	if (*outstanding > 0)
		(*outstanding)--;
	return SUCCESS;
}

static void trace_print_server_receive(struct trace_bw_runtime *rt)
{
	uint64_t first_qword = 0;

	if (!rt->trace.path || rt->user_param.machine != SERVER)
		return;

	memcpy(&first_qword, (void *)(uintptr_t)rt->ctx.my_addr[0],
		   sizeof(first_qword));
	fprintf(stderr,
			"[trace recv] rank=%d pair=%d->%d ctx.buf[0]=%p my_addr[0]=0x%lx my_dest.vaddr=0x%lx rkey=0x%x records=%zu bytes=%llu first_qword=0x%016llx\n",
			rt->trace.rank,
			rt->trace.active_pair.src, rt->trace.active_pair.dst,
			rt->ctx.buf ? rt->ctx.buf[0] : NULL,
			(unsigned long)rt->ctx.my_addr[0],
			(unsigned long)rt->my_dest[0].vaddr,
			rt->my_dest[0].rkey,
			rt->trace.record_count,
			(unsigned long long)rt->trace.total_bytes,
			(unsigned long long)first_qword);
}

int trace_bw_prepare(int argc, char **argv, struct trace_bw_runtime *rt){
	int				ret_parser, i = 0, rc;
	struct ibv_device		*ib_dev = NULL;
	// replace below by rt->xxx
	// struct pingpong_context		ctx;
	// struct pingpong_dest		*my_dest,*rem_dest;
	// struct perftest_parameters	user_param;
	// struct perftest_comm		user_comm;
	// struct bw_report_data		my_bw_rep, rem_bw_rep;
	// int rdma_cm_flow_destroyed = 0;

	/* init default values to user's parameters */
	memset(&rt->user_param,0,sizeof(struct perftest_parameters));
	memset(&rt->user_comm,0,sizeof(struct perftest_comm));
	memset(&rt->ctx,0,sizeof(struct pingpong_context));
	rt->trace.rank = TRACE_RANK_AUTO;
	rt->trace.pair_src = TRACE_PAIR_AUTO;
	rt->trace.pair_dst = TRACE_PAIR_AUTO;

	argc = trace_strip_args(argc, argv, rt);
	if (argc < 0)
		return FAILURE;

	rt->user_param.verb    = WRITE;
	rt->user_param.tst     = BW;
	strncpy(rt->user_param.version, VERSION, sizeof(rt->user_param.version));

	/* Configure the parameters values according to user arguments or default values. */
	ret_parser = parser(&rt->user_param,argv,argc);
	if (ret_parser) {
		if (ret_parser != VERSION_EXIT && ret_parser != HELP_EXIT)
			fprintf(stderr," Parser function exited with Error\n");
		return FAILURE;
	}

	if((rt->user_param.connection_type == DC || rt->user_param.use_xrc) && rt->user_param.duplex) {
		rt->user_param.num_of_qps *= 2;
	}

	if (trace_resolve_runtime(rt))
		return FAILURE;
	if (trace_load_file(rt))
		return FAILURE;

	/* Finding the IB device selected (or default if none is selected). */
	ib_dev = ctx_find_dev(&rt->user_param.ib_devname);
	if (!ib_dev) {
		fprintf(stderr," Unable to find the Infiniband/RoCE device\n");
		return FAILURE;
	}

	/* Set the affinity to the CPU cores or NUMA node */
	if (set_process_affinity(ib_dev->ibdev_path, &rt->user_param)) {
		return FAILURE;
	}

	/* Getting the relevant context from the device */
	rt->ctx.context = ctx_open_device(ib_dev, &rt->user_param);
	if (!rt->ctx.context) {
		fprintf(stderr, " Couldn't get context for the device\n");
		return FAILURE;
	}

	/* Verify user parameters that require the device context,
	 * the function will print the relevent error info. */
	if (verify_params_with_device_context(rt->ctx.context, &rt->user_param))
	{
		fprintf(stderr, " Couldn't get context for the device\n");
		return FAILURE;
	}

	/* See if link type is valid and supported. */
	if (check_link(rt->ctx.context,&rt->user_param)) {
		fprintf(stderr, " Couldn't get context for the device\n");
		return FAILURE;
	}

	/* copy the relevant user parameters to the comm struct + creating rdma_cm resources. */
	if (create_comm_struct(&rt->user_comm,&rt->user_param)) {
		fprintf(stderr," Unable to create RDMA_CM resources\n");
		return FAILURE;
	}

	if (rt->user_param.output == FULL_VERBOSITY && rt->user_param.machine == SERVER) {
		printf("\n************************************\n");
		printf("* Waiting for client to connect... *\n");
		printf("************************************\n");
	}

	/* Initialize the connection and print the local data. */
	if (establish_connection(&rt->user_comm)) {
		fprintf(stderr," Unable to init the socket connection\n");
		dealloc_comm_struct(&rt->user_comm,&rt->user_param);
		return FAILURE;
	}
	sleep(1);
	exchange_versions(&rt->user_comm, &rt->user_param);
	check_version_compatibility(&rt->user_param);
	check_sys_data(&rt->user_comm, &rt->user_param);

	/* See if MTU is valid and supported. */
	if (check_mtu(rt->ctx.context,&rt->user_param, &rt->user_comm)) {
		fprintf(stderr, " Couldn't get context for the device\n");
		dealloc_comm_struct(&rt->user_comm,&rt->user_param);
		return FAILURE;
	}

	// MAIN_ALLOC(rt->my_dest , struct pingpong_dest , rt->user_param.num_of_qps , free_rdma_params);
	// memset(rt->my_dest, 0, sizeof(struct pingpong_dest)*rt->user_param.num_of_qps);
	// MAIN_ALLOC(rt->rem_dest , struct pingpong_dest , rt->user_param.num_of_qps , free_rdma_params);
	// memset(rt->rem_dest, 0, sizeof(struct pingpong_dest)*rt->user_param.num_of_qps);

	rt->my_dest = calloc(rt->user_param.num_of_qps, sizeof(struct pingpong_dest));
	if (!rt->my_dest) {
		fprintf(stderr, "Failed to allocate my_dest\n");
		return FAILURE;
	}

	rt->rem_dest = calloc(rt->user_param.num_of_qps, sizeof(struct pingpong_dest));
	if (!rt->rem_dest) {
		fprintf(stderr, "Failed to allocate rem_dest\n");
		free(rt->my_dest);
		rt->my_dest = NULL;
		return FAILURE;
	}



	/* Allocating arrays needed for the test. */
	if(alloc_ctx(&rt->ctx,&rt->user_param)){
		fprintf(stderr, "Couldn't allocate context\n");
		return FAILURE;
	}

	/* Negotiate parameters. */
	if (negotiate_params(&rt->ctx, &rt->user_comm, &rt->user_param)) {
		fprintf(stderr, " Failed to negotiate parameters\n");
		dealloc_ctx(&rt->ctx, &rt->user_param);
		return FAILURE;
	}

	/* Create RDMA CM resources and connect through CM. */
	if (rt->user_param.work_rdma_cm == ON) {
		rc = create_rdma_cm_connection(&rt->ctx, &rt->user_param, &rt->user_comm,
			rt->my_dest, rt->rem_dest);
		if (rc) {
			fprintf(stderr,
				"Failed to create RDMA CM connection with resources.\n");
			dealloc_ctx(&rt->ctx, &rt->user_param);
			return FAILURE;
		}
	} else {
		/* create all the basic IB resources (data buffer, PD, MR, CQ and events channel) */
		if (ctx_init(&rt->ctx, &rt->user_param)) {
			fprintf(stderr, " Couldn't create IB resources\n");
			dealloc_ctx(&rt->ctx, &rt->user_param);
			return FAILURE;
		}
	}

	/* Initialize data validation for receiver side */
	if (rt->user_param.data_validation &&
		(rt->user_param.machine == SERVER || rt->user_param.duplex) &&
		data_validation_init(&rt->ctx, &rt->user_param)) {
		return FAILURE;
	}

	/* Set up the Connection. */
	if (set_up_connection(&rt->ctx,&rt->user_param,rt->my_dest)) {
		fprintf(stderr," Unable to set up socket connection\n");
		return FAILURE;
	}

	/* Print basic test information. */
	ctx_print_test_info(&rt->user_param);

	for (i=0; i < rt->user_param.num_of_qps; i++) {

		if (ctx_hand_shake(&rt->user_comm,&rt->my_dest[i],&rt->rem_dest[i])) {
			fprintf(stderr," Failed to exchange data between server and clients\n");
			return FAILURE;
		}
	}

	if (rt->user_param.work_rdma_cm == OFF) {
		if (ctx_check_gid_compatibility(&rt->my_dest[0], &rt->rem_dest[0])) {
			fprintf(stderr,"\n Found Incompatibility issue with GID types.\n");
			fprintf(stderr," Please Try to use a different IP version.\n\n");
			return FAILURE;
		}
	}

	if (rt->user_param.work_rdma_cm == OFF) {
		if (ctx_connect(&rt->ctx,rt->rem_dest,&rt->user_param,rt->my_dest)) {
			fprintf(stderr," Unable to Connect the HCA's through the link\n");
			return FAILURE;
		}
	}

	if (rt->user_param.connection_type == DC)
	{
		/* Set up connection one more time to send qpn properly for DC */
		if (set_up_connection(&rt->ctx, &rt->user_param, rt->my_dest))
		{
			fprintf(stderr," Unable to set up socket connection\n");
			return FAILURE;
		}
	}

	/* Print this machine QP information */
	for (i=0; i < rt->user_param.num_of_qps; i++)
		ctx_print_pingpong_data(&rt->my_dest[i],&rt->user_comm);

	rt->user_comm.rdma_params->side = REMOTE;

	for (i=0; i < rt->user_param.num_of_qps; i++) {
		if (ctx_hand_shake(&rt->user_comm,&rt->my_dest[i],&rt->rem_dest[i])) {
			fprintf(stderr," Failed to exchange data between server and clients\n");
			return FAILURE;
		}

		ctx_print_pingpong_data(&rt->rem_dest[i],&rt->user_comm);
	}

	if (rt->user_param.use_event) {
		if (ibv_req_notify_cq(rt->ctx.send_cq, 0)) {
			fprintf(stderr, " Couldn't request CQ notification\n");
			return FAILURE;
		}
		if (ibv_req_notify_cq(rt->ctx.recv_cq, 0)) {
			fprintf(stderr, " Couldn't request CQ notification\n");
			return FAILURE;
		}
	}

	/* An additional handshake is required after moving qp to RTR. */
	if (ctx_hand_shake(&rt->user_comm,&rt->my_dest[0],&rt->rem_dest[0])) {
		fprintf(stderr," Failed to exchange data between server and clients\n");
		return FAILURE;
	}

	if (rt->user_param.output == FULL_VERBOSITY) {
		if (rt->user_param.report_per_port) {
			printf(RESULT_LINE_PER_PORT);
			printf((rt->user_param.report_fmt == MBS ? RESULT_FMT_PER_PORT : RESULT_FMT_G_PER_PORT));
		}
		else {
			printf(RESULT_LINE);
			printf((rt->user_param.report_fmt == MBS ? RESULT_FMT : RESULT_FMT_G));
		}

		printf((rt->user_param.cpu_util_data.enable ? RESULT_EXT_CPU_UTIL : RESULT_EXT));
	}

		uintptr_t buf_base = (uintptr_t)rt->ctx.buf;
		uintptr_t rdma_base = (uintptr_t)rt->my_dest[0].vaddr;

		fprintf(stderr,
				"[debug recv] ctx.buf=%p my_dest[0].vaddr=0x%lx diff=%ld size=%lu cycle_buffer=%lu\n",
				rt->ctx.buf,
				(unsigned long)rdma_base,
				(long)(rdma_base - buf_base),
				(unsigned long)rt->user_param.size,
				(unsigned long)rt->ctx.cycle_buffer);

	//END AT LINE 272
	return 0;
};

int trace_bw_run_once(struct trace_bw_runtime *rt){
	if (rt->trace.path) {
		size_t i;
		unsigned int outstanding = 0;
		struct timespec start;

		if (rt->user_param.machine == SERVER)
			return SUCCESS;

		if (rt->user_param.tx_depth <= 0) {
			fprintf(stderr, "trace replay requires tx_depth > 0\n");
			return FAILURE;
		}

		ctx_set_send_wqes(&rt->ctx,&rt->user_param,rt->rem_dest);

		if (clock_gettime(CLOCK_MONOTONIC, &start)) {
			fprintf(stderr, "clock_gettime failed: %s\n", strerror(errno));
			return FAILURE;
		}

		for (i = 0; i < rt->trace.record_count; i++) {
			struct trace_record *record = &rt->trace.records[i];
			struct timespec deadline = start;
			struct ibv_send_wr *bad_wr = NULL;
			uint64_t magic = 0x7472636500000000ULL | (i & 0xffffffffULL);
			int err;

			if (record->delta > 0.0) {
				trace_add_seconds(&deadline, record->delta);
				if (trace_sleep_until(&deadline))
					return FAILURE;
			}

			while (outstanding >= (unsigned int)rt->user_param.tx_depth) {
				if (trace_poll_send_cq(&rt->ctx, &outstanding))
					return FAILURE;
			}

			memset((void *)(uintptr_t)rt->ctx.my_addr[0], 0, record->size);
			if (record->size >= sizeof(magic))
				*(uint64_t *)(uintptr_t)rt->ctx.my_addr[0] = magic;

			rt->ctx.sge_list[0].addr = rt->ctx.my_addr[0];
			rt->ctx.sge_list[0].length = record->size;
			rt->ctx.sge_list[0].lkey = rt->ctx.mr[0]->lkey;
			rt->ctx.wr[0].sg_list = &rt->ctx.sge_list[0];
			rt->ctx.wr[0].num_sge = 1;
			rt->ctx.wr[0].opcode = IBV_WR_RDMA_WRITE;
			rt->ctx.wr[0].send_flags = IBV_SEND_SIGNALED;
			rt->ctx.wr[0].wr.rdma.remote_addr = rt->ctx.rem_addr[0];
			rt->ctx.wr[0].wr.rdma.rkey = rt->rem_dest[0].rkey;
			rt->ctx.wr[0].wr_id = i;
			rt->ctx.wr[0].next = NULL;

			if (i < TRACE_DEBUG_RECORDS) {
				fprintf(stderr,
						"[trace send] idx=%zu local=0x%lx remote=0x%lx size=%u timestamp=%.9f delta=%.9f\n",
						i, (unsigned long)rt->ctx.sge_list[0].addr,
						(unsigned long)rt->ctx.wr[0].wr.rdma.remote_addr,
						record->size, record->timestamp, record->delta);
			}

			err = ibv_post_send(rt->ctx.qp[0], &rt->ctx.wr[0], &bad_wr);
			if (err) {
				fprintf(stderr, "Couldn't post trace RDMA WRITE: record=%zu err=%d\n",
						i, err);
				return FAILURE;
			}
			outstanding++;
		}

		while (outstanding > 0) {
			if (trace_poll_send_cq(&rt->ctx, &outstanding))
				return FAILURE;
		}

		fprintf(stderr, "[trace] replay completed records=%zu bytes=%llu pair=%d->%d\n",
				rt->trace.record_count,
				(unsigned long long)rt->trace.total_bytes,
				rt->trace.active_pair.src, rt->trace.active_pair.dst);
		return SUCCESS;
	}

	//SERVER EARLY RETURN
	if (rt->user_param.machine == SERVER &&
		rt->user_param.verb == WRITE &&
		!rt->user_param.duplex) {
		return SUCCESS;
	}

	/* REGULAR */
	{
		if (rt->user_param.machine == CLIENT || rt->user_param.duplex)
			ctx_set_send_wqes(&rt->ctx,&rt->user_param,rt->rem_dest);

		if (rt->user_param.verb != SEND && rt->user_param.verb != WRITE_IMM) {
			if (rt->user_param.perform_warm_up) {
				if(perform_warm_up(&rt->ctx, &rt->user_param)) {
					fprintf(stderr, "Problems with warm up\n");
					return FAILURE;
				}
			}
		}

		if (rt->user_param.machine == CLIENT || rt->user_param.verb != WRITE_IMM) {

			if ((rt->user_param.data_validation ? run_iter_bw_dv : my_run_iter_bw)(&rt->ctx,&rt->user_param)) {
				fprintf(stderr," Failed to complete run_iter_bw function successfully\n");
				return FAILURE;
			}

		} else if (rt->user_param.machine == SERVER) {

			if(run_iter_bw_server(&rt->ctx,&rt->user_param)) {
				fprintf(stderr," Failed to complete run_iter_bw_server function successfully\n");
				return FAILURE;
			}
		}

		print_report_bw(&rt->user_param,&rt->my_bw_rep);

		if (rt->user_param.report_both && rt->user_param.duplex) {
			printf(RESULT_LINE);
			printf("\n Local results: \n");
			printf(RESULT_LINE);
			printf((rt->user_param.report_fmt == MBS ? RESULT_FMT : RESULT_FMT_G));
			printf((rt->user_param.cpu_util_data.enable ? RESULT_EXT_CPU_UTIL : RESULT_EXT));
			print_full_bw_report(&rt->user_param, &rt->my_bw_rep, NULL);
			printf(RESULT_LINE);

			printf("\n Remote results: \n");
			printf(RESULT_LINE);
			printf((rt->user_param.report_fmt == MBS ? RESULT_FMT : RESULT_FMT_G));
			printf((rt->user_param.cpu_util_data.enable ? RESULT_EXT_CPU_UTIL : RESULT_EXT));
			print_full_bw_report(&rt->user_param, &rt->rem_bw_rep, NULL);
		}
	} 

	if (rt->user_param.output == FULL_VERBOSITY) {
		if (rt->user_param.report_per_port)
			printf(RESULT_LINE_PER_PORT);
		else
			printf(RESULT_LINE);
	}
	return 0;
};


int trace_bw_cleanup(struct trace_bw_runtime *rt){
	

	if (rt->user_param.machine == SERVER && rt->user_param.verb == WRITE && !rt->user_param.duplex) {

		if (ctx_hand_shake(&rt->user_comm,&rt->my_dest[0],&rt->rem_dest[0])) {
			fprintf(stderr," Failed to exchange data between server and clients\n");
			goto destroy_context;
		}
		
		// YJT: TODO: ADD CHECK FOR DATA , ALL PRINT OUT
		uint64_t print_len = rt->user_param.size < 256 ? rt->user_param.size : 256;

		uintptr_t buf_base = (uintptr_t)rt->ctx.buf;
		uintptr_t rdma_base = (uintptr_t)rt->my_dest[0].vaddr;

		char *base = (char *)rt->ctx.buf + (rdma_base - buf_base);
		
		for (uint64_t off = 0; off + 8 <= print_len; off += 8) {
			uint64_t val = *(uint64_t *)(base + off);

			printf("[recv] base=%p addr=%p offset=%lu value=%016lx\n",
				base,
				base + off,
				(unsigned long)off,
				val);
		}
		// uintptr_t buf = (uintptr_t)rt->ctx.buf;
		// uintptr_t vaddr = (uintptr_t)rt->my_dest[0].vaddr;
		// uintptr_t off = vaddr - buf;

		// fprintf(stderr, "[debug recv] buf=0x%lx vaddr=0x%lx off=%lu\n",
		// 		(unsigned long)buf,
		// 		(unsigned long)vaddr,
		// 		(unsigned long)off);

		trace_print_server_receive(rt);

		if (ctx_close_connection(&rt->user_comm,&rt->my_dest[0],&rt->rem_dest[0])) {
			fprintf(stderr,"Failed to close connection between server and client\n");
			goto destroy_context;
		}

		if (rt->user_param.output == FULL_VERBOSITY) {
			if (rt->user_param.report_per_port)
				printf(RESULT_LINE_PER_PORT);
			else
				printf(RESULT_LINE);
		}

		if (rt->user_param.work_rdma_cm == ON) {
			if (rt->user_param.data_validation)
				data_validation_destroy(&rt->ctx);
			if (destroy_ctx(&rt->ctx,&rt->user_param)) {
				fprintf(stderr, "Failed to destroy resources\n");
				goto destroy_cm_context;
			}
			rt->user_comm.rdma_params->work_rdma_cm = OFF;
			free(rt->my_dest);
			free(rt->rem_dest);
			free(rt->user_param.ib_devname);
			if(destroy_ctx(rt->user_comm.rdma_ctx, rt->user_comm.rdma_params)) {
				free(rt->user_comm.rdma_params);
				free(rt->user_comm.rdma_ctx);
				return FAILURE;
			}
			free(rt->user_comm.rdma_params);
			free(rt->user_comm.rdma_ctx);
			return SUCCESS;
		}

		if (rt->user_param.data_validation)
			data_validation_destroy(&rt->ctx);
		free(rt->my_dest);
		free(rt->rem_dest);
		free(rt->user_param.ib_devname);
		if(destroy_ctx(&rt->ctx, &rt->user_param)) {
			free(rt->user_comm.rdma_params);
			return FAILURE;
		}
		free(rt->user_comm.rdma_params);
		return SUCCESS;
	}


	/* For half duplex write tests, server just waits for client to exit */
	if (rt->user_param.machine == CLIENT && rt->user_param.verb == WRITE && !rt->user_param.duplex) {
		if (ctx_hand_shake(&rt->user_comm,&rt->my_dest[0],&rt->rem_dest[0])) {
			fprintf(stderr," Failed to exchange data between server and clients\n");
			goto destroy_context;
		}
	}

	/* Closing connection. */
	if (ctx_close_connection(&rt->user_comm,&rt->my_dest[0],&rt->rem_dest[0])) {
		fprintf(stderr,"Failed to close connection between server and client\n");
		goto destroy_context;
	}

	if (!rt->user_param.is_bw_limit_passed && (rt->user_param.is_limit_bw == ON ) ) {
		fprintf(stderr,"Error: BW result is below bw limit\n");
		goto destroy_context;
	}

	if (!rt->user_param.is_msgrate_limit_passed && (rt->user_param.is_limit_bw == ON )) {
		fprintf(stderr,"Error: Msg rate  is below msg_rate limit\n");
		goto destroy_context;
	}
	if (rt->user_param.work_rdma_cm == ON) {
		if (rt->user_param.data_validation)
			data_validation_destroy(&rt->ctx);
		if (destroy_ctx(&rt->ctx,&rt->user_param)) {
			fprintf(stderr, "Failed to destroy resources\n");
			goto destroy_cm_context;
		}

		rt->user_comm.rdma_params->work_rdma_cm = OFF;
		free(rt->rem_dest);
		free(rt->my_dest);
		free(rt->user_param.ib_devname);
		if(destroy_ctx(rt->user_comm.rdma_ctx, rt->user_comm.rdma_params)) {
			free(rt->user_comm.rdma_params);
			free(rt->user_comm.rdma_ctx);
			return FAILURE;
		}
		free(rt->user_comm.rdma_params);
		free(rt->user_comm.rdma_ctx);
		return SUCCESS;
	}

	if (rt->user_param.data_validation)
		data_validation_destroy(&rt->ctx);
	free(rt->rem_dest);
	free(rt->my_dest);
	free(rt->user_param.ib_devname);
	if(destroy_ctx(&rt->ctx, &rt->user_param)){
		free(rt->user_comm.rdma_params);
		return FAILURE;
	}
	free(rt->user_comm.rdma_params);
	return SUCCESS;

destroy_context:
	if (rt->user_param.data_validation)
		data_validation_destroy(&rt->ctx);
	if (destroy_ctx(&rt->ctx,&rt->user_param))
		fprintf(stderr, "Failed to destroy resources\n");
destroy_cm_context:
	if (rt->user_param.work_rdma_cm == ON) {
		rt->rdma_cm_flow_destroyed = 1;
		rt->user_comm.rdma_params->work_rdma_cm = OFF;
		destroy_ctx(rt->user_comm.rdma_ctx,rt->user_comm.rdma_params);
	}
free_mem:
	free(rt->rem_dest);
free_my_dest:
	free(rt->my_dest);
free_rdma_params:
	if (rt->user_param.use_rdma_cm == ON && rt->rdma_cm_flow_destroyed == 0)
		dealloc_comm_struct(&rt->user_comm, &rt->user_param);

	else {
		if(rt->user_param.use_rdma_cm == ON)
			free(rt->user_comm.rdma_ctx);
		free(rt->user_comm.rdma_params);
	}
free_devname:
	free(rt->user_param.ib_devname);
return_error:
	//coverity[leaked_storage]
	return FAILURE;
};



int main(int argc, char **argv)
{
    int ret = FAILURE;
    struct trace_bw_runtime rt;

    memset(&rt, 0, sizeof(rt));

    if (trace_bw_prepare(argc, argv, &rt))
        goto out;

    if (trace_bw_run_once(&rt))
        goto out_cleanup;

    ret = SUCCESS;

out_cleanup:
    trace_bw_cleanup(&rt);
out:
	trace_free(&rt);
    return ret;
}
