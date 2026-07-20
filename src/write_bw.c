
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <ctype.h>
#include <limits.h>

#include "perftest_parameters.h"
#include "perftest_resources.h"
#include "perftest_communication.h"

#define TRACE_FILE_PATH "data/alistorage60_filtered.txt"
#define TRACE_MAX_FLOWS 10000
#define TRACE_MAX_WR_SIZE ((uint64_t)UINT_MAX / 2)
#define TRACE_MAX_LANES 1024

struct trace_bw_runtime {
    struct pingpong_context ctx;
    struct perftest_parameters user_param;
    struct perftest_comm user_comm;

    struct pingpong_dest *my_dest;
    struct pingpong_dest *rem_dest;

    struct bw_report_data my_bw_rep;
    struct bw_report_data rem_bw_rep;
	int rdma_cm_flow_destroyed;
	
	uint64_t time_ns_list[TRACE_MAX_FLOWS];
	uint32_t size_list[TRACE_MAX_FLOWS];
	uint64_t trace_count;
	uint64_t trace_max_size;
	uint64_t trace_first_ts_ns;
	uint64_t trace_duration_ns;
	int trace_src_id;
	int trace_dst_id;
	int trace_lane_id;
	int trace_lane_count;
};


static int getenv_int_default(const char *name, int def)
{
    const char *s = getenv(name);
    if (!s || !*s)
        return def;
    return atoi(s);
}

static int getenv_lane_value(const char *name, int def, int *value)
{
	const char *s = getenv(name);
	char *end = NULL;
	long parsed;

	if (!s || !*s) {
		*value = def;
		return SUCCESS;
	}

	errno = 0;
	parsed = strtol(s, &end, 10);
	if (errno || end == s || *end != '\0' || parsed < INT_MIN || parsed > INT_MAX) {
		fprintf(stderr, "Invalid %s=%s: expected an integer\n", name, s);
		return FAILURE;
	}

	*value = (int)parsed;
	return SUCCESS;
}

static int trace_timestamp_to_ns(const char *value, uint64_t *ns)
{
	char *end = NULL;
	long double seconds;

	errno = 0;
	seconds = strtold(value, &end);
	if (errno || end == value || *end != '\0' || seconds < 0.0L)
		return FAILURE;

	*ns = (uint64_t)(seconds * 1000000000.0L + 0.5L);
	return SUCCESS;
}



static int trace_load_for_channel(struct trace_bw_runtime *rt, int src_id, int dst_id,
				  int lane_id, int lane_count)
{
	FILE *fp;
	char line[512];
	uint64_t channel_match_count = 0, selected_count = 0, lane_max_size = 0;
	uint64_t channel_first_ts_ns = 0, first_selected_ts_ns = 0;
	uint64_t last_selected_ts_ns = 0;
	int have_channel_first = 0;
	unsigned long line_no = 0;

	fp = fopen(TRACE_FILE_PATH, "r");
	if (!fp) {
		fprintf(stderr, "Failed to open trace file %s: %s\n",
			TRACE_FILE_PATH, strerror(errno));
		return FAILURE;
	}

	while (fgets(line, sizeof(line), fp)) {
		int src, dst, nranks;
		unsigned long long size;
		char timestamp[128];
		uint64_t timestamp_ns;

		line_no++;
		if (sscanf(line, "%d %d %d %llu %127s", &src, &dst, &nranks, &size, timestamp) != 5)
			continue;

		if (trace_timestamp_to_ns(timestamp, &timestamp_ns)) {
			fprintf(stderr, "%s:%lu: malformed timestamp %s\n",
				TRACE_FILE_PATH, line_no, timestamp);
			fclose(fp);
			return FAILURE;
		}

		if (src != src_id || dst != dst_id)
			continue;

		if (!have_channel_first) {
			channel_first_ts_ns = timestamp_ns;
			have_channel_first = 1;
		}

		{
			uint64_t ordinal = channel_match_count;
			channel_match_count++;
			if ((ordinal % (uint64_t)lane_count) != (uint64_t)lane_id)
				continue;
		}

		if (size == 0) {
			fprintf(stderr, "%s:%lu: trace record size must be > 0\n",
				TRACE_FILE_PATH, line_no);
			fclose(fp);
			return FAILURE;
		}

		if (size > TRACE_MAX_WR_SIZE) {
			fprintf(stderr, "%s:%lu: trace record size %llu exceeds supported max %lu\n",
				TRACE_FILE_PATH, line_no, size,
				(unsigned long)TRACE_MAX_WR_SIZE);
			fclose(fp);
			return FAILURE;
		}

		if (selected_count == TRACE_MAX_FLOWS) {
			fprintf(stderr, "%s:%lu: selected flow count exceeds capacity %d for lane=%d/%d\n",
				TRACE_FILE_PATH, line_no, TRACE_MAX_FLOWS,
				lane_id, lane_count);
			fclose(fp);
			return FAILURE;
		}

		if (selected_count == 0)
			first_selected_ts_ns = timestamp_ns;

		rt->time_ns_list[selected_count] = timestamp_ns - channel_first_ts_ns;
		rt->size_list[selected_count] = (uint32_t)size;
		selected_count++;
		last_selected_ts_ns = timestamp_ns;
		if (size > lane_max_size)
			lane_max_size = size;
	}

	if (ferror(fp)) {
		fprintf(stderr, "Failed while reading trace file %s\n", TRACE_FILE_PATH);
		fclose(fp);
		return FAILURE;
	}
	fclose(fp);

	if (selected_count == 0) {
		rt->trace_count = 0;
		rt->trace_max_size = 1;
		rt->trace_first_ts_ns = have_channel_first ? channel_first_ts_ns : 0;
		rt->trace_duration_ns = 0;
		rt->trace_src_id = src_id;
		rt->trace_dst_id = dst_id;
		rt->trace_lane_id = lane_id;
		rt->trace_lane_count = lane_count;

		rt->user_param.iters = 5;

		rt->user_param.size = 1024;

		if (rt->user_comm.rdma_params) {
			rt->user_comm.rdma_params->iters = 5;
			rt->user_comm.rdma_params->size = 1024;
		}

		fprintf(stderr,
				"[trace] empty lane file=%s src=%d dst=%d lane=%d/%d channel_match_count=%lu\n",
				TRACE_FILE_PATH, src_id, dst_id, lane_id, lane_count,
				(unsigned long)channel_match_count);

		return SUCCESS;
	}

	rt->trace_count = selected_count;
	rt->trace_max_size = lane_max_size;
	rt->trace_first_ts_ns = channel_first_ts_ns;
	rt->trace_duration_ns = last_selected_ts_ns - channel_first_ts_ns;
	rt->trace_src_id = src_id;
	rt->trace_dst_id = dst_id;
	rt->trace_lane_id = lane_id;
	rt->trace_lane_count = lane_count;
	rt->user_param.iters = selected_count;
	rt->user_param.size = lane_max_size;
	if (rt->user_comm.rdma_params) {
		rt->user_comm.rdma_params->iters = selected_count;
		rt->user_comm.rdma_params->size = lane_max_size;
	}

	fprintf(stderr,
		"[trace] file=%s src=%d dst=%d lane_id=%d lane_count=%d channel_match_count=%lu selected_count=%lu lane_max_size=%lu channel_first_ts_ns=%lu first_selected_ts_ns=%lu first_selected_offset_ns=%lu last_selected_ts_ns=%lu duration_ns=%lu\n",
		TRACE_FILE_PATH, src_id, dst_id, lane_id, lane_count,
		(unsigned long)channel_match_count, (unsigned long)selected_count,
		(unsigned long)lane_max_size, (unsigned long)channel_first_ts_ns,
		(unsigned long)first_selected_ts_ns,
		(unsigned long)(first_selected_ts_ns - channel_first_ts_ns),
		(unsigned long)last_selected_ts_ns,
		(unsigned long)rt->trace_duration_ns);

	return SUCCESS;
}

static int trace_check_scope(struct perftest_parameters *user_param)
{
	if (user_param->verb != WRITE) {
		fprintf(stderr, "trace write_bw currently supports normal RDMA WRITE only\n");
		return FAILURE;
	}
	if (user_param->duplex) {
		fprintf(stderr, "trace write_bw currently supports one-way WRITE only\n");
		return FAILURE;
	}
	if (user_param->num_of_qps != 1) {
		fprintf(stderr, "trace write_bw currently supports exactly one QP\n");
		return FAILURE;
	}
	if (user_param->post_list != 1) {
		fprintf(stderr, "trace write_bw currently supports post_list == 1\n");
		return FAILURE;
	}
	if (user_param->test_type != ITERATIONS) {
		fprintf(stderr, "trace write_bw currently supports iterations mode only\n");
		return FAILURE;
	}

	return SUCCESS;
}

static int trace_configure_runtime(struct trace_bw_runtime *rt)
{
    int my_rank = getenv_int_default("TRACE_MY_RANK", -1);
    int src_rank = getenv_int_default("TRACE_SRC_RANK", -1);
    int dst_rank = getenv_int_default("TRACE_DST_RANK", -1);
    int peer_rank = getenv_int_default("TRACE_PEER_RANK", -1);
    int lane_id;
    int lane_count;

    if (trace_check_scope(&rt->user_param))
        return FAILURE;

    if (getenv_lane_value("TRACE_LANE_ID", 0, &lane_id) ||
        getenv_lane_value("TRACE_LANE_COUNT", 1, &lane_count))
        return FAILURE;

    if (lane_count < 1 || lane_count > TRACE_MAX_LANES ||
        lane_id < 0 || lane_id >= lane_count) {
        fprintf(stderr,
                "Invalid trace lane configuration: lane_id=%d lane_count=%d (required: 0 <= lane_id < lane_count <= %d)\n",
                lane_id, lane_count, TRACE_MAX_LANES);
        return FAILURE;
    }

    rt->trace_src_id = src_rank;
    rt->trace_dst_id = dst_rank;
    rt->trace_lane_id = lane_id;
    rt->trace_lane_count = lane_count;

    // fprintf(stderr,
    //         "[trace] role=%s my_rank=%d peer_rank=%d channel=%d->%d lane=%d/%d\n",
    //         rt->user_param.machine == CLIENT ? "client" : "server",
    //         my_rank,
    //         peer_rank,
    //         rt->trace_src_id,
    //         rt->trace_dst_id,
    //         rt->trace_lane_id,
    //         rt->trace_lane_count);

    /*
     * Both CLIENT and SERVER scan the same channel before resource
     * allocation and parameter negotiation. This gives both sides
     * identical iters and size without a separate metadata exchange.
     *
     * SERVER does not use time_ns_list/size_list later, but filling them
     * here keeps the change minimal by reusing the existing parser.
     */
    return trace_load_for_channel(
        rt,
        rt->trace_src_id,
        rt->trace_dst_id,
        rt->trace_lane_id,
        rt->trace_lane_count);
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

	if (trace_configure_runtime(rt))
		return FAILURE;

	if((rt->user_param.connection_type == DC || rt->user_param.use_xrc) && rt->user_param.duplex) {
		rt->user_param.num_of_qps *= 2;
	}

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

		// uintptr_t buf_base = (uintptr_t)rt->ctx.buf;
		// uintptr_t rdma_base = (uintptr_t)rt->my_dest[0].vaddr;

		// fprintf(stderr,
		// 		"[debug recv] ctx.buf=%p my_dest[0].vaddr=0x%lx diff=%ld size=%lu cycle_buffer=%lu\n",
		// 		rt->ctx.buf,
		// 		(unsigned long)rdma_base,
		// 		(long)(rdma_base - buf_base),
		// 		(unsigned long)rt->user_param.size,
		// 		(unsigned long)rt->ctx.cycle_buffer);

	//END AT LINE 272
	return 0;
};

int trace_bw_run_once(struct trace_bw_runtime *rt){
	//SERVER EARLY RETURN
	if (rt->user_param.machine == SERVER &&
		rt->user_param.verb == WRITE &&
		!rt->user_param.duplex) {
		return SUCCESS;
	}

	/* REGULAR */
	{

		if (rt->user_param.machine == CLIENT &&
			rt->user_param.verb == WRITE &&
			!rt->user_param.duplex &&
			rt->trace_count == 0) {

			fprintf(stderr,
					"[trace] empty lane: no WR will be posted, finish as SUCCESS\n");

        	return trace_report_empty_lane_result();
    	}
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

			if ( my_run_iter_bw(&rt->ctx,&rt->user_param, rt->time_ns_list, rt->size_list)) {
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
		
		// // YJT: TODO: ADD CHECK FOR DATA , ALL PRINT OUT
		// uint64_t print_len = rt->user_param.size < 256 ? rt->user_param.size : 256;

		// uintptr_t buf_base = (uintptr_t)rt->ctx.buf;
		// uintptr_t rdma_base = (uintptr_t)rt->my_dest[0].vaddr;

		// char *base = (char *)rt->ctx.buf + (rdma_base - buf_base);
		
		// for (uint64_t off = 0; off + 8 <= print_len; off += 8) {
		// 	uint64_t val = *(uint64_t *)(base + off);

		// 	printf("[recv] base=%p addr=%p offset=%lu value=%016lx\n",
		// 		base,
		// 		base + off,
		// 		(unsigned long)off,
		// 		val);
		// }

		// uintptr_t buf = (uintptr_t)rt->ctx.buf;
		// uintptr_t vaddr = (uintptr_t)rt->my_dest[0].vaddr;
		// uintptr_t off = vaddr - buf;

		// fprintf(stderr, "[debug recv] buf=0x%lx vaddr=0x%lx off=%lu\n",
		// 		(unsigned long)buf,
		// 		(unsigned long)vaddr,
		// 		(unsigned long)off);

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
    return ret;
}
