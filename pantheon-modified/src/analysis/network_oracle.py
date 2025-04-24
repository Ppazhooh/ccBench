from __future__ import print_function, division
import numpy as np
import pandas as pd
import sys
import bisect

def analyze_trace_file(filename):
    print('READING:', filename)

    # Read trace, skipping header (first 7) and footer (last line)
    with open(filename, 'r') as f:
        raw_lines = f.readlines()[7:-1]
    print('FINISHED READING TRACE')

    # Parse trace lines
    parsed_lines = []
    timestamps = []
    for line in raw_lines:
        parts = line.rstrip().split()
        t = float(parts[0])
        label = parts[1]
        size = int(parts[2]) if len(parts) > 2 else 0
        delay = int(parts[3]) if len(parts) > 3 else 0
        parsed_lines.append((t, label, size, delay))
        timestamps.append(t)

    # Extract scenario parameters from parent directory
    dirname = filename.split('/')[-2]  # e.g. "single-flow-scenario-100-200"
    capacity_pkts = int(dirname.split('-')[-2])  # buffer capacity in packets
    mRTT = int(dirname.split('-')[-3])           # in ms

    # Read cwnd timestamps (ms)
    cwnd_id = dirname.replace("single-flow-scenario-", "")
    cwnd_file = '/d1/ccBench/pantheon-modified/third_party/tcpdatagen/dataset/{0}-cwnd.txt'.format(cwnd_id)
    with open(cwnd_file, 'r') as f:
        times = np.array([float(line.split()[0]) * 1000 for line in f])  # ms

    time_step = 10.0  # ms
    num_points = len(times)
    T_end = times[-1]

    # Pre-allocate arrays for metrics
    times_df                   = np.zeros(num_points)
    channel_capacities         = np.zeros(num_points)
    avg_queuing_delays         = np.zeros(num_points)
    queueing_delay_gradients   = np.zeros(num_points)
    free_buffer_pkts           = np.zeros(num_points)
    buffer_occupancies_bytes   = np.zeros(num_points)
    queue_length_var_bytes     = np.zeros(num_points)
    drop_rates                 = np.zeros(num_points)
    loss_burst_length_pkts     = np.zeros(num_points)
    rec_rates                  = np.zeros(num_points)
    inter_arrival_jitter_ms    = np.zeros(num_points)
    mRTTs                      = np.full(num_points, mRTT)
    time_to_buffer_full_ms     = np.zeros(num_points)
    link_busyness_fraction     = np.zeros(num_points)

    # State for occupancy tracking
    occupancy_pkts = 0

    for i in range(num_points):
        start_t = times[i] - time_step
        end_t   = times[i]

        # select events in [start_t, end_t)
        left  = bisect.bisect_left(timestamps, start_t)
        right = bisect.bisect_left(timestamps, end_t)
        window_events = parsed_lines[left:right]

        # Trackers for new metrics
        burst_lengths = []
        current_burst = 0
        arrival_times = []
        occupancy_samples = []
        busy_ms_set = set()

        # Original accumulators
        channel_bytes = 0
        egress_pkts   = 0
        delay_sum     = 0
        drops         = 0
        ingress_pkts  = 0
        rec_bytes     = 0

        for (t, label, size, delay) in window_events:
            # channel throughput marker
            if '#' in label:
                channel_bytes += size
                busy_ms_set.add(int(t))  # mark this ms as busy

            # successful egress ('-')
            if '-' in label:
                egress_pkts += 1
                delay_sum += delay
                rec_bytes += size
                arrival_times.append(t)
                # end any loss burst
                if current_burst > 0:
                    burst_lengths.append(current_burst)
                    current_burst = 0

            # drop ('d')
            elif 'd' in label:
                drops += 1
                current_burst += 1

            # ingress ('+')
            elif '+' in label:
                ingress_pkts += 1

            occupancy_samples.append(occupancy_pkts)

        # finalize any ongoing burst
        if current_burst > 0:
            burst_lengths.append(current_burst)

        # Update occupancy (packets)
        net_pkts = ingress_pkts - egress_pkts
        occupancy_pkts = min(max(occupancy_pkts + net_pkts, 0), capacity_pkts)
        free_pkts = capacity_pkts - occupancy_pkts

        # Original metric computations
        channel_capacity_mbps = (channel_bytes / time_step) * 8 * 1000 / 1e6
        avg_q_delay_ms        = delay_sum / egress_pkts if egress_pkts else 0
        drop_rate_val         = drops / (drops + egress_pkts) if (drops + egress_pkts) else 0
        rec_rate_mbps         = (rec_bytes / time_step) * 8 * 1000 / 1e6

        # New metric: time to buffer full (ms), capped at end of trace
        if net_pkts > 0:
            remaining_pkts = capacity_pkts - occupancy_pkts
            ttf = remaining_pkts / (net_pkts / time_step)
            ttf = min(ttf, T_end - times[i])
        else:
            ttf = T_end - times[i]

        # New metric: link busyness fraction in this window
        busy_ms = len(busy_ms_set)
        busyness = busy_ms / time_step

        # Store into arrays
        times_df[i]                   = end_t
        channel_capacities[i]         = channel_capacity_mbps
        avg_queuing_delays[i]         = avg_q_delay_ms
        queueing_delay_gradients[i]   = (avg_q_delay_ms - avg_queuing_delays[i-1]) if i > 0 else 0
        free_buffer_pkts[i]           = free_pkts
        buffer_occupancies_bytes[i]   = occupancy_pkts
        queue_length_var_bytes[i]     = np.var(occupancy_samples) if occupancy_samples else 0
        drop_rates[i]                 = drop_rate_val
        loss_burst_length_pkts[i]     = np.mean(burst_lengths) if burst_lengths else 0
        rec_rates[i]                  = rec_rate_mbps
        inter_arrival_jitter_ms[i]    = np.std(np.diff(arrival_times)) if len(arrival_times) > 1 else 0
        mRTTs[i]                      = mRTT
        time_to_buffer_full_ms[i]     = ttf
        link_busyness_fraction[i]     = busyness

    # Assemble DataFrame
    df_out = pd.DataFrame({
        'time_ms':                    times_df,
        'channel_capacity_Mbps':      channel_capacities,
        'avg_queuing_delay_ms':       avg_queuing_delays,
        'queueing_delay_gradient_ms': queueing_delay_gradients,
        'free_buffer_pkts':           free_buffer_pkts,
        'buffer_occupancy_pkts':      buffer_occupancies_bytes,
        'queue_length_var_pkts':      queue_length_var_bytes,
        'drop_rate':                  drop_rates,
        'loss_burst_length_pkts':     loss_burst_length_pkts,
        'rec_rate_Mbps':              rec_rates,
        'inter_arrival_jitter_ms':    inter_arrival_jitter_ms,
        # 'time_to_buffer_full_ms':  time_to_buffer_full_ms,
        'link_busyness_fraction':     link_busyness_fraction,
        'mRTT_ms':                    mRTTs
    })

    # Write CSV
    output_name = dirname.replace('single-flow-scenario-', '') + '.csv'
    out_path = '/d1/ccBench/pantheon-modified/third_party/tcpdatagen/dataset/{0}'.format(output_name)
    print('WRITING:', out_path)
    df_out.to_csv(out_path, index=False)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python script.py <trace_file_base_without_.log>")
        sys.exit(1)
    base = sys.argv[1]
    analyze_trace_file(base + 'tcpdatagen_mm_datalink_run1.log')
