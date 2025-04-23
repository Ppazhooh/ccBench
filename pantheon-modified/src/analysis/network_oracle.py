import numpy as np
import pandas as pd
import sys
import bisect

def analyze_trace_file(filename):
    print('READING: ' + filename)

    # Read trace, skipping header and footer
    with open(filename) as file:
        raw_lines = file.readlines()[7:-1]
    print('FINISHED READING')

    # Parse trace lines once
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

    # Extract scenario parameters from path
    # e.g. .../single-flow-scenario-100-200/...
    dirname = filename.split('/')[-2]
    mRTT = int(dirname.split('-')[-3])
    capacity_pkts = int(dirname.split('-')[-2])  # initial buffer capacity in packets

    # Read cwnd timestamps (ms)
    cwnd_id = dirname.replace("single-flow-scenario-", "")
    cwnd_file = f'/d1/ccBench/pantheon-modified/third_party/tcpdatagen/dataset/{cwnd_id}-cwnd.txt'
    with open(cwnd_file) as f:
        times = [float(line.split()[0]) * 1000 for line in f]

    time_step = 10.0  # ms window size
    num_points = len(times)

    # Pre-allocate arrays for metrics
    times_df                  = np.zeros(num_points)
    channel_capacities        = np.zeros(num_points)
    avg_queuing_delays        = np.zeros(num_points)
    free_buffer_pkts          = np.zeros(num_points)
    drop_rates                = np.zeros(num_points)
    rec_rates                 = np.zeros(num_points)
    mRTTs                     = np.full(num_points, mRTT)

    # New metrics
    loss_burst_lengths        = np.zeros(num_points)
    inter_arrival_jitters     = np.zeros(num_points)
    buffer_occupancies        = np.zeros(num_points)
    queue_length_variances    = np.zeros(num_points)
    queueing_delay_gradients  = np.zeros(num_points)

    # Track current occupancy in packets and bytes
    occupancy_pkts = 0
    buffer_bytes   = 0

    # Slide through each window
    for i in range(num_points):
        start_time = times[i] - time_step
        end_time   = times[i]

        left_idx  = bisect.bisect_left(timestamps, start_time)
        right_idx = bisect.bisect_left(timestamps, end_time)
        window_events = parsed_lines[left_idx:right_idx]

        # Trackers
        burst_lengths = []
        current_burst = 0
        arrival_times = []
        occupancy_samples = []

        # Accumulators
        channel_bytes   = 0
        egress_lines    = 0
        delay_sum       = 0
        drops           = 0
        ingress_pkts    = 0
        received_bytes  = 0

        for (t, label, size, delay) in window_events:
            # Channel throughput (#)
            if '#' in label:
                channel_bytes += size

            # Egress = successful delivery (-)
            if '-' in label:
                egress_lines += 1
                delay_sum += delay
                received_bytes += size
                arrival_times.append(t)
                # release bytes from queue
                buffer_bytes = max(buffer_bytes - size, 0)

                # end loss burst if active
                if current_burst > 0:
                    burst_lengths.append(current_burst)
                    current_burst = 0

            # Packet drop (d)
            elif 'd' in label:
                drops += 1
                current_burst += 1

            # Ingress = enqueue (+)
            elif '+' in label:
                ingress_pkts += 1
                buffer_bytes += size
                occupancy_pkts += 1

            # record occupancy sample
            occupancy_samples.append(buffer_bytes)

        # finalize burst
        if current_burst > 0:
            burst_lengths.append(current_burst)

        # Compute original metrics
        channel_capacity    = (channel_bytes / time_step) * 8 * 1000 / 1e6  # Mbps
        avg_queuing_delay   = delay_sum / egress_lines if egress_lines else 0
        drop_rate           = drops / (drops + egress_lines) if (drops + egress_lines) else 0
        rec_rate            = (received_bytes / time_step) * 8 * 1000 / 1e6  # Mbps

        # Update and clamp packet occupancy
        occupancy_pkts += ingress_pkts - egress_lines
        occupancy_pkts = min(max(occupancy_pkts, 0), capacity_pkts)
        free_slots = capacity_pkts - occupancy_pkts

        # Store metrics
        times_df[i]                   = end_time
        channel_capacities[i]         = channel_capacity
        avg_queuing_delays[i]         = avg_queuing_delay
        free_buffer_pkts[i]           = free_slots
        drop_rates[i]                 = drop_rate
        rec_rates[i]                  = rec_rate

        # New metrics
        loss_burst_lengths[i]        = np.mean(burst_lengths) if burst_lengths else 0
        if len(arrival_times) > 1:
            inter_arrival_jitters[i] = np.std(np.diff(arrival_times))
        buffer_occupancies[i]       = buffer_bytes
        queue_length_variances[i]   = np.var(occupancy_samples) if occupancy_samples else 0
        if i > 0:
            queueing_delay_gradients[i] = avg_queuing_delay - avg_queuing_delays[i-1]

    # Build DataFrame
    df = pd.DataFrame({
        'time':                    times_df,
        'channel_capacity_Mbps':   channel_capacities,
        'avg_queuing_delay_ms':    avg_queuing_delays,
        'queueing_delay_gradient_ms': queueing_delay_gradients,
        'free_buffer_pkts':        free_buffer_pkts,
        'buffer_occupancy_bytes':  buffer_occupancies,
        'queue_length_var_bytes':  queue_length_variances,
        'drop_rate':               drop_rates,
        'loss_burst_length_pkts':  loss_burst_lengths,
        'rec_rate_Mbps':           rec_rates,
        'inter_arrival_jitter_ms': inter_arrival_jitters,
        'mRTT_ms':                 mRTTs
    })

    # Write CSV
    output_name = dirname.replace('single-flow-scenario-', '') + '.csv'
    out_path    = '/d1/ccBench/pantheon-modified/third_party/tcpdatagen/dataset/' + output_name
    print('WRITING:', out_path)
    df.to_csv(out_path, index=False)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python script.py <trace_file_base_without_.log>")
        sys.exit(1)

    trace_base = sys.argv[1]
    analyze_trace_file(trace_base + 'tcpdatagen_mm_datalink_run1.log')
