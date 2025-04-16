import numpy as np
import pandas as pd
import sys
import bisect

def analyze_trace_file(filename):
    print('READING: ' + filename)

    with open(filename) as file:
        raw_lines = file.readlines()[7:-1]  # Skip first 7 and last line
    print('FINISHED READING')

    # Parse trace lines just once
    parsed_lines = []
    timestamps = []

    for line in raw_lines:
        parts = line.rstrip().split()
        time = float(parts[0])
        label = parts[1]
        size = int(parts[2]) if len(parts) > 2 else 0
        delay = int(parts[3]) if len(parts) > 3 else 0
        parsed_lines.append((time, label, size, delay))
        timestamps.append(time)

    mRTT = int(filename.split('/')[-2].split('-')[-3])
    free_buffer = int(filename.split('/')[-2].split('-')[-2])

    cwnd_file = filename.split('/')[-2].replace("single-flow-scenario-", "")
    cwnd_file_full_path = '/d1/ccBench/pantheon-modified/third_party/tcpdatagen/dataset/{}-cwnd.txt'.format(cwnd_file)

    with open(cwnd_file_full_path, "r") as f:
        times = [float(line.split()[0]) * 1000 for line in f]  # ms

    time_step = 10  # ms
    num_points = len(times)

    times_df = np.zeros(num_points)
    channel_capacities = np.zeros(num_points)
    avg_queuing_delays = np.zeros(num_points)
    free_buffers = np.zeros(num_points)
    drop_rates = np.zeros(num_points)
    rec_rates = np.zeros(num_points)
    mRTTs = np.full(num_points, mRTT)

    # Use bisect to efficiently slice relevant lines for each time window
    for i in range(num_points):
        start_time = times[i] - time_step
        end_time = times[i]

        left_idx = bisect.bisect_left(timestamps, start_time)
        right_idx = bisect.bisect_left(timestamps, end_time)
        selected_lines = parsed_lines[left_idx:right_idx]

        channel_bytes = 0
        egress_lines = 0
        delay_sum = 0
        drops = 0
        ingress_packets = 0
        received_bytes = 0

        for time, label, size, delay in selected_lines:
            if '#' in label:
                channel_bytes += size
            if '-' in label:
                egress_lines += 1
                delay_sum += delay
                received_bytes += size
            elif 'd' in label:
                drops += 1
            elif '+' in label:
                ingress_packets += 1

        channel_capacity = (channel_bytes / float(time_step)) * 8 * 1000 / 1000000
        avg_queuing_delay = delay_sum / float(egress_lines) if egress_lines else 0
        drop_rate = float(drops) / (drops + egress_lines) if (drops + egress_lines) > 0 else 0
        rec_rate = (received_bytes / float(time_step)) * 8 * 1000 / 1000000

        buffer_increment = ingress_packets - egress_lines
        free_buffer -= buffer_increment

        times_df[i] = end_time
        channel_capacities[i] = channel_capacity
        avg_queuing_delays[i] = avg_queuing_delay
        free_buffers[i] = free_buffer
        drop_rates[i] = drop_rate
        rec_rates[i] = rec_rate

    df = pd.DataFrame({
        'time': times_df,
        'channel_capacity': channel_capacities,
        'avg_queuing_delay': avg_queuing_delays,
        'free_buffer': free_buffers,
        'drop_rate': drop_rates,
        'rec_rate': rec_rates,
        'mRTT': mRTTs
    })

    output_filename = filename.split('/')[-2] + '.csv'
    output_filename = output_filename.replace('single-flow-scenario-', '')
    output_base_address = '/d1/ccBench/pantheon-modified/third_party/tcpdatagen/dataset/'

    print(output_filename)
    df.to_csv(output_base_address + output_filename, index=False, header=True)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python script.py <trace_file>")
        sys.exit(1)

    trace_filename = sys.argv[1]
    analyze_trace_file(trace_filename + 'tcpdatagen_mm_datalink_run1.log')
