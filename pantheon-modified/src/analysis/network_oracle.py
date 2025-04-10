import numpy as np
import pandas as pd
import sys

def analyze_trace_file(filename):
    print('READING: ' + filename)
    
    # Read the trace file once
    with open(filename) as file:
        lines = file.readlines()[7:-1]  # Skip first 7 and last line
        lines = [line.rstrip() for line in lines]

    print('FINISHED READING')

    # Extract the cwnd file path
    cwnd_file = filename.split('/')[-2].replace("single-flow-scenario-", "")
    cwnd_file_full_path = '/d1/ccBench/pantheon-modified/third_party/tcpdatagen/dataset/{}-cwnd.txt'.format(cwnd_file)

    # Read the cwnd file once
    with open(cwnd_file_full_path, "r") as f:
        times = [float(line.split()[0]) * 1000 for line in f.readlines()]  # Convert to ms in one pass

    # Pre-allocate lists for DataFrame
    time_step = 10  # in ms
    num_points = len(times)
    
    times_df = np.zeros(num_points)
    channel_capacities = np.zeros(num_points)
    avg_queuing_delays = np.zeros(num_points)
    packets_change = np.zeros(num_points)

    # Process the data
    for i in range(num_points):
        start_time = times[i] - time_step
        end_time = times[i]
 
        selected_lines = [line for line in lines if start_time <= float(line.split()[0]) < end_time]
        
        # Calculate channel capacity
        channel_bytes = sum(int(line.split()[2]) for line in selected_lines if '#' in line.split()[1])
        channel_capacity = (channel_bytes / time_step) * 8 * 1000 / 1000000  # Convert to Mbps

        # Calculate avg queuing delay
        egress_lines = [line for line in selected_lines if '-' in line.split()[1]]
        egress_packets = len(egress_lines)
        delay = sum(int(line.split()[3]) for line in egress_lines)
        avg_queuing_delay = delay / egress_packets if egress_packets else 0

        # Calculate packet change
        ingress_packets = sum(1 for line in selected_lines if '+' in line.split()[1])
        change = egress_packets - ingress_packets

        # Store the results
        times_df[i] = end_time

        channel_capacities[i] = channel_capacity
        avg_queuing_delays[i] = avg_queuing_delay
        packets_change[i] = change

    # Create DataFrame from the pre-allocated arrays
    df = pd.DataFrame({
        'time': times_df,
        'channel_capacity': channel_capacities,
        'avg_queuing_delay': avg_queuing_delays,
        'packets_change': packets_change
    })



    output_filename = filename.split('/')[-2] + '.csv'
    output_filename = output_filename.replace('single-flow-scenario-', '')
    output_base_address = '/d1/ccBench/pantheon-modified/third_party/tcpdatagen/dataset/'
    print(output_filename)

    # Write the DataFrame to CSV in one call
    df.to_csv(output_base_address + output_filename, index=False, header=False)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python script.py <trace_file>")
        sys.exit(1)
    
    trace_filename = sys.argv[1]
    analyze_trace_file(trace_filename + 'tcpdatagen_mm_datalink_run1.log')
