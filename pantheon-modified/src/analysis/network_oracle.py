import numpy as np
import pandas as pd
import sys

def analyze_trace_file(filename):
    print('READING: ' + filename)

    with open(filename) as file:
        lines = file.readlines()
        lines = lines[7:-1]
        lines = [line.rstrip() for line in lines]

    print('FINISHED READING')
    df = pd.DataFrame(columns=['time', 'channel_capacity', 'avg_queuing_delay', 'packets_change'])

    times = []
    channel_capacities = []
    avg_queuing_delays = []
    packets_change = []

    print(lines[0])
    t0 = float(lines[0].split()[0])
    time_step = 100
    last_time = float(lines[-1].split()[0])

    start_time = t0 
    end_time = t0 + time_step

    while end_time < last_time:
        selected_lines = [line for line in lines if start_time <= float(line.split()[0]) <= end_time]

        channel_bytes = sum(int(line.split()[2]) for line in selected_lines if '#' in line.split()[1])
        channel_capacity = channel_bytes * 8 / 100000  # Convert to Mbps

        egress_lines = [line for line in selected_lines if '-' in line.split()[1]]
        egress_packts = len(egress_lines)
        delay = sum(int(line.split()[3]) for line in egress_lines)
        avg_queuing_delay = delay / egress_packts if egress_packts else 0

        ingress_packts = sum(1 for line in selected_lines if '+' in line.split()[1])
        change = egress_packts - ingress_packts

        times.append(start_time)
        channel_capacities.append(channel_capacity)
        avg_queuing_delays.append(avg_queuing_delay)
        packets_change.append(change)

        start_time = end_time
        end_time += time_step

    df['time'] = times
    df['channel_capacity'] = channel_capacities
    df['avg_queuing_delay'] = avg_queuing_delays
    df['packets_change'] = packets_change
    print('FINISHED ANALYSIS')

    output_filename = filename.split('/')[-2] + '.csv'
    output_filename = output_filename.replace('single-flow-scenario-', '')
        
    output_base_address = '/d1/ccBench/pantheon-modified/third_party/tcpdatagen/dataset/'
    print(output_filename)
    df.to_csv( output_base_address + output_filename, index=False)

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python script.py <trace_file>")
        sys.exit(1)
    trace_filename = sys.argv[1]
    analyze_trace_file( trace_filename + 'tcpdatagen_mm_datalink_run1.log')
