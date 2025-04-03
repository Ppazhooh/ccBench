import numpy as np
import pandas as pd
import sys
filename = sys.argv[1]
print('READING: ' + filename)

with open(filename) as file:
    lines = file.readlines()
    lines = lines[7:-1]
    lines = [line.rstrip() for line in lines]

print('FINISHED READING')
df = pd.DataFrame(columns=['time', 'channel_capacity', 'avg_queuing_delay','packets_change'])

times = []
channel_capacities = []
avg_queuing_delays = []
packets_change = []

print(lines[0])
# t0 is the time of the first line
t0 = float(lines[0].split()[0])
time_step = 100
# last time stamp
last_time = float(lines[-1].split()[0])

start_time = t0 
end_time = t0 + time_step


while end_time < last_time:
    # now select all the lines that lie within this time window
    selected_lines = []
    for line in lines:
        time = float(line.split()[0])
        if time >= start_time and time <= end_time:
            selected_lines.append(line)

    # now count the third number of all the lines that have # in their second column
    channel_bytes = 0
    for line in selected_lines:
        if '#' in line.split()[1]:
            channel_bytes += int(line.split()[2])
    # now convert to Mbps
    channel_capacity = channel_bytes * 8 / 100000

    # now for all the lines that have -, average on their fourth column and done
    egress_packts = 0
    delay = 0
    for line in selected_lines:
        if '-' in line.split()[1]:
            delay += int(line.split()[3])
            egress_packts += 1
    if egress_packts == 0:
        avg_queuing_delay = 0
    else:
        avg_queuing_delay = delay / egress_packts
        
    
    ingress_packts = 0
    for line in selected_lines:
        if '+' in line.split()[1]:
            ingress_packts += 1
    
    change = egress_packts - ingress_packts
    
     
    start_time = end_time
    end_time = end_time + time_step
    # put this into a dataframe
    times.append(start_time)
    channel_capacities.append(channel_capacity)
    avg_queuing_delays.append(avg_queuing_delay)
    packets_change.append(change)

# now put this into a dataframe
df['time'] = times
df['channel_capacity'] = channel_capacities
df['avg_queuing_delay'] = avg_queuing_delays
df['packets_change'] = packets_change
print('FINISHED ANALYSIS')

# take the final part of the name of the files and put it in the name of the csv file
filename = filename.split('/')[-2]
df.to_csv(filename+ '.csv', index=False)
    