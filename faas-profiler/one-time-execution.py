#!/usr/bin/env python3

import sys
sys.path.insert(1, '/home/jamesqiu/faas-profiler/synthetic_workload_invoker')
sys.path.insert(1, '/home/jamesqiu/faas-profiler/workload_analyzer')
import WorkloadInvoker
import WorkloadAnalyzer

import time
import csv
import json
import os

# from a config file
FROM_CONFIG_FILE = True

# functions
function = 'base64'

# memory
# base64 from 256, primes from 192
memory = 256

# cpu share - 64, 128, 192, ..., 1024 (step size = 64)
cpu = 1024

# memory capacity (MB)
memory_capacity = 3072.0 # 2048, 1792, 1536, 1280, 1024, 768, 512

# invocation rates
rate = 10

# wait 30s for invocation records to be ready
interval = 30

start_time = round(time.time()*1000)
print('Experiment start-timestamp:', start_time)

# e.g., base64_mem512-1024
config_file_name = 'test-central-rm.json'
if not FROM_CONFIG_FILE:
    config_file_name = function + '_mem' + str(memory) + '_cpu' + str(cpu) + '_rate' + str(rate) + '.json'
    print('\nInvoker Capacity:', memory_capacity, 'MB Invocation Rate:', rate, 'Memory:', memory, 'MB CPU Share:', cpu, 'Config File:', config_file_name)

# not from an existing config file
if not FROM_CONFIG_FILE:
    # create config file
    content = {
        "test_name": "base64_mem" + str(memory) + "_cpu" + str(cpu) + "_rate" + str(rate) +"_test",
        "test_duration_in_seconds": 20,
        "random_seed": 100,
        "blocking_cli": False,
        "instances":{
            "instance1":{
                "application": 'base64_mem384_c4', # function + "_mem" +str(memory) + "-" + str(cpu),
                "distribution": "Uniform",
                "rate": rate,
                "activity_window": [2, 7]
            }
        }
    }
    with open(config_file_name, 'w+') as f:
        json.dump(content, f)

WorkloadInvoker.main(['./WorkloadInvoker', '-c', config_file_name])

time.sleep(interval)

print('Analyzing results...\n')
data_dict = WorkloadAnalyzer.main()

if not FROM_CONFIG_FILE:
    # remove the generated config file
    os.remove(config_file_name)

if data_dict == None:
    print('No invocation found!')
    exit()

"""
results = {
    'waitTime_warm': warm_starts_test_df['waitTime'].tolist(),
    'waitTime_cold': cold_starts_test_df['waitTime'].tolist(),
    'duration_warm': warm_starts_test_df['execution'].tolist(),
    'duration_cold': cold_starts_test_df['execution'].tolist(),
    'initTime_cold': cold_starts_test_df['initTime'].tolist(),
    'latency_warm': warm_starts_test_df['latency'].tolist(),
    'latency_cold': cold_starts_test_df['latency'].tolist(),
    'actual_invocation_rates': 1000.0/mean_invocation_period,
    'actual_throughput': stat_df['throughput'][0]
}
"""
# data_dict['function'] = functions[1]
# data_dict['memory'] = memory
# data_dict['cpu'] = cpu
# data_dict['rate'] = rate
# data_dict['concurrency'] = concurrency
# print(data_dict)

data_list = []

# write cold-start stats
for i in range(len(data_dict['waitTime_cold'])):
    item = {
        'function': function,
        'memory': memory,
        'cpu': cpu,
        'rate': rate,
        'actual_invocation_rates': data_dict['actual_invocation_rates'],
        'actual_throughput': data_dict['actual_throughput'],
        'type': 'cold',
        'waitTime': data_dict['waitTime_cold'][i],
        'initTime': data_dict['initTime_cold'][i],
        'execTime': data_dict['duration_cold'][i],
        'latency': data_dict['latency_cold'][i]
    }
    data_list.append(item)

# write warm-start stats
for i in range(len(data_dict['waitTime_warm'])):
    item = {
        'function': function,
        'memory': memory,
        'cpu': cpu,
        'rate': rate,
        'actual_invocation_rates': data_dict['actual_invocation_rates'],
        'actual_throughput': data_dict['actual_throughput'],
        'type': 'warm',
        'waitTime': data_dict['waitTime_warm'][i],
        'initTime': 0,
        'execTime': data_dict['duration_warm'][i],
        'latency': data_dict['latency_warm'][i]
    }
    data_list.append(item)

keys = data_list[0].keys()
with open('one-time-data.csv', 'w+', encoding='utf8', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(data_list)
print('Done writing data!')

print('Experiment start-timestamp:', start_time)
print('Experiment end-timestamp:', round(time.time()*1000))
