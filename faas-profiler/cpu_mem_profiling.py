#!/usr/bin/env python3

import sys
sys.path.insert(1, '/home/jamesqiu/faas-profiler/synthetic_workload_invoker')
sys.path.insert(1, '/home/jamesqiu/faas-profiler/workload_analyzer')
import WorkloadInvoker
import WorkloadAnalyzer

import time
import csv
import json

# functions
functions = ['primeNumber', 'base64']

# memory - 128, 192, 256, ..., 512 (step size = 64 MB)
# base64 from 256, primes from 192
# cpu share - 64, 128, 192, ..., 1024 (step size = 64)

# sleep for 2 minute for the results to be ready at the CouchDB
interval = 120

# invocation rates
rates = [10, 20, 30, 40, 50]

for rate in rates:
    data_list = []
    for memory in range(256, 512+1, 64):
        for cpu in range(128, 1024+1, 128):
            # e.g., base64_mem512-1024
            config_file_name = functions[1] + '_mem' + str(memory) + '_cpu' + str(cpu) + '_rate' + str(rate) + '.json'
            print('Invocation Rate:', rate, 'Memory:', memory, 'MB CPU Share:', cpu, 'Config File:', config_file_name)
            
            # create config file
            content = {
                "test_name": "base64_mem" + str(memory) + "_cpu" + str(cpu) + "_rate" + str(rate) +"_test",
                "test_duration_in_seconds": 20,
                "random_seed": 100,
                "blocking_cli": False,
                "instances":{
                    "instance1":{
                        "application": "base64_mem" +str(memory) + "-" + str(cpu),
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
            print('Analyzing results...')
            data_dict = WorkloadAnalyzer.main()
            print(data_dict)
            if data_dict == None:
                print('No invocation found!')
                continue
            data_dict['function'] = functions[1]
            data_dict['memory'] = memory
            data_dict['cpu'] = cpu
            data_dict['rate'] = rate
            print(data_dict)
            data_list.append(data_dict)

    if rate == rates[0]:
        keys = data_list[0].keys()
        with open('data.csv', 'w+', encoding='utf8', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(data_list)
        print('Done writing header and data!')
    else:
        with open('data.csv', 'a', encoding='utf8', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(data_list)
        print('Done writing data!')
