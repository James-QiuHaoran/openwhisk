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
functions = ['primes', 'base64', 'json']

# sleep for 1 minute for the results to be ready at the CouchDB
interval = 60

for function in functions:
    data_list = []
    # e.g., base64_mem2564
    config_file_name = function + '_mem256.json'
            
    # create config file
    content = {
	"test_name": function + "_mem256_test",
	"test_duration_in_seconds": 20,
	"random_seed": 100,
	"blocking_cli": False,
	"instances":{
	    "instance1":{
		"application": function + "_mem256",
		"param_file": "params.json",
                "distribution": "Uniform",
		"rate": 20,
		"activity_window": [2, 7]
	    }
	}
    }
    with open(config_file_name, 'w+') as f:
        json.dump(content, f)

    for i in range(3):
        WorkloadInvoker.main(['./WorkloadInvoker', '-c', config_file_name])
        time.sleep(interval)
        print('Analyzing results...')
        data_dict = WorkloadAnalyzer.main()
        print(data_dict)
        if data_dict == None:
            print('No invocation found!')
            continue
        data_dict['function'] = function
        print(data_dict)
        data_list.append(data_dict)

    if function == functions[0]:
        keys = data_list[0].keys()
        with open('data-horizontal.csv', 'w+', encoding='utf8', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(data_list)
        print('Done writing header and data!')
    else:
        with open('data-horizontal.csv', 'a', encoding='utf8', newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writerows(data_list)
        print('Done writing data!')
