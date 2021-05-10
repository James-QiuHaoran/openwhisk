#!/usr/bin/env python3

from datetime import datetime
import pandas as pd
import sys
sys.path = ['../workload_analyzer', '../'] + sys.path
from ContactDB import GetActivationRecordsSince

"""
Extracts deep information from activation json record.
"""
def ExtractExtraAnnotations(json_annotations_data):
    extra_data = {'waitTime': [], 'initTime': [], 'kind': []}

    for item in json_annotations_data:
        if item['key'] in extra_data.keys():
            extra_data[item['key']] = item['value']

    for key in extra_data.keys():
        if extra_data[key] == []:
            extra_data[key] = 0

    return extra_data

"""
Constructs a dataframe for the performance information of all invocations.
"""    
def ConstructTestDataframe(since, limit=1000, read_results=False):
    perf_data = {'func_name': [], 'activationId': [], 'start': [], 'end': [
    ], 'duration': [], 'waitTime': [], 'initTime': [], 'latency': [], 'lang': []}
    if read_results:
        perf_data['results'] = []

    activations = GetActivationRecordsSince(since=since, limit=limit)
    if 'error' in activations.keys():
        print('Encountered an error getting data from the DB! Check the logs for more info.')
        print('DB error: ' + activations['reason'])
        return None
    activations = activations['docs']

    for activation in activations:
        if 'invokerHealthTestAction' in activation['name']:
            # skipping OpenWhisk's health check invocations
            continue
        perf_data['func_name'].append(activation['name'])
        perf_data['activationId'].append(activation['_id'])
        perf_data['start'].append(activation['start'])
        perf_data['end'].append(activation['end'])
        perf_data['duration'].append(activation['duration'])
        extra_data = ExtractExtraAnnotations(activation['annotations'])
        perf_data['waitTime'].append(extra_data['waitTime'])
        perf_data['initTime'].append(extra_data['initTime'])
        perf_data['lang'].append(extra_data['kind'])
        perf_data['latency'].append(
            perf_data['duration'][-1]+perf_data['waitTime'][-1])
        if read_results:
            perf_data['results'].append(activation['response']['result'])
        # perf_data['statusCode'].append(activation['response']['statusCode'])

    return pd.DataFrame(perf_data)

def main(start_timestamp, mem_capacity):
    # test_start_time = 1619375660000 # run-02 04/25
    test_start_time = int(start_timestamp)
    print('Start Time:', test_start_time)

    test_df = ConstructTestDataframe(since=test_start_time, limit=3000000, read_results=False) #3000000
    if (test_df is None):
        print('Error: Test result dataframe could not be constructed!')
        return False
    print('Records read from CouchDB: ' + str(len(test_df['start'])))
    print('\nTest Dataframe:')
    print(test_df)
    print('\n')

    # ref = test_df['start'].min()
    # test_df['start'] -= ref
    # test_df['end'] -= ref
    test_df['execution'] = test_df['duration'] - test_df['initTime']
    test_df['mem_capacity'] = mem_capacity

    test_df = test_df.drop(columns=['duration', 'lang'])

    test_df.to_csv('activations-' + mem_capacity + '.csv', index=False)

    """
    print('All Invocations Performance Summary')
    for dim in ['initTime', 'execution', 'latency']:
        print('Mean ' + dim + ' (ms): ' + str(test_df[dim].mean()))
        print('Std ' + dim + ' (ms): ' + str(test_df[dim].std()))
        print('***********')
    warm_starts_test_df = test_df[test_df['initTime'] == 0]
    print('Warm-started Invocations Performance Summary (count: ' +
          str(len(warm_starts_test_df[dim])) + ')')
    for dim in ['initTime', 'execution', 'latency']:
        print('Mean ' + dim + ' (ms): ' + str(warm_starts_test_df[dim].mean()))
        print('Std ' + dim + ' (ms): ' + str(warm_starts_test_df[dim].std()))
        print('***********')
    cold_starts_test_df = test_df[test_df['initTime'] != 0]
    print('Cold-started Invocations Performance Summary (count: ' + str(len(cold_starts_test_df[dim])) + ')')
    if len(cold_starts_test_df[dim]) < 1:
        print('***********')
    else:    
        for dim in ['initTime', 'execution', 'latency']:
            print('Mean ' + dim + ' (ms): ' + str(cold_starts_test_df[dim].mean()))
            if len(cold_starts_test_df[dim]) >= 2:
                print('Std ' + dim + ' (ms): ' + str(cold_starts_test_df[dim].std()))
            print('***********')

    """
    """
    results = {'waitTime_warm_mean': warm_starts_test_df['waitTime'].mean(), 'waitTime_warm_std': warm_starts_test_df['waitTime'].std(),
               'waitTime_cold_mean': cold_starts_test_df['waitTime'].mean(), 'waitTime_cold_std': cold_starts_test_df['waitTime'].std(),
               'initTime_cold_mean': cold_starts_test_df['initTime'].mean(), 'initTime_cold_std': cold_starts_test_df['initTime'].std(),
               'duration_warm_mean': warm_starts_test_df['execution'].mean(), 'duration_warm_std': warm_starts_test_df['execution'].std(),
               'duration_cold_mean': cold_starts_test_df['execution'].mean(), 'duration_cold_std': cold_starts_test_df['execution'].std(),
               'latency_warm_mean': warm_starts_test_df['latency'].mean(), 'latency_warm_std': warm_starts_test_df['latency'].std(),
               'latency_cold_mean': cold_starts_test_df['latency'].mean(), 'latency_cold_std': cold_starts_test_df['latency'].std(),
               'actual_invocation_rates': 1000.0/mean_invocation_period, 'actual_throughput': stat_df['throughput'][0]}
    """

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
    return results
    """

if __name__ == "__main__":
    start_time = sys.argv[1]
    mem_capacity = sys.argv[2]
    main(start_time, mem_capacity)
