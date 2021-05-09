import time as time2
import sys
import json
import re
from influxdb import InfluxDBClient
from datetime import datetime
from dateutil import tz

# METHOD 1: Hardcode zones:
from_zone = tz.gettz('UTC')
to_zone = tz.gettz('America/Chicago')


def follow(fd_list):
	while True:
		for index, fd in enumerate(fd_list):
			line = fd.readline()
			if line:
				yield line
			else:
				continue

tDict = {}
containerID2Name = {}
count = 0

def convertTime(timeStr):
	t = datetime.strptime(timeStr,'%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=from_zone)
	localt = t.astimezone(to_zone)
	unixt = int(str(int(time2.mktime(localt.timetuple()))) + str(localt.microsecond*1000))
	return unixt

def writeColdStart2DB(metaData):
	global influx
	
	points = []
	point  = {}
	point['tags'] = {'machine': metaData['invoker'], 'container': metaData['container'], 'tid': metaData['tid']}	
	point['measurement'] = 'timeLine'
	point['time'] = convertTime(metaData['postTime'])
	point['fields'] = {'value': 1}
	points.append(point)
	if len(points) > 0:
		influx.write_points(points)

	points = []
	point['time'] = convertTime(metaData['post2Kafka'])
	point['fields'] = {'value': 2}
	points.append(point)
	if len(points) > 0:
		influx.write_points(points)

	points = []
	point['time'] = convertTime(metaData['recvTime'])
	point['fields'] = {'value': 3}
	points.append(point)
	if len(points) > 0:
		influx.write_points(points)

	points = []
	point['time'] = convertTime(metaData['coldstartBegin'])
	point['fields'] = {'value': 4}
	points.append(point)
	if len(points) > 0:
		influx.write_points(points)

	points = []
	point['time'] = convertTime(metaData['initBegin'])
	point['fields'] = {'value': 5}
	points.append(point)
	if len(points) > 0:
		influx.write_points(points)

	points = []
	point['time'] = convertTime(metaData['execBegin'])
	point['fields'] = {'value': 6}
	points.append(point)
	if len(points) > 0:
		influx.write_points(points)

	points = []
	point['time'] = convertTime(metaData['execEnd'])
	point['fields'] = {'value': 7}
	points.append(point)
	if len(points) > 0:
		influx.write_points(points)

def writeWarmStart2DB(metaData):
	global influx
	
	points = []
	point  = {}
	point['tags'] = {'machine': metaData['invoker'], 'container': metaData['container'], 'tid': metaData['tid']}	
	point['measurement'] = 'timeLine'
	point['time'] = convertTime(metaData['postTime'])
	point['fields'] = {'value': 1}
	points.append(point)
	if len(points) > 0:
		influx.write_points(points)

	points = []
	point['time'] = convertTime(metaData['post2Kafka'])
	point['fields'] = {'value': 2}
	points.append(point)
	if len(points) > 0:
		influx.write_points(points)

	points = []
	point['time'] = convertTime(metaData['recvTime'])
	point['fields'] = {'value': 3}
	points.append(point)
	if len(points) > 0:
		influx.write_points(points)

	points = []
	point['time'] = convertTime(metaData['execBegin'])
	point['fields'] = {'value': 4}
	points.append(point)
	if len(points) > 0:
		influx.write_points(points)

	points = []
	point['time'] = convertTime(metaData['execEnd'])
	point['fields'] = {'value': 5}
	points.append(point)
	if len(points) > 0:
		influx.write_points(points)

def parseLog(log):
	global count
	if 'POST' in log:
		# this is start time
		tokens = re.findall(r"\[.*?\]",log)		
		tid = tokens[2][1:-1]
		if tid == '#tid_sid_invokerHealth' or tid == '#tid_sid_invokerWarmup':
			return
		time = tokens[0][1:-1]
		message = log.split('] ')[-1]
		actionName = message.split(' ')[1].split('/')[-1]
		if tid in tDict.keys():
			tDict[tid]['postTime'] = time
			tDict[tid]['action'] = actionName
		else: 
			tDict[tid] = {'tid': tid, 'finish':-1, 'postTime':time, 'action':actionName, 'firstLog': 'controller'}

	
	elif '[ShardingContainerPoolBalancer] posting topic' in log:
		tokens = re.findall(r"\[.*?\]",log)		
		tid = tokens[2][1:-1]
		if tid == '#tid_sid_invokerHealth' or tid == '#tid_sid_invokerWarmup':
			return
		message = log.split('] ')[-1]
		invoker = message.split(' ')[2][1:-1]
		activationID = re.findall(r"\'.*?\'", log)[1][1:-1]
		tDict[tid]['activationID'] = activationID
		tDict[tid]['invoker'] = invoker
	elif '[ShardingContainerPoolBalancer] posted to invoker' in log:
		tokens = re.findall(r"\[.*?\]",log)		
		time = tokens[0][1:-1]
		tid = tokens[2][1:-1]
		if tid == '#tid_sid_invokerHealth' or tid == '#tid_sid_invokerWarmup':
			return
		tDict[tid]['post2Kafka'] = time
		if 'execEnd' in tDict[tid].keys():
			tDict[tid]['finish'] = 1
			#write2influxdb
			print(json.dumps(tDict[tid], indent=4))
			count+=1
			print(count)
			sys.stdout.flush()
			if tDict[tid]['isWarmStart'] == 'true':
				writeWarmStart2DB(tDict[tid])
			else:
				writeColdStart2DB(tDict[tid])
	
	elif '[InvokerReactive]  [marker:invoker_activation_start' in log:
		# this is start time
		tokens = re.findall(r"\[.*?\]",log)		
		tid = tokens[2][1:-1]
		if tid == '#tid_sid_invokerHealth' or tid == '#tid_sid_invokerWarmup':
			return
		time = tokens[0][1:-1]
		if tid in tDict.keys():
			tDict[tid]['recvTime'] = time
		else:
			tDict[tid] = {'tid': tid, 'finish':-1, 'recvTime':time, 'firstLog': 'invoker'}
	elif '[DockerClientWithFileAccess] running /usr/bin/docker run' in log:
		tokens = re.findall(r"\[.*?\]",log)		
		tid = tokens[2][1:-1]
		if tid == '#tid_sid_invokerHealth' or tid == '#tid_sid_invokerWarmup':
			return
		time = tokens[0][1:-1]
		container = log.split('wsk')[1].split(' ')[0]
		container ='wsk' + container
		tDict[tid]['coldstartBegin'] = time
		tDict[tid]['isWarmStart'] = 'false'
		tDict[tid]['container'] = container
	elif '[DockerContainer] sending initialization to' in log:
		tokens = re.findall(r"\[.*?\]",log)		
		time = tokens[0][1:-1]
		tid = tokens[2][1:-1]
		containerID = log.split('ContainerId(')[1].split(')')[0]
		print(containerID)
		containerID2Name[containerID] = tDict[tid]['container']
		if tid == '#tid_sid_invokerHealth' or tid == '#tid_sid_invokerWarmup':
			return
		tDict[tid]['initBegin'] = time
	elif '[DockerContainer] sending arguments to' in log:
		tokens = re.findall(r"\[.*?\]",log)		
		time = tokens[0][1:-1]
		tid = tokens[2][1:-1]
		if tid == '#tid_sid_invokerHealth' or tid == '#tid_sid_invokerWarmup':
			return
		tDict[tid]['execBegin'] = time
	elif '[DockerContainer] running result: ok' in log:
		tokens = re.findall(r"\[.*?\]",log)		
		time = tokens[0][1:-1]
		tid = tokens[2][1:-1]
		if tid == '#tid_sid_invokerHealth' or tid == '#tid_sid_invokerWarmup':
			return
		tDict[tid]['execEnd'] = time
		if 'post2Kafka' in tDict[tid].keys():
			tDict[tid]['finish'] = 1
			print(json.dumps(tDict[tid], indent=4))
			count+=1
			print(count)
			sys.stdout.flush()
			#write2influxdb
			if tDict[tid]['isWarmStart'] == 'true':
				writeWarmStart2DB(tDict[tid])
			else:
				writeColdStart2DB(tDict[tid])

	elif '[ContainerPool] containerStart containerState: warmed container:' in log:
		tokens = re.findall(r"\[.*?\]",log)		
		tid = tokens[2][1:-1]
		if tid == '#tid_sid_invokerHealth' or tid == '#tid_sid_invokerWarmup':
			return
		tDict[tid]['isWarmStart'] = 'true'
		containerID = log.split('ContainerId(')[1].split(')')[0]
		tDict[tid]['container'] = containerID2Name[containerID]

def checkFinish(tDict):
	for key,val in tDict.items():
		if val['finish'] == -1:
			return False
	return True

if __name__ == '__main__':
#	line = "[2021-05-07T05:53:31.928Z] [WARN] [#tid_670e74b0bcc5d97e497b4e54b21486bc] [ShardingContainerPoolBalancer] system is overloaded. Chose invoker1 by random assignment."
	#lines = ["[2021-05-07T05:53:28.824Z] [INFO] [#tid_85ab7e3525b1f057b7787d433301fa8f] POST /api/v1/namespaces/guest/actions/nodejs_base64_300000_mem448-1024 blocking=False&result=false", 
	#	"[2021-05-07T05:53:28.828Z] [INFO] [#tid_85ab7e3525b1f057b7787d433301fa8f] [ShardingContainerPoolBalancer] posting topic 'invoker0' with activation id 'cae05a6d48224ceda05a6d4822dcedca' [marker:controller_kafka_start:5]",
	#	"[2021-05-07T05:53:28.838Z] [INFO] [#tid_85ab7e3525b1f057b7787d433301fa8f] [ShardingContainerPoolBalancer] posted to invoker0[0][276] [marker:controller_kafka_finish:14:9]"]
	#for line in lines:
	#	parseControllerLog(line)
	try:
		influx = InfluxDBClient('localhost', 8086, 'root', 'root', 'cadvisor')

		fileList = ["controller0/controller0/controller0_logs.log"]
		for i in range(8):
			fileList.append("invoker" + str(i) + "/invoker" + str(i) + "/invoker" + str(i) + "_logs.log")
		#print(fileList)
		fd_list = []
		for f in fileList:
			fd = open(f, 'r')
			fd.seek(0,2)
			fd_list.append(fd)
		logLines = follow(fd_list)
		for line in logLines:
			parseLog(line)
	except KeyboardInterrupt:
		print(len(tDict))
		print(json.dumps(tDict, indent=4))	
