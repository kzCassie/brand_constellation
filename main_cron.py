#!usr/bin/env python3
import sys
import boto3
import csv
import time
import pickle
import os
from botocore.exceptions import ClientError


def start_instance(ec2,tagname,valuelist):
	instance_info = []
	instances = filter_instance(ec2,tagname,valuelist)

	for instance in instances:
		instance.start()
		instance_id = instance.instance_id
		instance_ip = instance.public_ip_address
		# get name value of the instance
		for tag in instance.tags:
			if tag['Key'] == 'Name':
				name = tag['Value']   # Name: post_01
				rank = int(name.split('_')[1])

		instance_info.append((instance_id,instance_ip,rank))

	for instance in instances:
		instance.wait_until_running()
	
	return instance_info

def stop_instance(ec2,tagname,valuelist):
	id_list = []
	instances = filter_instance(ec2,tagname,valuelist)
	
	for instance in instances:
		instance.stop()
		id_list.append(instance.instance_id)
	
	for instance in instances:
		instance.wait_until_stopped()

	return id_list

def command_run_post_py(ssm_client, instance_info, totalrank):
	command_info = [] # tuple(command_id, list[instance id])
	
	# loop through all instances
	for info in instance_info:
		instance_id,instance_ip,rank = info

		shell_commands = ['cd /home/ubuntu/',
						  'sudo chmod -R a+w ig/data',
						  'sudo chmod -R a+w ig/log',
						  'cd ig/code',
						  'pwd',
						  'nohup python3 post.py '+str(rank)+' '+str(totalrank)+' &']
		
		response = ssm_client.send_command( 
			InstanceIds=[instance_id],
			DocumentName="AWS-RunShellScript",
			TimeoutSeconds=60,
			Comment="Run post.py with instance-specific rank and totalrank number.",
			Parameters={'commands': shell_commands,
						'executionTimeout': ['10800']}, 
		)
		command_id = response['Command']['CommandId']
		command_info.append((command_id,[instance_id]))
	
	# return command output
	return command_info

def command_get_response(ssm_client,command_info):
	command_responses = []
	for t in command_info:
		command_id,instance_id_list = t
		for instance_id in instance_id_list:
			output = ssm_client.get_command_invocation(
					CommandId=command_id,
					InstanceId=instance_id,
			)
			command_responses.append(output)
	return command_responses

########################## Helper Functions ##########################
def filter_instance(ec2,tagname,valuelist):
	filtered_instances = ec2.instances.filter(
	    Filters=[{'Name': 'tag:'+tagname,'Values': valuelist}],
	    # DryRun=True, #TODO
	    # InstanceIds=['string',],
	    # MaxResults=123,
	    # NextToken='string'
	)
	return filtered_instances

def gen_instance_info(ec2,tagname,valuelist):
	instance_info = []
	instances = filter_instance(ec2,tagname,valuelist)

	for instance in instances:
		instance_id = instance.instance_id
		instance_ip = instance.public_ip_address
		# get name value of the instance
		for tag in instance.tags:
			if tag['Key'] == 'Name':
				name = tag['Value']   # Name: post_01
				rank = int(name.split('_')[1])
		instance_info.append((instance_id,instance_ip,rank))
	
	return instance_info

def write_instance_info(instance_info,path):
	with open(path, "w") as f:
	    writer = csv.writer(f)
	    writer.writerows(instance_info)

def read_instance_info(path):
	with open(path,'r') as f:
		data=[tuple(line) for line in csv.reader(f)]
	return data

def write_command_info(command_info,path):
	with open(path,'wb') as f:
		pickle.dump(command_info,f)

def read_command_info(path):
	with open(path,'rb') as f:
		data = pickle.load(f)
	return data

########################## MAIN ##########################
def main():
	#CONSTANTS
	RANK_TOTAL = 20
	TAG_NAME = 'Task'
	VALUE_LIST = ['get_post']
	INSTANCE_CSV = "Desktop/ig/input/instance.csv"
	COMMAND_INFO = "Desktop/ig/input/command"
	
	# initialization
	ec2 = boto3.resource('ec2')
	ssm_client = boto3.client('ssm')

	# # stop instance
	# id_list = stop_instance(ec2,TAG_NAME,VALUE_LIST)
	# print('Following instances have been stopped: '+str(id_list))
	
	# # start instance with certain tags
	# instance_info = start_instance(ec2,TAG_NAME,VALUE_LIST)
	# print("Following instances have been started:")
	# print(instance_info)
	
	# write instance info to csv
	instance_info = gen_instance_info(ec2,TAG_NAME,VALUE_LIST)
	write_instance_info(instance_info,INSTANCE_CSV)
	print("instance info has been written to instance.csv")
	time.sleep(60)

	# run post.py, save command info
	instance_info = read_instance_info(INSTANCE_CSV)
	command_info = command_run_post_py(ssm_client, instance_info, RANK_TOTAL)
	write_command_info(command_info,COMMAND_INFO)

if __name__ == '__main__':
	main()
