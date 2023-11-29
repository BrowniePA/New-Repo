# -*- coding: utf-8 -*-
"""
Created on Tue Jul 19 13:46:57 2022

@author: Oleg

source: https://gist.github.com/jacobtomlinson/ee5ba79228e42bcc9975faf0179c3d1a

"""
import boto3
import time
import sys
import webbrowser


class ecs_cluster:

    def __init__(self, aws_profile=""):
        if not aws_profile:
            raise Exception("Please specifiy aws profile")
        self.session = boto3.Session(profile_name=aws_profile)
        self.region = self.session.region_name
        self.log_group = "/possehl-analytics/dask-fargate"
        self.log_prefix = "dask-fargate"
        self.log_retention = 30
        try:
            self.execution_role_arn = self.read_roles("dask-fargate-execution")
            self.task_role_arn = self.read_roles("dask-fargate-task")
        except:
            pass

    def create_cluster(self, cluster_name="PA-Cluster"):
        ecs = self.session.client('ecs')
        cluster_arn = ecs.create_cluster(clusterName=cluster_name)
        return cluster_arn['cluster']['clusterArn']

    def create_execution_role(self, execution_role_name="dask-fargate-execution"):
        iam = self.session.client('iam')
        try:
            response = iam.create_role(
                RoleName=execution_role_name,
                AssumeRolePolicyDocument="""{
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "ecs-tasks.amazonaws.com"},
                            "Action": "sts:AssumeRole"
                        }
                    ]
                }""",
                Description='A role for Fargate to use when executing'
            )

            iam.attach_role_policy(
                RoleName=execution_role_name,
                PolicyArn='arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly'
            )

            iam.attach_role_policy(
                RoleName=execution_role_name,
                PolicyArn='arn:aws:iam::aws:policy/CloudWatchLogsFullAccess'
            )

            iam.attach_role_policy(
                RoleName=execution_role_name,
                PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceRole'
            )

        except:
            response = iam.get_role(RoleName=execution_role_name)

        self.execution_role_arn = response['Role']['Arn']

    def create_task_role(self, task_role_name="dask-fargate-task"):
        iam = self.session.client('iam')
        try:
            response = iam.create_role(
                RoleName=task_role_name,
                AssumeRolePolicyDocument="""{
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "ecs-tasks.amazonaws.com"},
                            "Action": "sts:AssumeRole"
                        }
                    ]
                }""",
                Description='A role for dask containers to use when executing'
            )

            iam.attach_role_policy(
                RoleName=task_role_name, PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')

        except:
            response = iam.get_role(RoleName=task_role_name)

        self.task_role_arn = response['Role']['Arn']

    def create_log_group(self, log_group="", log_retention=0):
        logs = self.session.client('logs')
        if not log_group:
            log_group = self.log_group

        if log_retention == 0:
            log_retention = self.log_retention

        try:
            logs.create_log_group(logGroupName=log_group)
            logs.put_retention_policy(
                logGroupName=log_group, retentionInDays=log_retention)
        except:
            pass

    def read_vpc_net(self):
        ec2 = self.session.client('ec2')
        [default_vpc] = [vpc for vpc in ec2.describe_vpcs()['Vpcs'] if vpc['IsDefault']]
        subnet_list = [subnet['SubnetId'] for subnet in ec2.describe_subnets(
        )['Subnets'] if subnet['VpcId'] == default_vpc['VpcId']]
        return default_vpc, subnet_list

    def setup_network_interfaces(self, vpc_security_group_name="dask"):
        ec2 = self.session.client('ec2')
        default_vpc, vpc_subnets = self.read_vpc_net()
        try:
            response = ec2.create_security_group(
                Description='A security group for dask',
                GroupName=vpc_security_group_name,
                VpcId=default_vpc['VpcId'],
                DryRun=False
            )

            ec2.authorize_security_group_ingress(
                GroupId=response['GroupId'],
                IpPermissions=[
                    {
                        'IpProtocol': 'TCP',
                        'FromPort': 0,
                        'ToPort': 65535,
                        'UserIdGroupPairs': [
                            {
                                'GroupName': vpc_security_group_name
                            },
                        ]
                    },
                ],
                DryRun=False
            )
        except:
            [response] = ec2.describe_security_groups(
                GroupNames=[vpc_security_group_name])['SecurityGroups']

    def read_roles(self, role_name):
        iam = self.session.client('iam')
        role = iam.get_role(RoleName=role_name)
        return role['Role']['Arn']

    def create_scheduler_definition(self, scheduler_cpu=1024, scheduler_mem=2048):
        ecs = self.session.client('ecs')
        scheduler_task_definition = ecs.register_task_definition(
            family='dask-scheduler',
            taskRoleArn=self.task_role_arn,
            executionRoleArn=self.execution_role_arn,
            networkMode='awsvpc',
            containerDefinitions=[
                {
                    'name': 'dask-scheduler',
                    'image': "256991223769.dkr.ecr.eu-central-1.amazonaws.com/dask-cluster:latest",
                    'cpu': scheduler_cpu,
                    'memory': scheduler_mem,
                    'memoryReservation': scheduler_mem,
                    'essential': True,
                    'command': ['dask-scheduler'],
                    'logConfiguration': {
                        'logDriver': 'awslogs',
                        'options': {
                            'awslogs-region': self.region,
                            'awslogs-group': self.log_group,
                            'awslogs-stream-prefix': self.log_prefix,
                            'awslogs-create-group': 'true'
                        }
                    }
                },
            ],
            volumes=[],
            requiresCompatibilities=['FARGATE'],
            cpu=str(scheduler_cpu),
            memory=str(scheduler_mem)
        )
        return scheduler_task_definition['taskDefinition']['taskDefinitionArn']

    def create_worker_definition(self, worker_cpu=1024, worker_mem=2048):
        ecs = self.session.client('ecs')
        worker_task_definition = ecs.register_task_definition(
            family='dask-worker',
            taskRoleArn=self.task_role_arn,
            executionRoleArn=self.execution_role_arn,
            networkMode='awsvpc',
            containerDefinitions=[
                {
                    'name': 'dask-worker',
                    'image': "256991223769.dkr.ecr.eu-central-1.amazonaws.com/dask-cluster:latest",
                    'cpu': worker_cpu,
                    'memory': worker_mem,
                    'memoryReservation': worker_mem,
                    'essential': True,
                    'command': ['dask-worker', '--nthreads', '{}'.format(int(worker_cpu / 1024)),
                                '--no-bokeh',
                                '--memory-limit',
                                '{}GB'.format(int(worker_mem / 1024)),
                                '--death-timeout', '60'],
                    'logConfiguration': {
                        'logDriver': 'awslogs',
                        'options': {
                            'awslogs-region': self.region,
                            'awslogs-group': self.log_group,
                            'awslogs-stream-prefix': self.log_prefix,
                            'awslogs-create-group': 'true'
                        }
                    }
                },
            ],
            volumes=[],
            requiresCompatibilities=['FARGATE'],
            cpu=str(worker_cpu),
            memory=str(worker_mem)
        )
        return worker_task_definition['taskDefinition']['taskDefinitionArn']

    def start_scheduler(self, cluster_arn, scheduler_definition_arn, vpc_subnets, security_group):
        ecs = self.session.client('ecs')
        ec2 = self.session.client('ec2')
        [scheduler_task] = ecs.run_task(
            cluster=cluster_arn,
            taskDefinition=scheduler_definition_arn,
            overrides={},
            count=1,
            launchType='FARGATE',
            networkConfiguration={
                'awsvpcConfiguration': {
                    'subnets': vpc_subnets,
                    'securityGroups': security_group,
                    'assignPublicIp': 'ENABLED'
                }
            }
        )['tasks']
        self.scheduler_task_arn = scheduler_task['taskArn']
        # print('Waiting for scheduler', end="")
        i = 0
        while scheduler_task['lastStatus'] in ['PENDING', 'PROVISIONING']:
            [scheduler_task] = ecs.describe_tasks(cluster=cluster_arn, tasks=[
                                                  self.scheduler_task_arn])['tasks']
            x = i % 4
            if x != 0:
                print('\rWait for scheduler' + "." * x, end='')
            else:
                print('\033[2K\033[1G', end='')
                i = 0
            time.sleep(0.5)
            sys.stdout.flush()
            i = i + 1

        print('\033[2K\033[1G', end='')

        if scheduler_task['lastStatus'] != 'RUNNING':
            print("""Failed to start""")
            for container in scheduler_task['containers']:
                print(container['reason'])
            return

        print("Ready!")

        connectivity = (scheduler_task['connectivityAt'] -
                        scheduler_task['createdAt']).total_seconds()
        pullstart = (scheduler_task['pullStartedAt'] -
                     scheduler_task['createdAt']).total_seconds()
        pullstop = (scheduler_task['pullStoppedAt'] -
                    scheduler_task['createdAt']).total_seconds()
        containerstart = (scheduler_task['startedAt'] -
                          scheduler_task['createdAt']).total_seconds()

        print(f"Container placed on Fargate after {connectivity}s")
        print(f"Image started pulling after {pullstart}s")
        print(f"Image finished pulling after {pullstop}s")
        print(f"Container started after {containerstart}s")

        [scheduler_eni] = [attachment for attachment in scheduler_task['attachments']
                           if attachment['type'] == 'ElasticNetworkInterface']
        [scheduler_network_interface_id] = [detail['value']
                                            for detail in scheduler_eni['details'] if detail['name'] == 'networkInterfaceId']
        scheduler_eni = ec2.describe_network_interfaces(
            NetworkInterfaceIds=[scheduler_network_interface_id])
        interface = scheduler_eni['NetworkInterfaces'][0]

        print(f"Public IP: {interface['Association']['PublicIp']}")
        print(f"Private IP: {interface['PrivateIpAddresses'][0]['PrivateIpAddress']}")
        webbrowser.open_new_tab(f"http://{interface['Association']['PublicIp']}:8787")
        return interface['PrivateIpAddresses'][0]['PrivateIpAddress'], interface['Association']['PublicIp']

    def start_worker(self, cluster_arn, worker_definition_arn, vpc_subnets, security_group, num_workers, scheduler_private_ip):
        ecs = self.session.client('ecs')
        task_batches = ([10] * int(num_workers / 10)) + [num_workers % 10]
        for batch in task_batches:
            worker_task = ecs.run_task(
                cluster=cluster_arn,
                taskDefinition=worker_definition_arn,
                overrides={
                    'containerOverrides': [{
                        'name': 'dask-worker',
                        'environment': [
                            {
                                'name': 'DASK_SCHEDULER_ADDRESS',
                                'value': 'tcp://{}:8786'.format(scheduler_private_ip)
                            },
                        ]
                    }]
                },
                count=batch,
                launchType='FARGATE',
                networkConfiguration={
                    'awsvpcConfiguration': {
                        'subnets': vpc_subnets,
                        'securityGroups': security_group,
                        'assignPublicIp': 'ENABLED'
                    }
                }
            )

    def initialize_aws(self):
        cluster_arn = self.create_cluster()
        execution_role_arn = self.create_execution_role()
        task_role_arn = self.create_task_role()
        self.create_log_group()
        self.setup_network_interfaces()
        scheduler_definition_arn = self.create_scheduler_definition()
        worker_definition_arn = self.create_worker_definition()

    def start_cluster(self, cluster_name="PA-Cluster", scheduler_cpu=1024, scheduler_mem=2048, worker_cpu=1024, worker_mem=2048, num_workers=2, logging=False):
        ecs = self.session.client('ecs')
        ec2 = self.session.client('ec2')

        # Check existing cluster
        cluster_arn_list = ecs.list_clusters()['clusterArns']
        for item in cluster_arn_list:
            if cluster_name in item:
                self.cluster_arn = item
                break

        # Create new cluster if not available
        try:
            self.cluster_arn
        except:
            self.cluster_arn = self.create_cluster(cluster_name)

        # Check for existing task definitions
        task_definition_list = ecs.list_task_definitions()['taskDefinitionArns']
        scheduler_definition_arn = ""
        worker_definition_arn = ""
        for item in task_definition_list:
            if 'dask-scheduler' in item and not scheduler_definition_arn:
                task_definition = ecs.describe_task_definition(taskDefinition=item)[
                    'taskDefinition']
                if (task_definition['cpu'] == str(scheduler_cpu)) and (task_definition['memory'] == str(scheduler_mem)):
                    scheduler_definition_arn = item
                    continue
            if 'dask-worker' in item and not worker_definition_arn:
                task_definition = ecs.describe_task_definition(taskDefinition=item)[
                    'taskDefinition']
                if (task_definition['cpu'] == str(worker_cpu)) and (task_definition['memory'] == str(worker_mem)):
                    worker_definition_arn = item

        # Create new scheduler definition if not available
        if not scheduler_definition_arn:
            scheduler_definition_arn = self.create_scheduler_definition(
                scheduler_cpu=scheduler_cpu, scheduler_mem=scheduler_mem)

        # Create new worker definition if not available
        if not worker_definition_arn:
            worker_definition_arn = self.create_worker_definition(
                worker_cpu=worker_cpu, worker_mem=worker_mem)

        # Get subnets
        _, vpc_subnets = self.read_vpc_net()

        # Get security group
        security_group = [ec2.describe_security_groups(GroupNames=['dask'], DryRun=False)[
            'SecurityGroups'][0]['GroupId']]

        # Start scheduler and return IPs
        scheduler_private_ip, scheduler_public_ip = self.start_scheduler(
            self.cluster_arn, scheduler_definition_arn, vpc_subnets, security_group)

        # Start worker
        self.start_worker(self.cluster_arn, worker_definition_arn,
                          vpc_subnets, security_group, num_workers, scheduler_private_ip)

        return f"tcp://{scheduler_public_ip}:8786"

    def stop_cluster(self):
        ecs = self.session.client('ecs')

        try:
            self.cluster_arn
            self.scheduler_task_arn
        except:
            print("No active cluster or scheduler")
            return

        scheduler_task = ecs.stop_task(
            cluster=self.cluster_arn, task=self.scheduler_task_arn)['task']
        del self.cluster_arn, self.scheduler_task_arn
        print(f"Scheduler status: {scheduler_task['desiredStatus']}")
