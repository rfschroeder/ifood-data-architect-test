class EmrSettings:
    job_flow_role = "EMR_EC2_DefaultRole"
    service_role = "EMR_DefaultRole"
    auto_scaling_role = "EMR_AutoScaling_DefaultRole"
    dft_block_size = "134217728"  # 128 MiB
    spark_serializer = "org.apache.spark.serializer.KryoSerializer"
    driver_memory_overhead = "1024"
    executor_memory_overhead = "1024"
    s3_object_acl_default = "public-read"

    def __init__(
        self,
        aws_account_id,
        aws_region,
        ec2_key_pair,
        ec2_subnet_id,
        cluster_name,
        master_instance_type,
        master_instance_count,
        core_instance_type,
        core_instance_count,
        core_instance_market,
        task_instance_type,
        task_instance_count,
        task_instance_market,
        step_concurrency_level
    ):
        self.aws_account_id = aws_account_id
        self.aws_region = aws_region
        self.ec2_key_pair = ec2_key_pair
        self.ec2_subnet_id = ec2_subnet_id
        self.cluster_name = cluster_name
        self.master_instance_type = master_instance_type
        self.master_instance_count = master_instance_count
        self.core_instance_type = core_instance_type
        self.core_instance_count = core_instance_count
        self.core_instance_market = core_instance_market
        self.task_instance_type = task_instance_type
        self.task_instance_count = task_instance_count
        self.task_instance_market = task_instance_market
        self.step_concurrency_level = step_concurrency_level

    def crete_job_flow_overrides(self):
        return {
            "Name": f"{self.cluster_name}",
            "LogUri": f"s3://aws-logs-{self.aws_account_id}-{self.aws_region}/elasticmapreduce/",
            "ReleaseLabel": "emr-5.30.1",
            "Applications": [{"Name": "Spark"}, {"Name": "Hive"}],
            "Configurations": self.get_cluster_configurations(),
            "Instances": {
                "InstanceGroups": self.get_instance_groups(),
                "Ec2KeyName": self.ec2_key_pair,
                "KeepJobFlowAliveWhenNoSteps": True,
                "TerminationProtected": False,
                "Ec2SubnetId": self.ec2_subnet_id,
            },
            "Steps": [],
            "BootstrapActions": [],
            "VisibleToAllUsers": True,
            "JobFlowRole": self.job_flow_role,
            "ServiceRole": self.service_role,
            "AutoScalingRole": self.auto_scaling_role,
            "Tags": [],
            "StepConcurrencyLevel": self.step_concurrency_level,
            "ManagedScalingPolicy": {},
        }

    @staticmethod
    def _get_scale_out_yarn_memory_available():
        return {
            "Name": "scale-out-yarn-memory-available",
            "Description": "Scale Out Yarn Memory Available",
            "Action": {
                "SimpleScalingPolicyConfiguration": {
                    "AdjustmentType": "CHANGE_IN_CAPACITY",
                    "ScalingAdjustment": 1,
                    "CoolDown": 300,
                }
            },
            "Trigger": {
                "CloudWatchAlarmDefinition": {
                    "ComparisonOperator": "LESS_THAN_OR_EQUAL",
                    "EvaluationPeriods": 1,
                    "MetricName": "YARNMemoryAvailablePercentage",
                    "Period": 300,
                    "Statistic": "AVERAGE",
                    "Threshold": 15,
                    "Unit": "PERCENT",
                    "Dimensions": [{"Key": "JobFlowID", "Value": "${emr.clusterId}"}],
                }
            },
        }

    @staticmethod
    def _get_scale_out_container_pending_ratio():
        return {
            "Name": "scale-out-container-pending-ratio",
            "Description": "Scale Out Container Pending Ratio",
            "Action": {
                "SimpleScalingPolicyConfiguration": {
                    "AdjustmentType": "CHANGE_IN_CAPACITY",
                    "ScalingAdjustment": 1,
                    "CoolDown": 300,
                }
            },
            "Trigger": {
                "CloudWatchAlarmDefinition": {
                    "ComparisonOperator": "GREATER_THAN_OR_EQUAL",
                    "EvaluationPeriods": 1,
                    "MetricName": "ContainerPendingRatio",
                    "Period": 300,
                    "Statistic": "AVERAGE",
                    "Threshold": 0.75,
                    "Unit": "COUNT",
                    "Dimensions": [{"Key": "JobFlowID", "Value": "${emr.clusterId}"}],
                }
            },
        }

    @staticmethod
    def _get_scale_out_hdfs_utilization():
        return {
            "Name": "scale-out-hdfs-utilization",
            "Description": "Scale Out Memory Available",
            "Action": {
                "SimpleScalingPolicyConfiguration": {
                    "AdjustmentType": "CHANGE_IN_CAPACITY",
                    "ScalingAdjustment": 1,
                    "CoolDown": 300,
                }
            },
            "Trigger": {
                "CloudWatchAlarmDefinition": {
                    "ComparisonOperator": "GREATER_THAN_OR_EQUAL",
                    "EvaluationPeriods": 1,
                    "MetricName": "HDFSUtilization",
                    "Period": 300,
                    "Statistic": "AVERAGE",
                    "Threshold": 80,
                    "Unit": "PERCENT",
                    "Dimensions": [{"Key": "JobFlowID", "Value": "${emr.clusterId}"}],
                }
            },
        }

    @staticmethod
    def _get_scale_out_memory_available():
        return {
            "Name": "scale-out-memory-available",
            "Description": "Scale Out Memory Available",
            "Action": {
                "SimpleScalingPolicyConfiguration": {
                    "AdjustmentType": "CHANGE_IN_CAPACITY",
                    "ScalingAdjustment": 1,
                    "CoolDown": 300,
                }
            },
            "Trigger": {
                "CloudWatchAlarmDefinition": {
                    "ComparisonOperator": "LESS_THAN_OR_EQUAL",
                    "EvaluationPeriods": 1,
                    "MetricName": "MemoryAvailableMB",
                    "Period": 300,
                    "Statistic": "AVERAGE",
                    "Threshold": 10000,
                    "Unit": "COUNT",
                    "Dimensions": [{"Key": "JobFlowID", "Value": "${emr.clusterId}"}],
                }
            },
        }

    @staticmethod
    def _get_scale_in_yarn_memory_available():
        return {
            "Name": "scale-in-yarn-memory-available",
            "Description": "Scale In Yarn Memory Available",
            "Action": {
                "SimpleScalingPolicyConfiguration": {
                    "AdjustmentType": "CHANGE_IN_CAPACITY",
                    "ScalingAdjustment": -1,
                    "CoolDown": 300,
                }
            },
            "Trigger": {
                "CloudWatchAlarmDefinition": {
                    "ComparisonOperator": "GREATER_THAN_OR_EQUAL",
                    "EvaluationPeriods": 1,
                    "MetricName": "YARNMemoryAvailablePercentage",
                    "Period": 300,
                    "Statistic": "AVERAGE",
                    "Threshold": 75,
                    "Unit": "PERCENT",
                    "Dimensions": [{"Key": "JobFlowID", "Value": "${emr.clusterId}"}],
                }
            },
        }

    def _get_core_scaling_rules(self):
        return [
            self._get_scale_out_yarn_memory_available(),
            self._get_scale_out_container_pending_ratio(),
            self._get_scale_out_hdfs_utilization(),
            self._get_scale_out_memory_available(),
            self._get_scale_in_yarn_memory_available(),
        ]

    def _get_task_scaling_rules(self):
        return [
            self._get_scale_out_yarn_memory_available(),
            self._get_scale_out_container_pending_ratio(),
            self._get_scale_out_memory_available(),
            self._get_scale_in_yarn_memory_available(),
        ]

    def get_cluster_configurations(self):
        cluster_configurations = [
            {
                "Classification": "hdfs-site",
                "Properties": {
                    "dfs.block.size": self.dft_block_size,
                    "parquet.page.size": self.dft_block_size,
                    "parquet.block.size": self.dft_block_size,
                    "fs.s3a.block.size": self.dft_block_size,
                    "spark.hadoop.fs.s3a.canned.acl": self.s3_object_acl_default,
                    "fs.s3a.acl.default": self.s3_object_acl_default,
                },
            },
            {
                "Classification": "mapred-site",
                "Properties": {
                    "mapreduce.fileoutputcommitter.marksuccessfuljobs": "false",
                    "mapreduce.fileoutputcommitter.algorithm.version": "2",
                },
            },
            {
                "Classification": "yarn-site",
                "Properties": {"yarn.nodemanager.vmem-check-enabled": "false"},
            },
            {
                "Classification": "core-site",
                "Properties": {
                    "fs.s3n.multipart.uploads.enabled": "true",
                    "fs.s3n.multipart.uploads.split.size": self.dft_block_size,
                },
            },
            {
                "Classification": "spark",
                "Properties": {
                    "spark.serializer": self.spark_serializer,
                    "spark.yarn.driver.memoryOverhead": self.driver_memory_overhead,
                    "spark.yarn.executor.memoryOverhead": self.executor_memory_overhead,
                },
            },
            {
                "Classification": "hive-site",
                "Properties": {
                    "hive.metastore.client.factory.class": (
                        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                    )
                },
            },
            {
                "Classification": "spark-hive-site",
                "Properties": {
                    "hive.metastore.client.factory.class": (
                        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                    )
                },
            },
        ]
        return cluster_configurations

    def get_instance_groups(self):
        instance_groups = list()
        instance_groups.append(
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": self.master_instance_type,
                "InstanceCount": self.master_instance_count,
            }
        )
        instance_groups.append(
            {
                "Name": "Slave Core Nodes",
                "Market": self.core_instance_market,
                "InstanceRole": "CORE",
                "InstanceType": self.core_instance_type,
                "InstanceCount": self.core_instance_count,
                "AutoScalingPolicy": {
                    "Constraints": {"MinCapacity": 1, "MaxCapacity": 10},
                    "Rules": self._get_core_scaling_rules(),
                },
            }
        )
        instance_groups.append(
            {
                "Name": "Slave Task Nodes",
                "Market": self.task_instance_market,
                "InstanceRole": "TASK",
                "InstanceType": self.task_instance_type,
                "InstanceCount": self.task_instance_count,
                "AutoScalingPolicy": {
                    "Constraints": {"MinCapacity": 0, "MaxCapacity": 10},
                    "Rules": self._get_task_scaling_rules(),
                },
            }
        )
        return instance_groups
