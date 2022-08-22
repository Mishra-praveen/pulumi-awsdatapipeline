"""An AWS Python Pulumi program"""

from contextlib import AsyncExitStack
from dataclasses import Field, field
from distutils.command.config import config
import json
from multiprocessing.sharedctypes import Value
from unicodedata import name
import pulumi
import pulumi_aws as aws
config = pulumi.Config()

##Create IAM resources required for pipeline
pipeline_role = aws.iam.Role(
    resource_name="DataPipelineRole",
    assume_role_policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": "sts:AssumeRole",
                    "Effect": "Allow",
                    "Sid": "",
                    "Principal": {
                        "Service": [
                            "datapipeline.amazonaws.com",
                            "elasticmapreduce.amazonaws.com"
                        ]
                    },



                }
            ]
        }
    )
)

role_policy_attach = aws.iam.RolePolicyAttachment(
    resource_name="role_policy_attach",
    role=pipeline_role,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSDataPipelineRole"
)

pipeline_resource_role = aws.iam.Role(
    resource_name="DataPipelineResourceRole",
    assume_role_policy=json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": "sts:AssumeRole",
                    "Effect": "Allow",
                    "Sid": "",
                    "Principal": {
                        "Service": [
                            "ec2.amazonaws.com",
                        ]
                    },
                }
            ]
        }
    )
)

resource_role_policy_attach = aws.iam.RolePolicyAttachment(
    resource_name="resource_role_policy_attach",
    role=pipeline_resource_role,
    policy_arn="arn:aws:iam::aws:policy/service-role/AmazonEC2RoleforDataPipelineRole"
)

pipeline_instance_profile = aws.iam.InstanceProfile(
    resource_name="instance_profile",
    role=pipeline_resource_role
)
pipeline = aws.datapipeline.Pipeline(
    resource_name="s3_to_dynamodb",
    name="s3_to_dynamodb",

)

pipeline_definition = aws.datapipeline.PipelineDefinition(
    resource_name="pipeline_definition",
    pipeline_id=pipeline.id,
    pipeline_objects=[
        aws.datapipeline.PipelineDefinitionPipelineObjectArgs(
            id="Default",
            name="Default",
            fields=[
                aws.datapipeline.PipelineDefinitionPipelineObjectFieldArgs(
                    key="role",
                    string_value=pipeline_role
                ),
                aws.datapipeline.PipelineDefinitionPipelineObjectFieldArgs(
                    key="scheduleType",
                    string_value="ONDEMAND"
                ),
                aws.datapipeline.PipelineDefinitionPipelineObjectFieldArgs(
                    key="pipelineLogUri",
                    string_value="s3://datapipeline-sample-bucket-dev"
                )
            ]
        ),
        aws.datapipeline.PipelineDefinitionPipelineObjectArgs(
            id="TableLoadActivity",
            name="TableLoadActivity",
            fields=[
                aws.datapipeline.PipelineDefinitionPipelineObjectFieldArgs(
                    key="type",
                    string_value="EmrActivity"
                ),
                aws.datapipeline.PipelineDefinitionPipelineObjectFieldArgs(
                    key="resizeClusterBeforeRunning",
                    string_value="true"
                ),
                aws.datapipeline.PipelineDefinitionPipelineObjectFieldArgs(
                    key="input",
                    ref_value="S3InputDataNode"
                ),
                aws.datapipeline.PipelineDefinitionPipelineObjectFieldArgs(
                    key="output",
                    ref_value="DDBDestinationTable"
                ),
                aws.datapipeline.PipelineDefinitionPipelineObjectFieldArgs(
                    key="step",
                    string_value="s3://dynamodb-dpl-#{myDDBRegion}/emr-ddb-storage-handler/4.11.0/emr-dynamodb-tools-4.11.0-SNAPSHOT-jar-with-dependencies.jar,org.apache.hadoop.dynamodb.tools.DynamoDBImport,#{input.directoryPath},#{output.tableName},#{output.writeThroughputPercent}"
                ),
                aws.datapipeline.PipelineDefinitionPipelineObjectFieldArgs(
                    key="runsOn",
                    ref_value="EmrClusterForLoad"
                )
        
            ]
        ),
        aws.datapipeline.PipelineDefinitionPipelineObjectArgs(
            id="EmrClusterForLoad",
            name="EmrClusterForLoad",
            fields=[
                aws.datapipeline.PipelineDefinitionPipelineObjectFieldArgs(
                    key="type",
                    string_value="EmrCluster"
                ),
                aws.datapipeline.PipelineDefinitionPipelineObjectFieldArgs(
                    key="releaseLabel",
                    string_value="emr-5.23.0"
                ),
                aws.datapipeline.PipelineDefinitionPipelineObjectFieldArgs(
                    key="coreInstanceCount",
                    string_value="1"
                ),
                aws.datapipeline.PipelineDefinitionPipelineObjectFieldArgs(
                    key="coreInstanceType",
                    string_value="m3.xlarge"

                ),
                aws.datapipeline.PipelineDefinitionPipelineObjectFieldArgs(
                    key="masterInstanceType",
                    string_value="m3.xlarge"

                ),
                aws.datapipeline.PipelineDefinitionPipelineObjectFieldArgs(
                    key="region",
                    string_value="#{myDDBRegion}"
                ),
                aws.datapipeline.PipelineDefinitionPipelineObjectFieldArgs(
                    key="resourceRole",
                    string_value=pipeline_instance_profile
                )
            ]

        ),
        aws.datapipeline.PipelineDefinitionPipelineObjectArgs(
            id="DDBDestinationTable",
            name="DDBDestinationTable",
            fields=[
                aws.datapipeline.PipelineDefinitionPipelineObjectFieldArgs(
                    key="type",
                    string_value="DynamoDBDataNode"
                ),
                aws.datapipeline.PipelineDefinitionPipelineObjectFieldArgs(
                    key="tableName",
                    string_value="#{myDDBTableName}"
                ),
                aws.datapipeline.PipelineDefinitionPipelineObjectFieldArgs(
                    key="writeThroughputPercent",
                    string_value="#{myDDBWriteThroughputRatio}"
                )
            ]
        ),
        aws.datapipeline.PipelineDefinitionPipelineObjectArgs(
            id="S3InputDataNode",
            name="S3InputDataNode",
            fields=[
                aws.datapipeline.PipelineDefinitionPipelineObjectFieldArgs(
                    key="type",
                    string_value="S3DataNode"
                ),
                aws.datapipeline.PipelineDefinitionPipelineObjectFieldArgs(
                    key="directoryPath",
                    string_value="#{myInputS3Loc}"
                )
            ]
        ),          
        
    ],
    parameter_objects=[aws.datapipeline.PipelineDefinitionParameterObjectArgs(
        id="myInputS3Loc",
        attributes=[aws.datapipeline.PipelineDefinitionParameterObjectAttributeArgs(
            key="type",
            string_value="AWS::S3::ObjectKey"

        )]
    ),
    aws.datapipeline.PipelineDefinitionParameterObjectArgs(
        id="myDDBTableName",
        attributes=[aws.datapipeline.PipelineDefinitionParameterObjectAttributeArgs(
            key="type",
            string_value="String"

        ),
        aws.datapipeline.PipelineDefinitionParameterObjectAttributeArgs(
        key="description",
        string_value="Target DynamoDB table name"
        )
       ],
     )
   ],
   parameter_values=[
    aws.datapipeline.PipelineDefinitionParameterValueArgs(
        id="myInputS3Loc",
        string_value=config.require("source_s3_bucket")
    ),
    aws.datapipeline.PipelineDefinitionParameterValueArgs(
        id="myDDBTableName",
        string_value=config.require("destination_table_name")
    ),
    aws.datapipeline.PipelineDefinitionParameterValueArgs(
        id="myDDBRegion",
        string_value=aws.config.region
    ),
    aws.datapipeline.PipelineDefinitionParameterValueArgs(
        id="myDDBWriteThroughputRatio",
        string_value="0.25"
    )
   ]
)