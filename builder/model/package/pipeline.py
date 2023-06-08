from dataclasses import (
    dataclass,
    field,
)
from os import path
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

from builder.model.config.pipeline import (
    ChoicePipelineConfig,
    EventRulePipelineTriggerConfig,
    GluePipelineConfig,
    LambdaPipelineConfig,
    PipelineConfig,
    S3PipelineTriggerConfig,
)
from builder.model.package.abstract import Package
from builder.model.property.bucket import DatalakeBucketSet
from builder.model.resource.abstract import Resource
from builder.model.resource.event_rule import EventRuleResource
from builder.model.resource.glue_job import GlueJobResource
from builder.model.resource.iam_role import RoleResource
from builder.model.resource.lambda_ import LambdaResource
from builder.model.resource.s3_notification import S3NotificationResource
from builder.model.resource.sns_topic import SnsTopicResource
from builder.model.resource.step_function import StepFunctionResource
from builder.model.resource.vpc import VpcResource


@dataclass
class PipelinePackage(Package):
    region: str
    account_id: str
    bucket_set: DatalakeBucketSet
    sns_topic: SnsTopicResource
    root_path: str
    config: PipelineConfig
    vpc: Optional[VpcResource] = None
    build_deps: bool = True

    resources: List[Resource] = field(default_factory=list)

    def build(self) -> "PipelinePackage":
        state_machine_arn = f"arn:aws:states:{self.region}:{self.account_id}:stateMachine:{self.config.name.value}"
        roles = self.__create_roles(state_machine_arn)
        catch = self.__create_lambda_catch(roles)
        trigger = self.__create_lambda_trigger(state_machine_arn, roles)
        notifications = self.__create_trigger_notifications(trigger)
        sfn_steps, tasks = self.__create_steps(roles)
        sfn = self.__create_step_function(roles, sfn_steps, catch)

        self.resources.extend([role for _, role in roles.items()])
        self.resources.append(catch)
        self.resources.append(trigger)
        self.resources.extend(notifications)
        self.resources.extend(tasks)
        self.resources.append(sfn)

        return self

    def __create_roles(self, state_machine_arn: str) -> Dict[str, RoleResource]:
        roles: Dict[str, RoleResource] = {}

        domain_resources = []
        domain_resources.append(
            f"arn:aws:glue:{self.region}:{self.account_id}:catalog"
        )

        buckets = self.bucket_set.get(domains=[self.config.domain])

        for bucket in buckets:
            domain_resources.append(bucket.arn)
            domain_resources.append(f"{bucket.arn}/*")
            domain_resources.append(bucket.database_arn)
            domain_resources.append(
                f"arn:aws:glue:{self.region}:{self.account_id}:table/{bucket.database.value}/*"
            )

        roles["catch"] = RoleResource.from_pydict(
            name=self.config.name.add_suffix("role-catch"),
            tags=self.config.tags,
            pydict={
                "region": self.region,
                "account_id": self.account_id,
                "assumed_by": "lambda.amazonaws.com",
                "effect": "allow",
                "actions": [
                    "sns:Publish",
                ],
                "resources": [
                    self.sns_topic.arn,
                ],
                "managed_policies": [
                    "service-role/AWSLambdaBasicExecutionRole",
                    "service-role/AWSLambdaVPCAccessExecutionRole",
                ],
            },
        )

        roles["trigger"] = RoleResource.from_pydict(
            name=self.config.name.add_suffix("role-trigger"),
            tags=self.config.tags,
            pydict={
                "region": self.region,
                "account_id": self.account_id,
                "assumed_by": "lambda.amazonaws.com",
                "effect": "allow",
                "actions": [
                    "states:StartExecution",
                ],
                "resources": [state_machine_arn],
                "managed_policies": [
                    "service-role/AWSLambdaBasicExecutionRole",
                    "service-role/AWSLambdaVPCAccessExecutionRole",
                ],
            },
        )

        roles["lambda"] = RoleResource.from_pydict(
            name=self.config.name.add_suffix("role-lambda"),
            tags=self.config.tags,
            pydict={
                "region": self.region,
                "account_id": self.account_id,
                "assumed_by": "lambda.amazonaws.com",
                "effect": "allow",
                "actions": [
                    "s3:*",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:GetTable",
                    "glue:GetPartition",
                    "glue:CreatePartition",
                    "glue:UpdatePartition",
                ],
                "resources": domain_resources,
                "managed_policies": [
                    "service-role/AWSLambdaBasicExecutionRole",
                    "service-role/AWSLambdaVPCAccessExecutionRole",
                ],
            },
        )

        roles["glue"] = RoleResource.from_pydict(
            name=self.config.name.add_suffix("role-glue"),
            tags=self.config.tags,
            pydict={
                "region": self.region,
                "account_id": self.account_id,
                "assumed_by": "glue.amazonaws.com",
                "effect": "allow",
                "actions": [
                    "glue:*",
                    "s3:*",
                    "ec2:DescribeVpcEndpoints",
                    "ec2:DescribeRouteTables",
                    "ec2:CreateNetworkInterface",
                    "ec2:DeleteNetworkInterface",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:DescribeSecurityGroups",
                    "ec2:DescribeSubnets",
                    "ec2:DescribeVpcAttribute",
                    "iam:ListRolePolicies",
                    "iam:GetRole",
                    "iam:GetRolePolicy",
                    "cloudwatch:PutMetricData",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                "resources": domain_resources
                + [
                    "arn:aws:ec2:*",
                    "arn:aws:iam:*",
                    "arn:aws:logs:*",
                    "arn:aws:cloudwatch:*",
                ],
            },
        )

        roles["sfn"] = RoleResource.from_pydict(
            name=self.config.name.add_suffix("role-sfn"),
            tags=self.config.tags,
            pydict={
                "region": self.region,
                "account_id": self.account_id,
                "assumed_by": "states.amazonaws.com",
                "effect": "allow",
                "actions": [
                    "lambda:InvokeFunction",
                    "lambda:InvokeAsync",
                    "glue:StartJobRun",
                    "glue:GetJobRun",
                    "glue:GetJobRuns",
                    "glue:GetDatabase",
                    "glue:GetTables",
                    "glue:GetTable",
                    "glue:GetPartitions",
                    "glue:GetPartition",
                    "glue:GetConnection",
                    "glue:GetConnections",
                ],
            },
        )

        return roles

    def __create_lambda_catch(
        self, roles: Dict[str, RoleResource]
    ) -> LambdaResource:
        return LambdaResource.from_pydict(
            name=self.config.name.add_suffix("catch"),
            tags=self.config.tags,
            pydict={
                "region": self.region,
                "account_id": self.account_id,
                "role": roles["catch"],
                "timeout": 60,
                "memory_size": 128,
                "root": self.root_path,
                "source_folder": path.join(self.root_path, "catch"),
                "environment": {
                    "PIPELINE_NAME": self.config.name.value,
                    "SNS_TOPIC_ARN": self.sns_topic.arn,
                },
                "vpc": self.vpc,
                "vpc_subnet": "private",
                "build_deps": self.build_deps,
            },
        )

    def __create_lambda_trigger(
        self, state_machine_arn: str, roles: Dict[str, RoleResource]
    ) -> LambdaResource:
        return LambdaResource.from_pydict(
            name=self.config.name.add_suffix("trigger"),
            tags=self.config.tags,
            pydict={
                "region": self.region,
                "account_id": self.account_id,
                "role": roles["trigger"],
                "timeout": 60,
                "memory_size": 128,
                "root": self.root_path,
                "source_folder": path.join(self.root_path, "trigger"),
                "environment": {
                    "STATE_MACHINE_ARN": state_machine_arn,
                    "TRIGGER_LAYER": self.config.layers.origin.value,
                    "TARGET_LAYER": self.config.layers.target.value,
                },
                "vpc": self.vpc,
                "vpc_subnet": "private",
                "build_deps": self.build_deps,
            },
        )

    def __create_trigger_notifications(
        self, lambda_: LambdaResource
    ) -> List[Union[S3NotificationResource, EventRuleResource]]:
        trigger_bucket = self.bucket_set.get(
            domains=[self.config.domain],
            layers=[self.config.layers.origin],
        )[0].name

        notification: Union[S3NotificationResource, EventRuleResource]
        notifications: List[
            Union[S3NotificationResource, EventRuleResource]
        ] = []
        for i, trigger in enumerate(self.config.triggers):
            if isinstance(trigger, S3PipelineTriggerConfig):
                notification = S3NotificationResource.from_pydict(
                    name=self.config.name.add_suffix(f"event-{i}"),
                    tags=self.config.tags,
                    pydict={
                        "bucket": trigger_bucket,
                        "lambda": lambda_.name,
                        "event_type": "OBJECT_CREATED",
                        "prefix": trigger.prefix,
                        "suffix": trigger.suffix,
                    },
                )

            elif isinstance(trigger, EventRulePipelineTriggerConfig):
                notification = EventRuleResource.from_pydict(
                    name=self.config.name.add_suffix(f"event-{i}"),
                    tags=self.config.tags,
                    pydict={
                        "targets": [lambda_],
                        "event_pattern": trigger.to_dict(),
                    },
                )

            else:
                raise ValueError(f"Unknown trigger type: {type(trigger)}")

            notifications.append(notification)

        return notifications

    def __create_steps(
        self, roles: Dict[str, RoleResource]
    ) -> Tuple[List[Dict[str, Any]], List[Resource]]:
        pipeline_args = [arg for arg, _ in self.config.contract.items()]

        temp_uri = (
            self.bucket_set.get(
                domains=[self.config.domain],
                layers=[self.config.layers.target],
            )[0].uri
            + "/temp"
        )

        sfn_steps: List[Dict[str, Any]] = []
        tasks: List[Resource] = []
        task: Optional[Resource]

        for step in self.config.steps:
            if isinstance(step, LambdaPipelineConfig):
                pydict = {
                    "region": self.region,
                    "account_id": self.account_id,
                    "role": roles["lambda"],
                    "root": self.root_path,
                    "source_folder": path.join(
                        self.root_path, "steps", step.module
                    ),
                    "vpc": self.vpc,
                    "vpc_subnet": "private",
                    "build_deps": self.build_deps,
                }
                if step.properties:
                    pydict.update(step.properties)

                task = LambdaResource.from_pydict(
                    name=self.config.name.add_suffix(step.step_name),
                    tags=self.config.tags,
                    pydict=pydict,
                )

                sfn_step = {
                    "type": "lambda",
                    "step_name": step.step_name,
                    "resource": task,
                    "next_step": step.next_step,
                }

            elif isinstance(step, GluePipelineConfig):
                pydict = {
                    "role": roles["glue"],
                    "temp_uri": temp_uri,
                    "root": self.root_path,
                    "source_folder": path.join(
                        self.root_path, "steps", step.module
                    ),
                    "default_args": {arg: "" for arg in pipeline_args},
                    "build_deps": self.build_deps,
                }
                if step.properties:
                    pydict.update(step.properties)

                task = GlueJobResource.from_pydict(
                    name=self.config.name.add_suffix(step.step_name),
                    tags=self.config.tags,
                    pydict=pydict,
                )

                sfn_step = {
                    "type": "glue",
                    "step_name": step.step_name,
                    "resource": task,
                    "next_step": step.next_step,
                    "args": pipeline_args,
                }

            elif isinstance(step, ChoicePipelineConfig):
                task = None
                sfn_step = {
                    "type": "choice",
                    "step_name": step.step_name,
                    "choices": [vars(choice) for choice in step.choices],
                }

            sfn_steps.append(sfn_step)
            if task:
                tasks.append(task)

        return sfn_steps, tasks

    def __create_step_function(
        self,
        roles: Dict[str, RoleResource],
        steps: List[Dict[str, Any]],
        catch: LambdaResource,
    ) -> StepFunctionResource:
        return StepFunctionResource.from_pydict(
            name=self.config.name,
            tags=self.config.tags,
            pydict={
                "role": roles["sfn"],
                "steps": steps,
                "catch_lambda": catch,
            },
        )
