import unittest
from os import path
from typing import List

import yaml
from aws_cdk import (
    App,
    Stack,
)
from aws_cdk.assertions import Template

from builder.model.config.pipeline import PipelineConfig
from builder.model.package.pipeline import PipelinePackage
from builder.model.property.bucket import DatalakeBucketSet
from builder.model.property.environment import Environment
from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.s3_bucket import S3BucketResource
from builder.model.resource.sns_topic import SnsTopicResource
from builder.model.resource.vpc import VpcResource
from builder.utils.stack_cache import StackCache


class TestPipelinePackage(unittest.TestCase):
    def setUp(self) -> None:
        self.region = "us-east-1"
        self.account_id = "1234567890"
        self.env = Environment.TEST

        self.bucket_set = DatalakeBucketSet(
            region=self.region,
            account_id=self.account_id,
            domains=["test-domain"],
            env=self.env,
        )

        self.buckets_resource: List[S3BucketResource] = []
        for bucket in self.bucket_set.buckets:
            self.buckets_resource.append(
                S3BucketResource.from_pydict(
                    name=bucket.name, tags=Tags(), pydict={}
                )
            )

        self.sns_topic = SnsTopicResource.from_pydict(
            name=Name("test-topic", self.env),
            tags=Tags(),
            pydict={
                "region": self.region,
                "account_id": self.account_id,
                "display_name": "test-topic",
                "subscriptions": [
                    {"protocol": "email", "endpoint": "test@test.com"}
                ],
            },
        )

        self.root_path = path.join(path.dirname(path.abspath(__file__)), "mock")

        with open(path.join(self.root_path, "config.yml")) as f:
            config_dict = yaml.safe_load(f)

        self.config = PipelineConfig.from_pydict(self.env, config_dict)

        self.vpc = VpcResource.from_pydict(
            name=Name("test-vpc", self.env),
            tags=Tags(),
            pydict={
                "cidr": "192.168.228.0/22",
            },
        )

        self.pipeline1 = PipelinePackage(
            region=self.region,
            account_id=self.account_id,
            bucket_set=self.bucket_set,
            sns_topic=self.sns_topic,
            root_path=self.root_path,
            config=self.config,
            vpc=self.vpc,
            build_deps=False,
        )

        self.pipeline2 = PipelinePackage(
            region=self.region,
            account_id=self.account_id,
            bucket_set=self.bucket_set,
            sns_topic=self.sns_topic,
            root_path=self.root_path,
            config=self.config,
            build_deps=False,
        )

    def test_build(self) -> None:
        self.pipeline1.build()
        resources = self.pipeline1.resources

        self.assertEqual(len(resources), 14)

    def test_add_to_cdk_1(self) -> None:
        app = App()
        stack = Stack(app, "test-stack")
        cache = StackCache()

        self.pipeline1.build()

        self.vpc.add_to_cdk(stack, cache)
        for bucket in self.buckets_resource:
            bucket.add_to_cdk(stack, cache)

        for resource in self.pipeline1.resources:
            resource.add_to_cdk(stack, cache)

        template = Template.from_stack(stack)

        template.resource_count_is("AWS::EC2::VPC", 1)
        template.resource_count_is("AWS::EC2::Subnet", 4)
        template.resource_count_is("AWS::EC2::RouteTable", 4)
        template.resource_count_is("AWS::EC2::SubnetRouteTableAssociation", 4)
        template.resource_count_is("AWS::EC2::Route", 2)
        template.resource_count_is("AWS::EC2::InternetGateway", 1)
        template.resource_count_is("AWS::EC2::VPCGatewayAttachment", 1)
        template.resource_count_is("AWS::S3::Bucket", 3)
        template.resource_count_is("Custom::S3BucketNotifications", 1)
        template.resource_count_is("AWS::Lambda::Permission", 2)
        template.resource_count_is("AWS::IAM::Role", 6)
        template.resource_count_is("AWS::IAM::Policy", 8)
        template.resource_count_is("AWS::EC2::SecurityGroup", 4)
        template.resource_count_is("AWS::Lambda::Function", 5)
        template.resource_count_is("AWS::Glue::Job", 2)
        template.resource_count_is("AWS::Events::Rule", 1)
        template.resource_count_is("AWS::StepFunctions::StateMachine", 1)

        resources = template.to_json()["Resources"]
        self.assertEqual(len(resources), 50)

    def test_add_to_cdk_2(self) -> None:
        app = App()
        stack = Stack(app, "test-stack")
        cache = StackCache()

        self.pipeline2.build()

        for bucket in self.buckets_resource:
            bucket.add_to_cdk(stack, cache)

        for resource in self.pipeline2.resources:
            resource.add_to_cdk(stack, cache)

        template = Template.from_stack(stack)

        template.resource_count_is("AWS::S3::Bucket", 3)
        template.resource_count_is("Custom::S3BucketNotifications", 1)
        template.resource_count_is("AWS::Lambda::Permission", 2)
        template.resource_count_is("AWS::IAM::Role", 6)
        template.resource_count_is("AWS::IAM::Policy", 8)
        template.resource_count_is("AWS::Lambda::Function", 5)
        template.resource_count_is("AWS::Glue::Job", 2)
        template.resource_count_is("AWS::Events::Rule", 1)
        template.resource_count_is("AWS::StepFunctions::StateMachine", 1)

        resources = template.to_json()["Resources"]
        self.assertEqual(len(resources), 29)
