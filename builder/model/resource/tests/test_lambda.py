import unittest
from os import path

from aws_cdk import (
    App,
    Duration,
    Stack,
)
from aws_cdk.assertions import Template

from builder.model.property.environment import Environment
from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.iam_role import RoleResource
from builder.model.resource.lambda_ import LambdaResource
from builder.model.resource.vpc import VpcResource
from builder.utils.stack_cache import StackCache


class TestLambdaResource(unittest.TestCase):
    def setUp(self) -> None:
        self.cache = StackCache()

        self.root = path.join(path.dirname(path.abspath(__file__)), "mock")
        self.source_folder = path.join(self.root, "code")

        self.name = Name("test-lambda", Environment.TEST)
        self.region = "us-east-1"
        self.account_id = "1234567890"
        self.tags = Tags()
        self.tags.add("tag1", "value1")

        self.timeout = Duration.seconds(30)
        self.memory_size = 256
        self.role = RoleResource.from_pydict(
            name=self.name,
            tags=self.tags,
            pydict={
                "region": self.region,
                "account_id": self.account_id,
                "assumed_by": "lambda.amazonaws.com",
                "effect": "allow",
                "actions": ["s3:*"],
                "resources": ["*"],
            },
        )
        self.vpc = VpcResource.from_pydict(
            name=self.name, tags=self.tags, pydict={"cidr": "192.168.228.0/22"}
        )
        self.vpc_subnets = "public"
        self.environment = {"VAR1": "value1"}

    def test_init(self) -> None:
        func = LambdaResource(
            name=self.name,
            tags=self.tags,
            region=self.region,
            account_id=self.account_id,
            role=self.role,
            root=self.root,
            source_folder=self.source_folder,
            timeout=self.timeout,
            memory_size=self.memory_size,
            environment=self.environment,
            vpc=self.vpc,
            vpc_subnets=self.vpc_subnets,
            build_deps=False,
        )

        self.assertEqual(func.name, self.name)
        self.assertEqual(func.tags, self.tags)
        self.assertEqual(func.region, self.region)
        self.assertEqual(func.account_id, self.account_id)
        self.assertEqual(func.role, self.role)
        self.assertEqual(func.timeout, self.timeout)
        self.assertEqual(func.memory_size, self.memory_size)
        self.assertEqual(func.environment, self.environment)
        self.assertEqual(func.vpc, self.vpc)
        self.assertEqual(func.vpc_subnets, self.vpc_subnets)

    def test_from_pydict(self) -> None:
        pydict = {
            "region": self.region,
            "account_id": self.account_id,
            "role": self.role,
            "timeout": 120,
            "memory_size": 512,
            "root": self.root,
            "source_folder": self.source_folder,
            "environment": {"VAR1": "value1"},
            "vpc": self.vpc,
            "vpc_subnet": "public",
            "build_deps": False,
        }
        func = LambdaResource.from_pydict(
            name=self.name, tags=self.tags, pydict=pydict
        )

        self.assertEqual(func.name, self.name)
        self.assertEqual(func.tags, self.tags)
        self.assertEqual(func.region, self.region)
        self.assertEqual(func.account_id, self.account_id)
        self.assertEqual(func.role, self.role)
        self.assertEqual(func.timeout.to_minutes(), 2)
        self.assertEqual(func.memory_size, 512)
        self.assertEqual(func.environment, self.environment)
        self.assertEqual(func.vpc, self.vpc)

    def test_add_to_cdk(self) -> None:
        app = App()
        stack = Stack(app, "test-stack")

        func = LambdaResource(
            name=self.name,
            tags=self.tags,
            region=self.region,
            account_id=self.account_id,
            role=self.role,
            root=self.root,
            source_folder=self.source_folder,
            timeout=self.timeout,
            memory_size=self.memory_size,
            environment=self.environment,
            build_deps=False,
        )

        func.add_to_cdk(scope=stack, cache=self.cache)

        template = Template.from_stack(stack)

        template.resource_count_is("AWS::Lambda::Function", 1)
        template.has_resource_properties(
            "AWS::Lambda::Function",
            {
                "FunctionName": self.name.value,
                "PackageType": "Image",
            },
        )
