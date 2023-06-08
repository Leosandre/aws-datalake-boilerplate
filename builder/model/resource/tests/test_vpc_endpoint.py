import unittest
from os import path

from aws_cdk import (
    App,
    Stack,
)
from aws_cdk.assertions import Template

from builder.model.property.environment import Environment
from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.vpc import VpcResource
from builder.model.resource.vpc_endpoint import VpcEndpointResource
from builder.utils.stack_cache import StackCache


class TestVpcEndpointResource(unittest.TestCase):
    def setUp(self) -> None:
        self.cache = StackCache()
        self.name = Name("test-vpc", Environment.TEST)
        self.vpc_name = Name("test-vpc", Environment.TEST)
        self.vpc_tags = Tags()
        self.vpc_tags.add("tag1", "value1")

        self.vpc = VpcResource.from_pydict(
            name=self.vpc_name,
            tags=self.vpc_tags,
            pydict={
                "cidr": "192.168.228.0/22",
            },
        )

        self.service = "glue"

    def test_init(self) -> None:
        vpc_endpoint = VpcEndpointResource(
            name=self.name,
            tags=self.vpc_tags,
            vpc=self.vpc,
            service=self.service,
        )

        self.assertEqual(vpc_endpoint.vpc, self.vpc)
        self.assertEqual(vpc_endpoint.service, self.service)

    def test_from_pydict(self) -> None:
        vpc_endpoint = VpcEndpointResource.from_pydict(
            name=self.name,
            tags=self.vpc_tags,
            pydict={
                "vpc": self.vpc,
                "service": self.service,
            },
        )

        self.assertEqual(vpc_endpoint.vpc, self.vpc)
        self.assertEqual(vpc_endpoint.service, self.service)

    def test_add_to_cdk(self) -> None:
        app = App()
        stack = Stack(app, "test-stack")

        vpc_endpoint = VpcEndpointResource(
            name=self.name,
            tags=self.vpc_tags,
            vpc=self.vpc,
            service=self.service,
        )

        self.vpc.add_to_cdk(stack, self.cache)
        vpc_endpoint.add_to_cdk(stack, self.cache)

        template = Template.from_stack(stack)

        template.resource_count_is("AWS::EC2::VPC", 1)
        template.resource_count_is("AWS::EC2::VPCEndpoint", 1)
