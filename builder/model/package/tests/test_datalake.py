import unittest

from aws_cdk import (
    App,
    Stack,
)
from aws_cdk.assertions import Template

from builder.model.package.datalake import DatalakePackage
from builder.model.property.environment import Environment
from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.utils.stack_cache import StackCache


class TestDatalakePackage(unittest.TestCase):
    def setUp(self) -> None:
        self.datalake = DatalakePackage(
            name=Name("test-datalake", Environment.TEST),
            tags=Tags(),
            region="us-east-1",
            account_id="1234567890",
            domains=["domain1", "domain2"],
            env=Environment.TEST,
            enable_vpc=True,
            sns_display_name="Test SNS",
            subscriptions=[{"protocol": "email", "endpoint": "test@test.com"}],
        )

    def test_build(self) -> None:
        self.datalake.build()
        resources = self.datalake.resources

        self.assertEqual(len(resources), 21)

    def test_add_to_cdk(self) -> None:
        app = App()
        stack = Stack(app, "test-stack")
        cache = StackCache()

        self.datalake.build()

        for resource in self.datalake.resources:
            resource.add_to_cdk(stack, cache)

        template = Template.from_stack(stack)

        template.resource_count_is("AWS::S3::Bucket", 6)
        template.resource_count_is("AWS::Glue::Database", 6)
        template.resource_count_is("AWS::Glue::Crawler", 2)
        template.resource_count_is("AWS::IAM::Role", 1)
        template.resource_count_is("AWS::IAM::Policy", 1)
        template.resource_count_is("AWS::SNS::Topic", 1)
        template.resource_count_is("AWS::SNS::Subscription", 1)
        template.resource_count_is("AWS::EC2::VPC", 1)
        template.resource_count_is("AWS::EC2::Route", 2)
        template.resource_count_is("AWS::EC2::Subnet", 4)
        template.resource_count_is("AWS::EC2::RouteTable", 4)
        template.resource_count_is("AWS::EC2::SubnetRouteTableAssociation", 4)
        template.resource_count_is("AWS::EC2::InternetGateway", 1)
        template.resource_count_is("AWS::EC2::VPCGatewayAttachment", 1)
        template.resource_count_is("AWS::EC2::SecurityGroup", 3)
        template.resource_count_is("AWS::EC2::VPCEndpoint", 4)

        resources = template.to_json()["Resources"]
        self.assertEqual(len(resources), 42)
