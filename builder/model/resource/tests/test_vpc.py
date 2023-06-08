import unittest

from aws_cdk import (
    App,
    Stack,
)
from aws_cdk import aws_ec2 as ec2_
from aws_cdk.assertions import Template

from builder.model.property.environment import Environment
from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.vpc import VpcResource
from builder.utils.stack_cache import StackCache


class TestVpcResource(unittest.TestCase):
    def setUp(self) -> None:
        self.cache = StackCache()
        self.name = Name("test-vpc", Environment.TEST)
        self.tags = Tags()
        self.tags.add("tag1", "value1")

        self.cidr = ec2_.IpAddresses.cidr("192.168.228.0/22")
        self.default_instance_tenancy = ec2_.DefaultInstanceTenancy.DEFAULT
        self.enable_dns_hostnames = True
        self.enable_dns_support = True
        self.max_azs = 2
        self.subnet_configuration = [
            ec2_.SubnetConfiguration(
                name="public",
                subnet_type=ec2_.SubnetType.PUBLIC,
                cidr_mask=24,
            ),
            ec2_.SubnetConfiguration(
                name="private",
                subnet_type=ec2_.SubnetType.PRIVATE_ISOLATED,
                cidr_mask=24,
            ),
        ]

    def test_init(self) -> None:
        vpc = VpcResource(
            name=self.name,
            tags=self.tags,
            cidr=self.cidr,
            default_instance_tenancy=self.default_instance_tenancy,
            enable_dns_hostnames=self.enable_dns_hostnames,
            enable_dns_support=self.enable_dns_support,
            max_azs=self.max_azs,
            subnet_configuration=self.subnet_configuration,
        )

        self.assertEqual(vpc.name, self.name)
        self.assertEqual(vpc.tags, self.tags)
        self.assertEqual(vpc.cidr, self.cidr)
        self.assertEqual(
            vpc.default_instance_tenancy, self.default_instance_tenancy
        )
        self.assertEqual(vpc.enable_dns_hostnames, self.enable_dns_hostnames)
        self.assertEqual(vpc.enable_dns_support, self.enable_dns_support)
        self.assertEqual(vpc.max_azs, self.max_azs)
        self.assertEqual(vpc.subnet_configuration, self.subnet_configuration)

    def test_from_pydict(self) -> None:
        pydict = {
            "cidr": "192.168.228.0/22",
        }

        vpc = VpcResource.from_pydict(
            name=self.name, tags=self.tags, pydict=pydict
        )

        self.assertEqual(vpc.name, self.name)
        self.assertEqual(vpc.tags, self.tags)
        self.assertEqual(vpc.max_azs, 2)

    def test_add_to_cdk(self) -> None:
        app = App()
        stack = Stack(app, "test-stack")

        vpc = VpcResource(
            name=self.name,
            tags=self.tags,
            cidr=self.cidr,
            default_instance_tenancy=self.default_instance_tenancy,
            enable_dns_hostnames=self.enable_dns_hostnames,
            enable_dns_support=self.enable_dns_support,
            max_azs=self.max_azs,
            subnet_configuration=self.subnet_configuration,
        )

        vpc.add_to_cdk(stack, self.cache)

        template = Template.from_stack(stack)

        template.resource_count_is("AWS::EC2::VPC", 1)
        template.has_resource_properties(
            "AWS::EC2::VPC",
            {
                "CidrBlock": "192.168.228.0/22",
                "EnableDnsHostnames": True,
                "EnableDnsSupport": True,
                "InstanceTenancy": "default",
            },
        )
