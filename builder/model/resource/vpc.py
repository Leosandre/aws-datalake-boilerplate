from dataclasses import dataclass
from typing import List

from aws_cdk import Tags as AwsTags
from aws_cdk import aws_ec2 as ec2_
from constructs import Construct

from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.abstract import Resource
from builder.utils.stack_cache import StackCache
from builder.utils.validation import type_validation


@dataclass
class VpcResource(Resource):
    name: Name
    tags: Tags
    cidr: ec2_.IIpAddresses
    default_instance_tenancy: ec2_.DefaultInstanceTenancy
    enable_dns_hostnames: bool
    enable_dns_support: bool
    max_azs: int
    subnet_configuration: List[ec2_.SubnetConfiguration]

    @staticmethod
    def __pydict_validation(pydict: dict) -> None:
        pydict_map = {
            "cidr": str,
            "max_azs?": int,
            "enable_dns_hostnames?": bool,
            "enable_dns_support?": bool,
        }

        type_validation(pydict_map, pydict)

    @staticmethod
    def from_pydict(name: Name, tags: Tags, pydict: dict) -> "VpcResource":
        VpcResource.__pydict_validation(pydict)

        props = {
            "cidr": ec2_.IpAddresses.cidr(pydict["cidr"]),
            "default_instance_tenancy": ec2_.DefaultInstanceTenancy.DEFAULT,
            "enable_dns_hostnames": pydict.get("enable_dns_hostnames", True),
            "enable_dns_support": pydict.get("enable_dns_support", True),
            "max_azs": pydict.get("max_azs", 2),
            "subnet_configuration": [
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
            ],
        }

        return VpcResource(name=name, tags=tags, **props)

    def add_to_cdk(self, scope: Construct, cache: StackCache) -> None:
        vpc = ec2_.Vpc(
            scope,
            self.name.value,
            vpc_name=self.name.value,
            ip_addresses=self.cidr,
            default_instance_tenancy=self.default_instance_tenancy,
            enable_dns_hostnames=self.enable_dns_hostnames,
            enable_dns_support=self.enable_dns_support,
            max_azs=self.max_azs,
            subnet_configuration=self.subnet_configuration,
        )

        for tag_key, tag_value in self.tags.items:
            AwsTags.of(vpc).add(tag_key, tag_value)

        cache.add(self.name.value, vpc)
