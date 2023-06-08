from dataclasses import dataclass

from aws_cdk import aws_ec2 as ec2_
from constructs import Construct

from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.abstract import Resource
from builder.model.resource.vpc import VpcResource
from builder.utils.stack_cache import StackCache
from builder.utils.validation import type_validation


@dataclass
class VpcEndpointResource(Resource):
    name: Name
    tags: Tags
    service: str
    vpc: VpcResource

    @staticmethod
    def __pydict_validation(pydict: dict) -> None:
        pydict_map = {
            "service": str,
            "vpc": VpcResource,
        }

        type_validation(pydict_map, pydict)

    @staticmethod
    def from_pydict(
        name: Name, tags: Tags, pydict: dict
    ) -> "VpcEndpointResource":
        VpcEndpointResource.__pydict_validation(pydict)

        return VpcEndpointResource(
            name=name,
            tags=tags,
            service=pydict["service"],
            vpc=pydict["vpc"],
        )

    def add_to_cdk(self, scope: Construct, cache: StackCache) -> None:
        if self.service == "s3":
            endpoint_type = "gateway"
            service = ec2_.GatewayVpcEndpointAwsService.S3
        elif self.service == "dynamodb":
            endpoint_type = "gateway"
            service = ec2_.GatewayVpcEndpointAwsService.DYNAMODB
        else:
            endpoint_type = "interface"
            service = ec2_.InterfaceVpcEndpointAwsService(name=self.service)

        vpc: ec2_.Vpc = cache.get(self.vpc.name.value)

        if endpoint_type == "gateway":
            vpc.add_gateway_endpoint(self.name.value, service=service)
        else:
            vpc.add_interface_endpoint(self.name.value, service=service)
