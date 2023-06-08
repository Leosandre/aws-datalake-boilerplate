from dataclasses import dataclass
from os import path
from typing import (
    Any,
    Dict,
    Optional,
)
from uuid import uuid4

from aws_cdk import Duration
from aws_cdk import Tags as AwsTags
from aws_cdk import aws_ec2 as ec2_
from aws_cdk import aws_iam as iam_
from aws_cdk import aws_lambda as lambda_
from constructs import Construct

from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.abstract import Resource
from builder.model.resource.iam_role import RoleResource
from builder.model.resource.vpc import VpcResource
from builder.utils.dockerbuild import (
    DockerBuilder,
    DockerBuilderMethod,
    LambdaDockerProperties,
)
from builder.utils.stack_cache import StackCache
from builder.utils.validation import type_validation


@dataclass
class LambdaResource(Resource):
    name: Name
    tags: Tags
    region: str
    account_id: str
    role: RoleResource
    root: str
    source_folder: str
    timeout: Duration
    memory_size: int
    environment: Dict[str, str]
    vpc: Optional[VpcResource] = None
    vpc_subnets: Optional[str] = None
    build_deps: bool = True

    @staticmethod
    def __pydict_validation(pydict: dict) -> None:
        pydict_map = {
            "region": str,
            "account_id": str,
            "role": RoleResource,
            "root": str,
            "source_folder": str,
            "timeout?": int,
            "memory_size?": int,
            "environment?": dict,
            "vpc?": VpcResource,
            "vpc_subnet?": str,
            "build_deps?": bool,
        }

        type_validation(pydict_map, pydict)

        if not path.isdir(pydict["source_folder"]):
            raise NotADirectoryError(
                f"Folder not found: {pydict['source_folder']}"
            )

        if not path.isfile(
            path.join(pydict["source_folder"], "src", "index.py")
        ):
            raise FileNotFoundError(
                f"File not found: {path.join(pydict['source_folder'], 'src', 'index.py')}"
            )

        if pydict.get("vpc"):
            if not pydict.get("vpc_subnet"):
                raise ValueError(
                    "vpc_subnet must be specified if vpc is specified"
                )

            if pydict["vpc_subnet"] not in [
                "public",
                "private",
            ]:
                raise ValueError(
                    "vpc_subnet must be either 'public' or 'private'"
                )

    @staticmethod
    def from_pydict(name: Name, tags: Tags, pydict: dict) -> "LambdaResource":
        LambdaResource.__pydict_validation(pydict)

        props: Dict[str, Any] = {
            "region": pydict["region"],
            "account_id": pydict["account_id"],
            "role": pydict["role"],
            "root": pydict["root"],
            "source_folder": pydict["source_folder"],
            "timeout": Duration.seconds(pydict.get("timeout", 30)),
            "memory_size": pydict.get("memory_size", 512),
            "environment": pydict.get("environment", {}),
            "build_deps": pydict.get("build_deps", True),
        }

        if pydict.get("vpc"):
            props["vpc"] = pydict["vpc"]

            if pydict["vpc_subnet"] == "private":
                props["vpc_subnets"] = ec2_.SubnetSelection(
                    subnet_type=ec2_.SubnetType.PRIVATE_ISOLATED
                )
            elif pydict["vpc_subnet"] == "public":
                props["vpc_subnets"] = ec2_.SubnetSelection(
                    subnet_type=ec2_.SubnetType.PUBLIC
                )

        return LambdaResource(name=name, tags=tags, **props)

    def add_to_cdk(self, scope: Construct, cache: StackCache) -> None:
        random_id = "a" + str(uuid4())[0:8]
        role = iam_.Role.from_role_arn(scope, random_id, self.role.arn)

        docker_builder = DockerBuilder(
            root_path=self.root,
            module_path=path.join(self.source_folder),
        )

        docker_props: LambdaDockerProperties
        if self.build_deps:
            docker_props = docker_builder.build(DockerBuilderMethod.LAMBDA)  # type: ignore
        else:
            docker_props = docker_builder.get_properties(
                DockerBuilderMethod.LAMBDA
            )  # type: ignore

        code = lambda_.DockerImageCode.from_image_asset(
            directory=docker_props.root,
            file=docker_props.dockerfile,
            build_args={
                "REQUIREMENTS": docker_props.requirements,
                "SHARED_MODULES": docker_props.shared,
                "SOURCE_FOLDER": docker_props.source,
            },
        )

        props: Dict[str, Any] = {
            "function_name": self.name.value,
            "role": role,
            "code": code,
            "timeout": self.timeout,
            "memory_size": self.memory_size,
            "environment": self.environment,
        }

        if self.vpc:
            props["vpc"] = cache.get(self.vpc.name.value)
            props["vpc_subnets"] = self.vpc_subnets

        func = lambda_.DockerImageFunction(scope, self.name.value, **props)

        for tag_key, tag_value in self.tags.items:
            AwsTags.of(func).add(tag_key, tag_value)

        cache.add(self.name.value, func)

    @property
    def arn(self) -> str:
        return f"arn:aws:lambda:{self.region}:{self.account_id}:function:{self.name.value}"
