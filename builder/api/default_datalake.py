from dataclasses import dataclass
from glob import glob
from os import path
from typing import (
    Dict,
    List,
    Optional,
    Tuple,
)

from aws_cdk import Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_sns as sns
from constructs import Construct
from yaml import safe_load

from builder.model.config.pipeline import PipelineConfig
from builder.model.package.datalake import DatalakePackage
from builder.model.package.pipeline import PipelinePackage
from builder.model.property.bucket import DatalakeBucketSet
from builder.model.property.environment import Environment
from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.s3_bucket import S3BucketResource
from builder.utils.stack_cache import StackCache


@dataclass
class DatalakeBuilder:
    scope: Construct
    lake_name: str
    region: str
    account_id: str
    env: str
    lake_domains: List[str]
    enable_vpc: bool
    sns_subscriptions: List[Dict[str, str]]
    pipelines_path: str
    tags: Dict[str, str]

    def __post_init__(self) -> None:
        self.bucket_set: Optional[DatalakeBucketSet] = None
        self.vpc: Optional[ec2.Vpc] = None
        self.sns_topic: Optional[sns.Topic] = None

    def __set_properties(self) -> Tuple[Name, Environment, Tags]:
        env = Environment(self.env)
        name = Name(self.lake_name, env)
        tags = Tags([(k, v) for k, v in self.tags.items()])
        return name, env, tags

    def __get_pipeline_configs(self) -> List[Tuple[PipelineConfig, str]]:
        config_paths = glob(
            path.join(self.pipelines_path, "*", "config.yml"), recursive=True
        )

        configs: List[Tuple[PipelineConfig, str]] = []
        for config_path in config_paths:
            with open(config_path) as f:
                config = safe_load(f)

            configs.append(
                (
                    PipelineConfig.from_pydict(Environment(self.env), config),
                    path.dirname(config_path),
                )
            )

        return configs

    def build(self) -> None:
        cache = StackCache()
        name, env, tags = self.__set_properties()

        datalake = DatalakePackage(
            name=name,
            tags=tags,
            region=self.region,
            account_id=self.account_id,
            domains=self.lake_domains,
            env=env,
            enable_vpc=self.enable_vpc,
            sns_display_name=self.lake_name,
            subscriptions=self.sns_subscriptions,
        ).build()

        storage_stack: Stack = Stack(
            self.scope, Name("lake-storage-stack", env).value
        )
        shared_stack: Stack = Stack(
            self.scope, Name("lake-shared-stack", env).value
        )

        shared_stack.add_dependency(storage_stack)

        for resource in datalake.resources:
            if isinstance(resource, S3BucketResource):
                resource.add_to_cdk(storage_stack, cache)
            else:
                resource.add_to_cdk(shared_stack, cache)

        pipeline_configs = self.__get_pipeline_configs()
        for pipeline_config, pipeline_path in pipeline_configs:
            stack_name: Name = pipeline_config.name.add_prefix(
                "lake"
            ).add_suffix("stack")
            pipeline_stack: Stack = Stack(self.scope, stack_name.value)

            pipeline_package = PipelinePackage(
                region=self.region,
                account_id=self.account_id,
                bucket_set=datalake.bucket_set,
                sns_topic=datalake.sns_topic,
                root_path=pipeline_path,
                config=pipeline_config,
                vpc=datalake.vpc,
            ).build()

            for resource in pipeline_package.resources:
                resource.add_to_cdk(pipeline_stack, cache)

        self.bucket_set = datalake.bucket_set
        self.sns_topic = cache.get(datalake.sns_topic.name.value)
        if datalake.vpc:
            self.vpc = cache.get(datalake.vpc.name.value)
