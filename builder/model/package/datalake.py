from dataclasses import (
    dataclass,
    field,
)
from itertools import product
from typing import (
    Dict,
    List,
    Optional,
)

from builder.model.package.abstract import Package
from builder.model.property.bucket import (
    DatalakeBucket,
    DatalakeBucketSet,
)
from builder.model.property.environment import Environment
from builder.model.property.layer import DatalakeLayer
from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.abstract import Resource
from builder.model.resource.glue_crawler import GlueCrawlerResource
from builder.model.resource.glue_database import GlueDatabaseResource
from builder.model.resource.iam_role import RoleResource
from builder.model.resource.s3_bucket import S3BucketResource
from builder.model.resource.sns_topic import SnsTopicResource
from builder.model.resource.vpc import VpcResource
from builder.model.resource.vpc_endpoint import VpcEndpointResource


@dataclass
class DatalakePackage(Package):
    name: Name
    tags: Tags
    region: str
    account_id: str
    domains: List[str]
    env: Environment
    enable_vpc: bool
    sns_display_name: str
    subscriptions: List[Dict[str, str]] = field(default_factory=list)
    vpc_cidr: str = "192.168.228.0/22"
    crawler_schedule: str = "cron(0 0 * * ? *)"
    bucket_removal_policy: str = "retain"
    resources: List[Resource] = field(default_factory=list)

    def build(self) -> "DatalakePackage":
        self.vpc: Optional[VpcResource] = None
        if self.enable_vpc:
            self.vpc = VpcResource.from_pydict(
                name=Name("vpc", self.env),
                tags=self.tags,
                pydict={
                    "cidr": self.vpc_cidr,
                },
            )
            self.resources.append(self.vpc)

            for service in ["s3", "sns", "states", "glue"]:
                endpoint = VpcEndpointResource.from_pydict(
                    name=Name(f"vpce-{service}", self.env),
                    tags=self.tags,
                    pydict={
                        "service": service,
                        "vpc": self.vpc,
                    },
                )
                self.resources.append(endpoint)

        self.sns_topic = SnsTopicResource.from_pydict(
            name=Name("sns-topic", self.env),
            tags=self.tags,
            pydict={
                "region": self.region,
                "account_id": self.account_id,
                "display_name": self.sns_display_name,
                "subscriptions": self.subscriptions,
            },
        )

        crawler_role = RoleResource.from_pydict(
            name=Name("glue-crawler", self.env),
            tags=self.tags,
            pydict={
                "region": self.region,
                "account_id": self.account_id,
                "assumed_by": "glue.amazonaws.com",
                "effect": "allow",
                "actions": [
                    "glue:*",
                    "s3:*",
                    "iam:ListRolePolicies",
                    "iam:GetRole",
                    "iam:GetRolePolicy",
                    "cloudwatch:PutMetricData",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
            },
        )

        self.resources.append(self.sns_topic)
        self.resources.append(crawler_role)

        self.bucket_set = DatalakeBucketSet(
            region=self.region,
            account_id=self.account_id,
            domains=self.domains,
            env=self.env,
        )

        for bucket in self.bucket_set.buckets:
            s3_bucket = S3BucketResource.from_pydict(
                name=bucket.name,
                tags=self.tags,
                pydict={
                    "removal_policy": self.bucket_removal_policy,
                },
            )

            database = GlueDatabaseResource(
                name=bucket.database,
                tags=self.tags,
                catalog_id=self.account_id,
                location_uri=bucket.uri,
            )

            self.resources.append(s3_bucket)
            self.resources.append(database)

            if bucket.layer == DatalakeLayer.RAW:
                crawler = GlueCrawlerResource.from_pydict(
                    name=bucket.crawler,
                    tags=self.tags,
                    pydict={
                        "role": crawler_role,
                        "database_name": bucket.database.value,
                        "s3_targets": [bucket.uri],
                        "schedule": self.crawler_schedule,
                    },
                )

                self.resources.append(crawler)

        return self
