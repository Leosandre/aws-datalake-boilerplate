from dataclasses import dataclass
from typing import (
    Any,
    Dict,
    List,
    Optional,
)

from aws_cdk import Tags as AwsTags
from aws_cdk import aws_glue as glue_
from constructs import Construct

from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.abstract import Resource
from builder.model.resource.iam_role import RoleResource
from builder.utils.stack_cache import StackCache
from builder.utils.validation import type_validation


@dataclass
class GlueCrawlerResource(Resource):
    name: Name
    tags: Tags
    role: RoleResource
    database_name: str
    s3_targets: List[glue_.CfnCrawler.S3TargetProperty]
    schedule: Optional[glue_.CfnCrawler.ScheduleProperty] = None

    @staticmethod
    def __pydict_validation(pydict: dict) -> None:
        pydict_map = {
            "role": RoleResource,
            "s3_targets": list,
            "database_name": str,
            "schedule?": str,
        }

        type_validation(pydict_map, pydict)

        if pydict.get("schedule") and not pydict["schedule"].startswith(
            "cron("
        ):
            raise ValueError("Invalid schedule, expected cron() format")

    @staticmethod
    def from_pydict(
        name: Name, tags: Tags, pydict: dict
    ) -> "GlueCrawlerResource":
        GlueCrawlerResource.__pydict_validation(pydict)

        s3_targets = []
        for s3_target in pydict["s3_targets"]:
            s3_targets.append(
                glue_.CfnCrawler.S3TargetProperty(
                    path=s3_target,
                )
            )

        props = {
            "role": pydict["role"],
            "s3_targets": s3_targets,
            "database_name": pydict["database_name"],
        }

        if pydict.get("schedule"):
            props["schedule"] = glue_.CfnCrawler.ScheduleProperty(
                schedule_expression=pydict["schedule"],
            )

        return GlueCrawlerResource(name=name, tags=tags, **props)

    def add_to_cdk(self, scope: Construct, cache: StackCache) -> None:
        props: Dict[str, Any] = {
            "name": self.name.value,
            "role": self.role.arn,
            "database_name": self.database_name,
            "targets": glue_.CfnCrawler.TargetsProperty(
                s3_targets=self.s3_targets,
            ),
            "schema_change_policy": glue_.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="LOG",
                delete_behavior="LOG",
            ),
            "recrawl_policy": glue_.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior="CRAWL_NEW_FOLDERS_ONLY",
            ),
        }

        if self.schedule:
            props["schedule"] = self.schedule

        crawler = glue_.CfnCrawler(scope, self.name.value, **props)

        for key, value in self.tags.items:
            AwsTags.of(crawler).add(key, value)
