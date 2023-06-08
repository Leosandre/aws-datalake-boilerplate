import unittest

from aws_cdk import (
    App,
    Stack,
)
from aws_cdk import aws_glue as glue_
from aws_cdk.assertions import Template

from builder.model.property.environment import Environment
from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.glue_crawler import GlueCrawlerResource
from builder.model.resource.iam_role import RoleResource
from builder.utils.stack_cache import StackCache


class TestGlueCrawlerResource(unittest.TestCase):
    def setUp(self) -> None:
        self.cache = StackCache()

        self.name = Name("test-glue-crawler", Environment.TEST)
        self.tags = Tags()
        self.tags.add("tag1", "value1")

        self.role_name = Name("test-role", Environment.TEST)
        self.role = RoleResource.from_pydict(
            name=self.role_name,
            tags=self.tags,
            pydict={
                "region": "us-east-1",
                "account_id": "1234567890",
                "assumed_by": "glue.amazonaws.com",
                "effect": "allow",
                "actions": ["s3:*"],
                "resources": ["*"],
            },
        )

        self.database_name = "test-database"
        self.schedule = "cron(0 12 * * ? *)"

        self.schedule_obj = glue_.CfnCrawler.ScheduleProperty(
            schedule_expression=self.schedule,
        )

        self.s3_targets_str = [
            "s3://test-bucket/test-prefix1",
            "s3://test-bucket/test-prefix2",
        ]

        self.s3_targets_obj = [
            glue_.CfnCrawler.S3TargetProperty(
                path="s3://test-bucket/test-prefix1",
            ),
            glue_.CfnCrawler.S3TargetProperty(
                path="s3://test-bucket/test-prefix2",
            ),
        ]

    def test_init(self) -> None:
        crawler = GlueCrawlerResource(
            name=self.name,
            tags=self.tags,
            role=self.role,
            database_name=self.database_name,
            schedule=self.schedule_obj,
            s3_targets=self.s3_targets_obj,
        )

        self.assertEqual(crawler.name, self.name)
        self.assertEqual(crawler.tags, self.tags)
        self.assertEqual(crawler.role, self.role)
        self.assertEqual(crawler.database_name, self.database_name)
        self.assertEqual(crawler.schedule, self.schedule_obj)
        self.assertEqual(crawler.s3_targets, self.s3_targets_obj)

    def test_from_pydict(self) -> None:
        pydict = {
            "role": self.role,
            "database_name": self.database_name,
            "schedule": self.schedule,
            "s3_targets": self.s3_targets_str,
        }

        crawler = GlueCrawlerResource.from_pydict(
            name=self.name,
            tags=self.tags,
            pydict=pydict,
        )

        self.assertEqual(crawler.name, self.name)
        self.assertEqual(crawler.tags, self.tags)
        self.assertEqual(crawler.role, self.role)
        self.assertEqual(crawler.database_name, self.database_name)
        self.assertIsInstance(
            crawler.schedule, glue_.CfnCrawler.ScheduleProperty
        )
        self.assertEqual(crawler.schedule.schedule_expression, self.schedule)  # type: ignore
        self.assertIsInstance(
            crawler.s3_targets[0], glue_.CfnCrawler.S3TargetProperty
        )
        self.assertIsInstance(
            crawler.s3_targets[1], glue_.CfnCrawler.S3TargetProperty
        )
        self.assertEqual(crawler.s3_targets[0].path, self.s3_targets_str[0])
        self.assertEqual(crawler.s3_targets[1].path, self.s3_targets_str[1])

    def test_add_to_cdk(self) -> None:
        app = App()
        stack = Stack(app, "test-stack")

        crawler = GlueCrawlerResource(
            name=self.name,
            tags=self.tags,
            role=self.role,
            database_name=self.database_name,
            schedule=self.schedule_obj,
            s3_targets=self.s3_targets_obj,
        )

        crawler.add_to_cdk(stack, self.cache)

        template = Template.from_stack(stack)

        template.resource_count_is("AWS::Glue::Crawler", 1)
        template.has_resource_properties(
            "AWS::Glue::Crawler",
            {
                "Name": self.name.value,
                "RecrawlPolicy": {
                    "RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY",
                },
                "Schedule": {
                    "ScheduleExpression": self.schedule,
                },
            },
        )
