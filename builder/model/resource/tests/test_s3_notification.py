import unittest
from os import path

from aws_cdk import (
    App,
    Stack,
)
from aws_cdk import aws_s3 as s3_
from aws_cdk.assertions import Template

from builder.model.property.environment import Environment
from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.iam_role import RoleResource
from builder.model.resource.lambda_ import LambdaResource
from builder.model.resource.s3_bucket import S3BucketResource
from builder.model.resource.s3_notification import S3NotificationResource
from builder.utils.stack_cache import StackCache


class TestS3NotificationResource(unittest.TestCase):
    def setUp(self) -> None:
        self.cache = StackCache()

        self.root = path.join(path.dirname(path.abspath(__file__)), "mock")
        self.source_folder = path.join(self.root, "code")

        self.role_name = Name("test-role", Environment.TEST)
        self.s3_bucket_name = Name("test-s3-bucket", Environment.TEST)
        self.lambda_name = Name("test-lambda", Environment.TEST)
        self.name = Name("test-s3-notification", Environment.TEST)

        self.region = "us-east-1"
        self.account_id = "1234567890"
        self.tags = Tags()
        self.tags.add("tag1", "value1")

        self.role = RoleResource.from_pydict(
            name=self.role_name,
            tags=self.tags,
            pydict={
                "region": self.region,
                "account_id": self.account_id,
                "assumed_by": "lambda.amazonaws.com",
                "effect": "allow",
                "actions": ["s3:*"],
                "resources": ["*"],
            },
        )

        self.s3_bucket = S3BucketResource.from_pydict(
            name=self.s3_bucket_name,
            tags=self.tags,
            pydict={
                "removal_policy": "destroy",
            },
        )

        self.lambda_ = LambdaResource.from_pydict(
            name=self.lambda_name,
            tags=self.tags,
            pydict={
                "region": self.region,
                "account_id": self.account_id,
                "timeout": 30,
                "memory_size": 256,
                "root": self.root,
                "source_folder": self.source_folder,
                "role": self.role,
                "environment": {"VAR1": "value1"},
                "build_deps": False,
            },
        )

        self.event = "OBJECT_CREATED"
        self.event_type = s3_.EventType.OBJECT_CREATED
        self.prefix = "test/"
        self.suffix = ".txt"

    def test_init(self) -> None:
        notification = S3NotificationResource(
            name=self.name,
            tags=self.tags,
            bucket=self.s3_bucket_name,
            lambda_=self.lambda_name,
            event_type=self.event_type,
            prefix=self.prefix,
            suffix=self.suffix,
        )

        self.assertEqual(notification.name, self.name)
        self.assertEqual(notification.tags, self.tags)
        self.assertEqual(notification.bucket, self.s3_bucket_name)
        self.assertEqual(notification.lambda_, self.lambda_name)
        self.assertEqual(notification.event_type, self.event_type)
        self.assertEqual(notification.prefix, self.prefix)
        self.assertEqual(notification.suffix, self.suffix)

    def test_from_pydict(self) -> None:
        pydict = {
            "bucket": self.s3_bucket_name,
            "lambda": self.lambda_name,
            "event_type": self.event,
        }

        notification = S3NotificationResource.from_pydict(
            name=self.name,
            tags=self.tags,
            pydict=pydict,
        )

        self.assertEqual(notification.name, self.name)
        self.assertEqual(notification.tags, self.tags)
        self.assertEqual(notification.bucket, self.s3_bucket_name)
        self.assertEqual(notification.lambda_, self.lambda_name)
        self.assertEqual(notification.event_type, self.event_type)
        self.assertEqual(notification.prefix, None)
        self.assertEqual(notification.suffix, None)

    def test_add_to_cdk(self) -> None:
        app = App()
        stack = Stack(app, "test-stack")

        notification = S3NotificationResource(
            name=self.name,
            tags=self.tags,
            bucket=self.s3_bucket_name,
            lambda_=self.lambda_name,
            event_type=self.event_type,
            prefix=self.prefix,
            suffix=self.suffix,
        )

        self.role.add_to_cdk(stack, self.cache)
        self.s3_bucket.add_to_cdk(stack, self.cache)
        self.lambda_.add_to_cdk(stack, self.cache)
        notification.add_to_cdk(stack, self.cache)

        template = Template.from_stack(stack)

        template.resource_count_is("Custom::S3BucketNotifications", 1)
