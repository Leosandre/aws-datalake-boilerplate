import unittest

from aws_cdk import (
    App,
    RemovalPolicy,
    Stack,
)
from aws_cdk.assertions import Template

from builder.model.property.environment import Environment
from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.s3_bucket import S3BucketResource
from builder.utils.stack_cache import StackCache


class TestS3BucketResource(unittest.TestCase):
    def setUp(self) -> None:
        self.cache = StackCache()
        self.name = Name("test-s3-bucket", Environment.TEST)
        self.tags = Tags()
        self.tags.add("tag1", "value1")
        self.removal_policy = RemovalPolicy.DESTROY

    def test_init(self) -> None:
        bucket = S3BucketResource(
            name=self.name,
            tags=self.tags,
            removal_policy=self.removal_policy,
        )

        self.assertEqual(bucket.name, self.name)
        self.assertEqual(bucket.tags, self.tags)
        self.assertEqual(bucket.removal_policy, self.removal_policy)

    def test_from_pydict(self) -> None:
        pydict = {
            "removal_policy": "destroy",
        }

        bucket = S3BucketResource.from_pydict(
            name=self.name,
            tags=self.tags,
            pydict=pydict,
        )

        self.assertEqual(bucket.name, self.name)
        self.assertEqual(bucket.tags, self.tags)
        self.assertEqual(bucket.removal_policy, self.removal_policy)

    def test_add_to_cdk(self) -> None:
        app = App()
        stack = Stack(app, "test-stack")

        bucket = S3BucketResource(
            name=self.name,
            tags=self.tags,
            removal_policy=self.removal_policy,
        )

        bucket.add_to_cdk(stack, self.cache)

        template = Template.from_stack(stack)

        template.resource_count_is("AWS::S3::Bucket", 1)
        template.has_resource_properties(
            "AWS::S3::Bucket",
            {
                "BucketName": self.name.value,
            },
        )
