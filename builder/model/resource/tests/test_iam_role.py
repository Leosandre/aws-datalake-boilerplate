import unittest

from aws_cdk import (
    App,
    Stack,
)
from aws_cdk import aws_iam as iam_
from aws_cdk.assertions import Template

from builder.model.property.environment import Environment
from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.iam_role import RoleResource
from builder.utils.stack_cache import StackCache


class TestRoleResource(unittest.TestCase):
    def setUp(self) -> None:
        self.cache = StackCache()
        self.name = Name("test-lambda", Environment.TEST)
        self.region = "us-east-1"
        self.account_id = "1234567890"
        self.tags = Tags()
        self.tags.add("tag1", "value1")

        self.effect = "allow"
        self.actions = ["s3:*"]
        self.resources = ["*"]
        self.assumed_by = iam_.ServicePrincipal("lambda.amazonaws.com")
        self.managed_policies = [
            iam_.ManagedPolicy.from_aws_managed_policy_name(
                "AmazonS3FullAccess"
            )
        ]

    def test_init(self) -> None:
        role = RoleResource(
            name=self.name,
            tags=self.tags,
            region=self.region,
            account_id=self.account_id,
            assumed_by=self.assumed_by,
            effect=iam_.Effect.ALLOW,
            actions=self.actions,
            resources=self.resources,
            managed_policies=self.managed_policies,
        )

        self.assertEqual(role.name, self.name)
        self.assertEqual(role.tags, self.tags)
        self.assertEqual(role.region, self.region)
        self.assertEqual(role.account_id, self.account_id)
        self.assertEqual(role.assumed_by, self.assumed_by)
        self.assertEqual(role.effect, iam_.Effect.ALLOW)
        self.assertEqual(role.actions, self.actions)
        self.assertEqual(role.resources, self.resources)
        self.assertEqual(role.managed_policies, self.managed_policies)

    def test_from_pydict(self) -> None:
        pydict = {
            "region": self.region,
            "account_id": self.account_id,
            "assumed_by": "lambda.amazonaws.com",
            "effect": "allow",
            "actions": ["s3:*"],
            "resources": ["*"],
            "managed_policies": ["AmazonS3FullAccess"],
        }

        role = RoleResource.from_pydict(self.name, self.tags, pydict)

        self.assertEqual(role.name, self.name)
        self.assertEqual(role.tags, self.tags)
        self.assertEqual(role.region, self.region)
        self.assertEqual(role.account_id, self.account_id)
        self.assertEqual(role.effect, iam_.Effect.ALLOW)
        self.assertEqual(role.actions, self.actions)
        self.assertEqual(role.resources, self.resources)
        self.assertIsInstance(role.assumed_by, iam_.ServicePrincipal)
        self.assertEqual(role.assumed_by.service, "lambda.amazonaws.com")

    def test_add_to_cdk(self) -> None:
        app = App()
        stack = Stack(app, "test-stack")

        role = RoleResource(
            name=self.name,
            tags=self.tags,
            region=self.region,
            account_id=self.account_id,
            assumed_by=self.assumed_by,
            effect=iam_.Effect.ALLOW,
            actions=self.actions,
            resources=self.resources,
            managed_policies=self.managed_policies,
        )

        role.add_to_cdk(stack, self.cache)

        template = Template.from_stack(stack)

        template.resource_count_is("AWS::IAM::Role", 1)
        template.has_resource_properties(
            "AWS::IAM::Role",
            {
                "RoleName": self.name.value,
                "AssumeRolePolicyDocument": {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "lambda.amazonaws.com"},
                        }
                    ]
                },
            },
        )
