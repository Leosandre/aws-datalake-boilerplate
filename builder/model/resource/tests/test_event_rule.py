import unittest
from os import path

from aws_cdk import (
    App,
    Duration,
    Stack,
)
from aws_cdk.assertions import Template

from builder.model.property.environment import Environment
from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.event_rule import EventRuleResource
from builder.model.resource.iam_role import RoleResource
from builder.model.resource.lambda_ import LambdaResource
from builder.utils.stack_cache import StackCache


class TestEventRuleResouce(unittest.TestCase):
    def setUp(self) -> None:
        self.cache = StackCache()

        self.role_name = Name("test-role", Environment.TEST)
        self.lambda_name = Name("test-lambda", Environment.TEST)
        self.name = Name("test-event-rule", Environment.TEST)

        self.tags = Tags()
        self.tags.add("tag1", "value1")

        self.region = "us-east-1"
        self.account_id = "1234567890"

        self.root = path.join(path.dirname(path.abspath(__file__)), "mock")
        self.source_folder = path.join(self.root, "code")

        self.role = RoleResource.from_pydict(
            name=self.role_name,
            tags=self.tags,
            pydict={
                "region": self.region,
                "account_id": self.account_id,
                "assumed_by": "lambda.amazonaws.com",
                "effect": "allow",
                "actions": ["s3:*"],
            },
        )

        self.func = LambdaResource.from_pydict(
            name=self.lambda_name,
            tags=self.tags,
            pydict={
                "region": self.region,
                "account_id": self.account_id,
                "root": self.root,
                "source_folder": self.source_folder,
                "role": self.role,
                "build_deps": False,
            },
        )

        self.event_pattern = {
            "source": ["aws.ec2"],
            "detail_type": ["EC2 Instance State-change Notification"],
            "detail": {"state": ["running"]},
        }

    def test_from_pydict(self) -> None:
        event_rule = EventRuleResource.from_pydict(
            name=self.name,
            tags=self.tags,
            pydict={
                "event_pattern": self.event_pattern,
                "targets": [self.func],
            },
        )

        self.assertEqual(event_rule.name, self.name)
        self.assertEqual(event_rule.tags, self.tags)
        self.assertEqual(event_rule.event_pattern, self.event_pattern)
        self.assertEqual(event_rule.targets, [self.func])

    def test_add_to_cdk(self) -> None:
        app = App()
        stack = Stack(app, "test-stack")

        event_rule = EventRuleResource(
            name=self.name,
            tags=self.tags,
            event_pattern=self.event_pattern,
            targets=[self.func],
        )

        self.role.add_to_cdk(stack, self.cache)
        self.func.add_to_cdk(stack, self.cache)
        event_rule.add_to_cdk(stack, self.cache)

        template = Template.from_stack(stack)

        template.resource_count_is("AWS::Events::Rule", 1)
        template.has_resource_properties(
            "AWS::Events::Rule",
            {
                "EventPattern": {
                    "source": ["aws.ec2"],
                    "detail-type": ["EC2 Instance State-change Notification"],
                    "detail": {"state": ["running"]},
                }
            },
        )
