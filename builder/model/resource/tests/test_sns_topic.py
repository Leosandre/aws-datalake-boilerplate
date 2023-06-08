import unittest
from typing import (
    List,
    Union,
)

from aws_cdk import (
    App,
    Stack,
)
from aws_cdk import aws_sns_subscriptions as sns_subs
from aws_cdk.assertions import Template

from builder.model.property.environment import Environment
from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.sns_topic import SnsTopicResource
from builder.utils.stack_cache import StackCache


class TestSnsTopicResource(unittest.TestCase):
    def setUp(self) -> None:
        self.cache = StackCache()

        self.name = Name("test-sns-topic", Environment.TEST)
        self.tags = Tags()
        self.tags.add("tag1", "value1")

        self.region = "us-east-1"
        self.account_id = "1234567890"
        self.display_name = "Test SNS Topic"
        self.subscriptions = [
            {
                "protocol": "email",
                "endpoint": "test@test.com",
            }
        ]

        self.subscriptions_obj: List[
            Union[
                sns_subs.EmailSubscription,
                sns_subs.LambdaSubscription,
                sns_subs.SmsSubscription,
                sns_subs.SqsSubscription,
                sns_subs.UrlSubscription,
            ]
        ] = [sns_subs.EmailSubscription("test@mail.com")]

    def test_init(self) -> None:
        topic = SnsTopicResource(
            name=self.name,
            tags=self.tags,
            region=self.region,
            account_id=self.account_id,
            display_name=self.display_name,
            subscriptions=self.subscriptions_obj,
        )

        self.assertEqual(topic.name, self.name)
        self.assertEqual(topic.tags, self.tags)
        self.assertEqual(topic.region, self.region)
        self.assertEqual(topic.account_id, self.account_id)
        self.assertEqual(topic.display_name, self.display_name)
        self.assertEqual(topic.subscriptions, self.subscriptions_obj)

    def test_from_pydict(self) -> None:
        pydict = {
            "region": self.region,
            "account_id": self.account_id,
            "display_name": self.display_name,
            "subscriptions": self.subscriptions,
        }

        topic = SnsTopicResource.from_pydict(
            name=self.name,
            tags=self.tags,
            pydict=pydict,
        )

        self.assertEqual(topic.name, self.name)
        self.assertEqual(topic.tags, self.tags)
        self.assertEqual(topic.region, self.region)
        self.assertEqual(topic.account_id, self.account_id)
        self.assertEqual(topic.display_name, self.display_name)
        self.assertIsInstance(
            topic.subscriptions[0], sns_subs.EmailSubscription
        )

    def test_add_to_cdk(self) -> None:
        app = App()
        stack = Stack(app, "test-stack")

        topic = SnsTopicResource(
            name=self.name,
            tags=self.tags,
            region=self.region,
            account_id=self.account_id,
            display_name=self.display_name,
            subscriptions=self.subscriptions_obj,
        )

        topic.add_to_cdk(stack, self.cache)

        template = Template.from_stack(stack)

        template.resource_count_is("AWS::SNS::Topic", 1)
        template.has_resource_properties(
            "AWS::SNS::Topic",
            {
                "DisplayName": self.display_name,
                "TopicName": self.name.value,
            },
        )
