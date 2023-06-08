from dataclasses import dataclass
from typing import (
    List,
    Union,
)

from aws_cdk import Tags as AwsTags
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as sns_subs
from constructs import Construct

from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.abstract import Resource
from builder.utils.stack_cache import StackCache
from builder.utils.validation import type_validation


@dataclass
class SnsTopicResource(Resource):
    name: Name
    tags: Tags
    region: str
    account_id: str
    display_name: str
    subscriptions: List[
        Union[
            sns_subs.EmailSubscription,
            sns_subs.LambdaSubscription,
            sns_subs.SmsSubscription,
            sns_subs.SqsSubscription,
            sns_subs.UrlSubscription,
        ]
    ]

    @staticmethod
    def __pydict_validation(pydict: dict) -> None:
        pydict_map = {
            "region": str,
            "account_id": str,
            "display_name": str,
            "subscriptions": list,
        }

        type_validation(pydict_map, pydict)

    @staticmethod
    def from_pydict(name: Name, tags: Tags, pydict: dict) -> "SnsTopicResource":
        SnsTopicResource.__pydict_validation(pydict)

        subs_type = {
            "email": sns_subs.EmailSubscription,
            "lambda": sns_subs.LambdaSubscription,
            "sms": sns_subs.SmsSubscription,
            "sqs": sns_subs.SqsSubscription,
            "url": sns_subs.UrlSubscription,
        }

        subs = []
        for sub in pydict["subscriptions"]:
            protocol = subs_type.get(sub["protocol"], None)
            if protocol:
                subs.append(protocol(sub["endpoint"]))

        return SnsTopicResource(
            name=name,
            tags=tags,
            region=pydict["region"],
            account_id=pydict["account_id"],
            display_name=pydict["display_name"],
            subscriptions=subs,
        )

    def add_to_cdk(self, scope: Construct, cache: StackCache) -> None:
        topic = sns.Topic(
            scope,
            self.name.value,
            topic_name=self.name.value,
            display_name=self.display_name,
        )

        for sub in self.subscriptions:
            topic.add_subscription(sub)

        for key, value in self.tags.items:
            AwsTags.of(topic).add(key, value)

        cache.add(self.name.value, topic)

    @property
    def arn(self) -> str:
        return f"arn:aws:sns:{self.region}:{self.account_id}:{self.name.value}"
