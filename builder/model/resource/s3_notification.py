from dataclasses import dataclass
from typing import Optional

from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_s3 as s3_
from aws_cdk import aws_s3_notifications as s3_notifications
from constructs import Construct

from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.abstract import Resource
from builder.utils.stack_cache import StackCache
from builder.utils.validation import type_validation


@dataclass
class S3NotificationResource(Resource):
    name: Name
    tags: Tags
    bucket: Name
    lambda_: Name
    event_type: s3_.EventType
    prefix: Optional[str] = None
    suffix: Optional[str] = None

    @staticmethod
    def __pydict_validation(pydict: dict) -> None:
        pydict_map = {
            "bucket": Name,
            "lambda": Name,
            "event_type": str,
            "prefix?": str,
            "suffix?": str,
        }

        type_validation(pydict_map, pydict)

    @staticmethod
    def from_pydict(
        name: Name, tags: Tags, pydict: dict
    ) -> "S3NotificationResource":
        S3NotificationResource.__pydict_validation(pydict)

        event_type = getattr(s3_.EventType, pydict["event_type"].upper())

        props = {
            "bucket": pydict["bucket"],
            "lambda_": pydict["lambda"],
            "event_type": event_type,
        }

        if pydict.get("prefix"):
            props["prefix"] = pydict["prefix"]

        if pydict.get("suffix"):
            props["suffix"] = pydict["suffix"]

        return S3NotificationResource(name=name, tags=tags, **props)

    def add_to_cdk(self, scope: Construct, cache: StackCache) -> None:
        s3: s3_.Bucket = cache.get(self.bucket.value)
        func: lambda_.Function = cache.get(self.lambda_.value)

        notification = s3_notifications.LambdaDestination(func)

        if self.prefix and self.suffix:
            key_filter = s3_.NotificationKeyFilter(
                prefix=self.prefix, suffix=self.suffix
            )
        elif self.prefix and not self.suffix:
            key_filter = s3_.NotificationKeyFilter(prefix=self.prefix)
        elif not self.prefix and self.suffix:
            key_filter = s3_.NotificationKeyFilter(suffix=self.suffix)
        else:
            raise ValueError(
                "At least one of prefix or suffix must be specified to create a key filter for S3 notification."
            )

        s3.add_event_notification(self.event_type, notification, key_filter)
