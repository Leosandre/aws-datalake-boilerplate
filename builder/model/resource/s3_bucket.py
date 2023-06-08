from dataclasses import dataclass

from aws_cdk import RemovalPolicy
from aws_cdk import Tags as AwsTags
from aws_cdk import aws_s3 as s3_
from constructs import Construct

from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.abstract import Resource
from builder.utils.stack_cache import StackCache
from builder.utils.validation import type_validation


@dataclass
class S3BucketResource(Resource):
    name: Name
    tags: Tags
    removal_policy: RemovalPolicy

    @staticmethod
    def __pydict_validation(pydict: dict) -> None:
        pydict_map = {
            "removal_policy?": str,
        }

        type_validation(pydict_map, pydict)

        if pydict.get("removal_policy", "destroy") not in [
            "retain",
            "destroy",
            "snapshot",
        ]:
            raise ValueError(
                "Invalid removal_policy, expected retain, destroy or snapshot"
            )

    @staticmethod
    def from_pydict(name: Name, tags: Tags, pydict: dict) -> "S3BucketResource":
        S3BucketResource.__pydict_validation(pydict)

        removal_policies = {
            "retain": RemovalPolicy.RETAIN,
            "destroy": RemovalPolicy.DESTROY,
            "snapshot": RemovalPolicy.SNAPSHOT,
        }

        return S3BucketResource(
            name=name,
            tags=tags,
            removal_policy=removal_policies.get(
                pydict.get("removal_policy", "destroy"),
                RemovalPolicy.DESTROY,
            ),
        )

    def add_to_cdk(self, scope: Construct, cache: StackCache) -> None:
        bucket = s3_.Bucket(
            scope,
            self.name.value,
            bucket_name=self.name.value,
            removal_policy=self.removal_policy,
        )

        for tag_key, tag_value in self.tags.items:
            AwsTags.of(bucket).add(tag_key, tag_value)

        cache.add(self.name.value, bucket)
