from dataclasses import dataclass
from typing import (
    List,
    Optional,
)

from aws_cdk import Tags as AwsTags
from aws_cdk import aws_iam as iam_
from constructs import Construct

from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.abstract import Resource
from builder.utils.stack_cache import StackCache
from builder.utils.validation import type_validation


@dataclass
class RoleResource(Resource):
    name: Name
    tags: Tags
    region: str
    account_id: str
    assumed_by: iam_.ServicePrincipal
    effect: iam_.Effect
    actions: List[str]
    resources: List[str]
    managed_policies: Optional[List[iam_.IManagedPolicy]] = None

    @staticmethod
    def __pydict_validation(pydict: dict) -> None:
        pydict_map = {
            "region": str,
            "account_id": str,
            "assumed_by": str,
            "effect": str,
            "actions": list,
            "resources?": list,
            "managed_policies?": list,
        }

        type_validation(pydict_map, pydict)

        if pydict["effect"] not in ["allow", "deny"]:
            raise ValueError("Invalid effect, expected allow or deny")

    @staticmethod
    def from_pydict(name: Name, tags: Tags, pydict: dict) -> "RoleResource":
        RoleResource.__pydict_validation(pydict)

        effect = (
            iam_.Effect.ALLOW
            if pydict["effect"] == "allow"
            else iam_.Effect.DENY
        )
        resources = pydict.get("resources", ["*"])

        props = {
            "region": pydict["region"],
            "account_id": pydict["account_id"],
            "assumed_by": iam_.ServicePrincipal(pydict["assumed_by"]),
            "effect": effect,
            "actions": pydict["actions"],
            "resources": resources,
        }

        if pydict.get("managed_policies"):
            managed_policies = []
            for policy in pydict["managed_policies"]:
                managed_policies.append(
                    iam_.ManagedPolicy.from_aws_managed_policy_name(policy)
                )

            props["managed_policies"] = managed_policies

        return RoleResource(name=name, tags=tags, **props)

    def add_to_cdk(self, scope: Construct, cache: StackCache) -> None:
        role = iam_.Role(
            scope,
            self.name.value,
            role_name=self.name.value,
            assumed_by=self.assumed_by,
        )

        role.add_to_policy(
            iam_.PolicyStatement(
                effect=self.effect,
                actions=self.actions,
                resources=self.resources,
            )
        )

        if self.managed_policies:
            for policy in self.managed_policies:
                role.add_managed_policy(policy)

        for tag_key, tag_value in self.tags.items:
            AwsTags.of(role).add(tag_key, tag_value)

        cache.add(self.name.value, role)

    @property
    def arn(self) -> str:
        return f"arn:aws:iam::{self.account_id}:role/{self.name.value}"
