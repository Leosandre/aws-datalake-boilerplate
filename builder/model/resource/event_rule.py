from dataclasses import dataclass
from typing import List

from aws_cdk import Tags as AwsTags
from aws_cdk import aws_events as events_
from aws_cdk import aws_events_targets as targets_
from constructs import Construct

from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.abstract import Resource
from builder.model.resource.lambda_ import LambdaResource
from builder.utils.stack_cache import StackCache
from builder.utils.validation import type_validation


@dataclass
class EventRuleResource(Resource):
    name: Name
    tags: Tags
    targets: List[Resource]
    event_pattern: dict

    @staticmethod
    def __pydict_validation(pydict: dict) -> None:
        pydict_map = {
            "targets": list,
            "event_pattern": dict,
        }

        type_validation(pydict_map, pydict)

    @staticmethod
    def from_pydict(
        name: Name, tags: Tags, pydict: dict
    ) -> "EventRuleResource":
        EventRuleResource.__pydict_validation(pydict)

        return EventRuleResource(
            name=name,
            tags=tags,
            targets=pydict["targets"],
            event_pattern=pydict["event_pattern"],
        )

    def add_to_cdk(self, scope: Construct, cache: StackCache) -> None:
        targets = []
        for target in self.targets:
            cached_target = cache.get(target.name.value)

            if isinstance(target, LambdaResource):
                targets.append(targets_.LambdaFunction(cached_target))

            else:
                raise NotImplementedError(
                    f"Target type not implemented: {type(target)}"
                )

        rule = events_.Rule(
            scope,
            self.name.value,
            rule_name=self.name.value,
            event_pattern=events_.EventPattern(**self.event_pattern),
            targets=targets,
        )

        for tag_key, tag_value in self.tags.items:
            AwsTags.of(rule).add(tag_key, tag_value)
