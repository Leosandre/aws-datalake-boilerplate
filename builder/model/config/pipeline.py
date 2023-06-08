from dataclasses import dataclass
from typing import (
    Dict,
    List,
    Optional,
    Union,
)

from builder.model.property.environment import Environment
from builder.model.property.layer import DatalakeLayer
from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.utils.validation import type_validation


@dataclass
class PipelineLayerConfig:
    origin: DatalakeLayer
    target: DatalakeLayer


@dataclass
class S3PipelineTriggerConfig:
    prefix: str
    suffix: str


@dataclass
class EventRulePipelineTriggerConfig:
    source: str
    detail_type: str

    def to_dict(self) -> Dict[str, str]:
        return {
            "source": self.source,
            "detail_type": self.detail_type,
        }


@dataclass
class PipelineStepConfig:
    step_name: str


@dataclass
class ChoiceOptionPipelineConfig:
    variable: str
    equals: str
    next_step: str


@dataclass
class ChoicePipelineConfig(PipelineStepConfig):
    step_name: str
    choices: List[ChoiceOptionPipelineConfig]


@dataclass
class LambdaPipelineConfig(PipelineStepConfig):
    step_name: str
    module: str
    next_step: Optional[str] = None
    properties: Optional[Dict[str, str]] = None


@dataclass
class GluePipelineConfig(PipelineStepConfig):
    step_name: str
    module: str
    next_step: Optional[str] = None
    properties: Optional[Dict[str, str]] = None


@dataclass
class PipelineConfig:
    name: Name
    domain: str
    layers: PipelineLayerConfig
    triggers: List[
        Union[S3PipelineTriggerConfig, EventRulePipelineTriggerConfig]
    ]
    tags: Tags
    contract: Dict[str, str]
    steps: List[PipelineStepConfig]

    @staticmethod
    def __pydict_validation(pydict: dict) -> None:
        pydict_map = {
            "name": str,
            "domain": str,
            "layers": dict,
            "triggers": list,
            "contract": dict,
            "steps": dict,
            "tags?": dict,
        }

        type_validation(pydict_map, pydict)

    @staticmethod
    def from_pydict(env: Environment, pydict: dict) -> "PipelineConfig":
        PipelineConfig.__pydict_validation(pydict)

        tags = Tags()
        tags.add("pipeline", pydict["name"])
        tags.add("domain", pydict["domain"])
        if pydict.get("tags"):
            for key, value in pydict["tags"].items():
                tags.add(key, value)

        props = {
            "name": Name(pydict["name"], env),
            "domain": pydict["domain"],
            "layers": PipelineConfig.__get_layer_config(pydict["layers"]),
            "triggers": PipelineConfig.__get_triggers(pydict["triggers"]),
            "tags": tags,
            "contract": pydict["contract"],
            "steps": PipelineConfig.__get_steps(pydict["steps"]),
        }

        return PipelineConfig(**props)

    @staticmethod
    def __get_layer_config(layers: Dict[str, str]) -> PipelineLayerConfig:
        return PipelineLayerConfig(
            origin=DatalakeLayer(layers["origin"]),
            target=DatalakeLayer(layers["target"]),
        )

    @staticmethod
    def __get_triggers(
        trigger_list: list,
    ) -> List[Union[S3PipelineTriggerConfig, EventRulePipelineTriggerConfig]]:
        triggers: List[
            Union[S3PipelineTriggerConfig, EventRulePipelineTriggerConfig]
        ] = []
        for trigger_dict in trigger_list:
            trigger_key = list(trigger_dict.keys())[0]
            trigger_props = trigger_dict[trigger_key]

            if trigger_key == "s3":
                triggers.append(S3PipelineTriggerConfig(**trigger_props))

            elif trigger_key == "event_rule":
                triggers.append(EventRulePipelineTriggerConfig(**trigger_props))

        return triggers

    @staticmethod
    def __get_steps(step_dict: dict) -> List[PipelineStepConfig]:
        steps: List[PipelineStepConfig] = []
        for step_name, step_props in step_dict.items():
            type_ = step_props["type"]
            properties: dict = step_props["properties"]
            module = properties.pop("module", None)
            next_step = properties.pop("next", None)
            choices = properties.pop("choices", None)

            if type_ == "lambda":
                steps.append(
                    LambdaPipelineConfig(
                        step_name=step_name,
                        module=module,
                        next_step=next_step,
                        properties=properties,
                    )
                )
            elif type_ == "glue":
                steps.append(
                    GluePipelineConfig(
                        step_name=step_name,
                        module=module,
                        next_step=next_step,
                        properties=properties,
                    )
                )
            elif type_ == "choice":
                choice_objs = [
                    ChoiceOptionPipelineConfig(**choice) for choice in choices
                ]
                steps.append(
                    ChoicePipelineConfig(
                        step_name=step_name, choices=choice_objs
                    )
                )
            else:
                raise ValueError(f"Invalid step type: {type_}")

        return steps
