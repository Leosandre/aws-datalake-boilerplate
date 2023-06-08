from abc import (
    ABC,
    abstractmethod,
)
from dataclasses import dataclass
from typing import (
    Any,
    Dict,
    List,
    Optional,
)

from aws_cdk import Tags as AwsTags
from aws_cdk import aws_stepfunctions as sfn
from constructs import Construct

from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.abstract import Resource
from builder.model.resource.glue_job import GlueJobResource
from builder.model.resource.iam_role import RoleResource
from builder.model.resource.lambda_ import LambdaResource
from builder.utils.stack_cache import StackCache
from builder.utils.validation import type_validation


@dataclass
class StepProps(ABC):
    step_name: str

    @abstractmethod
    def to_pydict(self) -> dict:
        pass


@dataclass
class CatchStepProps(StepProps):
    step_name: str
    resource: LambdaResource

    def to_pydict(self) -> dict:
        return {
            "Type": "Task",
            "Resource": "arn:aws:states:::lambda:invoke",
            "Parameters": {
                "FunctionName": self.resource.name.value,
                "Payload.$": "$",
            },
            "End": True,
        }


@dataclass
class LambdaStepProps(StepProps):
    step_name: str
    resource: LambdaResource
    catch_to: CatchStepProps
    next_step: Optional[StepProps] = None

    def to_pydict(self) -> dict:
        pydict: Dict[str, Any] = {
            "Type": "Task",
            "Resource": self.resource.arn,
            "Catch": [
                {
                    "ErrorEquals": ["States.ALL"],
                    "Next": self.catch_to.step_name,
                }
            ],
        }

        if self.next_step:
            pydict["Next"] = self.next_step.step_name
        else:
            pydict["End"] = True

        return pydict


@dataclass
class GlueStepProps(StepProps):
    step_name: str
    resource: GlueJobResource
    catch_to: CatchStepProps
    next_step: Optional[StepProps] = None
    args: Optional[list] = None

    def to_pydict(self) -> dict:
        args = {}
        if self.args:
            for arg in self.args:
                args[f"--{arg}.$"] = f"$.{arg}"

        pydict: Dict[str, Any] = {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
                "JobName": self.resource.name.value,
                "Arguments": args,
            },
            "Catch": [
                {
                    "ErrorEquals": ["States.ALL"],
                    "Next": self.catch_to.step_name,
                }
            ],
            "ResultPath": sfn.JsonPath.DISCARD,
        }

        if self.next_step:
            pydict["Next"] = self.next_step.step_name
        else:
            pydict["End"] = True

        return pydict


@dataclass
class ChoiceStepProps(StepProps):
    step_name: str
    choices: List[dict]
    catch_to: CatchStepProps

    def to_pydict(self) -> dict:
        parsed_choices = []
        for choice in self.choices:
            variable = f"$.{choice['variable']}"
            string_equals = choice["equals"]
            next_step = choice["next_step"]

            parsed_choices.append(
                {
                    "Variable": variable,
                    "StringEquals": string_equals,
                    "Next": next_step,
                }
            )

        return {
            "Type": "Choice",
            "Choices": parsed_choices,
            "Default": self.catch_to.step_name,
        }


@dataclass
class StepFunctionResource(Resource):
    name: Name
    tags: Tags
    role: RoleResource
    steps: List[StepProps]

    @staticmethod
    def __pydict_validation(pydict: dict) -> None:
        pydict_map = {
            "role": RoleResource,
            "steps": list,
            "catch_lambda": LambdaResource,
        }

        type_validation(pydict_map, pydict)

    @staticmethod
    def from_pydict(
        name: Name, tags: Tags, pydict: dict
    ) -> "StepFunctionResource":
        StepFunctionResource.__pydict_validation(pydict)
        steps_obj: List[StepProps] = []
        step_names: List[str] = []

        error_catch_step = CatchStepProps(
            step_name="ErrorCatch",
            resource=pydict["catch_lambda"],
        )

        steps_obj.append(error_catch_step)
        step_names.append("ErrorCatch")

        step_props: dict
        for step_props in reversed(pydict["steps"]):
            step: StepProps

            if step_props.get("next") and step_props["next"] not in step_names:
                raise ValueError(f"Step '{step_props['next']}' does not exist")

            step_types = {
                "lambda": LambdaStepProps,
                "glue": GlueStepProps,
                "choice": ChoiceStepProps,
            }

            step_type = step_props.pop("type")
            step_props["catch_to"] = error_catch_step
            if step_props.get("next_step"):
                for obj in steps_obj:
                    if obj.step_name == step_props["next_step"]:
                        step_props["next_step"] = obj
                        break

            step = step_types[step_type](**step_props)

            steps_obj.append(step)
            step_names.append(step.step_name)

        return StepFunctionResource(
            name=name, tags=tags, role=pydict["role"], steps=steps_obj
        )

    def add_to_cdk(self, scope: Construct, cache: StackCache) -> None:
        states = {}
        for step in self.steps:
            states[step.step_name] = step.to_pydict()

        state_machine = sfn.CfnStateMachine(
            scope,
            self.name.value,
            state_machine_name=self.name.value,
            role_arn=self.role.arn,
            definition={"StartAt": self.steps[-1].step_name, "States": states},
        )

        for tag_key, tag_value in self.tags.items:
            AwsTags.of(state_machine).add(tag_key, tag_value)
