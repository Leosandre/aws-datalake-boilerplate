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
from builder.model.resource.glue_job import GlueJobResource
from builder.model.resource.iam_role import RoleResource
from builder.model.resource.lambda_ import LambdaResource
from builder.model.resource.step_function import StepFunctionResource
from builder.utils.stack_cache import StackCache


class TestStepFunctionResource(unittest.TestCase):
    def setUp(self) -> None:
        self.cache = StackCache()

        self.root = path.join(path.dirname(path.abspath(__file__)), "mock")
        self.source_folder = path.join(self.root, "code")

        self.region = "us-east-1"
        self.account_id = "1234567890"
        self.tags = Tags()
        self.tags.add("tag1", "value1")

        self.role_name = Name("test-role", Environment.TEST)
        self.role = RoleResource.from_pydict(
            name=self.role_name,
            tags=self.tags,
            pydict={
                "region": self.region,
                "account_id": self.account_id,
                "assumed_by": "lambda.amazonaws.com",
                "effect": "allow",
                "actions": ["s3:*"],
                "resources": ["*"],
            },
        )

        self.lambda_name = Name("test-lambda", Environment.TEST)
        self.lambda_ = LambdaResource.from_pydict(
            name=self.lambda_name,
            tags=self.tags,
            pydict={
                "region": self.region,
                "account_id": self.account_id,
                "timeout": 30,
                "memory_size": 256,
                "root": self.root,
                "source_folder": self.source_folder,
                "role": self.role,
                "environment": {"VAR1": "value1"},
            },
        )

        self.glue_name = Name("test-glue", Environment.TEST)
        self.glue = GlueJobResource.from_pydict(
            name=self.glue_name,
            tags=self.tags,
            pydict={
                "glue_version": "pythonshell",
                "root": self.root,
                "source_folder": self.source_folder,
                "role": self.role,
                "temp_uri": "s3://test-bucket/test-prefix",
                "build_deps": False,
            },
        )

        self.steps = [
            {
                "step_name": "step1",
                "type": "lambda",
                "resource": self.lambda_,
                "next_step": "step2",
            },
            {
                "step_name": "step2",
                "type": "choice",
                "choices": [
                    {
                        "variable": "x",
                        "equals": "1",
                        "next_step": "step3",
                    },
                    {
                        "variable": "x",
                        "equals": "2",
                        "next_step": "step4",
                    },
                ],
            },
            {
                "step_name": "step3",
                "type": "glue",
                "resource": self.glue,
                "next_step": "step4",
                "args": ["arg1", "arg2"],
            },
            {
                "step_name": "step4",
                "type": "lambda",
                "resource": self.lambda_,
            },
        ]

    def test_add_to_cdk(self) -> None:
        app = App()
        stack = Stack(app, "test-stack")

        pydict = {
            "role": self.role,
            "steps": self.steps,
            "catch_lambda": self.lambda_,
        }

        sfn = StepFunctionResource.from_pydict(
            name=self.lambda_name,
            tags=self.tags,
            pydict=pydict,
        )

        sfn.add_to_cdk(stack, self.cache)

        template = Template.from_stack(stack)

        template.resource_count_is("AWS::StepFunctions::StateMachine", 1)
        template.has_resource_properties(
            "AWS::StepFunctions::StateMachine",
            {
                "Definition": {
                    "StartAt": "step1",
                }
            },
        )
