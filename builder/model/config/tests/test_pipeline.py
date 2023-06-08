import unittest

from builder.model.config.pipeline import (
    EventRulePipelineTriggerConfig,
    PipelineConfig,
    PipelineLayerConfig,
    PipelineStepConfig,
    S3PipelineTriggerConfig,
)
from builder.model.property.environment import Environment


class TestPipelineConfig(unittest.TestCase):
    def test_from_pydict(self) -> None:
        pydict = {
            "layers": {"origin": "raw", "target": "trusted"},
            "domain": "example",
            "steps": {
                "RouteFile": {
                    "type": "lambda",
                    "properties": {
                        "timeout_seconds": 60,
                        "memory_size": 128,
                        "module": "route_file",
                        "next_step": "RouteChoice",
                    },
                },
                "RouteChoice": {
                    "type": "choice",
                    "properties": {
                        "choices": [
                            {
                                "variable": "route",
                                "equals": "type1",
                                "next_step": "ProcessType1",
                            },
                            {
                                "variable": "route",
                                "equals": "type2",
                                "next_step": "ProcessType2",
                            },
                        ]
                    },
                },
                "ProcessType2": {
                    "type": "glue",
                    "properties": {
                        "timeout_minutes": 30,
                        "glue_version": "pythonshell",
                        "max_concurrent_runs": 25,
                        "module": "process_type2",
                        "next_step": "AddToDatabase",
                    },
                },
                "AddToDatabase": {
                    "type": "lambda",
                    "properties": {
                        "timeout_seconds": 60,
                        "memory_size": 128,
                        "module": "add_to_database",
                    },
                },
                "ProcessType1": {
                    "type": "glue",
                    "properties": {
                        "timeout_minutes": 30,
                        "glue_version": "pythonshell",
                        "max_concurrent_runs": 25,
                        "module": "process_type1",
                        "next_step": "AddToDatabase",
                    },
                },
            },
            "name": "pipeline_example",
            "tags": {"version": "1.0.0"},
            "triggers": [
                {"s3": {"prefix": "example/", "suffix": ".json"}},
                {
                    "event_rule": {
                        "source": ["aws.s3"],
                        "detail_type": ["AWS API Call via CloudTrail"],
                    }
                },
            ],
            "contract": {
                "origin_key": "str",
                "target_bucket": "str",
                "database": "str",
                "target_key": "str",
                "route": "str",
                "table": "str",
                "origin_bucket": "str",
            },
        }

        pipeline = PipelineConfig.from_pydict(Environment.TEST, pydict)

        self.assertIsInstance(pipeline, PipelineConfig)
        self.assertIsInstance(pipeline.layers, PipelineLayerConfig)
        self.assertIsInstance(pipeline.triggers, list)
        self.assertIsInstance(pipeline.triggers[0], S3PipelineTriggerConfig)
        self.assertIsInstance(
            pipeline.triggers[1], EventRulePipelineTriggerConfig
        )
        self.assertIsInstance(pipeline.steps, list)
        self.assertIsInstance(pipeline.steps[0], PipelineStepConfig)
