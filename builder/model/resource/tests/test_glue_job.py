import unittest
from os import path

from aws_cdk import (
    App,
    Duration,
    Stack,
)
from aws_cdk import aws_glue_alpha as glue_
from aws_cdk.assertions import Template

from builder.model.property.environment import Environment
from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.glue_job import GlueJobResource
from builder.model.resource.iam_role import RoleResource
from builder.utils.stack_cache import StackCache


class TestGlueJobResource(unittest.TestCase):
    def setUp(self) -> None:
        self.cache = StackCache()

        self.root = path.join(path.dirname(path.abspath(__file__)), "mock")
        self.source_folder = path.join(self.root, "code")

        self.name = Name("test-lambda", Environment.TEST)
        self.region = "us-east-1"
        self.account_id = "1234567890"
        self.tags = Tags()
        self.tags.add("tag1", "value1")

        self.role = RoleResource.from_pydict(
            name=self.name,
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

        self.glue_version = "glueetl"
        self.temp_uri = "s3://test-bucket/test-prefix"
        self.max_concurrent_runs = 25
        self.max_retries = 0
        self.timeout = Duration.minutes(30)
        self.job_bookmark = "disable"
        self.default_args = {
            "test": "test",
        }
        self.build_deps = False
        self.worker_type = glue_.WorkerType.G_2_X
        self.worker_count = 2
        self.max_capacity = 0.0625

    def test_init(self) -> None:
        job = GlueJobResource(
            name=self.name,
            tags=self.tags,
            glue_version=self.glue_version,
            root=self.root,
            source_folder=self.source_folder,
            role=self.role,
            temp_uri=self.temp_uri,
            max_retries=self.max_retries,
            max_concurrent_runs=self.max_concurrent_runs,
            timeout=self.timeout,
            job_bookmark=self.job_bookmark,
            default_args=self.default_args,
            build_deps=self.build_deps,
            worker_type=self.worker_type,
            worker_count=self.worker_count,
            max_capacity=self.max_capacity,
        )

        self.assertEqual(job.name, self.name)
        self.assertEqual(job.tags, self.tags)
        self.assertEqual(job.glue_version, self.glue_version)
        self.assertEqual(job.source_folder, self.source_folder)
        self.assertEqual(job.role, self.role)
        self.assertEqual(job.temp_uri, self.temp_uri)
        self.assertEqual(job.max_concurrent_runs, self.max_concurrent_runs)
        self.assertEqual(job.timeout, self.timeout)
        self.assertEqual(job.job_bookmark, self.job_bookmark)
        self.assertEqual(job.default_args, self.default_args)
        self.assertEqual(job.build_deps, self.build_deps)

    def test_from_pydict(self) -> None:
        pydict = {
            "glue_version": self.glue_version,
            "root": self.root,
            "source_folder": self.source_folder,
            "role": self.role,
            "temp_uri": self.temp_uri,
            "max_retries": self.max_retries,
            "max_concurrent_runs": self.max_concurrent_runs,
            "timeout_minutes": 30,
            "job_bookmark": self.job_bookmark,
            "default_args": self.default_args,
            "build_deps": self.build_deps,
        }

        job = GlueJobResource.from_pydict(
            name=self.name,
            tags=self.tags,
            pydict=pydict,
        )

        self.assertEqual(job.name, self.name)
        self.assertEqual(job.tags, self.tags)
        self.assertEqual(job.glue_version, self.glue_version)
        self.assertEqual(job.source_folder, self.source_folder)
        self.assertEqual(job.role, self.role)
        self.assertEqual(job.temp_uri, self.temp_uri)
        self.assertEqual(job.max_concurrent_runs, self.max_concurrent_runs)
        self.assertEqual(job.timeout.to_minutes(), 30)
        self.assertEqual(job.job_bookmark, self.job_bookmark)
        self.assertEqual(job.default_args, self.default_args)
        self.assertEqual(job.build_deps, self.build_deps)

    def test_add_to_cdk(self) -> None:
        app = App()
        stack = Stack(app, "test-stack")

        job = GlueJobResource(
            name=self.name,
            tags=self.tags,
            glue_version=self.glue_version,
            root=self.root,
            source_folder=self.source_folder,
            role=self.role,
            temp_uri=self.temp_uri,
            max_retries=self.max_retries,
            max_concurrent_runs=self.max_concurrent_runs,
            timeout=self.timeout,
            job_bookmark=self.job_bookmark,
            default_args=self.default_args,
            build_deps=self.build_deps,
            worker_type=self.worker_type,
            worker_count=self.worker_count,
            max_capacity=self.max_capacity,
        )

        job.add_to_cdk(stack, self.cache)

        template = Template.from_stack(stack)

        template.resource_count_is("AWS::Glue::Job", 1)
        template.has_resource_properties(
            "AWS::Glue::Job",
            {
                "Command": {
                    "Name": "glueetl",
                    "PythonVersion": "3",
                }
            },
        )
