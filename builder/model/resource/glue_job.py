from dataclasses import dataclass
from os import path
from typing import (
    Any,
    Dict,
    Optional,
    Tuple,
)
from uuid import uuid4

from aws_cdk import Duration
from aws_cdk import Tags as AwsTags
from aws_cdk import aws_glue_alpha as glue_
from aws_cdk import aws_iam as iam_
from constructs import Construct

from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.abstract import Resource
from builder.model.resource.iam_role import RoleResource
from builder.utils.dockerbuild import (
    DockerBuilder,
    DockerBuilderMethod,
    GlueDockerProperties,
)
from builder.utils.stack_cache import StackCache
from builder.utils.validation import type_validation


@dataclass
class GlueJobResource(Resource):
    name: Name
    tags: Tags
    glue_version: str
    root: str
    source_folder: str
    role: RoleResource
    temp_uri: str
    max_retries: int
    max_concurrent_runs: int
    timeout: Duration
    job_bookmark: str
    worker_type: glue_.WorkerType
    worker_count: int
    max_capacity: float
    default_args: Optional[Dict[str, str]] = None
    build_deps: bool = True

    @staticmethod
    def __pydict_validation(pydict: dict) -> None:
        pydict_map = {
            "glue_version": str,
            "root": str,
            "source_folder": str,
            "role": RoleResource,
            "temp_uri": str,
            "default_args?": dict,
            "max_retries?": int,
            "max_concurrent_runs?": int,
            "timeout_minutes?": int,
            "job_bookmark?": str,
            "worker_type?": str,
            "worker_count?": int,
            "max_capacity?": float,
            "build_deps?": bool,
        }

        type_validation(pydict_map, pydict)

        if not path.isdir(pydict["source_folder"]):
            raise NotADirectoryError(
                f"Folder not found: {pydict['source_folder']}"
            )

        allowed_glue_versions = [
            "pythonshell",
            "glueetl",
        ]

        if pydict["glue_version"] not in allowed_glue_versions:
            raise ValueError(
                f"Invalid glue version, expected one of {allowed_glue_versions}"
            )

        allowed_job_bookmarks = ["enable", "disable", "pause"]

        if (
            pydict.get("job_bookmark")
            and pydict["job_bookmark"] not in allowed_job_bookmarks
        ):
            raise ValueError(
                f"Invalid job bookmark, expected one of {allowed_job_bookmarks}"
            )

    @staticmethod
    def from_pydict(name: Name, tags: Tags, pydict: dict) -> "GlueJobResource":
        GlueJobResource.__pydict_validation(pydict)

        return GlueJobResource(
            name=name,
            tags=tags,
            glue_version=pydict["glue_version"],
            root=pydict["root"],
            source_folder=pydict["source_folder"],
            role=pydict["role"],
            temp_uri=pydict["temp_uri"],
            max_retries=pydict.get("max_retries", 0),
            max_concurrent_runs=pydict.get("max_concurrent_runs", 1),
            timeout=Duration.minutes(pydict.get("timeout_minutes", 5)),
            job_bookmark=pydict.get("job_bookmark", "disable"),
            default_args=pydict.get("default_args"),
            worker_type=getattr(
                glue_.WorkerType, pydict.get("worker_type", "G_2_X")
            ),
            worker_count=pydict.get("worker_count", 2),
            max_capacity=pydict.get("max_capacity", 0.0625),
            build_deps=pydict.get("build_deps", True),
        )

    def add_to_cdk(self, scope: Construct, cache: StackCache) -> None:
        kwargs: Dict[str, Any]
        glue_versions = {
            "pythonshell": self.__pythonshell_executable,
            "glueetl": self.__glueetl_executable,
        }

        docker_builder = DockerBuilder(
            root_path=self.root,
            module_path=path.join(self.source_folder),
        )

        docker_props: GlueDockerProperties
        if self.build_deps:
            docker_props = docker_builder.build(DockerBuilderMethod.GLUE)  # type: ignore
        else:
            docker_props = docker_builder.get_properties(
                DockerBuilderMethod.GLUE
            )  # type: ignore

        executable, kwargs = glue_versions[self.glue_version](docker_props)

        default_args = {
            "--job-bookmark-option": f"job-bookmark-{self.job_bookmark}",
            "--TempDir": self.temp_uri,
        }

        if self.default_args:
            for key, value in self.default_args.items():
                default_args[f"--{key}"] = value

        random_id = "a" + str(uuid4())[0:8]
        role = iam_.Role.from_role_arn(scope, random_id, self.role.arn)

        job = glue_.Job(
            scope,
            self.name.value,
            job_name=self.name.value,
            executable=executable,
            default_arguments=default_args,
            max_retries=self.max_retries,
            max_concurrent_runs=self.max_concurrent_runs,
            role=role,
            timeout=self.timeout,
            **kwargs,
        )

        for tag_key, tag_value in self.tags.items:
            AwsTags.of(job).add(tag_key, tag_value)

    def __pythonshell_executable(
        self, build_props: GlueDockerProperties
    ) -> Tuple[glue_.JobExecutable, Dict[str, Any]]:
        executable_kwargs: Dict[str, Any] = {}
        executable_kwargs["extra_python_files"] = [
            glue_.Code.from_asset(build_props.dependencies_zip)
        ]

        executable = glue_.JobExecutable.python_shell(
            glue_version=glue_.GlueVersion.V1_0,
            python_version=glue_.PythonVersion.THREE_NINE,
            script=glue_.Code.from_asset(build_props.script),
            **executable_kwargs,
        )

        kwargs = {"max_capacity": self.max_capacity}
        return executable, kwargs

    def __glueetl_executable(
        self, build_props: GlueDockerProperties
    ) -> Tuple[glue_.JobExecutable, Dict[str, Any]]:
        executable_kwargs: Dict[str, Any] = {}
        executable_kwargs["extra_python_files"] = [
            glue_.Code.from_asset(build_props.dependencies_zip)
        ]
        executable_kwargs["extra_jars"] = [
            glue_.Code.from_asset(j) for j in build_props.extra_jars_built
        ]

        executable = glue_.JobExecutable.python_etl(
            glue_version=glue_.GlueVersion.V3_0,
            python_version=glue_.PythonVersion.THREE,
            script=glue_.Code.from_asset(build_props.script),
            **executable_kwargs,
        )

        kwargs = {
            "worker_type": self.worker_type,
            "worker_count": self.worker_count,
            "continuous_logging": glue_.ContinuousLoggingProps(enabled=True),
        }
        return executable, kwargs
