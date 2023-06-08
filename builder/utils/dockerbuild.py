from dataclasses import dataclass
from enum import Enum
from json import load
from os import (
    makedirs,
    path,
)
from platform import system
from subprocess import check_call  # nosec
from typing import (
    List,
    Optional,
    Union,
)
from uuid import uuid4


class DockerBuilderMethod(Enum):
    LAMBDA = "lambda"
    GLUE = "glue"


@dataclass
class LambdaDockerProperties:
    root: str
    dockerfile: str
    source: str
    requirements: str
    shared: str

    def __post_init__(self) -> None:
        self.dockerfile = path.relpath(self.dockerfile, self.root)


@dataclass
class GlueDockerProperties:
    script: str
    dependencies_zip: str
    extra_jars_built: List[str]


@dataclass
class DockerAssetsPaths:
    assets_folder: str
    requirements: str
    extra_jars: str
    shared_modules: str
    script: str
    dependencies_zip: str
    extra_jars_built: List[str]

    def to_unix(self) -> None:
        self.extra_jars_built = [
            jar.replace("\\", "/") for jar in self.extra_jars_built
        ]

        fields = [
            field for field in self.__dict__ if field != "extra_jars_built"
        ]
        for field in fields:
            setattr(self, field, getattr(self, field).replace("\\", "/"))

    def to_system(self) -> None:
        system_name = system()
        if system_name == "Windows":
            self.extra_jars_built = [
                jar.replace("/", "\\") for jar in self.extra_jars_built
            ]

            fields = [
                field for field in self.__dict__ if field != "extra_jars_built"
            ]
            for field in fields:
                setattr(self, field, getattr(self, field).replace("/", "\\"))

        else:
            self.to_unix()

    def relative_to(self, root: str) -> "DockerAssetsPaths":
        return DockerAssetsPaths(
            assets_folder=path.relpath(self.assets_folder, root),
            requirements=path.relpath(self.requirements, root),
            extra_jars=path.relpath(self.extra_jars, root),
            shared_modules=path.relpath(self.shared_modules, root),
            script=path.relpath(self.script, root),
            dependencies_zip=path.relpath(self.dependencies_zip, root),
            extra_jars_built=[
                path.relpath(jar, root) for jar in self.extra_jars_built
            ],
        )


@dataclass
class DockerBuilder:
    root_path: str
    module_path: str

    def __post_init__(self) -> None:
        self.module_path = path.relpath(self.module_path, self.root_path)
        self.assets_path = path.join(
            self.root_path, ".builder_cache", self.module_path
        )
        self.shared_path = path.join(self.root_path, "shared")
        self.shared_manifest = path.join(self.shared_path, "manifest.json")

        self.dependencies_json = path.join(
            self.root_path, self.module_path, "requirements.json"
        )
        self.source_path = path.join(self.root_path, self.module_path, "src")
        self.index_py = path.join(self.source_path, "index.py")

        if not path.isdir(path.join(self.root_path, self.module_path)):
            raise FileNotFoundError(
                f'Folder "{self.module_path}" does not exist.'
            )

        if not path.isfile(self.dependencies_json):
            raise FileNotFoundError(
                f'File "requirements.json" does not exist in folder "{self.module_path}".'
            )

        if not path.isdir(self.source_path):
            raise FileNotFoundError(
                f'Folder "src" does not exist in folder "{self.module_path}".'
            )

        if not path.isfile(self.index_py):
            raise FileNotFoundError(
                f'File "index.py" does not exist in folder "{self.module_path}/src".'
            )

    def get_properties(
        self,
        method: DockerBuilderMethod,
        assets_paths: Optional[DockerAssetsPaths] = None,
    ) -> Union[LambdaDockerProperties, GlueDockerProperties]:
        if not assets_paths:
            assets_paths = self.__get_assets_paths()

        if method == DockerBuilderMethod.LAMBDA:
            dockerfile = self.__get_dockerfile(method)
            assets_paths.to_unix()
            return LambdaDockerProperties(
                root=self.root_path,
                dockerfile=dockerfile,
                source=self.source_path,
                requirements=assets_paths.requirements,
                shared=assets_paths.shared_modules,
            )
        elif method == DockerBuilderMethod.GLUE:
            assets_paths.to_system()
            return GlueDockerProperties(
                script=assets_paths.script,
                dependencies_zip=assets_paths.dependencies_zip,
                extra_jars_built=assets_paths.extra_jars_built,
            )
        else:
            raise ValueError(f'Invalid DockerBuilderMethod "{method}".')

    def build(
        self, method: DockerBuilderMethod
    ) -> Union[LambdaDockerProperties, GlueDockerProperties]:
        assets_paths = self.__get_assets_paths()

        if method == DockerBuilderMethod.LAMBDA:
            self.__parse_dependencies(assets_paths)
            return self.get_properties(method, assets_paths)

        elif method == DockerBuilderMethod.GLUE:
            self.__parse_dependencies(assets_paths)
            self.__dockerbuild_glue(assets_paths)
            return self.get_properties(method, assets_paths)

        else:
            raise ValueError(f'Invalid DockerBuilderMethod "{method}".')

    def __get_dockerfile(self, method: DockerBuilderMethod) -> str:
        dockerfiles = {
            DockerBuilderMethod.LAMBDA: "Dockerfile.lambda",
            DockerBuilderMethod.GLUE: "Dockerfile.glue",
        }

        dockerfile = dockerfiles.get(method)
        if dockerfile is None:
            raise ValueError(f'Invalid DockerBuilderMethod "{method}".')

        return path.join(self.root_path, "docker", dockerfile)

    def __get_assets_paths(self) -> DockerAssetsPaths:
        requirements = path.join(self.assets_path, "requirements.txt")
        extra_jars = path.join(self.assets_path, "extra_jars.txt")
        shared_modules = path.join(self.assets_path, "shared_modules.txt")
        dependencies_zip = path.join(self.assets_path, "requirements.zip")

        if path.isfile(extra_jars):
            with open(extra_jars, "r") as f:
                jars_built = f.read().splitlines()

            jars_built = [path.basename(jar) for jar in jars_built]
            jars_built = [
                path.join(self.assets_path, "jars", jar) for jar in jars_built
            ]
        else:
            jars_built = []

        return DockerAssetsPaths(
            assets_folder=self.assets_path,
            requirements=requirements,
            extra_jars=extra_jars,
            shared_modules=shared_modules,
            script=self.index_py,
            dependencies_zip=dependencies_zip,
            extra_jars_built=jars_built,
        )

    def __parse_dependencies(self, assets: DockerAssetsPaths) -> None:
        if not path.isdir(self.assets_path):
            makedirs(self.assets_path)

        requirements = []
        extra_jars = []
        shared_modules = []

        with open(self.dependencies_json, "r") as f:
            dependencies = load(f)

        with open(self.shared_manifest, "r") as f:
            manifest = load(f)

        requirements.extend(dependencies["packages"])
        extra_jars.extend(dependencies["extra_jars"])

        for module in dependencies["shared_modules"]:
            if module not in manifest:
                raise Exception(
                    f'Module "{module}" does not exist in shared manifest.'
                )

            module_folder = path.join(self.shared_path, module)

            if not path.isdir(module_folder):
                raise Exception(
                    f'Module "{module}" does not exist in shared folder.'
                )

            requirements.extend(manifest[module]["packages"])
            extra_jars.extend(manifest[module]["extra_jars"])
            shared_modules.append(module)

        requirements = list(set(requirements))
        extra_jars = list(set(extra_jars))
        shared_modules = list(set(shared_modules))

        relative_shared_path = path.relpath(self.shared_path, self.root_path)
        shared_modules = [
            path.join(relative_shared_path, module).replace("\\", "/")
            for module in shared_modules
        ]

        passed_requirements = []
        separators = ["==", ">=", "<=", ">", "<", "~="]
        separator_default = "=="
        for requirement in requirements:
            for s in separators:
                requirement = requirement.replace(s, separator_default)
            requirement_name = requirement.split(separator_default)[0]
            if requirement_name in passed_requirements:
                raise Exception(
                    f'Package "{requirement_name}" has two different versions in {self.dependencies_json}.'
                )
            passed_requirements.append(requirement_name)

        with open(assets.requirements, "w") as f:
            f.write("\n".join(requirements) + "\n")

        with open(assets.extra_jars, "w") as f:
            f.write("\n".join(extra_jars) + "\n")

        with open(assets.shared_modules, "w") as f:
            f.write("\n".join(shared_modules) + "\n")

    def __dockerbuild_glue(self, assets: DockerAssetsPaths) -> None:
        dockerfile = self.__get_dockerfile(DockerBuilderMethod.GLUE)
        image_name = str(uuid4()).replace("-", "")[0:10]
        relative_assets = assets.relative_to(self.root_path)
        relative_assets.to_unix()

        check_call(
            [
                "docker",
                "build",
                "-t",
                image_name,
                "-f",
                dockerfile,
                "--progress=plain",
                "--build-arg",
                f"REQUIREMENTS={relative_assets.requirements}",
                "--build-arg",
                f"EXTRA_JARS={relative_assets.extra_jars}",
                "--build-arg",
                f"SHARED_MODULES={relative_assets.shared_modules}",
                "--output",
                assets.assets_folder,
                ".",
            ],
            cwd=self.root_path,
        )  # nosec
