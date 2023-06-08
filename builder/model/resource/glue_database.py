from dataclasses import dataclass
from typing import List

from aws_cdk import Tags as AwsTags
from aws_cdk import aws_glue as glue_
from constructs import Construct

from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.abstract import Resource
from builder.utils.stack_cache import StackCache
from builder.utils.validation import type_validation


@dataclass
class GlueDatabaseResource(Resource):
    name: Name
    tags: Tags
    catalog_id: str
    location_uri: str

    @staticmethod
    def __pydict_validation(pydict: dict) -> None:
        pydict_map = {
            "catalog_id": str,
            "location_uri": str,
        }

        type_validation(pydict_map, pydict)

    @staticmethod
    def from_pydict(
        name: Name, tags: Tags, pydict: dict
    ) -> "GlueDatabaseResource":
        GlueDatabaseResource.__pydict_validation(pydict)

        props = {
            "catalog_id": pydict["catalog_id"],
            "location_uri": pydict["location_uri"],
        }

        return GlueDatabaseResource(name=name, tags=tags, **props)

    def add_to_cdk(self, scope: Construct, cache: StackCache) -> None:
        database = glue_.CfnDatabase(
            scope,
            self.name.value,
            catalog_id=self.catalog_id,
            database_input=glue_.CfnDatabase.DatabaseInputProperty(
                name=self.name.value,
                location_uri=self.location_uri,
            ),
        )

        for key, value in self.tags.items:
            AwsTags.of(database).add(key, value)

        cache.add(self.name.value, database)
