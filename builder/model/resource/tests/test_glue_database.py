import unittest

from aws_cdk import (
    App,
    Stack,
)
from aws_cdk.assertions import Template

from builder.model.property.environment import Environment
from builder.model.property.name import Name
from builder.model.property.tags import Tags
from builder.model.resource.glue_database import GlueDatabaseResource
from builder.utils.stack_cache import StackCache


class TestGlueDatabaseResource(unittest.TestCase):
    def setUp(self) -> None:
        self.cache = StackCache()

        self.name = Name("test-glue-database", Environment.TEST)
        self.tags = Tags()
        self.tags.add("tag1", "value1")

        self.catalog_id = "1234567890"
        self.location_uri = "s3://test-bucket/test-prefix"

    def test_init(self) -> None:
        database = GlueDatabaseResource(
            name=self.name,
            tags=self.tags,
            catalog_id=self.catalog_id,
            location_uri=self.location_uri,
        )

        self.assertEqual(database.name, self.name)
        self.assertEqual(database.tags, self.tags)
        self.assertEqual(database.catalog_id, self.catalog_id)
        self.assertEqual(database.location_uri, self.location_uri)

    def test_from_pydict(self) -> None:
        pydict = {
            "catalog_id": self.catalog_id,
            "location_uri": self.location_uri,
        }

        database = GlueDatabaseResource.from_pydict(
            name=self.name,
            tags=self.tags,
            pydict=pydict,
        )

        self.assertEqual(database.name, self.name)
        self.assertEqual(database.tags, self.tags)
        self.assertEqual(database.catalog_id, self.catalog_id)
        self.assertEqual(database.location_uri, self.location_uri)

    def test_add_to_cdk(self) -> None:
        app = App()
        stack = Stack(app, "test-stack")

        database = GlueDatabaseResource(
            name=self.name,
            tags=self.tags,
            catalog_id=self.catalog_id,
            location_uri=self.location_uri,
        )

        database.add_to_cdk(stack, self.cache)

        template = Template.from_stack(stack)

        template.resource_count_is("AWS::Glue::Database", 1)
        template.has_resource_properties(
            "AWS::Glue::Database",
            {
                "CatalogId": self.catalog_id,
                "DatabaseInput": {
                    "Name": self.name.value,
                    "LocationUri": self.location_uri,
                },
            },
        )
