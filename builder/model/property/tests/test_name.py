import unittest

from builder.model.property.environment import Environment
from builder.model.property.name import Name


class TestName(unittest.TestCase):
    def test_valid_env(self) -> None:
        valid_name = Name("test_name", Environment.TEST)
        self.assertEqual(valid_name.env, Environment.TEST)

    def test_add_prefix(self) -> None:
        name = Name("test_name", Environment.TEST)
        prefixed_name = name.add_prefix("prefix")
        self.assertEqual(prefixed_name.value, "prefix-test-name-test")

    def test_add_suffix(self) -> None:
        name = Name("test_name", Environment.TEST)
        suffixed_name = name.add_suffix("suffix")
        self.assertEqual(suffixed_name.value, "test-name-suffix-test")
