import unittest

from builder.model.property.tags import Tags


class TestTags(unittest.TestCase):
    def setUp(self) -> None:
        self.tags = Tags()

    def test_add(self) -> None:
        self.tags.add("Key 1", "Value 1")
        self.assertEqual(self.tags.items, [("key-1", "Value 1")])

    def test_multiple_adds(self) -> None:
        self.tags.add("Key 1", "Value 1")
        self.tags.add("Key_2", "Value 2")
        self.assertEqual(
            self.tags.items, [("key-1", "Value 1"), ("key-2", "Value 2")]
        )

    def test_iteration(self) -> None:
        examples = [
            ("key-1", "value-1"),
            ("key-2", "value-2"),
        ]

        for key, value in examples:
            self.tags.add(key, value)

        for index, (key, value) in enumerate(self.tags.items):
            self.assertEqual(key, examples[index][0])
            self.assertEqual(value, examples[index][1])
