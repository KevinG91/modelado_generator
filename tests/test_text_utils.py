import unittest
from utils import normalizer, sorter


class TestTextUtils(unittest.TestCase):
    def test_normalizer(self):
        self.assertEqual(normalizer("Test String"), "test_string")
        self.assertEqual(normalizer("Test-String"), "teststring")
        self.assertEqual(normalizer("  Test  String  "), "test_string")

    def test_sorter(self):
        origin = ["Field 1", "Field 2", "Field 3"]
        normalized = ["field_1", "field_2", "field_3"]
        alias = ["field_1", "field_2", "field_3"]

        result = sorter(origin, normalized, alias)
        self.assertEqual(result, ["field_1", "field_2", "field_3"])

        # Test with missing aliases
        alias = ["field_1", "field_3"]
        result = sorter(origin, normalized, alias)
        self.assertEqual(result, ["field_1", "field_2", "field_3"])
