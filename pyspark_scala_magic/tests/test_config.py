import unittest

from ..config import Config


class TestConfig(unittest.TestCase):
    def setUp(self):
        self.c = Config()

    def test_a(self):
        self.c.spark.app.name = "spark_app_name"
        self.assertEqual(self.c.spark.app.name, "spark_app_name")

