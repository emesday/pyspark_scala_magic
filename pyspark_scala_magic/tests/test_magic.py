import unittest

from ..config import Config
from ..magic import get_or_create, scala


class TestMagic(unittest.TestCase):
    def setUp(self):
        c = Config()
        c.spark.app.name = "test_magic"
        self.spark = get_or_create()

    def test_print_value(self):
        x = 100
        res = scala("val x = {{ x }}; println(x)")
        self.assertEqual(res, "100")
