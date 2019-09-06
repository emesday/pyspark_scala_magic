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

    def test_udf_plus1(self):
        scala('''val plus1 = udf { x: Int => x + 1 }; spark.udf.register("plus1", plus1)''')
        from pyspark.sql.functions import expr
        res = self.spark.createDataFrame(range(10), "int").select(expr('plus1(value) value')).collect()
        res = [x.value for x in res]
        self.assertListEqual(res, list(range(1, 11)))


