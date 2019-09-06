# pyspark_scala_magic

## Install
```
pip install --upgrade git+https://github.com/mskimm/pyspark_scala_magic.git
```

## Usage

Note: [3], [4], and [5] are the same.

```ipython
$ ipython
Python 3.6.9 |Anaconda custom (64-bit)| (default, Jul 30 2019, 13:42:17)
Type 'copyright', 'credits' or 'license' for more information
IPython 7.7.0 -- An enhanced Interactive Python. Type '?' for help.

In [1]: from pyspark_scala_magic import get_or_create, scala

In [2]: spark = get_or_create()
http://0.0.0.0:4040

In [3]: scala('''val plus1 = udf { x: Int => x + 1 }; spark.udf.register("plus1", plus1)''')
   ...:

plus1: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,IntegerType,Some(List(IntegerType)))
res0: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,IntegerType,Some(List(IntegerType)))

Out[3]: ''

In [4]: %scala val plus1 = udf { x: Int => x + 1 }; spark.udf.register("plus1", plus1)
plus1: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,IntegerType,Some(List(IntegerType)))
res1: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,IntegerType,Some(List(IntegerType)))

Out[4]: ''

In [5]: %%scala
   ...: val plus1 = udf { x: Int => x + 1 }
   ...: spark.udf.register("plus1", plus1)
   ...:
   ...:
plus1: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,IntegerType,Some(List(IntegerType)))
res2: org.apache.spark.sql.expressions.UserDefinedFunction = UserDefinedFunction(<function1>,IntegerType,Some(List(IntegerType)))   
   
In [6]: from pyspark.sql.functions import *

In [7]: spark.createDataFrame(range(10), "int").select(expr('plus1(`value`)')).show()
+----------+
|UDF(value)|
+----------+
|         1|
|         2|
|         3|
|         4|
|         5|
|         6|
|         7|
|         8|
|         9|
|        10|
+----------+
```
