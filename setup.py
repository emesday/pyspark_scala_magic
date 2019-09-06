from setuptools import setup

setup(
    name="pyspark_scala_magic",
    version="0.0.1",
    packages=["pyspark_scala_magic"],
    install_requires=[
        'spylon',
        'spylon-kernel',
        'jinja2'
    ])