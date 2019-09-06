import sys
from io import StringIO

from IPython.core.interactiveshell import InteractiveShell
from IPython.core.magic import register_line_cell_magic
from jinja2 import Template
from spylon.spark.launcher import SparkConfiguration
from spylon_kernel.scala_interpreter import init_spark, get_scala_interpreter

from .config import Config

_spark = None
_interpreter = None
_stdout = None
_io = StringIO()


def get_or_create(c=None, stdout=sys.stdout, stderr=sys.stderr):
    """
    :param c: Config or dictionary
    :param stdout:
    :param stderr:
    :return: sparkSession
    """
    global _spark, _interpreter, _stdout
    if _spark is not None:
        return _spark

    if c is None:
        c = {}
    elif isinstance(c, Config):
        c = c.as_dict()

    conf = SparkConfiguration()
    for key, value in c.items():
        if key.startswith('spark.'):
            conf.conf.set(key, value)

    init_spark(conf)
    _interpreter = get_scala_interpreter()
    if _io not in _interpreter._stdout_handlers:
        _interpreter.register_stdout_handler(_io.write)
    if stdout and stdout.write not in _interpreter._stdout_handlers:
        _interpreter.register_stdout_handler(stdout.write)
    if stderr and stderr.write not in _interpreter._stderr_handlers:
        _interpreter.register_stderr_handler(stderr.write)
    _spark = _interpreter.spark_session
    _stdout = stdout
    print(_interpreter.web_ui_url.split(',')[0])
    return _spark


def _scala(code):
    global _interpreter, _stdout, _io
    _io.truncate(0)
    _io.seek(0)
    if _interpreter is None:
        raise RuntimeError("no spark session. call get_or_crate first!")

    ns = {}
    if InteractiveShell.initialized():
        ns.update(InteractiveShell.instance().user_ns)
    try:
        frame = sys._getframe(2)
    except ValueError:
        pass
    else:
        ns.update(frame.f_locals)
    if 'self' in ns:
        del ns['self']
    code = Template(code).render(**ns)
    res = _interpreter.interpret(code)
    if _stdout is not None:
        _stdout.write(res + '\n')
    _io.seek(0)
    ret = _io.read().strip().rsplit('\n', 1)[-1]
    return ret


if InteractiveShell.initialized():
    @register_line_cell_magic
    def scala(line, cell=None):
        if cell is not None:
            return _scala(cell)
        else:
            return _scala(line)
else:
    def scala(line, cell=None):
        if cell is not None:
            return _scala(cell)
        else:
            return _scala(line)


