"""Concurrency abstractions.

Allows concurrency primitives to be abstracted such that both eventlet and
gevent can be used to run nameko. The backend is chosen at import time and may
be set using the global nameko config object. Valid values are "eventlet" and
"gevent".

To write nameko code and extensions that is compatible with multiple
concurrency backend, use this compatability layer package instead of importing
from a specific backend:

NO:

>>> from eventlet import sleep

YES:

>>> from nameko.concurrency import sleep

"""
import importlib
import os
import sys
from types import ModuleType

from nameko import config
from nameko.constants import CONCURRENCY_BACKEND_CONFIG_KEY, DEFAULT_CONCURRENCY_BACKEND

_mode = os.environ.get(
    CONCURRENCY_BACKEND_CONFIG_KEY,
    config.get(CONCURRENCY_BACKEND_CONFIG_KEY, DEFAULT_CONCURRENCY_BACKEND)
)

if _mode not in ('eventlet', 'gevent'):
    raise NotImplementedError(
        "Concurrency backend '{}' is not available. Choose 'eventlet' or 'gevent'."
        .format(_mode)
    )

_backend_module = 'nameko.concurrency.{}_backend'.format(_mode)


class module(ModuleType):
    """Module lookalike to import objects dynamically from submodules.

    See `werkzeug.__init__` for more complex example of this pattern.
    """
    mode = _mode

    def __getattr__(self, name):
        module = importlib.import_module(_backend_module)
        value = getattr(module, name)
        setattr(self, name, value)
        return value


# setup the new module and patch it into the dict of loaded modules
module_name = "nameko.concurrency"
old_module = sys.modules[module_name]
new_module = sys.modules[module_name] = module(module_name)

new_module.__dict__.update(
    {
        "__file__": __file__,
        "__package__": module_name,
        "__doc__": __doc__,
        '__path__': __path__,  # noqa: F821
    }
)
