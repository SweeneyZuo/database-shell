import abc
import os
import sys


class Cmd:
    def __init__(self, opt, **params):
        self._opt = opt
        self._params = params

    @property
    def opt(self):
        return self._opt

    @property
    def params(self):
        return self._params

    @abc.abstractmethod
    def exe(self):
        pass

    def get_proc_home(self):
        return os.path.dirname(os.path.abspath(sys.argv[0]))












