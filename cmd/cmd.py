import abc
import os
import sys


class EntityAutoRegistryMetaClass(type):
    __model_type_attr_name = 'name'
    __registered_map = {}

    def __new__(mcs, *args, **kwargs):
        cls = super(EntityAutoRegistryMetaClass, mcs).__new__(mcs, *args, **kwargs)
        mcs.__register_model_entity(cls)
        return cls

    @classmethod
    def __register_model_entity(mcs, cls):
        cls_entity_name = getattr(cls, mcs.__model_type_attr_name, None)
        if not cls_entity_name:
            return
        mcs.__registered_map[cls_entity_name] = cls

    def all(cls):
        return type(cls).__registered_map

    def get(cls, name: str):
        return cls.all().get(name, None)


class Cmd(metaclass=EntityAutoRegistryMetaClass):
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
