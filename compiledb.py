import os
import py_compile

from Cython.Build import cythonize
from setuptools import setup, Extension

dirs = ['cmd', 'core']


def pyc_compile(path):
    for i in os.listdir(path):
        path2 = os.path.join(path, i)  # 拼接绝对路径
        if os.path.isdir(path2):  # 判断如果是文件夹,调用本身
            pyc_compile(path2)
        elif i.endswith('.py'):
            py_compile.compile(path2, path2 + "c")
            os.remove(path2)


def c_compile(name, modules):
    """
    使用Cython进行编译
    :param str name: 项目名称
    :param list modules: [(module_name, source_file_path), (..., ...)]
    :return:
    """
    module_list = []
    for m in modules:
        module_list.append(
            Extension(name=m[0], sources=[m[1]])
        )
    setup(
        name=name,
        ext_modules=cythonize(
            module_list=module_list,
            language_level=3,  # python3
        ),
        zip_safe=False,
        install_requires=[
            'Cython',
        ]
    )


def find_py_file():
    def _find(path):
        for i in os.listdir(path):
            path2 = os.path.join(path, i)
            if os.path.isdir(path2):
                _find(path2)
            elif i.endswith('.py'):
                res.append(('{}.{}'.format(path, i[:len(i) - 3]), path2))

    res = []
    for d in dirs:
        _find(d)
    return res


def remove_file(path, suffix):
    for i in os.listdir(path):
        path2 = os.path.join(path, i)  # 拼接绝对路径
        if os.path.isdir(path2):  # 判断如果是文件夹,调用本身
            remove_file(path2, suffix)
        elif i.endswith(suffix):
            os.remove(path2)


if __name__ == '__main__':
    files = find_py_file()
    print(files)
    c_compile('database', files)

    for d in dirs:
        remove_file(d, '.py')
        remove_file(d, '.pyc')
        remove_file(d, '.c')
    pyc_compile('.')
