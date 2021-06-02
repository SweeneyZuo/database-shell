import os
import py_compile

dirs = ['.', 'cmd', 'core']


def compile(path):
    for i in os.listdir(path):
        path2 = os.path.join(path, i)  # 拼接绝对路径
        if os.path.isdir(path2):  # 判断如果是文件夹,调用本身
            compile(path2)
        elif i.endswith('.py'):
            py_compile.compile(path2, path2 + "c")
            os.remove(path2)


for dir in dirs:
    compile(dir)
