import py_compile

if __name__ == '__main__':
    py_compile.compile('database.py', optimize=1, cfile='database.pyc')
