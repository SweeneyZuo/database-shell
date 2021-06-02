#!/bin/bash

is_installed=`db test`

if [ $is_installed = 'test' ]; then
    echo 'db installed!!!'
    exit
fi

if [ ! -e dbtool ]; then
  mkdir dbtool
fi

# unzip zip file
cd dbtool && mv ../database-tool.zip . && unzip database-tool.zip && rm database-tool.zip

if [ -e database-tool ];then
  mv database-tool/* . && rm -rf database-tool
fi

if [ -e .git ];then
  rm -rf .git
fi

if [ -e .idea ];then
  rm -rf .idea
fi

if [ ! -e hist ]; then
  mkdir hist
fi

mv ./config/.db.info.template ./config/.db.info

# compile
python3 compiledb.py

proc_home=`pwd`

# create command file

echo """
#!/usr/bin/env bash
python3 ${proc_home}/database.pyc \"\$@\"
""" > db

chmod +x db

# set command env
username=`whoami`
if [ ! $username = 'root' ];then
echo """
# ADD db to PATH
export PATH=\$PATH:${proc_home}
""" >> ~/.bashrc
else
  mv db /usr/bin
fi

# delete py files
rm database.py compiledb.py test.py .gitignore
# delete sh files
rm install.sh
# install python env
pip3 install pymysql
pip3 install pymssql
pip3 install pymongo

# remove \r\n
dos2unix *

# test db
db test
