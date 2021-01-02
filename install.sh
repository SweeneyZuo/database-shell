# !/bin/bash

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

if [ ! -e hist ]; then
  mkdir hist
fi

mv ./config/.db.info.template ./config/.db.info

# compile
python compiledb.py

proc_home=`pwd`

# set command env
echo """
# ADD db to PATH
export PATH=\$PATH:${proc_home}
""" >> ~/.bashrc

# create command file

echo """
#!/usr/bin/env bash
source /etc/profile
source ~/.bashrc
source ~/.bash_profile
python3 ${proc_home}/database.pyc "\$@"
""" > db

chmod +x db
# delete py files
rm database.py compiledb.py test.py
# delete sh files
rm install.sh
# install python env
pip install pymysql
pip install pymssql

# remove \r\n
dos2unix *

# test db
db test
