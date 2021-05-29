import abc
from enum import Enum


class Query():
    def __init__(self,
                 server_type,
                 database,
                 sql,
                 table_name=None,
                 fold=True,
                 columns=None,
                 limit_rows=None,
                 human=False):
        self.__server_type = server_type
        self.__sql = sql
        self.__database = database
        self.__table_name = table_name
        self.__fold = fold
        self.__columns = columns
        self.__limit_rows = limit_rows
        self.__human = human

    __slots__ = (
        '__server_type',
        '__sql',
        '__database',
        '__table_name',
        '__fold',
        '__columns',
        '__limit_rows',
        '__human')

    @property
    def table_name(self):
        return self.__table_name

    @property
    def database(self):
        return self.__database

    @property
    def sql(self):
        return self.__sql

    @property
    def server_type(self):
        return self.__server_type

    @property
    def fold(self):
        return self.__fold

    @property
    def columns(self):
        return self.__columns

    @property
    def limit_rows(self):
        return self.__limit_rows

    @property
    def human(self):
        return self.__human


class DbException(Exception): pass


class Server():
    __slots__ = ('__server_name')

    def __init__(self, server_name):
        self.__server_name = server_name

    @property
    def server_name(self):
        return self.__server_name

    @server_name.setter
    def server_name(self, server_name):
        self.__server_name = server_name

    def get_count_table_sql(self, tab_name):
        return f'SELECT COUNT(*) row_count FROM {self.escape_value(tab_name)}'

    def get_peek_table_sql(self, tab_name):
        return f'SELECT * FROM {self.escape_value(tab_name)} LIMIT 1'

    def get_scan_table_sql(self, tab_name):
        return f"SELECT * FROM {self.escape_value(tab_name)}"

    @abc.abstractmethod
    def escape_value(self, value):
        pass

    @abc.abstractmethod
    def get_list_databases_sql(self):
        pass

    @abc.abstractmethod
    def get_list_tables_sql(self, database):
        pass

    @abc.abstractmethod
    def get_list_views_sql(self, database):
        pass


class MySQLServer(Server):
    def __init__(self):
        super().__init__('mysql')

    def escape_value(self, value):
        return f'`{value}`'

    def get_list_databases_sql(self):
        return f"SELECT SCHEMA_NAME `Database` FROM information_schema.SCHEMATA ORDER BY `Database`"

    def get_list_tables_sql(self, database):
        return f"SELECT TABLE_NAME `Table` FROM information_schema.tables WHERE TABLE_SCHEMA='{database}' ORDER BY TABLE_NAME"

    def get_list_views_sql(self, database):
        return f"SELECT DISTINCT TABLE_NAME `View` FROM information_schema.views WHERE TABLE_SCHEMA='{database}' ORDER BY `View`"


class SQLServer(Server):
    def __init__(self):
        super().__init__('sqlserver')

    def get_peek_table_sql(self, tab_name):
        return f'SELECT TOP 1 * FROM {self.escape_value(tab_name)}'

    def escape_value(self, value):
        return f'[{value}]'

    def get_list_databases_sql(self):
        return "SELECT name [Database] FROM sys.sysdatabases ORDER BY name"

    def get_list_tables_sql(self, database):
        return "SELECT name [Table] FROM sys.tables ORDER BY name"

    def get_list_views_sql(self, database):
        return f"SELECT DISTINCT TABLE_NAME [View] FROM information_schema.views WHERE TABLE_CATALOG='{database}' ORDER BY TABLE_NAME"


class MongoDBServer(Server):
    def __init__(self):
        super().__init__('mongo')

    def get_count_table_sql(self, tab_name):
        return f"db.{self.escape_value(tab_name)}.count()"

    def get_peek_table_sql(self, tab_name):
        return f'db.{self.escape_value(tab_name)}.find_one()'

    def get_scan_table_sql(self, tab_name):
        return f'db.{self.escape_value(tab_name)}.find()'

    def escape_value(self, value):
        return f'{value}'

    def get_list_databases_sql(self):
        return "print_result_set(['Database'], [[c] for c in sorted(conn.list_database_names())], columns, False, None, 0, dc)"

    def get_list_tables_sql(self, database):
        return "print_result_set(['Collection'], [[c] for c in sorted(db.list_collection_names())], columns, False, None, 0, dc)"

    def get_list_views_sql(self, database):
        raise DbException("Mongo does not support list views option!")


class DatabaseType(Enum):
    SQL_SERVER = SQLServer()
    MYSQL = MySQLServer()
    MONGO = MongoDBServer()

    @staticmethod
    def support(server_type):
        return server_type in set(map(lambda x: x.value.server_name, DatabaseType))

    @staticmethod
    def parse(server_type):
        for d in DatabaseType:
            if d.value.server_name == server_type:
                return d
        raise DbException("server type invalid!")


class EnvType(Enum):
    DEV = 'dev'
    PROD = 'prod'
    QA = 'qa'


class DatabaseConf(object):
    __slots__ = ('__server_type', '__host', '__port', '__user', '__password', '__database', '__charset', '__autocommit')

    def __init__(self, conf: dict):
        self.__server_type = DatabaseType.parse(conf.get('servertype', ''))
        self.__host = conf.get('host', '')
        self.__port = conf.get('port', '')
        self.__user = conf.get('user', '')
        self.__password = conf.get('password', '')
        self.__database = conf.get('database', '')
        self.__charset = conf.get('charset', 'UTF8')
        self.__autocommit = conf.get('autocommit', False)

    def is_mysql(self):
        return self.__server_type is DatabaseType.MYSQL

    def is_sql_server(self):
        return self.__server_type is DatabaseType.SQL_SERVER

    def is_mongo(self):
        return self.__server_type is DatabaseType.MONGO

    @property
    def server_type(self):
        return self.__server_type

    @server_type.setter
    def server_type(self, server_type):
        self.__server_type = server_type

    @property
    def host(self):
        return self.__host

    @host.setter
    def host(self, host):
        self.__host = host

    @property
    def port(self):
        return self.__port

    @port.setter
    def port(self, port):
        self.__port = port

    @property
    def user(self):
        return self.__user

    @user.setter
    def user(self, user):
        self.__user = user

    @property
    def password(self):
        return self.__password

    @password.setter
    def password(self, password):
        self.__password = password

    @property
    def database(self):
        return self.__database

    @database.setter
    def database(self, database):
        self.__database = database

    @property
    def charset(self):
        return self.__charset

    @charset.setter
    def charset(self, charset):
        self.__charset = charset

    @property
    def autocommit(self):
        return self.__autocommit

    @autocommit.setter
    def autocommit(self, autocommit):
        self.__autocommit = autocommit
