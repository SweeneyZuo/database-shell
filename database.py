import warnings

warnings.filterwarnings("ignore")
import os
import re
import sys
import json
import fcntl
import pymssql
import pymysql
import pymongo
import hashlib
import platform
import traceback
from enum import Enum
import printutils as pu
from datetime import datetime, timedelta
from databasecore import EnvType, DatabaseType, DatabaseConf, Query

DEFAULT_PC = pu.PrintConf()


class Stat(Enum):
    OK = 'ok'
    ERROR = 'error'


DEFAULT_CONF = 'default'
ENV_TYPE = EnvType.DEV
CONF_KEY = DEFAULT_CONF
WARN_COLOR = pu.Color.KHAKI
INFO_COLOR = pu.Color.GREEN
PRINT_FORMAT_SET = {'table', 'text', 'json', 'sql', 'html', 'html2', 'html3', 'html4', 'markdown', 'xml', 'csv'}
FOLD_LIMIT = 50
config = ['env', 'conf', 'servertype', 'host', 'port', 'user', 'password', 'database', 'charset', 'autocommit']


def check_conf(db_conf: dict):
    if db_conf['use']['conf'] == DEFAULT_CONF:
        pu.print_error_msg('please set conf!')
        return False
    conf_keys = db_conf['conf'][db_conf['use']['env']][db_conf['use']['conf']].keys()
    for conf in config[2:8]:
        if conf not in conf_keys:
            pu.print_error_msg(f'please set {conf}!')
            return False
    return True


def show_database_info(info):
    env = info['use']['env'] if info else ''
    conf_name = info['use']['conf'] if info else ''
    conf = info['conf'][env].get(conf_name, {}) if info else {}

    def print_start_info(table_width, pc):
        start_info_msg = '[CONNECTION INFO]'
        f = (table_width - len(start_info_msg)) >> 1
        b = table_width - len(start_info_msg) - f
        print(f'{"#" * f}{start_info_msg}{"#" * b}')

    pu.print_table(['env', 'conf', 'serverType', 'host', 'port', 'user', 'password', 'database', 'lockStat'],
                   [[env, conf_name,
                     conf.get('servertype', ''),
                     conf.get('host', ''),
                     conf.get('port', ''),
                     conf.get('user', ''),
                     conf.get('password', ''),
                     conf.get('database', ''),
                     is_locked()]],
                   DEFAULT_PC, start_func=print_start_info, end_func=lambda a, b, pc: print('#' * a))


def get_proc_home():
    return os.path.dirname(os.path.abspath(sys.argv[0]))


def write_history(option, content, stat):
    proc_home = get_proc_home()
    file = os.path.join(proc_home, f'hist/.{datetime.now().date()}_db.history')
    with open(os.path.join(proc_home, 'config/.db.history.lock'), mode='w+', encoding='UTF-8') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        with open(file, mode='a+', encoding='UTF-8') as history_file:
            history_file.write(f'{datetime.now().strftime("%H:%M:%S")}\0\0{option}\0\0{content}\0\0{stat.value}\n')
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def read_history():
    proc_home = get_proc_home()
    file = os.path.join(proc_home, f'hist/.{datetime.now().date()}_db.history')
    history_list = []
    if os.path.exists(file):
        with open(os.path.join(proc_home, 'config/.db.history.lock'), mode='w+', encoding='UTF-8') as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            with open(file, mode='r', encoding='UTF-8') as history_file:
                history_list = history_file.readlines()
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    return history_list


def show_history(fold):
    try:
        res = [list(map(lambda cell: cell.replace("\n", " "), line.split('\0\0'))) for line in read_history()]
        print_result_set(['time', 'option', 'value', 'stat'], res, Query(None, None, None, fold))
        write_history('history', out_format, Stat.OK)
    except BaseException as e:
        write_history('history', out_format, Stat.ERROR)
        pu.print_error_msg(e)


def get_connection():
    info = read_info()
    if out_format == 'table':
        show_database_info(info)
    if not check_conf(info):
        return None, None
    dc = DatabaseConf(info['conf'][info['use']['env']].get(info['use']['conf'], {}))
    try:
        if dc.is_mysql():
            return pymysql.connect(host=dc.host, user=dc.user, password=dc.password,
                                   database=dc.database, port=dc.port,
                                   charset=dc.charset,
                                   autocommit=dc.autocommit), dc
        if dc.is_sql_server():
            return pymssql.connect(host=dc.host, user=dc.user, password=dc.password,
                                   database=dc.database, port=dc.port,
                                   charset=dc.charset,
                                   autocommit=dc.autocommit), dc
        if dc.is_mongo():
            return pymongo.MongoClient(host=dc.host), dc
        pu.print_error_msg(f"invalid serverType: {dc.server_type}")
        return None, dc
    except BaseException as e:
        pu.print_error_msg(e)
        return None, dc


def get_tab_name_from_sql(src_sql):
    """
     提取sql中的表名，sql语句不能有嵌套结构。
    """

    def deal_tab_name(tab_name):
        if '.' in tab_name:
            tab_name = tab_name.split('.')[-1]
        tab_name = tab_name.replace('[', '').replace(']', '')
        b_index = sql.index(tab_name)
        return src_sql[b_index:b_index + len(tab_name)]

    sql = src_sql.strip().lower()
    if sql.startswith(('select', 'delete')) and 'from' in sql:
        return deal_tab_name(re.split('\\s+', sql[sql.index('from') + 4:].strip())[0])
    elif sql.startswith('update'):
        return deal_tab_name(re.split('\\s+', sql[sql.index('update') + 6:].strip())[0])
    elif sql.startswith(('create', 'alter',)) or (sql.startswith('drop') and re.split('\\s+', sql)[1] == 'table'):
        return deal_tab_name(re.split('\\s+', sql[sql.index('table') + 5:].strip())[0])
    elif sql.startswith(('desc', 'describe')):
        return deal_tab_name(re.split('\\s+', sql)[1].strip())
    else:
        return None


def exe_query(sql, conn):
    res_list, description = [], []
    try:
        cur = conn.cursor()
        cur.execute(sql)
        description.append(cur.description)
        res_list.append(cur.fetchall())
        while cur.nextset():
            description.append(cur.description)
            res_list.append(cur.fetchall())
        write_history('sql', sql, Stat.OK)
    except BaseException as be:
        write_history('sql', sql, Stat.ERROR)
        pu.print_error_msg(be)
    return description, res_list


def exe_no_query(sql, conn):
    effect_rows, description, res, success = None, [], [], False
    try:
        if sql.lower().startswith(('create', 'drop')):
            conn.autocommit(True)
        cur = conn.cursor()
        effect_rows = cur.execute(sql)
        description.append(cur.description)
        try:
            res.append(cur.fetchall())
            while cur.nextset():
                description.append(cur.description)
                res.append(cur.fetchall())
            conn.commit()
        except:
            if not effect_rows and out_format == 'table':
                print(WARN_COLOR.wrap('Empty Sets!'))
        success = True
        write_history('sql', sql, Stat.OK)
    except BaseException as be:
        write_history('sql', sql, Stat.ERROR)
        pu.print_error_msg(be)
    return effect_rows, description, res, success


def get_list_obj_sql(obj, dc: DatabaseConf):
    if obj in {'database', 'databases'}:
        return dc.server_type.value.get_list_databases_sql()
    elif obj in {'table', 'tables'}:
        return dc.server_type.value.get_list_tables_sql(dc.database)
    elif obj in {'view', 'views'}:
        return dc.server_type.value.get_list_views_sql(dc.database)
    else:
        return None


def show(obj='table'):
    conn, dc = get_connection()
    if dc is None:
        return
    sql = get_list_obj_sql('table' if obj == '' else obj, dc)
    if sql is None:
        pu.print_error_msg(f'invalid obj "{obj}"!')
        write_history('show', obj, Stat.ERROR)
    elif dc.is_mongo():
        db = conn[dc.database]
        eval(sql)
    else:
        run_sql(Query(dc.server_type, dc.database, sql, fold=False), conn)
    conn.close()


def get_create_table_ddl(conn, query: Query):
    def get_create_table_mysql_ddl():
        res = exe_no_query(f'show create table {query.table_name}', conn)[2]
        if not res:
            return None
        return res[0][0][1] if len(res[0][0]) == 2 else res[0][0][0]

    def get_create_table_sql_server_ddl():
        def is_number(data_type):
            return data_type in {'bit', 'int', 'tinyint', 'smallint', 'bigint', 'float',
                                 'decimal', 'numeric', 'real', 'money', 'smallmoney'}

        res_list = []
        sql = f"""sp_columns {query.table_name};SELECT TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME,ic.IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,tmp.definition FROM information_schema.columns ic LEFT JOIN (SELECT c.name col,c.definition FROM sys.computed_columns c \
JOIN sys.tables t ON c.object_id=t.object_id JOIN sys.schemas s ON s.schema_id=t.schema_id WHERE t.name='{query.table_name}' AND s.name='dbo') tmp ON ic.COLUMN_NAME=tmp.col WHERE ic.TABLE_NAME='{query.table_name}' AND ic.TABLE_SCHEMA='dbo'"""
        effect_rows, description, res, success = exe_no_query(sql, conn)
        if not res or not res[0]:
            pu.print_error_msg(f"{query.table_name} not found!")
            return
        header2, res1 = before_print(get_table_head_from_description(description[1]), res[1], query)
        header, res2 = before_print(get_table_head_from_description(description[0]), res[0], query)
        index_dict, foreign_dict = get_sqlserver_index_information_dict(conn, query.table_name)
        primary_key, mul_unique = [], {}
        for k, v in index_dict.items():
            if v[2] == 'PK':
                primary_key.append(f'[{k}]')
            elif v[2] == 'UQ':
                mul_unique[v[1]] = l = mul_unique.get(v[1], [])
                l.append(f'[{k}]')
        res_list.append(f"CREATE TABLE [{res1[0][1]}].[{query.table_name}] (\n")
        for index, (row, row2) in enumerate(zip(res1, res2)):
            col_name, data_type = row2[3], row[4]
            if row[8]:
                res_list.append(f"  [{col_name}] AS {row[8]}")
            else:
                res_list.append(f"  [{col_name}] {data_type}")
                if row[5] is not None and data_type not in ('text', 'ntext', 'xml'):
                    res_list.append(f"({'max' if row[5] == -1 else row[5]})")
                elif data_type in ('decimal', 'numeric'):
                    res_list.append(f"({row[6]},{row[7]})")
                if row2[5].endswith("identity"):
                    id_seed, id_incr = \
                        exe_no_query(f"SELECT IDENT_SEED('{query.table_name}'),IDENT_INCR('{query.table_name}')", conn)[
                            2][
                            0][0]
                    res_list.append(f" IDENTITY({id_seed},{id_incr})")
                if row2[12] is not None:
                    res_list.append(f" DEFAULT {row2[12]}")
                if row[3] == 'NO':
                    res_list.append(" NOT NULL")
            if index == len(res1) - 1:
                res_list.append(f",\n  PRIMARY KEY({','.join(primary_key)})" if primary_key else '')
                res_list.append(',\n' if mul_unique else '')
                res_list.append(
                    ',\n'.join([f"  CONSTRAINT [{k}] UNIQUE({','.join(v)})" for k, v in mul_unique.items()]))
                res_list.append('\n')
            else:
                res_list.append(",\n")
        res_list.append(");")
        comment = exe_query(
            f"""(SELECT col.name,CONVERT(varchar,ep.value),ep.name comment,CONVERT(varchar,SQL_VARIANT_PROPERTY(ep.value,'BaseType')) type,ep.minor_id FROM dbo.syscolumns col JOIN dbo.sysobjects obj ON col.id=obj.id AND obj.xtype='U' AND obj.status>=0 LEFT JOIN sys.extended_properties ep ON col.id=ep.major_id AND col.colid=ep.minor_id \
             WHERE obj.name='{query.table_name}' AND ep.value is not NULL) union (select obj.name,CONVERT(varchar,ep.value),ep.name comment,CONVERT(varchar,SQL_VARIANT_PROPERTY(ep.value,'BaseType')) type,ep.minor_id from dbo.sysobjects obj join sys.extended_properties ep on obj.id=ep.major_id where ep.minor_id=0 and obj.xtype='U' AND obj.status>=0 AND obj.name='{query.table_name}')""",
            conn)[1]
        if comment:
            for com in comment[0]:
                res_list.append(
                    f"""\nEXEC sp_addextendedproperty '{com[2]}' , {com[1] if is_number(com[3]) else "'{}'".format(com[1])}, 'SCHEMA', '{res1[0][1]}', 'TABLE', '{query.table_name}';""" if
                    com[4] == 0 else \
                        f"""\nEXEC sp_addextendedproperty '{com[2]}' , {com[1] if is_number(com[3]) else "'{}'".format(com[1])}, 'SCHEMA', '{res1[0][1]}', 'TABLE', '{query.table_name}', 'COLUMN', '{com[0]}';""")
        for k, v in foreign_dict.items():
            res_list.append(
                f'\nALTER TABLE [{res1[0][1]}].[{query.table_name}] WITH CHECK ADD CONSTRAINT [{v[1]}] FOREIGN KEY([{k}]) REFERENCES [{res1[0][1]}].[{v[3]}] ([{v[4]}]);\n')
            res_list.append(f'ALTER TABLE [{res1[0][1]}].[{query.table_name}] CHECK CONSTRAINT [{v[1]}];')
        return ''.join(res_list)

    if query.server_type is DatabaseType.MYSQL:
        return get_create_table_mysql_ddl()
    if query.server_type is DatabaseType.SQL_SERVER:
        return get_create_table_sql_server_ddl()


def get_sqlserver_index_information_dict(conn, tab_name, dbo='dbo'):
    index_formation = exe_query(
        f"""SELECT c.name colName,object_name(constraint_object_id) constName,'FK' type,object_name(referenced_object_id) refTabName,c1.name refColName FROM sys.foreign_key_columns f JOIN sys.tables t ON t.object_id=f.parent_object_id JOIN sys.columns c ON c.object_id=f.parent_object_id AND c.column_id=f.parent_column_id JOIN sys.columns c1 ON c1.object_id=f.referenced_object_id AND c1.column_id=f.referenced_column_id JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE t.name='{tab_name}' AND s.name='{dbo}' \
UNION ALL SELECT COLUMN_NAME colName,CONSTRAINT_NAME constName,k.type,NULL refTabName,NULL refColName FROM sys.key_constraints k LEFT JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE c ON k.name=c.CONSTRAINT_NAME WHERE c.TABLE_SCHEMA='{dbo}' AND c.TABLE_NAME='{tab_name}'""",
        conn)[1]
    # {colName:(colName,constName,indexType,refTabName,refColName)}
    return {fmt[0]: fmt for fmt in index_formation[0]} if index_formation else dict(), \
           {fmt[0]: fmt for fmt in index_formation[0] if fmt[2] == 'FK'} if index_formation else dict()


def print_table_description(conn, qy: Query):
    sql = f"""SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,COLUMN_KEY,COLUMN_DEFAULT,EXTRA,COLUMN_COMMENT FROM information_schema.columns WHERE table_schema='{qy.database}' AND table_name='{qy.table_name}'"""
    if qy.server_type is DatabaseType.SQL_SERVER:
        sql = f"""SELECT col.name,t.name dataType,isc.CHARACTER_MAXIMUM_LENGTH,isc.NUMERIC_PRECISION,isc.NUMERIC_SCALE,CASE WHEN col.isnullable=1 THEN 'YES' ELSE 'NO' END nullable,comm.text defVal,CASE WHEN COLUMNPROPERTY(col.id,col.name,'IsIdentity')=1 THEN 'IDENTITY' ELSE '' END Extra,ISNULL(CONVERT(varchar,ep.value), '') comment FROM dbo.syscolumns col LEFT JOIN dbo.systypes t ON col.xtype=t.xusertype JOIN dbo.sysobjects obj ON col.id=obj.id AND obj.xtype='U' AND obj.status>=0 LEFT JOIN dbo.syscomments comm ON col.cdefault=comm.id LEFT JOIN sys.extended_properties ep ON col.id=ep.major_id AND col.colid=ep.minor_id AND ep.name='MS_Description' LEFT JOIN information_schema.columns isc ON obj.name=isc.TABLE_NAME AND col.name=isc.COLUMN_NAME WHERE isc.TABLE_CATALOG='{qy.database}' AND obj.name='{qy.table_name}' ORDER BY col.colorder"""
    res = exe_query(sql, conn)[1]
    if not res or not res[0]:
        pu.print_error_msg(f"{qy.table_name} not found!")
        return
    res = list(map(lambda row: list(row), res[0]))
    if qy.server_type is DatabaseType.SQL_SERVER:
        index_dict, foreign_dict = get_sqlserver_index_information_dict(conn, qy.table_name)
        for row in res:
            data_type, cml, np, ns = row[1], row.pop(2), row.pop(2), row.pop(2)
            if cml and row[1] not in {'text', 'ntext', 'xml'}:
                row[1] = f'{row[1]}({"max" if cml == -1 else cml})'
            elif data_type in ('decimal', 'numeric'):
                row[1] = f'{row[1]}({np},{ns})'
            t = index_dict.get(row[0], ('', '', '', '', ''))
            f = foreign_dict.get(row[0], None)
            if t[2] == 'PK':
                key = 'PK'
            elif t[2] == 'UQ':
                key, row[4] = 'UQ', t[1]
            else:
                key = ''
            if f:
                key = f'{key},FK' if key else 'FK'
                row[4] = f'{row[4]},REFERENCES {f[3]}.{f[4]}' if row[4] else f'REFERENCES {f[3]}.{f[4]}'
            row.insert(3, key)
            row[4] = '' if row[4] is None else row[4]
    else:
        for row in res:
            row[4] = '' if row[4] is None else row[4]
    print_result_set(['Name', 'Type', 'Nullable', 'Key', 'Default', 'Extra', 'Comment'], res, qy)


def desc_table(tab_name, _fold, _columns, limit_rows, human):
    conn, dc = get_connection()
    if conn is None:
        return
    print_table_schema(conn, Query(dc.server_type, dc.database, None, tab_name, _fold, _columns, limit_rows, human))
    conn.close()


def print_table_schema(conn, query, attach_sql=False):
    if query.server_type is DatabaseType.MONGO:
        pu.print_error_msg("Mongo does not support desc option!")
        return
    if out_format != 'sql':
        print_table_description(conn, query)
        if not attach_sql:
            return
    if out_format in {'sql', 'markdown'}:
        ddl = get_create_table_ddl(conn, query)
        print(f'\n```sql\n{ddl}\n```' if out_format == 'markdown' else ddl)


def get_table_head_from_description(description):
    return [desc[0] for desc in description] if description else []


def print_mongo_result(mongo_result, query):
    if mongo_result is None:
        return
    header_list, result_list = [], []
    if isinstance(mongo_result, dict):
        result_list.append([])
        for (k, value) in mongo_result.items():
            header_list.append(k)
            result_list[0].append(value)
    elif isinstance(mongo_result, (pymongo.cursor.Cursor, pymongo.command_cursor.CommandCursor, list, tuple, set)):
        t_set, result_dict_list = set(), []
        for d in mongo_result:
            if isinstance(d, dict):
                result_dict_list.append(d)
                for (k, value) in d.items():
                    if k not in t_set:
                        t_set.add(k)
                        header_list.append(k)
            else:
                result_list.append([d])
        if result_list:
            header_list.append(f'result({type(result_list[0][0]).__name__})')
        for d in result_dict_list:
            result_list.append([d.get(h, None) for h in header_list])
    else:
        header_list.append(f'result({type(mongo_result).__name__})')
        result_list.append([mongo_result])
    print_result_set(header_list, result_list, query)


def exe_mongo(conn, query):
    if conn is None:
        return
    db = conn[query.database]
    try:
        res = eval(query.sql)
        print_mongo_result(res, query)
        write_history('sql', query.sql, Stat.OK)
    except Exception as e:
        write_history('sql', query.sql, Stat.ERROR)
        pu.print_error_msg(e)


def run_sql(query, conn):
    sql, description, res, effect_rows = query.sql.strip(), None, None, None
    if query.server_type is DatabaseType.MONGO:
        exe_mongo(conn, query)
        return
    if sql.lower().startswith(('select', 'show')):
        description, res = exe_query(sql, conn)
    else:
        effect_rows, description, res, success = exe_no_query(sql, conn)
    if description and res:
        for index, (d, r) in enumerate(zip(description, res)):
            print_result_set(get_table_head_from_description(d), r, query, index)
            print()
    if effect_rows and out_format == 'table':
        print(INFO_COLOR.wrap(f'Effect rows:{effect_rows}'))


def deal_res(res):
    for row in res:
        for cdx, e in enumerate(row):
            if isinstance(e, (bytearray, bytes)):
                try:
                    row[cdx] = str(e, 'utf8')
                except Exception:
                    row[cdx] = pu.HexObj(e.hex())
            elif isinstance(e, bool):
                row[cdx] = 1 if e else 0


def before_print(header, res, query):
    """
        需要注意不能改变原本数据的类型，除了需要替换成其它数据的情况
    """
    if res is None:
        return []
    if query.limit_rows:
        res = res[query.limit_rows[0]:query.limit_rows[1]]
    res = [row if isinstance(row, list) else list(row) for row in res]
    res.insert(0, header if isinstance(header, list) else list(header))
    res = [[line[i] for i in query.columns] for line in res] if query.columns else res
    deal_res(res)
    res = deal_human(res) if human else res
    res = [[e if len(str(e)) < FOLD_LIMIT else
            f'{str(e)[:FOLD_LIMIT - 3]}{DEFAULT_PC.fold_replace_str_with_color}' for e in row] for row in
           res] if query.fold else res
    return res.pop(0), res


def run_one_sql(sql, _fold, _columns, limit_rows, human):
    conn, dc = get_connection()
    if conn is None:
        return
    run_sql(Query(dc.server_type, dc.database, sql, None, _fold, _columns, limit_rows, human), conn)
    conn.close()


def scan(tab, _fold, _columns, limit_rows, human):
    conn, dc = get_connection()
    if conn is None:
        return
    run_sql(Query(dc.server_type, dc.database, dc.server_type.value.get_scan_table_sql(tab), tab,
                  _fold, _columns, limit_rows, human), conn)
    conn.close()


def peek(tab, _fold, _columns, limit_rows, human):
    conn, dc = get_connection()
    if conn is None:
        return
    run_sql(Query(dc.server_type, dc.database, dc.server_type.value.get_peek_table_sql(tab),
                  tab, _fold, _columns, limit_rows, human), conn)
    conn.close()


def count(tab):
    conn, dc = get_connection()
    if conn is None:
        return
    run_sql(Query(dc.server_type, dc.database, dc.server_type.value.get_count_table_sql(tab), tab, False), conn)
    conn.close()


def print_result_set(header, res, query, res_index=0):
    if not header:
        return
    if not res and out_format == 'table':
        print(WARN_COLOR.wrap('Empty Sets!'))
        return
    header, res = before_print(header, res, query)
    if out_format == 'table':
        pu.print_table(header, res, DEFAULT_PC,
                       start_func=lambda tw, pc: print(DEFAULT_PC.info_color.wrap(f'Result Sets [{res_index}]:')))
    elif out_format == 'sql' and query and query.server_type and query.sql:
        pu.print_insert_sql(header, res, query.table_name if query.table_name else get_tab_name_from_sql(query.sql),
                            query.server_type)
    elif out_format == 'json':
        pu.print_json(header, res)
    elif out_format == 'html':
        pu.print_html(header, res)
    elif out_format == 'html2':
        pu.print_html2(header, res)
    elif out_format == 'html3':
        pu.print_html3(header, res)
    elif out_format == 'html4':
        pu.print_html4(header, res)
    elif out_format == 'markdown':
        pu.print_markdown(header, res, DEFAULT_PC)
    elif out_format == 'xml':
        pu.print_xml(header, res)
    elif out_format == 'csv':
        pu.print_csv(header, res)
    elif out_format == 'text':
        pu.print_table(header, res, DEFAULT_PC, lambda a, pc: a, lambda a, b, c, d, e: a, lambda a, b, c: a)
    else:
        pu.print_error_msg(f'Invalid out format : "{out_format}"!')


def deal_human(rows):
    def can_human(field_name):
        return 'time' in field_name

    human_time_cols = set(i for i in range(len(rows[0])) if can_human(str(rows[0][i]).lower()))
    for rdx, row in enumerate(rows):
        if rdx == 0:
            continue
        for cdx, e in enumerate(row):
            if cdx in human_time_cols and isinstance(e, int):
                # 注意：时间戳被当成毫秒级时间戳处理，秒级时间戳格式化完是错误的时间
                rows[rdx][cdx] = (datetime(1970, 1, 1) + timedelta(milliseconds=e)).strftime("%Y-%m-%d %H:%M:%S")
    return rows


def print_info():
    try:
        read_config = read_info()
        show_database_info({} if read_config is None else read_config)
        write_history('info', '', Stat.OK)
    except BaseException as be:
        write_history('info', '', Stat.ERROR)
        pu.print_error_msg(be)


def disable_color():
    global DEFAULT_PC, INFO_COLOR, WARN_COLOR
    DEFAULT_PC.disable_color()
    INFO_COLOR = pu.Color.NO_COLOR
    WARN_COLOR = pu.Color.NO_COLOR


class Opt(Enum):
    UPDATE = 'update'
    WRITE = 'write'
    READ = 'read'


def parse_info_obj(_read_info, info_obj, opt=Opt.READ):
    if info_obj:
        for conf_key in config:
            if conf_key not in info_obj.keys():
                continue
            set_conf_value = info_obj[conf_key]
            if conf_key == 'env':
                _read_info['use']['env'] = EnvType(set_conf_value).value
                if opt is Opt.UPDATE:
                    print(INFO_COLOR.wrap(f"set env={set_conf_value} ok."))
                continue
            elif conf_key == 'conf':
                if set_conf_value in _read_info['conf'][_read_info['use']['env']].keys():
                    if opt is Opt.UPDATE:
                        _read_info['use']['conf'] = set_conf_value
                        print(INFO_COLOR.wrap(f"set conf={set_conf_value} ok."))
                elif opt is Opt.UPDATE:
                    i = input(WARN_COLOR.wrap("Are you sure you want to add this configuration? \nY/N:")).lower()
                    if i in ('y', 'yes'):
                        _read_info['use']['conf'] = set_conf_value
                        _read_info['conf'][_read_info['use']['env']][_read_info['use']['conf']] = {}
                        print(INFO_COLOR.wrap(f'Add "{set_conf_value}" conf in env={_read_info["use"]["env"]}'))
                continue
            elif conf_key == 'servertype' and not DatabaseType.support(set_conf_value):
                pu.print_error_msg(f'server type: "{set_conf_value}" not supported!')
                continue
            elif conf_key == 'autocommit':
                if set_conf_value.lower() not in {'true', 'false'}:
                    pu.print_error_msg(f'"{set_conf_value}" incorrect parameters!')
                    continue
                set_conf_value = set_conf_value.lower() == 'true'
            elif conf_key == 'port':
                if not set_conf_value.isdigit():
                    pu.print_error_msg(f'set port={set_conf_value} fail!')
                    continue
                set_conf_value = int(set_conf_value)
            _read_info['conf'][_read_info['use']['env']][_read_info['use']['conf']][conf_key] = set_conf_value
    return _read_info


def read_info():
    proc_home = get_proc_home()
    with open(os.path.join(proc_home, 'config/.db.info.lock'), mode='w+') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        with open(os.path.join(proc_home, 'config/.db.info'), mode='r', encoding='UTF8') as info_file:
            info_obj = json.loads(''.join(info_file.readlines()))
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    return info_obj


def write_info(info):
    proc_home = get_proc_home()
    with open(os.path.join(proc_home, 'config/.db.info.lock'), mode='w+') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        with open(os.path.join(proc_home, 'config/.db.info'), mode='w+', encoding='UTF8') as info_file:
            info_file.write(json.dumps(info, indent=2))
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def set_info(kv):
    info_obj = {}
    try:
        with open(os.path.join(get_proc_home(), 'config/.db.lock'), 'w') as lock:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                pu.print_error_msg("set fail, please retry!")
                write_history('set', kv, Stat.ERROR)
                return
            if is_locked():
                pu.print_error_msg("db is locked! can't set value.")
                write_history('set', kv, Stat.ERROR)
                return
            kv_pair = kv.split('=', 1)
            info_obj[kv_pair[0].lower()] = kv_pair[1]
            write_info(parse_info_obj(read_info(), info_obj, Opt.UPDATE))
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
            write_history('set', kv, Stat.OK)
    except BaseException as be:
        write_history('set', kv, Stat.ERROR)
        pu.print_error_msg(be)


def is_locked():
    return os.path.exists(os.path.join(get_proc_home(), 'config/.db.lock.value'))


def lock_value():
    lock_value_file = os.path.join(get_proc_home(), 'config/.db.lock.value')
    if is_locked():
        with open(lock_value_file, 'r') as f:
            return f.read()


def unlock(key):
    with open(os.path.join(get_proc_home(), 'config/.db.lock'), 'w') as t_lock_file:
        try:
            fcntl.flock(t_lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            pu.print_error_msg("unlock fail, please retry!")
            write_history('unlock', '*' * 6, Stat.ERROR)
            return
        lock_val = lock_value()
        if not lock_val:
            pu.print_error_msg('The db is not locked.')
            write_history('unlock', '*' * 6, Stat.ERROR)
            return
        key = key if key else ""
        m = hashlib.md5()
        m.update(key.encode('UTF-8'))
        if m.hexdigest() == lock_val:
            os.remove(os.path.join(get_proc_home(), 'config/.db.lock.value'))
            print(INFO_COLOR.wrap('db unlocked!'))
            write_history('unlock', '*' * 6, Stat.OK)
        else:
            pu.print_error_msg('Incorrect key!')
            write_history('unlock', key, Stat.ERROR)
        fcntl.flock(t_lock_file.fileno(), fcntl.LOCK_UN)


def lock(key: str):
    with open(os.path.join(get_proc_home(), 'config/.db.lock'), 'w+') as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            pu.print_error_msg("lock fail, please retry!")
            write_history('lock', '*' * 6, Stat.ERROR)
            return
        if is_locked():
            pu.print_error_msg('The db is already locked, you must unlock it first!')
            write_history('lock', '*' * 6, Stat.ERROR)
            return
        key = key if key else ""
        m = hashlib.md5()
        m.update(key.encode('UTF-8'))
        lock_file = os.path.join(get_proc_home(), 'config/.db.lock.value')
        with open(lock_file, mode='w+') as f:
            f.write(m.hexdigest())
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        print(INFO_COLOR.wrap('db locked!'))
        write_history('lock', '*' * 6, Stat.OK)


def print_usage():
    pu.print_config('.db.usage')


def shell():
    conn, dc = get_connection()
    if conn is None:
        return
    if dc.is_mongo():
        pu.print_error_msg("Mongo does not support shell option!")
        conn.close()
        return
    val = input('db>').strip()
    while val not in {'quit', '!q', 'exit'}:
        if not (val == '' or val.strip() == ''):
            run_sql(Query(dc.server_type, None, val), conn)
        val = input('db>')
    conn.close()
    print('Bye')
    write_history('shell', '', Stat.OK)


def load(path):
    def exe_sql(_cur, _query: Query):
        try:
            if not _query.sql:
                return 0, 0
            effect_rows = _cur.execute(_query.sql)
            description, res = [], []
            try:
                description.append(_cur.description)
                res.append(_cur.fetchall())
                while _cur.nextset():
                    description.append(_cur.description)
                    res.append(_cur.fetchall())
            except Exception:
                if effect_rows:
                    print(WARN_COLOR.wrap(f'Effect rows:{effect_rows}'))
            for r, d in zip(res, description):
                print_result_set(get_table_head_from_description(d), r, _query)
            return 1, 0
        except BaseException as be:
            pu.print_error_msg(f"SQL:{_query.sql}, ERROR MESSAGE:{be}")
            return 0, 1

    success_num, fail_num = 0, 0
    try:
        if os.path.exists(path):
            conn, dc = get_connection()
            if conn is None:
                return
            if dc.is_mongo():
                pu.print_error_msg("Mongo does not support load option!")
                conn.close()
                return
            cur = conn.cursor()
            with open(path, mode='r', encoding='UTF8') as sql_file:
                sql = ''
                for line in sql_file.readlines():
                    t_sql = line.strip()
                    if not t_sql or t_sql.startswith('--'):
                        continue
                    elif t_sql.lower().startswith(('insert', 'create', 'update', 'select', 'delete', 'alter', 'set',
                                                   'drop', 'go', 'use', 'if', 'with', 'show', 'desc', 'grant', 'exec')):
                        if sql == '':
                            sql = line
                            continue
                        success, fail = exe_sql(cur, Query(dc.server_type, None, sql, fold=False))
                        success_num += success
                        fail_num += fail
                        sql = line
                    else:
                        sql += line
                if sql != '':
                    success, fail = exe_sql(cur, Query(dc.server_type, None, sql, fold=False))
                    success_num += success
                    fail_num += fail
            if success_num > 0:
                conn.commit()
            cur.close()
            conn.close()
            print(INFO_COLOR.wrap(f'end load. {success_num} successfully executed, {fail_num} failed.'))
            write_history('load', path, Stat.OK)
        else:
            pu.print_error_msg(f"path:{path} not exist!")
            write_history('load', path, Stat.ERROR)
    except BaseException as be:
        pu.print_error_msg(be)
        write_history('load', path, Stat.ERROR)


def show_conf(_fold):
    print_content = []
    db_conf = read_info()['conf']
    head = ['env', 'conf', 'servertype', 'host', 'port', 'database', 'user', 'password', 'charset', 'autocommit']
    for env in db_conf.keys():
        for conf in db_conf[env].keys():
            new_row = [env, conf]
            new_row.extend([db_conf[env][conf].get(key, '') for key in head[2:]])
            print_content.append(new_row)
    header, res = before_print(head, print_content, Query(None, None, None, fold=_fold))
    pu.print_table(header, res, DEFAULT_PC)
    write_history('conf', '', Stat.OK)


def get_print_template_with_format():
    if out_format == 'markdown':
        return '>\n> {}\n>\n\n'
    elif out_format in {'html', 'html2', 'html3', 'html4'}:
        return '<h1>{}</h1>\n'
    else:
        return '--\n-- {}\n--\n\n'


def test():
    print('test', end='')
    write_history('test', '', Stat.OK)


def export(_fold):
    conn, dc = get_connection()
    if conn is None:
        return
    if dc.is_mongo():
        pu.print_error_msg("Mongo does not support export option!")
        conn.close()
        return
    try:
        tab_list = exe_query(get_list_obj_sql('table', dc), conn)[1]
        split_line, print_template = '\n---\n' if out_format == 'markdown' else '\n', get_print_template_with_format()
        attach_sql = export_type == 'ddl'
        for tab in tab_list[0]:
            if export_type in {'all', 'ddl'}:
                print(print_template.format(f'Table structure for {tab[0]}'))
                print_table_schema(conn, Query(dc.server_type, dc.database, None, tab[0], _fold), attach_sql)
                print(split_line)
            if export_type in {'all', 'data'}:
                q = Query(dc.server_type, dc.database, dc.server_type.value.get_scan_table_sql(tab[0]), tab[0], _fold)
                print(print_template.format(f'Dumping data for {tab[0]}'), end='')
                if dc.is_sql_server() and out_format == 'sql':
                    headers, results = exe_query(f'sp_columns [{tab[0]}];{q.sql}', conn)
                    set_identity_insert = len([1 for row in results[0] if row[5].endswith("identity")]) > 0
                    if set_identity_insert:
                        print(f'SET IDENTITY_INSERT [{tab[0]}] ON;')
                    print_result_set(get_table_head_from_description(headers[1]), results[1], q)
                    if set_identity_insert:
                        print(f'SET IDENTITY_INSERT [{tab[0]}] OFF;')
                else:
                    run_sql(q, conn)
                print(split_line)
        write_history('export', export_type, Stat.OK)
    except BaseException as be:
        write_history('export', export_type, Stat.ERROR)
        pu.print_error_msg(be)
    finally:
        conn.close()


def parse_args(args):
    def _error_param_exit(param):
        pu.print_error_msg(f'Invalid param : "{param}"!')
        sys.exit(-1)

    option = args[1].strip().lower() if len(args) > 1 else ''
    global out_format, export_type
    columns, fold, export_type, human, out_format, set_format, set_human, set_export_type, set_columns, set_fold, \
    set_raw, set_row_limit, limit_rows, set_show_obj = None, True, 'all', False, 'table', False, False, False, False, False, False, False, None, False
    option_val, parse_start_pos = args[2] if len(args) > 2 else '', 3 if option == 'sql' else 2
    if len(args) > parse_start_pos:
        for index in range(parse_start_pos, len(args)):
            p: str = args[index].strip().lower()
            limit_row_re = re.match("^(row)\[\s*(-?\d+)\s*:\s*(-?\d+)\s*(\])$", p)
            limit_column_re = re.match("^(col)\[((\s*\d+\s*-\s*\d+\s*)|(\s*(\d+)\s*(,\s*(\d+)\s*)*))(\])$", p)
            if option in {'info', 'shell', 'help', 'test', 'version'}:
                _error_param_exit(p)
            elif index == 2 and option in {'sql', 'scan', 'peek', 'count', 'desc', 'load', 'set', 'lock', 'unlock'}:
                # 第3个参数可以自定义输入的操作
                continue
            elif option == 'show':
                if p not in {'database', 'table', 'databases', 'tables', 'view', 'views'} or set_show_obj:
                    _error_param_exit(p)
                set_show_obj = set_fold = set_raw = set_row_limit = set_human = set_columns = set_format = True
            elif not set_fold and p == 'false':
                fold, set_fold = False, True
            elif not set_row_limit and option not in {'export'} and limit_row_re:
                set_row_limit, limit_rows = True, (int(limit_row_re.group(2)), int(limit_row_re.group(3)))
            elif not set_columns and option not in {'export'} and limit_column_re:
                group2 = limit_column_re.group(2).strip()
                if ',' in group2 or group2.isdigit():
                    columns = [int(i.strip()) for i in group2.split(',')]
                else:
                    t = [int(i.strip()) for i in group2.split('-')]
                    columns = [i for i in range(int(t[0]), int(t[1]) + 1)] if int(t[0]) <= int(t[1]) \
                        else [i for i in range(int(t[0]), int(t[1]) - 1, -1)]
                set_columns = True
            elif not set_format and option in {'export', 'sql', 'scan', 'peek', 'count', 'desc', 'hist', 'history'} and \
                    p in PRINT_FORMAT_SET:
                if option != 'table':
                    disable_color()
                else:
                    fold = False
                out_format, set_format = p, True
            elif not set_raw and out_format == 'table' and p == 'raw':
                set_raw = True
                disable_color()
            elif not set_export_type and option == 'export' and p in {'ddl', 'data', 'all'}:
                export_type, set_export_type = p, True
            elif not set_human and p == 'human':
                human, set_human = True, True
            else:
                _error_param_exit(p)
    return option, columns, fold, limit_rows, human, option_val


if __name__ == '__main__':
    if platform.system().lower() == 'windows':
        disable_color()
    try:
        opt, columns, fold, limit_rows, human, option_val = parse_args(sys.argv)
        if opt in {'info', ''}:
            print_info()
        elif opt == 'show':
            show(option_val.lower())
        elif opt == 'conf':
            show_conf(fold)
        elif opt in {'hist', 'history'}:
            show_history(fold)
        elif opt == 'desc':
            desc_table(option_val, fold, columns, limit_rows, human)
        elif opt == 'sql':
            run_one_sql(option_val, fold, columns, limit_rows, human)
        elif opt == 'scan':
            scan(option_val, fold, columns, limit_rows, human)
        elif opt == 'peek':
            peek(option_val, fold, columns, limit_rows, human)
        elif opt == 'count':
            count(option_val)
        elif opt == 'set':
            set_info(option_val)
        elif opt == 'lock':
            lock(option_val)
        elif opt == 'unlock':
            unlock(option_val)
        elif opt == 'shell':
            shell()
        elif opt == 'load':
            load(option_val)
        elif opt == 'export':
            export(fold)
        elif opt == 'help':
            print_usage()
        elif opt == 'test':
            test()
        elif opt == 'version':
            pu.print_config('.db.version')
        else:
            pu.print_error_msg("Invalid operation!")
            print_usage()
    except Exception as e:
        pu.print_error_msg(e)
        traceback.print_exc(chain=e)
