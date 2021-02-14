import warnings

warnings.filterwarnings("ignore")
import os
import re
import sys
import json
import time
import html
import fcntl
import base64
import pymssql
import pymysql
import hashlib
import platform
import traceback
from enum import Enum
from datetime import datetime

widths = [
    (126, 1), (159, 0), (687, 1), (710, 0), (711, 1), (727, 0), (733, 1), (879, 0), (1154, 1), (1161, 0), (4347, 1),
    (4447, 2), (7467, 1), (7521, 0), (8369, 1), (8426, 0), (9000, 1), (9002, 2), (11021, 1), (12350, 2), (12351, 1),
    (12438, 2), (12442, 0), (19893, 2), (19967, 1), (55203, 2), (63743, 1), (64106, 2), (65039, 1), (65059, 0),
    (65131, 2), (65279, 1), (65376, 2), (65500, 1), (65510, 2), (120831, 1), (262141, 2), (1114109, 1),
]


class DatabaseType(Enum):
    SQLSERVER = 'sqlserver'
    MYSQL = 'mysql'

    def support(servertype):
        return servertype in set(map(lambda x: x.lower(), DatabaseType.__members__))


class EnvType(Enum):
    DEV = 'dev'
    PROD = 'prod'
    QA = 'qa'


DEFAULT_CONF = 'default'
ENV_TYPE = EnvType.DEV
CONF_KEY = DEFAULT_CONF
PRINT_FORMAT_SET = {'table', 'text', 'json', 'sql', 'html', 'html2', 'html3', 'html4', 'markdown', 'xml', 'csv'}


class Align(Enum):
    ALIGN_LEFT = 1
    ALIGN_RIGHT = 2
    ALIGN_CENTER = 3


class Color(Enum):
    OFF_WHITE = "\033[29;1m{}\033[0m"
    WHITE = "\033[30;1m{}\033[0m"
    RED = "\033[31;1m{}\033[0m"
    YELLOW_GREEN = "\033[32;1m{}\033[0m"
    # 土黄色
    KHAKI = "\033[33;1m{}\033[0m"
    BLUE = "\033[34;1m{}\033[0m"
    PURPLE = "\033[35;1m{}\033[0m"
    GREEN = "\033[36;1m{}\033[0m"
    GRAY = "\033[37;1m{}\033[0m"
    # 黑白闪烁
    BLACK_WHITE_TWINKLE = "\033[5;30;47m{}\033[0m"
    # 无颜色
    NO_COLOR = "{}"

    def wrap(self, v):
        if self is Color.NO_COLOR:
            return v
        return self.value.format(v)


class Stat(Enum):
    OK = 'ok'
    ERROR = 'error'


FOLD_LIMIT = 50
SHOW_BOTTOM_THRESHOLD = 150
DATA_COLOR = Color.NO_COLOR
TABLE_HEAD_COLOR = Color.RED
INFO_COLOR = Color.GREEN
ERROR_COLOR = Color.RED
WARN_COLOR = Color.KHAKI
NULL_STR = Color.BLACK_WHITE_TWINKLE.wrap('NULL')
FOLD_REPLACE_STR = Color.BLACK_WHITE_TWINKLE.wrap("...")
FOLD_COLOR_LENGTH = len(Color.BLACK_WHITE_TWINKLE.wrap(""))
config = ['env', 'conf', 'servertype', 'host', 'port', 'user', 'password', 'database', 'charset', 'autocommit']


def check_conf(dbconf: dict):
    if dbconf['use']['conf'] == DEFAULT_CONF:
        print(WARN_COLOR.wrap('please set conf!'))
        return False
    conf_keys = dbconf['conf'][dbconf['use']['env']][dbconf['use']['conf']].keys()
    for conf in config[2:8]:
        if conf not in conf_keys:
            print(WARN_COLOR.wrap(f'please set {conf}!'))
            return False
    return True


def show_database_info(info):
    env = info['use']['env'] if info else ''
    conf_name = info['use']['conf'] if info else ''
    conf = info['conf'][env].get(conf_name, {}) if info else {}

    def print_start_info(table_width):
        start_info_msg = '[DATABASE INFO]'
        f = (table_width - len(start_info_msg)) >> 1
        b = table_width - len(start_info_msg) - f
        print(f'{"#" * f}{start_info_msg}{"#" * b}')

    def print_end_info(table_width, total):
        print('#' * table_width)

    header = ['env', 'conf', 'serverType', 'host', 'port', 'user', 'password', 'database', 'lockStat']
    print_table(header,
                [[env, conf_name,
                  conf.get('servertype', ''),
                  conf.get('host', ''),
                  conf.get('port', ''),
                  conf.get('user', ''),
                  conf.get('password', ''),
                  conf.get('database', ''),
                  is_locked()]],
                split_row_char='-', start_func=print_start_info, end_func=print_end_info)


def get_proc_home():
    return os.path.dirname(os.path.abspath(sys.argv[0]))


def write_history(option, content, stat):
    filename = f'hist/.{datetime.now().date()}_db.history'
    proc_home = get_proc_home()
    file = os.path.join(proc_home, filename)
    time = datetime.now().strftime('%H:%M:%S')
    data = f'{time}\0\0{option}\0\0{content}\0\0{stat.value}\n'
    with open(os.path.join(proc_home, 'config/.db.history.lock'), mode='w+', encoding='UTF-8') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        with open(file, mode='a+', encoding='UTF-8') as history_file:
            history_file.write(data)
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def read_history():
    filename = f'hist/.{datetime.now().date()}_db.history'
    proc_home = get_proc_home()
    file = os.path.join(proc_home, filename)
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
        table_head = ['time', 'option', 'value', 'stat']
        res = [list(map(lambda cell: cell.replace("\n", ""), line.split('\0\0'))) for line in read_history()]
        print_result_set(table_head, res, columns, fold, None)
        write_history('history', out_format, Stat.OK)
    except BaseException as e:
        write_history('history', out_format, Stat.ERROR)
        print(ERROR_COLOR.wrap(e))


def get_connection():
    info = read_info()
    if out_format == 'table':
        show_database_info(info)
    if not check_conf(info):
        return None, None
    config = info['conf'][info['use']['env']].get(info['use']['conf'], {})
    server_type = config['servertype']
    try:
        if server_type == 'mysql':
            return pymysql.connect(host=config['host'], user=config['user'], password=config['password'],
                                   database=config['database'], port=config['port'],
                                   charset=config.get('charset', 'UTF8'),
                                   autocommit=config.get('autocommit', False)), config
        elif server_type == 'sqlserver':
            return pymssql.connect(host=config['host'], user=config['user'], password=config['password'],
                                   database=config['database'], port=config['port'],
                                   charset=config.get('charset', 'UTF8'),
                                   autocommit=config.get('autocommit', False)), config
        else:
            print(ERROR_COLOR.wrap(f"servertype error! servertype={server_type}"))
            return None, config
    except BaseException as e:
        print(ERROR_COLOR.wrap(e))
        return None, config


def get_tab_name_from_sql(src_sql: str):
    """
     提取sql中的表名，sql语句不能有嵌套结构。
    """

    def deal_tab_name(tab_name: str):
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
    res_list = []
    description = []
    try:
        cur = conn.cursor()
        cur.execute(sql)
        description.append(cur.description)
        res_list.append(cur.fetchall())
        while cur.nextset():
            description.append(cur.description)
            res_list.append(cur.fetchall())
        write_history('sql', sql, Stat.OK)
    except BaseException as e:
        write_history('sql', sql, Stat.ERROR)
        print(ERROR_COLOR.wrap(e))
    return description, res_list


def exe_no_query(sql, conn):
    effect_rows, description, res, success = None, [], [], False
    try:
        cur = conn.cursor()
        effect_rows = cur.execute(sql)
        description.append(cur.description)
        try:
            res.append(cur.fetchall())
            while cur.nextset():
                description.append(cur.description)
                res.append(cur.fetchall())
        except:
            if not effect_rows and out_format == 'table':
                print(WARN_COLOR.wrap('Empty Sets!'))
        conn.commit()
        success = True
        write_history('sql', sql, Stat.OK)
    except BaseException as e:
        write_history('sql', sql, Stat.ERROR)
        print(ERROR_COLOR.wrap(e))
    return effect_rows, description, res, success


def calc_char_width(char):
    char_ord = ord(char)
    global widths
    if char_ord == 0xe or char_ord == 0xf:
        return 0
    for num, wid in widths:
        if char_ord <= num:
            return wid
    return 1


def str_width(any_str):
    if any_str == NULL_STR:
        return 4
    l = 0
    color_len = FOLD_COLOR_LENGTH if any_str.endswith(FOLD_REPLACE_STR) else 0
    for char in any_str:
        l += calc_char_width(char)
    return l - color_len


def table_row_str(row, head_length, align_list, color=Color.NO_COLOR, split_char='|'):
    def _table_row_str():
        end_str = f' {split_char} '
        yield f'{split_char} '
        for e, width, align_type in zip(row, head_length, align_list):
            space_num = abs(e[1] - width)
            if space_num == 0:
                yield color.wrap(e[0])
            elif align_type == Align.ALIGN_RIGHT:
                yield f"{' ' * space_num}{color.wrap(e[0])}"
            elif align_type == Align.ALIGN_LEFT:
                yield f"{color.wrap(e[0])}{' ' * space_num}"
            else:
                half_space_num = space_num >> 1
                yield ' ' * half_space_num
                yield color.wrap(e[0])
                yield ' ' * (space_num - half_space_num)
            yield end_str

    return ''.join(_table_row_str())


def get_max_length_each_fields(rows, func):
    length_head = len(rows[0]) * [0]
    for row in rows:
        for index, e in enumerate(row):
            row[index] = (e, func(e))
            length_head[index] = max(row[index][1], length_head[index])
    return length_head


def get_list_tab_sql(server_type, dbName):
    if server_type == 'mysql':
        return f"SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA='{dbName}' ORDER BY TABLE_NAME"
    else:
        return "SELECT name FROM sys.tables ORDER BY name"


def list_tables():
    conn, conf = get_connection()
    if conn is None:
        return
    run_sql(get_list_tab_sql(conf['servertype'], conf['database']), conn, False)
    conn.close()


def print_create_table(server_type, conn, tbName):
    def print_create_table_mysql():
        res = exe_no_query(f'show create table {tbName}', conn)[2]
        if not res:
            return
        print(res[0][0][1] if len(res[0][0]) == 2 else res[0][0][0])

    def print_create_table_sqlserver():
        res_list = []
        sql, sql2 = f"select * from information_schema.columns where table_name='{tbName}'", f"sp_columns {tbName}"
        effect_rows, description, res, success = exe_no_query(sql, conn)
        if not res or not res[0]:
            print(ERROR_COLOR.wrap(f"{tbName} not found!"))
            return
        effect_rows2, description2, res2, success = exe_no_query(sql2, conn)
        header, res = before_print(get_table_head_from_description(description), res[0], None, fold=False)
        header2, res2 = before_print(get_table_head_from_description(description2), res2[0], None, fold=False)
        indexDict = get_sqlserver_index_information_dict(conn, tbName)
        primary_key, mul_unique = [], {}
        for k, v in indexDict.items():
            if v[3] == 'YES':
                primary_key.append(k)
            elif v[4] == 'YES':
                mul_unique[v[2]] = l = mul_unique.get(v[2], [])
                l.append(k)
        res_list.append(f"CREATE TABLE [{res[0][0]}].[{res[0][1]}].[{res[0][2]}] (\n")
        for index, (row, row2) in enumerate(zip(res, res2)):
            col_name, data_type = row2[3], row[7]
            res_list.append(f"  [{col_name}] {data_type}")
            index_col = indexDict.get(col_name, ('', '', '', '', ''))
            is_primary_key = index_col[3] == 'YES'
            if row[8] is not None and data_type not in ('text', 'ntext', 'xml'):
                res_list.append(f"({'max' if row[8] == -1 else row[8]})")
            elif data_type in ('decimal', 'numeric'):
                res_list.append(f"({row[10]},{row[12]})")
            if row2[5].endswith("identity"):
                idSeed, idIncr = exe_no_query(f"SELECT IDENT_SEED('{tbName}'),IDENT_INCR('{tbName}')", conn)[2][0][0]
                res_list.append(f" identity({idSeed},{idIncr})")
            if row2[12] is not None:
                res_list.append(f" DEFAULT {row2[12]}")
            if row[6] == 'NO' and not is_primary_key:
                res_list.append(" NOT NULL")
            if index == len(res) - 1:
                res_list.append(f",\n  primary key({','.join(primary_key)})" if primary_key else '')
                res_list.append(',\n' if mul_unique else '')
                res_list.append(',\n'.join([f"  constraint {k} unique({','.join(v)})" for k, v in mul_unique.items()]))
                res_list.append('\n')
            else:
                res_list.append(",\n")
        res_list.append(");")
        comment = exe_query(
            f"""(SELECT col.name,ep.value,ep.name comment,ep.minor_id FROM dbo.syscolumns col JOIN dbo.sysobjects obj ON col.id=obj.id AND obj.xtype='U' AND obj.status>=0 LEFT JOIN sys.extended_properties ep ON col.id=ep.major_id AND col.colid=ep.minor_id
 WHERE obj.name='{tbName}' AND ep.value is not NULL) union (select obj.name,ep.value,ep.name comment,ep.minor_id from dbo.sysobjects obj join sys.extended_properties ep on obj.id=ep.major_id where ep.minor_id=0 and obj.xtype='U' AND obj.status>=0 AND obj.name='{tbName}')""",
            conn)[1]
        if comment:
            for com in comment[0]:
                res_list.append(
                    f"""\nexec sp_addextendedproperty '{com[2]}', '{str(com[1], "utf8")}', 'SCHEMA', '{res[0][1]}', 'TABLE', '{tbName}';""" if
                    com[3] == 0 else \
                        f"""\nexec sp_addextendedproperty '{com[2]}', '{str(com[1], "utf8")}', 'SCHEMA', '{res[0][1]}', 'TABLE', '{tbName}', 'COLUMN', '{com[0]}';""")
        print(''.join(res_list))

    if server_type == 'mysql':
        print_create_table_mysql()
    elif server_type == 'sqlserver':
        print_create_table_sqlserver()


def get_sqlserver_index_information_dict(conn, tab_name):
    """
    :param conn:
    :param tab_name:
    :return: {colName:(colName,indexType,indexName,primaryKey,isUnique)}
    """
    index_formation = exe_query(
        f"""SELECT columnName=C.Name,indexType=ISNULL(KC.type_desc,'Index'),indexName=IDX.Name,primaryKey=CASE WHEN IDX.is_primary_key=1 THEN 'YES' ELSE '' END,isUnique=CASE WHEN IDX.is_unique=1 THEN 'YES' ELSE '' END FROM sys.indexes IDX JOIN sys.index_columns IDXC ON IDX.object_id=IDXC.object_id AND IDX.index_id=IDXC.index_id LEFT JOIN sys.key_constraints KC ON IDX.object_id=KC.parent_object_id AND IDX.index_id = KC.unique_index_id JOIN sys.objects O ON O.object_id=IDX.object_id JOIN sys.columns C ON O.object_id=C.object_id AND O.type='U' AND O.is_ms_shipped=0 AND IDXC.Column_id=C.Column_id WHERE O.name='{tab_name}'""",
        conn)[1]
    return {fmt[0]: fmt for fmt in index_formation[0]} if index_formation else dict()


def print_table_description(conf, conn, tab_name, columns, fold):
    header = ['Name', 'Type', 'Nullable', 'Key', 'Default', 'Extra', 'Comment']
    sql = f"""SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,COLUMN_KEY,COLUMN_DEFAULT,EXTRA,COLUMN_COMMENT FROM information_schema.columns WHERE table_schema='{conf['database']}' AND table_name='{tab_name}'"""
    if conf['servertype'] == 'sqlserver':
        sql = f"""SELECT col.name,t.name dataType,isc.CHARACTER_MAXIMUM_LENGTH,isc.NUMERIC_PRECISION,isc.NUMERIC_SCALE,CASE WHEN col.isnullable=1 THEN 'YES' ELSE 'NO' END nullable,comm.text defVal,CASE WHEN COLUMNPROPERTY(col.id,col.name,'IsIdentity')=1 THEN 'auto_increment' ELSE '' END Extra,ISNULL(ep.value, '') comment FROM dbo.syscolumns col LEFT JOIN dbo.systypes t ON col.xtype=t.xusertype JOIN dbo.sysobjects obj ON col.id=obj.id AND obj.xtype='U' AND obj.status>=0 LEFT JOIN dbo.syscomments comm ON col.cdefault=comm.id LEFT JOIN sys.extended_properties ep ON col.id=ep.major_id AND col.colid=ep.minor_id AND ep.name='MS_Description' LEFT JOIN information_schema.columns isc ON obj.name=isc.TABLE_NAME AND col.name=isc.COLUMN_NAME WHERE isc.TABLE_CATALOG='{conf['database']}' AND obj.name='{tab_name}' ORDER BY col.colorder"""
    res = exe_query(sql, conn)[1]
    if not res or not res[0]:
        print(ERROR_COLOR.wrap(f"{tab_name} not found!"))
        return
    res = list(map(lambda row: list(row), res[0]))
    if conf['servertype'] == 'sqlserver':
        index_dict = get_sqlserver_index_information_dict(conn, tab_name)
        for row in res:
            data_type, cml, np, ns = row[1], row.pop(2), row.pop(2), row.pop(2)
            if cml and row[1] not in {'text', 'ntext', 'xml'}:
                row[1] = f'{row[1]}({"max" if cml == -1 else cml})'
            elif data_type in ('decimal', 'numeric'):
                row[1] = f'{row[1]}({np},{ns})'
            t = index_dict.get(row[0], ('', '', '', '', ''))
            if t[3] == 'YES':
                key = 'PRI'
            elif t[4] == 'YES':
                key, row[4] = 'UNI', t[2]
            else:
                key = t[1]
            row.insert(3, key)
            row[4] = '' if row[4] is None else row[4]
    else:
        for row in res:
            row[4] = '' if row[4] is None else row[4]
    print_result_set(header, res, columns, fold, None)


def desc_table(tab_name, fold, columns):
    conn, conf = get_connection()
    if conn is None:
        return
    print_table_schema(conf, conn, tab_name, columns, fold)
    conn.close()


def print_table_schema(conf, conn, tab_name, columns, fold=False, attach_sql=False):
    if out_format != 'sql':
        print_table_description(conf, conn, tab_name, columns, fold)
        if not attach_sql:
            return
    if out_format in {'sql', 'markdown'}:
        if out_format == 'markdown':
            print('\n```sql\n', end='')
        print_create_table(conf['servertype'], conn, tab_name)
        if out_format == 'markdown':
            print('```\n', end='')


def get_table_head_from_description(description):
    return [desc[0] for desc in description] if description else []


def print_json(header, res):
    global human
    for row in res:
        row = map(lambda e: e if isinstance(e, (str, int, float)) else str(e), row)
        print(json.dumps(dict(zip(header, row)), indent=2, ensure_ascii=False)) \
            if human else print(json.dumps(dict(zip(header, row)), ensure_ascii=False))


def print_html_head(html_head_file_name):
    with open(os.path.join(get_proc_home(), 'config/html/', html_head_file_name)) as html_head:
        for line in html_head.readlines():
            print(line, end='')


def print_header_with_html(header):
    l = len(header) - 1
    for index, head in enumerate(header):
        if index == 0:
            print("<tr>", end="")
        head = "" if head is None else head
        print(f"<th>{head}</th>", end="")
        if index == l:
            print("</tr>")
    return l


def deal_html_elem(e):
    if e is None:
        return "NULL"
    return html.escape(e).replace('\r\n', '<br>').replace('\0', '\\0').replace('\b', '\\b') \
        .replace('\n', '<br>').replace('\t', '&ensp;' * 4) if isinstance(e, str) else str(e)


def print_html3(header, res):
    print_html_head('html1.head')
    print_header_with_html(header)
    for row in res:
        for index, e in enumerate(row):
            if index == 0:
                print("<tr>", end="")
            print(f"<td>{deal_html_elem(e)}</td>", end="")
        else:
            print("</tr>")
    print("</table>\n</body>\n</html>")


def print_html2(header, res):
    print_html_head('html2.head')
    print_header_with_html(header)
    for row_num, row in enumerate(res):
        for index, e in enumerate(row):
            if index == 0:
                print("<tr>", end="") if row_num % 2 == 0 else print('<tr class="alt">', end="")
            print(f"<td>{deal_html_elem(e)}</td>", end="")
        else:
            print("</tr>")
    print("</table>\n</body>\n</html>")


def print_html(header, res):
    print_html_head('html3.head')
    print_header_with_html(header)
    for row in res:
        for index, e in enumerate(row):
            if index == 0:
                s = '<tr onmouseover="this.style.backgroundColor=\'#ffff66\';"onmouseout="this.style.backgroundColor=\'#d4e3e5\';">'
                print(s)
            print(f"<td>{deal_html_elem(e)}</td>", end="")
        else:
            print("</tr>")
    print("</table>")


def print_html4(header, res):
    print_html_head('html4.head')
    for index, head in enumerate(header):
        if index == 0:
            print("<thead><tr>", end="")
        print(f"<th>{'' if head is None else head}</th>", end="")
    else:
        print("</tr></thead>")
    print('<tbody>')
    for row in res:
        print('<tr>', end='')
        for e in row:
            print(f"<td>{deal_html_elem(e)}</td>", end="")
        print("</tr>")
    print("</tbody>\n</table>")


def print_xml(header, res):
    global human
    end = '\n' if human else ""
    intend = " " * 4 if human else ""
    for row in res:
        print("<RECORD>", end=end)
        for h, e in zip(header, row):
            print(f"{intend}<{h}/>" if e is None else f"{intend}<{h}>{e}</{h}>", end=end)
        print("</RECORD>")


def print_csv(header, res, split_char=','):
    res.insert(0, header)
    for row in res:
        new_row = []
        for data in row:
            print_data = "" if data is None else str(data)
            if split_char in print_data or '\n' in print_data or '\r' in print_data:
                print_data = '"{}"'.format(print_data.replace('"', '""'))
            new_row.append(print_data)
        print(split_char.join(new_row))


def print_markdown(header, res):
    res.insert(0, header)
    res = [[deal_html_elem(e) for e in row] for row in res]
    max_length_each_fields = get_max_length_each_fields(res, str_width)
    res.insert(1, map(lambda l: ('-' * l, l), max_length_each_fields))
    align_list = [Align.ALIGN_LEFT for i in header]
    for row in res:
        print(table_row_str(row, max_length_each_fields, align_list, Color.NO_COLOR))


def run_sql(sql: str, conn, fold=True, columns=None):
    sql = sql.strip()
    description, res, effect_rows = None, None, None
    if sql.lower().startswith(('select', 'show')):
        description, res = exe_query(sql, conn)
    else:
        effect_rows, description, res, success = exe_no_query(sql, conn)
    if description and res:
        for index, (d, r) in enumerate(zip(description, res)):
            print_result_set(get_table_head_from_description(d), r, columns, fold, sql, index)
            print()
    if effect_rows and out_format == 'table':
        print(INFO_COLOR.wrap(f'Effect rows:{effect_rows}'))


def deal_bin(res):
    for row in res:
        for index, e in enumerate(row):
            if isinstance(e, (bytearray, bytes)):
                try:
                    row[index] = str(e, 'utf8')
                except Exception as ex:
                    row[index] = str(base64.standard_b64encode(e), 'utf8')


def before_print(header, res, columns, fold=True):
    """
        需要注意不能改变原本数据的类型，除了需要替换成其它数据的情况
    """
    res = [] if res is None else [row if isinstance(row, list) else list(row) for row in res]
    res.insert(0, header if isinstance(header, list) else list(header))
    res = [[line[i] for i in columns] for line in res] if columns else res
    global limit_rows
    if limit_rows:
        header = res.pop(0)
        res = res[limit_rows[0]:limit_rows[1]]
        res.insert(0, header)
    deal_bin(res)
    res = deal_human(res) if human else res
    res = [[e if len(str(e)) < FOLD_LIMIT else
            f'{str(e)[:FOLD_LIMIT - 3]}{FOLD_REPLACE_STR}' for e in row] for row in res] if fold else res
    return res.pop(0), res


def run_one_sql(sql: str, fold=True, columns=None):
    conn, conf = get_connection()
    if conn is None:
        return
    run_sql(sql, conn, fold, columns)
    conn.close()


def scan_table(table_name, fold=True, columns=None):
    run_one_sql(f"select * from {table_name}", fold, columns)


def print_result_set(header, res, columns, fold, sql, index=0):
    if not header:
        return
    if not res and out_format == 'table':
        print(WARN_COLOR.wrap('Empty Sets!'))
        return
    header, res = before_print(header, res, columns, fold)
    if out_format == 'table':
        def _start(table_width):
            print(INFO_COLOR.wrap(f'Result Sets [{index}]:'))

        print_table(header, res, start_func=_start)
    elif out_format == 'sql' and sql is not None:
        print_insert_sql(header, res, get_tab_name_from_sql(sql))
    elif out_format == 'json':
        print_json(header, res)
    elif out_format == 'html':
        print_html(header, res)
    elif out_format == 'html2':
        print_html2(header, res)
    elif out_format == 'html3':
        print_html3(header, res)
    elif out_format == 'html4':
        print_html4(header, res)
    elif out_format == 'markdown':
        print_markdown(header, res)
    elif out_format == 'xml':
        print_xml(header, res)
    elif out_format == 'csv':
        print_csv(header, res)
    elif out_format == 'text':
        def empty_start_func(table_width):
            pass

        def empty_after_print_row(row_num, row_total_num, max_row_length, split_char, table_width):
            pass

        def empty_end_func(table_width, total):
            pass

        print_table(header, res, start_func=empty_start_func,
                    after_print_row_func=empty_after_print_row, end_func=empty_end_func)
    else:
        print(ERROR_COLOR.wrap(f'Invalid print format : "{out_format}"'))


def deal_human(rows):
    def _human_time(time_stamp):
        if time_stamp is None:
            return None
        if len(str(time_stamp)) not in (10, 13):
            return time_stamp
        if isinstance(time_stamp, str) and time_stamp.isdigit():
            time_stamp = int(time_stamp)
        if isinstance(time_stamp, int):
            struct_time = time.localtime((time_stamp / 1000) if len(str(time_stamp)) == 13 else time_stamp)
            return time.strftime("%Y-%m-%d %H:%M:%S", struct_time)
        return time

    human_time_cols = set([i for i in range(len(rows[0])) if 'time' in str(rows[0][i]).lower()])
    new_rows = []
    for row_index, row in enumerate(rows):
        if row_index == 0:
            new_rows.append(row)
            continue
        new_rows.append([])
        for col_index, e in enumerate(row):
            new_rows[row_index].append(_human_time(e) if col_index in human_time_cols else e)
    return new_rows


def print_insert_sql(header, res, tab_name):
    if tab_name is None:
        print(ERROR_COLOR.wrap("Can't get table name !"))
        return

    def _case_for_sql(row):
        for index, e in enumerate(row):
            if e is None:
                row[index] = "NULL"
            elif isinstance(e, (int, float)):
                row[index] = str(e)
            elif isinstance(e, str):
                row[index] = "'{}'".format(e.replace("'", "''").replace('\r', '\\r').replace('\n', '\\n')
                                           .replace('\t', '\\t').replace('\0', '\\0').replace('\b', '\\b'))
            else:
                print(WARN_COLOR.wrap(f"TYPE WARN:{type(e)}"))
                row[index] = f"'{e}'"
        return row

    if not res:
        return
    header = list(map(lambda x: str(x), header))
    print(f"""INSERT INTO {tab_name} ({','.join(header)}) VALUES {{}};"""
          .format(",\n".join(map(lambda row: f'({",".join(_case_for_sql(row))})', res))))


def default_print_start(table_width):
    print(INFO_COLOR.wrap('Result Sets:'))


def default_print_end(table_width, total):
    print(INFO_COLOR.wrap(f'Total Records: {total}'))


def default_after_print_row(row_num, row_total_num, max_row_length, split_char, table_width):
    if max_row_length > SHOW_BOTTOM_THRESHOLD and row_num < row_total_num - 1:
        print(split_char * table_width)


def print_table(header, res, split_row_char='-', start_func=default_print_start,
                after_print_row_func=default_after_print_row, end_func=default_print_end):
    def _deal_res(res, _align_list):
        for row in res:
            for index, e in enumerate(row):
                if isinstance(e, str):
                    row[index] = e.replace('\r', '\\r').replace('\n', '\\n').replace('\t', '\\t') \
                        .replace('\0', '\\0').replace('\b', '\\b')
                elif isinstance(e, (int, float)):
                    # 数字采用右对齐
                    _align_list[index] = Align.ALIGN_RIGHT
                    row[index] = str(e)
                elif e is None:
                    row[index] = NULL_STR
                else:
                    row[index] = str(e)
        return res, _align_list

    row_total_num = len(res)
    res.insert(0, header)
    default_align = [Align.ALIGN_LEFT for i in range(len(header))]
    res, align_list = _deal_res(res, default_align.copy())
    max_length_each_fields = get_max_length_each_fields(res, str_width)
    header = res.pop(0)
    # 数据的总长度
    max_row_length = sum(max_length_each_fields)
    # 表格的宽度(数据长度加上分割线)
    table_width = 1 + max_row_length + 3 * len(header)
    space_list_down = [(split_row_char * i, i) for i in max_length_each_fields]

    start_func(table_width)
    # 打印表格的上顶线
    print(table_row_str(space_list_down, max_length_each_fields, default_align, Color.NO_COLOR, '+'))
    # 打印HEADER部分，和数据的对其方式保持一致
    print(table_row_str(header, max_length_each_fields, align_list, TABLE_HEAD_COLOR))
    # 打印HEADER和DATA之间的分割线
    print(table_row_str(space_list_down, max_length_each_fields, default_align, Color.NO_COLOR, '+'))
    for row_num, row in enumerate(res):
        # 打印DATA部分
        print(table_row_str(row, max_length_each_fields, align_list, DATA_COLOR))
        after_print_row_func(row_num, row_total_num, max_row_length, split_row_char, table_width)
    if res:
        # 有数据时，打印表格的下底线
        print(table_row_str(space_list_down, max_length_each_fields, default_align, Color.NO_COLOR, '+'))
    end_func(table_width, row_total_num)


def print_info():
    try:
        read_config = read_info()
        show_database_info({} if read_config is None else read_config)
        write_history('info', '', Stat.OK)
    except BaseException as  e:
        write_history('info', '', Stat.ERROR)
        print(ERROR_COLOR.wrap(e))


def disable_color():
    global DATA_COLOR, TABLE_HEAD_COLOR, INFO_COLOR, ERROR_COLOR, WARN_COLOR, NULL_STR, FOLD_REPLACE_STR, FOLD_COLOR_LENGTH
    DATA_COLOR = Color.NO_COLOR
    TABLE_HEAD_COLOR = Color.NO_COLOR
    INFO_COLOR = Color.NO_COLOR
    WARN_COLOR = Color.NO_COLOR
    ERROR_COLOR = Color.NO_COLOR
    NULL_STR = 'NULL'
    FOLD_REPLACE_STR = "..."
    FOLD_COLOR_LENGTH = 0


class Opt(Enum):
    UPDATE = 'update'
    WRITE = 'write'
    READ = 'read'


def parse_info_obj(read_info, info_obj, opt=Opt.READ):
    if info_obj:
        for conf_key in config:
            if conf_key not in info_obj.keys():
                continue
            set_conf_value = info_obj[conf_key]
            if conf_key == 'env':
                read_info['use']['env'] = EnvType(set_conf_value).value
                if opt is Opt.UPDATE:
                    print(INFO_COLOR.wrap(f"set env={set_conf_value} ok."))
                continue
            elif conf_key == 'conf':
                if set_conf_value in read_info['conf'][read_info['use']['env']].keys():
                    if opt is Opt.UPDATE:
                        read_info['use']['conf'] = set_conf_value
                        print(INFO_COLOR.wrap(f"set conf={set_conf_value} ok."))
                elif opt is Opt.UPDATE:
                    i = input(WARN_COLOR.wrap("Are you sure you want to add this configuration? Y/N:")).lower()
                    if i in ('y', 'yes'):
                        read_info['use']['conf'] = set_conf_value
                        read_info['conf'][read_info['use']['env']][read_info['use']['conf']] = {}
                        print(INFO_COLOR.wrap(f'Add "{set_conf_value}" conf in env={read_info["use"]["env"]}'))
                continue
            elif conf_key == 'servertype' and not DatabaseType.support(set_conf_value):
                print(ERROR_COLOR.wrap(f'"{set_conf_value}" not supported!'))
                continue
            elif conf_key == 'autocommit':
                if set_conf_value.lower() not in {'true', 'false'}:
                    print(ERROR_COLOR.wrap(f'"{set_conf_value}" incorrect parameters!'))
                    continue
                set_conf_value = set_conf_value.lower() == 'true'
            elif conf_key == 'port':
                if not set_conf_value.isdigit():
                    print(ERROR_COLOR.wrap(f'set port={set_conf_value} fail!'))
                    continue
                set_conf_value = int(set_conf_value)
            read_info['conf'][read_info['use']['env']][read_info['use']['conf']][conf_key] = set_conf_value
    return read_info


def read_info():
    proc_home = get_proc_home()
    file = os.path.join(proc_home, 'config/.db.info')
    with open(os.path.join(proc_home, 'config/.db.info.lock'), mode='w+') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        with open(file, mode='r', encoding='UTF8') as info_file:
            info_obj = json.loads(''.join(info_file.readlines()))
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    return info_obj


def write_info(info):
    proc_home = get_proc_home()
    file = os.path.join(proc_home, 'config/.db.info')
    with open(os.path.join(proc_home, 'config/.db.info.lock'), mode='w+') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        with open(file, mode='w+', encoding='UTF8') as info_file:
            info_file.write(json.dumps(info, indent=2))
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def set_info(kv):
    info_obj = {}
    try:
        with open(os.path.join(get_proc_home(), 'config/.db.lock'), 'w') as lock:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as bioe:
                print(ERROR_COLOR.wrap("set fail, please retry!"))
                write_history('set', kv, Stat.ERROR)
                return
            if is_locked():
                print(ERROR_COLOR.wrap("db is locked! can't set value."))
                write_history('set', kv, Stat.ERROR)
                return
            kv_pair = kv.split('=')
            info_obj[kv_pair[0].lower()] = kv_pair[1]
            write_info(parse_info_obj(read_info(), info_obj, Opt.UPDATE))
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
            write_history('set', kv, Stat.OK)
    except BaseException as e:
        write_history('set', kv, Stat.ERROR)
        print(ERROR_COLOR.wrap(e))


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
        except BlockingIOError as bioe:
            print(ERROR_COLOR.wrap("unlock fail, please retry!"))
            write_history('unlock', '*' * 6, Stat.ERROR)
            return
        lock_val = lock_value()
        if not lock_val:
            print(ERROR_COLOR.wrap('The db is not locked.'))
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
            print(ERROR_COLOR.wrap('Incorrect key.'))
            write_history('unlock', key, Stat.ERROR)
        fcntl.flock(t_lock_file.fileno(), fcntl.LOCK_UN)


def lock(key: str):
    with open(os.path.join(get_proc_home(), 'config/.db.lock'), 'w+') as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as bioe:
            print(ERROR_COLOR.wrap("lock fail, please retry!"))
            write_history('lock', '*' * 6, Stat.ERROR)
            return
        if is_locked():
            print(ERROR_COLOR.wrap('The db is already locked, you must unlock it first.'))
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


def print_usage(error_condition=False):
    print('''usage: db [options]

help        display this help and exit.
info        show database info.
show        list all tables in the current database.
conf        list all database configurations.
hist        list today's command history.
desc        <table name> view the description information of the table.
load        <sql file> import sql file.
export      [type] export type: ddl, data and all.
shell       start an interactive shell.
scan        <table name> scan full table.
sql         <sql> [false] [raw] [human] [format] [column index]
            [false], disable fold.
            [raw], disable all color.
            [human], print timestamp in human readable, the premise is that the field contains "time".
            [format], Print format: text, csv, table, html, markdown, xml, json and sql, the default is table.
            [col limit], print specific columns, example: "col[0,1,2]" or "col[0-2]".
            [row limit], print specific rows, example: "row[0:-1]".
set         <key=val> set database configuration, example: "env=qa", "conf=main".
version     print product version and exit.
lock        <passwd> lock the current database configuration to prevent other users from switching database configuration operations.
unlock      <passwd> unlock database configuration.\n''')
    if not error_condition:
        write_history('help', '', Stat.OK)


def shell():
    conn, conf = get_connection()
    if conn is None:
        return
    val = input('db>')
    while val not in {'quit', '!q', 'exit'}:
        if not (val == '' or val.strip() == ''):
            run_sql(val, conn)
        val = input('db>')
    conn.close()
    print('Bye')
    write_history('shell', '', Stat.OK)


def load(path):
    def exe_sql(cur, sql):
        try:
            if not sql:
                return 0, 0
            effect_rows = cur.execute(sql)
            description, res = [], []
            try:
                description.append(cur.description)
                res.append(cur.fetchall())
                while cur.nextset():
                    description.append(cur.description)
                    res.append(cur.fetchall())
            except:
                if effect_rows:
                    print(WARN_COLOR.wrap(f'Effect rows:{effect_rows}'))
            for r, d in zip(res, description):
                print_result_set(get_table_head_from_description(d), r, None, False, sql)
            return 1, 0
        except BaseException as e:
            print(f"SQL:{ERROR_COLOR.wrap(sql)}, ERROR:{ERROR_COLOR.wrap(e)}")
            return 0, 1

    try:
        if os.path.exists(path):
            conn, config = get_connection()
            if conn is None:
                return
            cur = conn.cursor()
            success_num, fail_num = 0, 0
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
                        success, fail = exe_sql(cur, sql)
                        success_num += success
                        fail_num += fail
                        sql = line
                    else:
                        sql += line
                if sql != '':
                    success, fail = exe_sql(cur, sql)
                    success_num += success
                    fail_num += fail
            conn.commit()
            conn.close()
            print(INFO_COLOR.wrap(f'load ok. {success_num} successfully executed, {fail_num} failed.'))
            write_history('load', path, Stat.OK)
        else:
            print(ERROR_COLOR.wrap(f"path:{path} not exist!"))
            write_history('load', path, Stat.ERROR)
    except BaseException as e:
        print(ERROR_COLOR.wrap(f"load {path} fail! {e}"))
        write_history('load', path, Stat.ERROR)


def show_conf():
    print_content = []
    dbconf = read_info()['conf']
    head = ['env', 'conf', 'servertype', 'host', 'port', 'database', 'user', 'password', 'charset', 'autocommit']
    for env in dbconf.keys():
        for conf in dbconf[env].keys():
            new_row = [env, conf]
            new_row.extend([dbconf[env][conf].get(key, '') for key in head[2:]])
            print_content.append(new_row)
    header, res = before_print(head, print_content, columns, fold=False)
    print_table(header, res)
    write_history('conf', '', Stat.OK)


def get_print_template_with_format():
    if out_format == 'markdown':
        return '>\n> {}\n>\n\n'
    elif out_format in {'html', 'html2', 'html3', 'html4'}:
        return '<h1>{}</h1>\n'
    else:
        return '--\n-- {}\n--\n\n'


def get_print_split_line_with_format():
    if out_format == 'markdown':
        return '\n---\n'
    else:
        return '\n'


def test():
    print('test', end='')
    write_history('test', '', Stat.OK)


def export():
    conn, conf = get_connection()
    if conn is None:
        return
    try:
        tab_list = exe_query(get_list_tab_sql(conf['servertype'], conf['database']), conn)[1]
        split_line = get_print_split_line_with_format()
        print_template = get_print_template_with_format()
        for tab in tab_list[0]:
            if export_type in {'all', 'ddl'}:
                print(print_template.format(f'Table structure for table "{tab[0]}"'))
                print_table_schema(conf, conn, tab[0], columns, fold, export_type == 'ddl')
                print(split_line)
            if export_type in {'all', 'data'}:
                print(print_template.format(f'Dumping data for table "{tab[0]}"'), end='')
                run_sql(f'select * from {tab[0]}', conn, fold)
                print(split_line)
        write_history('export', export_type, Stat.OK)
    except BaseException as e:
        write_history('export', export_type, Stat.ERROR)
        print(ERROR_COLOR.wrap(e))
    finally:
        conn.close()


def parse_args(args):
    def _error_param_exit(param):
        print(ERROR_COLOR.wrap(f'Invalid param : "{param}"'))
        sys.exit(-1)

    option = args[1].strip().lower() if len(args) > 1 else ''
    global out_format, human, export_type, limit_rows
    columns, fold, export_type, human, out_format, set_format, set_human, set_export_type, set_columns, set_fold, \
    set_raw, set_row_limit, limit_rows = None, True, 'all', False, 'table', False, False, False, False, False, False, False, None
    option_val = args[2] if len(args) > 2 else ''
    parse_start_pos = 3 if option == 'sql' else 2
    if len(args) > parse_start_pos:
        for index in range(parse_start_pos, len(args)):
            p: str = args[index].strip().lower()
            limit_row_re = re.match("^(row)\[\s*(-?\d+)\s*:\s*(-?\d+)\s*(\])$", p)
            limit_column_re = re.match("^(col)\[((\s*\d+\s*-\s*\d+\s*)|(\s*(\d+)\s*(,\s*(\d+)\s*)*))(\])$", p)
            if option in {'info', 'shell', 'help', 'test', 'show', 'version'}:
                _error_param_exit(p)
            elif index == 2 and option in {'sql', 'scan', 'desc', 'load', 'set', 'lock', 'unlock'}:
                # 第3个参数可以自定义输入的操作
                continue
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
            elif not set_format and option in {'export', 'sql', 'scan', 'desc', 'hist', 'history'} and \
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

    return option, columns, fold, option_val


if __name__ == '__main__':
    if platform.system().lower() == 'windows':
        disable_color()
    try:
        opt, columns, fold, option_val = parse_args(sys.argv)
        if opt in {'info', ''}:
            print_info()
        elif opt == 'show':
            list_tables()
        elif opt == 'conf':
            show_conf()
        elif opt in {'hist', 'history'}:
            show_history(fold)
        elif opt == 'desc':
            desc_table(option_val, fold, columns)
        elif opt == 'sql':
            run_one_sql(option_val, fold, columns)
        elif opt == 'scan':
            scan_table(option_val, fold, columns)
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
            export()
        elif opt == 'help':
            print_usage()
        elif opt == 'test':
            test()
        elif opt == 'version':
            print('db 2.0.4')
        else:
            print(ERROR_COLOR.wrap("Invalid operation !"))
            print_usage(error_condition=True)
    except Exception as e:
        print(ERROR_COLOR.wrap(e))
        traceback.print_exc(chain=e)
