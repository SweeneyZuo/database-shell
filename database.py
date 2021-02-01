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
import traceback
from enum import Enum
from datetime import datetime

widths = [
    (126, 1), (159, 0), (687, 1), (710, 0), (711, 1), (727, 0), (733, 1), (879, 0),
    (1154, 1), (1161, 0), (4347, 1), (4447, 2), (7467, 1), (7521, 0), (8369, 1), (8426, 0),
    (9000, 1), (9002, 2), (11021, 1), (12350, 2), (12351, 1), (12438, 2), (12442, 0), (19893, 2),
    (19967, 1), (55203, 2), (63743, 1), (64106, 2), (65039, 1), (65059, 0), (65131, 2), (65279, 1),
    (65376, 2), (65500, 1), (65510, 2), (120831, 1), (262141, 2), (1114109, 1),
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
    """
      格式：\033[显示方式;前景色;背景色m要打印的字符串\033[0m
      前景色	背景色	颜色
      30	40	黑色
      31	41	红色
      32	42	绿色
      33	43	黃色
      34	44	蓝色
      35	45	紫红色
      36	46	青蓝色
      37	47	白色

      显示方式	意义
          0	终端默认设置
          1	高亮显示
          4	使用下划线
          5	闪烁
          7	反白显示
          8	不可见
      """
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
        return self.value.format(v)


class Stat(Enum):
    OK = 'ok'
    ERROR = 'error'


FOLD_LIMIT = 50
FOLD_REPLACE_STR = "..."
ROW_MAX_WIDTH = 234  # 165
SHOW_BOTTOM_THRESHOLD = 150
DATA_COLOR = Color.NO_COLOR
TABLE_HEAD_COLOR = Color.RED
INFO_COLOR = Color.GREEN
ERROR_COLOR = Color.RED
WARN_COLOR = Color.KHAKI
FOLD_COLOR = Color.BLACK_WHITE_TWINKLE
NULL_COLOR = Color.BLACK_WHITE_TWINKLE

config = ('env', 'conf', 'servertype', 'host', 'port', 'user', 'password', 'database', 'charset', 'autocommit')


def check_conf(dbconf: dict):
    if dbconf['use']['conf'] == DEFAULT_CONF:
        print(WARN_COLOR.wrap('please set conf!'))
        return False
    conf = dbconf['conf'][dbconf['use']['env']][dbconf['use']['conf']]
    if 'servertype' not in conf.keys():
        print(WARN_COLOR.wrap('please set servertype!'))
        return False
    elif 'host' not in conf.keys():
        print(WARN_COLOR.wrap('please set host!'))
        return False
    elif 'port' not in conf.keys():
        print(WARN_COLOR.wrap('please set port!'))
        return False
    elif 'user' not in conf.keys():
        print(WARN_COLOR.wrap('please set user!'))
        return False
    elif 'password' not in conf.keys():
        print(WARN_COLOR.wrap('please set password!'))
        return False
    elif 'database' not in conf.keys():
        print(WARN_COLOR.wrap('please set database!'))
        return False
    return True


def show_database_info(info):
    env = info['use']['env'] if info else ''
    conf_name = info['use']['conf'] if info else ''
    conf = info['conf'][env].get(conf_name, {}) if info else {}

    def print_start_info(table_width):
        start_info_msg = '[DATABASE INFO]'
        f = (int)((table_width - len(start_info_msg)) / 2)
        b = table_width - len(start_info_msg) - f
        print('{}{}{}'.format('#' * f, start_info_msg, '#' * b))

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


def get_script_name():
    return os.path.abspath(sys.argv[0])


def write_history(option, content, stat):
    filename = 'hist/.{}_db.history'.format(datetime.now().date())
    proc_home = get_proc_home()
    file = os.path.join(proc_home, filename)
    time = datetime.now().strftime('%H:%M:%S')
    data = '{}|{}|{}|{}\n'.format(time, option, content, stat.value)
    with open(os.path.join(proc_home, 'config/.db.history.lock'), mode='w+', encoding='UTF-8') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        with open(file, mode='a+', encoding='UTF-8') as history_file:
            history_file.write(data)
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def read_history():
    filename = 'hist/.{}_db.history'.format(datetime.now().date())
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
        res = [list(map(lambda cell: cell.replace("\n", ""), line.split('|'))) for line in read_history()]
        print_result_set(table_head, res, columns, fold, None)
        write_history('history', format, Stat.OK)
    except BaseException as e:
        write_history('history', format, Stat.ERROR)
        print(ERROR_COLOR.wrap(e))


def get_connection():
    info = read_info()
    if format == 'table':
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
            print(ERROR_COLOR.wrap("servertype error! servertype={}".format(server_type)))
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
    if (sql.startswith('select') or sql.startswith('delete')) and 'from' in sql:
        return deal_tab_name(re.split('\\s+', sql[sql.index('from') + 4:].strip())[0])
    elif sql.startswith('update'):
        return deal_tab_name(re.split('\\s+', sql[sql.index('update') + 6:].strip())[0])
    elif sql.startswith('alter') or sql.startswith('create') or (
            sql.startswith('drop') and re.split('\\s+', sql)[1] == 'table'):
        return deal_tab_name(re.split('\\s+', sql[sql.index('table') + 5:].strip())[0])
    elif sql.startswith('desc'):
        return deal_tab_name(re.split('\\s+', sql)[1].strip())
    else:
        return None


def exe_query(sql, conn):
    res_list = []
    description = None
    try:
        cur = conn.cursor()
        cur.execute(sql)
        res_list.extend(cur.fetchall())
        description = cur.description
        write_history('sql', sql, Stat.OK)
    except BaseException as e:
        write_history('sql', sql, Stat.ERROR)
        print(ERROR_COLOR.wrap(e))
    return description, res_list


def exe_no_query(sql, conn):
    sql = sql.strip()
    effect_rows, description, res = None, None, []
    try:
        cur = conn.cursor()
        effect_rows = cur.execute(sql)
        description = cur.description
        res = None
        try:
            res = cur.fetchall()
        except:
            if not effect_rows and format == 'table':
                print(WARN_COLOR.wrap('Empty Sets!'))
        conn.commit()
        write_history('sql', sql, Stat.OK)
    except BaseException as e:
        write_history('sql', sql, Stat.ERROR)
        print(ERROR_COLOR.wrap(e))
    return effect_rows, description, res


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
    if any_str == NULL_COLOR.wrap('NULL'):
        return 4
    l = 0
    fold_color_str = FOLD_COLOR.wrap(FOLD_REPLACE_STR)
    color_len = len(FOLD_COLOR.wrap('')) if any_str.endswith(fold_color_str) else 0
    for char in any_str:
        l += calc_char_width(char)
    return l - color_len


def align_str(data, space_num, align_type, color=Color.NO_COLOR, end_str=' | '):
    if space_num == 0:
        return color.wrap(data) + end_str
    if align_type == Align.ALIGN_RIGHT:
        return '{}{}{}'.format(' ' * space_num, color.wrap(data), end_str)
    elif align_type == Align.ALIGN_LEFT:
        return '{}{}{}'.format(color.wrap(data), ' ' * space_num, end_str)
    else:
        half_space_num = int(space_num / 2)
        left_space_num = space_num - half_space_num
        return '{}{}{}{}'.format(' ' * half_space_num, color.wrap(data), ' ' * left_space_num, end_str)


def isdigit(e):
    return isinstance(e, int) or isinstance(e, float)


def table_row_str(row, head_length, align_list, color=Color.NO_COLOR, split_char='|'):
    end_str = ' {} '.format(split_char)
    print_data = [split_char, ' ']
    for index, e in enumerate(row):
        space_num = abs(str_width(e) - head_length[index])
        print_data.append(align_str(e, space_num, align_type=align_list[index], color=color, end_str=end_str))
    return ''.join(print_data)


def get_max_length_each_fields(rows, func):
    length_head = len(rows[0]) * [0]
    for row in rows:
        for index, e in enumerate(row):
            func_length = func(e)
            length_head[index] = func_length if func_length > length_head[index] else length_head[index]
    return length_head


def get_list_tab_sql(server_type, database_name):
    if server_type == 'mysql':
        return "SELECT TABLE_NAME FROM information_schema.tables where TABLE_SCHEMA='{}'".format(database_name)
    else:
        return "SELECT name FROM sys.tables ORDER BY name"


def list_tables():
    conn, conf = get_connection()
    if conn is None:
        return
    run_sql(get_list_tab_sql(conf['servertype'], conf['database']), conn, False, [0])
    conn.close()


def print_create_table(server_type, conn, tab_name):
    def print_create_table_mysql():
        sql = 'show create table {}'.format(tab_name)
        effect_rows, description, res = exe_no_query(sql, conn)
        header, res = before_print(get_table_head_from_description(description), res, [1], fold=False)
        print(res[0][0])

    def print_create_table_sqlserver():
        sql = "select * from information_schema.columns where table_name = '{}'".format(tab_name)
        sql2 = "sp_columns {}".format(tab_name)
        effect_rows, description, res = exe_no_query(sql, conn)
        effect_rows2, description2, res2 = exe_no_query(sql2, conn)
        header, res = before_print(get_table_head_from_description(description), res, None, fold=False)
        header2, res2 = before_print(get_table_head_from_description(description2), res2, None, fold=False)
        print("CREATE TABLE [{}].[{}].[{}] (".format(res[0][0], res[0][1], res[0][2]))
        for index, (row, row2) in enumerate(zip(res, res2)):
            colum_name = row[3]
            data_type = row[7] if row2[5] in ('ntext',) else row2[5]
            print("  [{}] {}".format(colum_name, data_type), end="")
            if row[8] is not None and data_type not in ('text',):
                print("({})".format('max' if row[8] == -1 else row[8]), end="")
            elif data_type in ('decimal', 'numeric'):
                print("({},{})".format(row[10], row[12]), end="")
            elif data_type.endswith("identity"):
                ident_seed = exe_no_query("SELECT IDENT_SEED ('{}')".format(tab_name), conn)[2][0][0]
                ident_incr = exe_no_query("SELECT IDENT_INCR('{}')".format(tab_name), conn)[2][0][0]
                print("({},{})".format(ident_seed, ident_incr), end="")
            if row2[12] is not None:
                print(" DEFAULT {}".format(row2[12]), end="")
            if row[19] is not None:
                print(" COLLATE {}".format(row[19]), end="")
            if row[6] == 'YES':
                print(" NULL", end="")
            if row[6] == 'NO':
                print(" NOT NULL", end="")
            if index == len(res) - 1:
                print("")
            else:
                print(",")
        print(")")

    if server_type == 'mysql':
        print_create_table_mysql()
    elif server_type == 'sqlserver':
        print_create_table_sqlserver()


def print_table_description(conf, conn, tab_name, columns, fold):
    sql = "select COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,COLUMN_KEY,COLUMN_DEFAULT,EXTRA,COLUMN_COMMENT " \
          "from information_schema.columns where table_schema = '{}' and table_name = '{}' " \
        .format(conf['database'], tab_name)
    header = ['Name', 'Type', 'Nullable', 'Key', 'Default', 'Extra', 'Comment']
    if conf['servertype'] == 'sqlserver':
        sql = "SELECT col.name AS name, t.name AS type, isc.CHARACTER_MAXIMUM_LENGTH,CASE WHEN col.isnullable = 1 THEN 'YES' ELSE 'NO' END AS 允许空,CASE WHEN EXISTS ( SELECT 1 FROM dbo.sysindexes si INNER JOIN dbo.sysindexkeys sik ON si.id = sik.id AND si.indid = sik.indid INNER JOIN dbo.syscolumns sc ON sc.id = sik.id AND sc.colid = sik.colid INNER JOIN dbo.sysobjects so ON so.name = si.name AND so.xtype = 'PK' WHERE sc.id = col.id AND sc.colid = col.colid ) THEN 'YES' ELSE '' END AS 是否主键, comm.text AS 默认值 , CASE  WHEN COLUMNPROPERTY(col.id, col.name, 'IsIdentity') = 1 THEN 'auto_increment' ELSE '' END AS Extra, ISNULL(ep.value, '') AS 列说明 FROM dbo.syscolumns col LEFT JOIN dbo.systypes t ON col.xtype = t.xusertype INNER JOIN dbo.sysobjects obj ON col.id = obj.id AND obj.xtype = 'U' AND obj.status >= 0 LEFT JOIN dbo.syscomments comm ON col.cdefault = comm.id LEFT JOIN sys.extended_properties ep ON col.id = ep.major_id AND col.colid = ep.minor_id AND ep.name = 'MS_Description' LEFT JOIN sys.extended_properties epTwo ON obj.id = epTwo.major_id AND epTwo.minor_id = 0 AND epTwo.name = 'MS_Description' LEFT JOIN information_schema.columns isc ON obj.name = isc.TABLE_NAME AND col.name = isc.COLUMN_NAME WHERE isc.TABLE_CATALOG = '{}' AND obj.name = '{}' ORDER BY col.colorder" \
            .format(conf['database'], tab_name)
        header = ['Name', 'Type', 'Nullable', 'Is_Primary', 'Default', 'Extra', 'Comment']
    description, res = exe_query(sql, conn)
    res = [list(row) for row in res]
    if conf['servertype'] == 'sqlserver':
        for row in res:
            if row[2] and row[1] not in {'text'}:
                row[1] = '{}({})'.format(row[1],
                                         'max' if row[2] == -1 else row[2])
            row.pop(2)
    print_result_set(header, res, columns, fold, sql)


def desc_table(tab_name, fold, columns):
    conn, conf = get_connection()
    if conn is None:
        return
    print_table_schema(conf, conn, tab_name, fold, columns)
    conn.close()


def print_table_schema(conf, conn, tab_name, fold, columns, attach_sql=False):
    if format != 'sql':
        print_table_description(conf, conn, tab_name, columns, False)
        if not attach_sql:
            return
    if format in {'sql', 'markdown'}:
        if format == 'markdown':
            print('\n```sql\n', end='')
        print_create_table(conf['servertype'], conn, tab_name)
        if format == 'markdown':
            print('```\n', end='')


def get_max_len(list):
    max_len = 0
    for i in list:
        max_len = len(i) if len(i) > max_len else max_len
    return max_len


def get_table_head_from_description(description):
    return [desc[0] for desc in description] if description else []


def fold_res(res):
    if res:
        return [[e if len(str(e)) < FOLD_LIMIT else
                 str(e)[:FOLD_LIMIT - 3] + FOLD_COLOR.wrap(FOLD_REPLACE_STR) for e in line] for line in res]
    return res


def print_json(header, res):
    global human
    for row in res:
        row = [e if isdigit(e) or isinstance(e, str) else str(e) for e in row]
        print(json.dumps(dict(zip(header, row)), indent=2, ensure_ascii=False)) \
            if human else print(json.dumps(dict(zip(header, row)), ensure_ascii=False))


def print_html_head(html_head_file_name):
    with open(os.path.join(get_proc_home(), 'config/html/{}'.format(html_head_file_name))) as html_head:
        for line in html_head.readlines():
            print(line, end='')


def print_header_with_html(header):
    l = len(header) - 1
    for index, head in enumerate(header):
        if index == 0:
            print("<tr>", end="")
        head = "" if head is None else head
        print("<th>{}</th>".format(head), end="")
        if index == l:
            print("</tr>")
    return l


def deal_html_elem(e):
    e = "" if e is None else str(e)
    return html.escape(e).replace('\r\n', '<br>').replace('\n', '<br>') if isinstance(e, str) else e


def print_html3(header, res):
    print_html_head('html1.head')
    print_header_with_html(header)
    for row in res:
        for index, e in enumerate(row):
            if index == 0:
                print("<tr>", end="")
            print("<td>{}</td>".format(deal_html_elem(e)), end="")
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
            print("<td>{}</td>".format(deal_html_elem(e)), end="")
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
            print("<td>{}</td>".format(deal_html_elem(e)), end="")
        else:
            print("</tr>")
    print("</table>")


def print_html4(header, res):
    print_html_head('html4.head')
    for index, head in enumerate(header):
        if index == 0:
            print("<thead><tr>", end="")
        head = "" if head is None else head
        print("<th>{}</th>".format(head), end="")
    else:
        print("</tr></thead>")
    print('<tbody>')
    for row in res:
        for index, e in enumerate(row):
            if index == 0:
                print('<tr>', end='')
            print("<td>{}</td>".format(deal_html_elem(e)), end="")
        else:
            print("</tr>")
    print("</tbody>\n</table>")


def print_xml(header, res):
    global human
    end = '\n' if human else ""
    intend = " " * 4 if human else ""
    for row in res:
        print("<RECORD>", end=end)
        for h, e in zip(header, row):
            if e is None:
                print("{}<{}/>".format(intend, h), end=end)
            else:
                print("{}<{}>{}</{}>".format(intend, h, e, h), end=end)
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
    res.insert(1, ['-' * l for l in max_length_each_fields])
    align_list = [Align.ALIGN_LEFT for i in range(len(header))]
    for index, row in enumerate(res):
        print(table_row_str(row, max_length_each_fields, align_list=align_list, color=Color.NO_COLOR))


def run_sql(sql: str, conn, fold=True, columns=None):
    sql = sql.strip()
    if sql.lower().startswith('select'):
        description, res = exe_query(sql, conn)
        print_result_set(get_table_head_from_description(description), res, columns, fold, sql)
    else:
        run_no_sql(sql, conn, fold, columns)


def deal_bin(res):
    for row in res:
        for index, e in enumerate(row):
            if isinstance(e, bytearray) or isinstance(e, bytes):
                try:
                    row[index] = str(e, 'utf8')
                except Exception as ex:
                    row[index] = str(base64.standard_b64encode(e), 'utf8')


def limit_rows_func(res, limit):
    return res[limit[0]:limit[1]]


def before_print(header, res, columns, fold=True):
    """
        需要注意不能改变原本数据的类型，除了需要替换成其它数据的情况
    """
    res = [] if res is None else [list(row) for row in res]
    res.insert(0, header if isinstance(header, list) else list(header))
    res = [[line[i] for i in columns] for line in res] if columns else res
    global limit_rows
    if limit_rows:
        header = res.pop(0)
        res = limit_rows_func(res, limit_rows)
        res.insert(0, header)
    deal_bin(res)
    res = deal_human(res) if human else res
    res = fold_res(res) if fold else res
    return res.pop(0), res


def run_one_sql(sql: str, fold=True, columns=None):
    conn, conf = get_connection()
    if conn is None:
        return
    run_sql(sql, conn, fold, columns)
    conn.close()


def scan_table(table_name, fold=True, columns=None):
    sql = "select * from {}".format(table_name)
    run_one_sql(sql, fold, columns)


def print_result_set(header, res, columns, fold, sql):
    if not header:
        return
    if not res and format == 'table':
        print(WARN_COLOR.wrap('Empty Sets!'))
        return
    header, res = before_print(header, res, columns, fold)
    if format == 'sql' and sql is not None:
        print_insert_sql(header, res, get_tab_name_from_sql(sql))
    elif format == 'json':
        print_json(header, res)
    elif format == 'html':
        print_html(header, res)
    elif format == 'html2':
        print_html2(header, res)
    elif format == 'html3':
        print_html3(header, res)
    elif format == 'html4':
        print_html4(header, res)
    elif format == 'markdown':
        print_markdown(header, res)
    elif format == 'xml':
        print_xml(header, res)
    elif format == 'csv':
        print_csv(header, res)
    elif format == 'text':
        def empty_start_func(table_width):
            pass

        def empty_after_print_row(max_row_length, split_char, table_width):
            pass

        def empty_end_func(table_width, total):
            pass

        print_table(header, res, start_func=empty_start_func,
                    after_print_row_func=empty_after_print_row, end_func=empty_end_func)
    elif format == 'table':
        print_table(header, res)
    else:
        print(ERROR_COLOR.wrap('Invalid print format : "{}"'.format(format)))


def run_no_sql(no_sql, conn, fold=True, columns=None):
    no_sql = no_sql.strip()
    effect_rows, description, res = exe_no_query(no_sql, conn)
    res = list(res) if res else res
    if description and res:
        print_result_set(get_table_head_from_description(description), res, columns, fold, no_sql)
    if effect_rows and format == 'table':
        print(INFO_COLOR.wrap('Effect rows:{}'.format(effect_rows)))


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
        _res = []
        for e in row:
            if e is None:
                _res.append("NULL")
            elif isinstance(e, int) or isinstance(e, float):
                _res.append(str(e))
            elif isinstance(e, str):
                e = e.replace("'", "''")
                _res.append("'{}'".format(e))
            else:
                print(WARN_COLOR.wrap("TYPE WARN:{}".format(type(e))))
                _res.append("'{}'".format(e))
        return _res

    header = list(map(lambda x: str(x), header))
    template = "INSERT INTO {} ({}) VALUES ({});".format(tab_name, ",".join(header), "{}")
    for row in res:
        print(template.format(",".join(_case_for_sql(row))))


def default_print_start(table_width):
    print(INFO_COLOR.wrap('Result Sets:'))


def default_print_end(table_width, total):
    print(INFO_COLOR.wrap('Total Records: {}'.format(total)))


def default_after_print_row(max_row_length, split_char, table_width):
    if max_row_length > SHOW_BOTTOM_THRESHOLD:
        print('{}'.format(split_char * (table_width if table_width < ROW_MAX_WIDTH else ROW_MAX_WIDTH)))


def print_table(header, res, split_row_char='-', start_func=default_print_start,
                after_print_row_func=default_after_print_row, end_func=default_print_end):
    def _deal_res(res):
        _align_list = [Align.ALIGN_LEFT for i in range(len(res[0]))]
        for row in res:
            for index, e in enumerate(row):
                if isinstance(e, str):
                    row[index] = e.replace('\r', '\\r').replace('\n', '\\n')
                elif isdigit(e):
                    # 数字采用右对齐
                    _align_list[index] = Align.ALIGN_RIGHT
                    row[index] = str(e)
                elif e is None:
                    row[index] = NULL_COLOR.wrap('NULL')
                else:
                    row[index] = str(e)
        return res, _align_list

    res.insert(0, header)
    res, align_list = _deal_res(res)
    max_length_each_fields = get_max_length_each_fields(res, str_width)
    # 数据的总长度
    max_row_length = sum(max_length_each_fields)
    # 表格的宽度(数据长度加上分割线)
    table_width = 1 + max_row_length + 3 * len(max_length_each_fields)
    header = res.pop(0)
    space_list_down = [split_row_char * i for i in max_length_each_fields]
    start_func(table_width)
    default_align = [Align.ALIGN_LEFT for i in range(len(header))]
    # 打印表格的上顶线
    print(table_row_str(space_list_down, max_length_each_fields, default_align, color=Color.NO_COLOR, split_char='+'))
    # 打印HEADER部分
    print(table_row_str(header, max_length_each_fields, align_list, color=TABLE_HEAD_COLOR))
    # 打印HEADER和DATA之间的分割线
    print(table_row_str(space_list_down, max_length_each_fields, default_align, color=Color.NO_COLOR, split_char='+'))
    for row in res:
        # 打印DATA部分
        print(table_row_str(row, max_length_each_fields, align_list, color=DATA_COLOR))
        after_print_row_func(max_row_length, split_row_char, table_width)
    if res:
        # 有数据时，打印表格的下底线
        print(table_row_str(space_list_down, max_length_each_fields, default_align, color=Color.NO_COLOR, split_char='+'))
    end_func(table_width, len(res))


def print_info():
    try:
        config = read_info()
        config = {} if config is None else config
        show_database_info(config)
        write_history('info', '', Stat.OK)
    except BaseException as  e:
        write_history('info', '', Stat.ERROR)
        print(ERROR_COLOR.wrap(e))


def disable_color():
    global DATA_COLOR, TABLE_HEAD_COLOR, INFO_COLOR, ERROR_COLOR, WARN_COLOR, FOLD_COLOR, NULL_COLOR
    DATA_COLOR = Color.NO_COLOR
    TABLE_HEAD_COLOR = Color.NO_COLOR
    INFO_COLOR = Color.NO_COLOR
    WARN_COLOR = Color.NO_COLOR
    ERROR_COLOR = Color.NO_COLOR
    FOLD_COLOR = Color.NO_COLOR
    NULL_COLOR = Color.NO_COLOR


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
                    print(INFO_COLOR.wrap("set env={} ok.".format(set_conf_value)))
                continue
            elif conf_key == 'conf':
                if set_conf_value in read_info['conf'][read_info['use']['env']].keys():
                    if opt is Opt.UPDATE:
                        read_info['use']['conf'] = set_conf_value
                        print(INFO_COLOR.wrap("set conf={} ok.".format(set_conf_value)))
                elif opt is Opt.UPDATE:
                    i = input(WARN_COLOR.wrap("Are you sure you want to add this configuration? Y/N:")).lower()
                    if i in ('y', 'yes'):
                        read_info['use']['conf'] = set_conf_value
                        read_info['conf'][read_info['use']['env']][read_info['use']['conf']] = {}
                        print(INFO_COLOR.wrap(
                            'Add "{}" conf in env={}'.format(set_conf_value, read_info['use']['env'])))
                continue
            elif conf_key == 'servertype' and not DatabaseType.support(set_conf_value):
                print(ERROR_COLOR.wrap('"{}" not supported!'.format(set_conf_value)))
                continue
            elif conf_key == 'autocommit':
                if set_conf_value.lower() not in {'true', 'false'}:
                    print(ERROR_COLOR.wrap('"{}" incorrect parameters!'.format(set_conf_value)))
                    continue
                set_conf_value = set_conf_value.lower() == 'true'
            elif conf_key == 'port':
                if not set_conf_value.isdigit():
                    print(ERROR_COLOR.wrap('set port={} fail'.format(set_conf_value)))
                    continue
                set_conf_value = int(set_conf_value)
            read_info['conf'][read_info['use']['env']][read_info['use']['conf']][conf_key] = set_conf_value
    return read_info


def read_info():
    filename = 'config/.db.info'
    proc_home = get_proc_home()
    file = os.path.join(proc_home, filename)
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
    lock_value_file = os.path.join(get_proc_home(), 'config/.db.lock.value')
    return os.path.exists(lock_value_file)


def lock_value():
    lock_value_file = os.path.join(get_proc_home(), 'config/.db.lock.value')
    val = None
    if is_locked():
        with open(lock_value_file, 'r') as f:
            val = f.read()
    return val


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


def is_executable(sql):
    if sql is None:
        return False
    t_sql = sql.lower()
    for s in ('insert', 'create', 'update', 'select', 'delete', 'alter', 'set',
              'drop', 'go', 'use', 'if', 'with', 'show', 'desc', 'grant'):
        if t_sql.startswith(s):
            return True
    return False


def load(path):
    def exe_sql(cur, sql):
        try:
            if sql == '':
                return 0, 0
            effect_rows = cur.execute(sql)
            description = cur.description
            res = None
            try:
                res = cur.fetchall()
            except:
                print(WARN_COLOR.wrap('effect_rows:{}'.format(effect_rows)))
            if None not in (res, description):
                header, res = before_print(get_table_head_from_description(description), res, None, False)
                print_result_set(header, res, None, False, sql)
            return 1, 0
        except BaseException as e:
            print("sql:{}, error:{}".format(ERROR_COLOR.wrap(sql), ERROR_COLOR.wrap(e)))
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
                    if len(t_sql) == 0 \
                            or t_sql.startswith('--') \
                            or t_sql.startswith('#'):
                        continue
                    elif is_executable(t_sql):
                        if sql == '':
                            sql = line
                            continue
                        success, fail = exe_sql(cur, sql)
                        success_num += success
                        fail_num += fail
                        sql = line
                    else:
                        sql = "{}{}".format(sql, line)
                if sql != '':
                    success, fail = exe_sql(cur, sql)
                    success_num += success
                    fail_num += fail
            conn.commit()
            conn.close()
            print(INFO_COLOR.wrap('load ok. {} successfully executed, {} failed.'.format(success_num, fail_num)))
            write_history('load', path, Stat.OK)
        else:
            print(ERROR_COLOR.wrap("path:{} not exist!".format(path)))
            write_history('load', path, Stat.ERROR)
    except BaseException as e:
        print(ERROR_COLOR.wrap("load {} fail! {}".format(path, e)))
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
    if format == 'markdown':
        return '>\n> {}\n>\n\n'
    elif format in {'html', 'html2', 'html3', 'html4'}:
        return '<h1>{}</h1>\n'
    else:
        return '--\n-- {}\n--\n\n'


def get_print_split_line_with_format():
    if format == 'markdown':
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
        for tab in tab_list:
            if export_type in {'all', 'ddl'}:
                print(print_template.format('Table structure for table "{}"').format(tab[0]))
                print_table_schema(conf, conn, tab[0], fold, columns, export_type == 'ddl')
                print(split_line)
            if export_type in {'all', 'data'}:
                print(print_template.format('Dumping data for table "{}"').format(tab[0]), end='')
                sql = 'select * from {}'.format(tab[0])
                run_sql(sql, conn, False)
                print(split_line)
        write_history('export', export_type, Stat.OK)
    except BaseException as e:
        write_history('export', export_type, Stat.ERROR)
        print(ERROR_COLOR.wrap(e))
    finally:
        conn.close()


def parse_args(args):
    def _error_param_exit(param):
        print(ERROR_COLOR.wrap('Invalid param : "{}"'.format(param)))
        sys.exit(-1)

    option = args[1].strip().lower() if len(args) > 1 else ''
    global format, human, export_type, limit_rows
    columns, fold, export_type, human, format, \
    set_format, set_human, set_export_type, set_columns, set_fold, set_raw, set_row_limit, limit_rows = None, True, 'all', False, 'table' \
        , False, False, False, False, False, False, False, None
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
                format, set_format = p, True
            elif not set_raw and format == 'table' and p == 'raw':
                set_raw = True
                disable_color()
            elif not set_export_type and option == 'export' and p in {'ddl', 'data', 'all'}:
                export_type, fold, set_export_type = p, False, True
            elif not set_human and p == 'human':
                human, set_human = True, True
            else:
                _error_param_exit(p)

    return option, columns, fold, option_val


if __name__ == '__main__':
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
            print('db 2.0.0')
        else:
            print(ERROR_COLOR.wrap("Invalid operation !"))
            print_usage(error_condition=True)
    except Exception as e:
        print(ERROR_COLOR.wrap(e))
        traceback.print_exc(chain=e)
