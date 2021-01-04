import warnings

warnings.filterwarnings("ignore")
import fcntl
import hashlib
import json
import os
import pymysql
import re
import sys
import time
from datetime import datetime
from enum import Enum
import base64
import pymssql
import html


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

config = ['env', 'conf', 'servertype', 'host', 'port', 'user', 'password', 'database', 'charset', 'autocommit']


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
                  is_locked()]], [i for i in range(len(header))],
                split_char='-', start_func=print_start_info, end_func=print_end_info)


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
        # history_list.reverse()
    return history_list


def show_history(fold):
    try:
        table_head = ['time', 'option', 'value', 'stat']
        res = [list(map(lambda cell: cell.replace("\n", ""), line.split('|'))) for line in read_history()]
        colums = [i for i in range(len(table_head))]
        print_result_set(table_head, res, colums, fold, None)
        write_history('history', format, Stat.OK)
    except BaseException as e:
        write_history('history', format, Stat.ERROR)
        print(ERROR_COLOR.wrap(e))


def get_connection_v2():
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


def get_tab_name_from_sql(sql: str):
    """
     提取sql中的表名，sql语句不能有嵌套结构。
    """

    def deal_tab_name(tab_name: str):
        if '.' in tab_name:
            tab_name = tab_name.split('.')[-1]
        return tab_name.replace('[', '').replace(']', '')

    sql = sql.strip().lower()
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


def tbl_process_action_record():
    for i in range(10):
        yield 'tbl_process_action_record_{}'.format(i)


def exe_query(sql, conn):
    tab_name = get_tab_name_from_sql(sql)
    res_list = []
    description = None
    try:
        cur = conn.cursor()
        if tab_name and tab_name == 'tbl_process_action_record':
            for real_tab_name in tbl_process_action_record():
                cur.execute(sql.replace('tbl_process_action_record', real_tab_name))
                res_list.extend(cur.fetchall())
        else:
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
            print(WARN_COLOR.wrap('No Result Sets!'))
        conn.commit()
        write_history('sql', sql, Stat.OK)
    except BaseException as e:
        write_history('sql', sql, Stat.ERROR)
        print(ERROR_COLOR.wrap(e))
    return effect_rows, description, res


def chinese_length_str(obj):
    l = 0
    fold_color_str = FOLD_COLOR.wrap(FOLD_REPLACE_STR)
    str_obj = 'NULL' if obj is None else str(obj)
    color_len = len(FOLD_COLOR.wrap('')) if str_obj.endswith(fold_color_str) else 0
    for char in str_obj:
        l = l + 2 if ('\u4e00' <= char <= '\u9fff') or ('\uFF01' <= char <= '\uFF5E') else l + 1
    return l - color_len


def print_by_align(data, space_num, align_type, color=Color.NO_COLOR, end_str=' | '):
    if space_num == 0:
        print(color.wrap(data), end=end_str)
        return
    if align_type == Align.ALIGN_RIGHT:
        print('{}{}'.format(' ' * space_num, color.wrap(data)), end=end_str)
    elif align_type == Align.ALIGN_LEFT:
        print('{}{}'.format(color.wrap(data), ' ' * space_num), end=end_str)
    else:
        half_space_num = int(space_num / 2)
        left_space_num = space_num - half_space_num
        print('{}{}{}'.format(' ' * half_space_num, color.wrap(data), ' ' * left_space_num), end=end_str)


def print_row_format(row, head_length,
                     digit_align_type=Align.ALIGN_RIGHT,
                     other_align_type=Align.ALIGN_CENTER,
                     color=Color.NO_COLOR):
    def isdigit(e):
        if isinstance(e, int) or isinstance(e, float):
            return True
        elif isinstance(e, str):
            try:
                float(e)
                return True
            except ValueError:
                return False
        return False

    end_str = ' | '
    for index, e in enumerate(row):
        e_str = 'NULL' if e is None else str(e)
        space_num = abs(chinese_length_str(e_str) - head_length[index])
        if index == 0:
            print('| ', end='')
        if isdigit(e):
            # 数字采用右对齐
            print_by_align(e_str, space_num, align_type=digit_align_type, color=color, end_str=end_str)
        else:
            print_by_align(e_str, space_num, align_type=other_align_type, color=color, end_str=end_str)
        if index == len(row) - 1:
            # 行尾
            print()


def get_fields_length(rows, func):
    def length(iterable):
        l = 0
        for row in iterable:
            l = len(row) if l < len(row) else l
        return l

    length_head = [0 for i in range(length(rows))]
    for row in rows:
        for index, e in enumerate(row):
            length_head[index] = func(e) if func(e) > length_head[index] else length_head[index]
    return length_head


def get_list_tab_sql(server_type, database_name):
    if server_type == 'mysql':
        return 'SELECT TABLE_NAME FROM information_schema.tables where TABLE_SCHEMA=\'{}\''.format(database_name)
    else:
        return 'SELECT name FROM sys.tables ORDER BY name'


def list_tables():
    conn, conf = get_connection_v2()
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
            # elif ExecNonQuery("SELECT COLUMNPROPERTY(OBJECT_ID('{}'),'{}','IsIdentity')".format(tab_name, colum_name),
            #                   conn)[2][0][0] == 1:
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


def desc_table(tab_name, fold, columns):
    conn, conf = get_connection_v2()
    if conn is None:
        return
    if tab_name and tab_name.startswith('tbl_process_action_record'):
        tab_name = 'tbl_process_action_record_0'

    def print_description():
        sql = "select COLUMN_NAME,DATA_TYPE,IS_NULLABLE,COLUMN_KEY,COLUMN_DEFAULT,EXTRA,COLUMN_COMMENT " \
              "from information_schema.columns where table_schema = '{}' and table_name = '{}' " \
            .format(conf['database'], tab_name)
        if conf['servertype'] == 'sqlserver':
            sql = "select COLUMN_NAME,DATA_TYPE,IS_NULLABLE,COLUMN_DEFAULT,CHARACTER_MAXIMUM_LENGTH," \
                  "NUMERIC_PRECISION,NUMERIC_PRECISION_RADIX,NUMERIC_SCALE " \
                  "from information_schema.columns where table_name = '{}'".format(tab_name)
        run_sql(sql, conn, fold, columns)

    if format == 'sql':
        print_create_table(conf['servertype'], conn, tab_name)
    else:
        print_description()
    conn.close()


def get_fields_from_sql(sql: str):
    import re
    sql = sql.strip().lower()
    select_back_index = sql.index('select') + 6
    from_front_index = sql.index('from')
    res = sql[select_back_index:from_front_index].strip()
    res_split = re.split('\\s+', res)
    if res_split[0] == 'top' or '(' in res_split[0]:
        res = ''.join(res_split[2:])
    return res


def get_max_len(list):
    max_len = 0
    for i in list:
        max_len = len(i) if len(i) > max_len else max_len
    return max_len


def get_table_head_from_description(description):
    return [desc[0] for desc in description]


def fold_res(res):
    if res:
        res_new = []
        for line in res:
            line_new = []
            for e in line:
                str_e = str(e)
                line_new.append(e) if len(str_e) < FOLD_LIMIT else (
                    line_new.append(str_e[:FOLD_LIMIT - 3] + FOLD_COLOR.wrap(FOLD_REPLACE_STR)))
            res_new.append(line_new)
        return res_new
    return res


def print_json(header, res):
    global human
    for row in res:
        if human:
            print(json.dumps(dict(zip(header, row)), indent=2))
        else:
            print(json.dumps(dict(zip(header, row))))


def print_html_head(html_head_file_name):
    with open(os.path.join(get_proc_home(), 'config/html/{}'.format(html_head_file_name))) as html_head:
        for line in html_head.readlines():
            print(line, end='')


def print_html(header, res):
    print_html_head('html1.head')
    l = len(header) - 1
    for index, head in enumerate(header):
        if index == 0:
            print("<tr>", end="")
        head = "" if head is None else head
        print("<th>{}</th>".format(head), end="")
        if index == l:
            print("</tr>")
    for row in res:
        for index, e in enumerate(row):
            if index == 0:
                print("<tr>", end="")
            e = "" if e is None else e
            e = html.escape(e).replace('\r\n', '<br>').replace('\n', '<br>') if isinstance(e, str) else e
            print("<td>{}</td>".format(e), end="")
            if index == l:
                print("</tr>")
    print("</table>\n</body>\n</html>")


def print_html2(header, res):
    print_html_head('html2.head')
    l = len(header) - 1
    for index, head in enumerate(header):
        if index == 0:
            print("<tr>", end="")
        head = "" if head is None else head
        print("<th>{}</th>".format(head), end="")
        if index == l:
            print("</tr>")
    for row_num, row in enumerate(res):
        for index, e in enumerate(row):
            if index == 0:
                print("<tr>", end="") if row_num % 2 == 0 else print('<tr class="alt">', end="")
            e = "" if e is None else e
            e = html.escape(e).replace('\r\n', '<br>').replace('\n', '<br>') if isinstance(e, str) else e
            print("<td>{}</td>".format(e), end="")
            if index == l:
                print("</tr>")
    print("</table>\n</body>\n</html>")


def print_html3(header, res):
    print_html_head('html3.head')
    l = len(header) - 1
    for index, head in enumerate(header):
        if index == 0:
            print("<tr>", end="")
        head = "" if head is None else head
        print("<th>{}</th>".format(head), end="")
        if index == l:
            print("</tr>")
    for row in res:
        for index, e in enumerate(row):
            if index == 0:
                print(
                    '<tr onmouseover="this.style.backgroundColor=\'#ffff66\';" onmouseout="this.style.backgroundColor=\'#d4e3e5\';">')
            e = "" if e is None else e
            e = html.escape(e).replace('\r\n', '<br>').replace('\n', '<br>') if isinstance(e, str) else e
            print("<td>{}</td>".format(e), end="")
            if index == l:
                print("</tr>")
    print("</table>")


def print_html4(header, res):
    print_html_head('html4.head')
    l = len(header) - 1
    for index, head in enumerate(header):
        if index == 0:
            print("<thead><tr>", end="")
        head = "" if head is None else head
        print("<th>{}</th>".format(head), end="")
        if index == l:
            print("</tr></thead>")
    print('<tbody>')
    for row in res:
        for index, e in enumerate(row):
            if index == 0:
                print('<tr>', end='')
            e = "" if e is None else e
            e = html.escape(e).replace('\r\n', '<br>').replace('\n', '<br>') if isinstance(e, str) else e
            print("<td>{}</td>".format(e), end="")
            if index == l:
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
                print_data = print_data.replace('"', '""')
                print_data = '"{}"'.format(print_data)
            new_row.append(print_data)
        print(split_char.join(new_row))


def print_markdown(header, res):
    print('|', end='')
    l = len(header)
    for head in header:
        head = "" if head is None else str(head)
        print(html.escape(head), end="|")
    print('\n|', end='')
    for i in range(l):
        print(':{}:'.format('-' * 10), end='|')
    for row in res:
        print('\n|', end='')
        for e in row:
            e = "" if e is None else str(e)
            print(html.escape(e).replace('\r\n', '<br>').replace('\n', '<br>'), end="|")


def run_sql(sql: str, conn, fold=True, columns=None):
    sql = sql.strip()
    if sql.lower().startswith('select'):
        description, res = exe_query(sql, conn)
        if not res:
            return
        print_result_set(get_table_head_from_description(description), res, columns, fold, sql)
    else:
        run_no_sql(sql, conn, fold, columns)


def deal_bin(res):
    for row in res:
        for index, e in enumerate(row):
            if isinstance(e, bytearray) or isinstance(e, bytes):
                row[index] = str(base64.standard_b64encode(e), 'utf8')


def before_print(header, res, columns, fold=True):
    res = [] if res is None else list(res)
    res.insert(0, header)
    res = [[line[i] for i in columns] for line in res] if columns else res
    res = [list(row) for row in res]
    deal_bin(res)
    res = deal_human(res) if human else res
    res = fold_res(res) if fold else res
    header = res.pop(0)
    return header, res


def run_one_sql(sql: str, fold=True, columns=None):
    conn, conf = get_connection_v2()
    if conn is None:
        return
    run_sql(sql, conn, fold, columns)
    conn.close()


def print_result_set(header, res, columns, fold, sql):
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

        columns = columns if colums is not None else [i for i in range(len(header))]
        print_table(header, res, columns, start_func=empty_start_func,
                    after_print_row_func=empty_after_print_row, end_func=empty_end_func)
    elif format == 'table':
        print_table(header, res, columns)
    else:
        print(ERROR_COLOR.wrap("Invalid print format : \"{}\"".format(format)))


def run_no_sql(no_sql, conn, fold=True, columns=None):
    no_sql = no_sql.strip()
    effect_rows, description, res = exe_no_query(no_sql, conn)
    res = list(res) if res else res
    if description and res and len(description) > 0:
        print_result_set(get_table_head_from_description(description), res, columns, fold, no_sql)
    if effect_rows and format == 'table':
        print(INFO_COLOR.wrap('Effect rows:{}'.format(effect_rows)))


def deal_csv(res):
    def to_csv_cell(cell_data):
        if cell_data is None:
            return ''
        elif isinstance(cell_data, str) and (',' in cell_data or '\n' in cell_data or '\r' in cell_data):
            cell_data = cell_data.replace('"', '""')
            return '"{}"'.format(cell_data)
        else:
            return cell_data

    new_res = [[to_csv_cell(cell_data) for cell_data in row] for row in res]
    return new_res


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
    header = list(map(lambda x: str(x), header))

    def _case_for_sql(row):
        res = []
        for e in row:
            if e is None:
                res.append("NULL")
            elif isinstance(e, int) or isinstance(e, float):
                res.append(str(e))
            elif isinstance(e, str):
                e = e.replace("'", "''")
                res.append("'{}'".format(e))
            else:
                print(WARN_COLOR.wrap("TYPE WARN:{}".format(type(e))))
                res.append("'{}'".format(e))
        return res

    template = "INSERT INTO {} ({}) VALUES ({});".format(tab_name, ",".join(header), "{}")
    for row in res:
        print(template.format(",".join(_case_for_sql(row))))
    return


def default_print_start(table_width):
    print(INFO_COLOR.wrap('Result Sets:'))


def default_print_end(table_width, total):
    print(INFO_COLOR.wrap('Total Records: {}'.format(total)))


def default_after_print_row(max_row_length, split_char, table_width):
    if max_row_length > SHOW_BOTTOM_THRESHOLD:
        print('{}'.format(split_char * (table_width if table_width < ROW_MAX_WIDTH else ROW_MAX_WIDTH)))


def print_table(header, res, columns, split_char='-', start_func=default_print_start,
                after_print_row_func=default_after_print_row, end_func=default_print_end):
    # 表头加上index
    header = ['{}({})'.format(str(i), str(index)) for index, i in
              enumerate(header)] if not columns and format == 'table' else list(header)
    res.insert(0, header)
    chinese_head_length = get_fields_length(res, chinese_length_str)
    max_row_length = sum(chinese_head_length)
    table_width = 1 + max_row_length + 3 * len(chinese_head_length)
    header = res.pop(0)
    space_list_down = [split_char * i for i in chinese_head_length]
    start_func(table_width)
    # 打印表格的上顶线
    print('{}'.format(split_char * (table_width if table_width < ROW_MAX_WIDTH or format == 'text' else ROW_MAX_WIDTH)))
    for index, r in enumerate(res):
        if index == 0:
            # 打印HEADER部分
            print_row_format(header, chinese_head_length, other_align_type=Align.ALIGN_CENTER,
                             color=TABLE_HEAD_COLOR)
            # 打印HEADER和DATA之间的分割线
            print_row_format(space_list_down, chinese_head_length, color=Color.NO_COLOR)
        # 打印DATA部分
        print_row_format(r, chinese_head_length, other_align_type=Align.ALIGN_LEFT, color=DATA_COLOR)
        after_print_row_func(max_row_length, split_char, table_width)
    # 打印表格的下底线
    print('{}'.format(split_char * (table_width if table_width < ROW_MAX_WIDTH or format == 'text' else ROW_MAX_WIDTH)))
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
    global DATA_COLOR, TABLE_HEAD_COLOR, INFO_COLOR, ERROR_COLOR, WARN_COLOR, FOLD_COLOR
    DATA_COLOR = Color.NO_COLOR
    TABLE_HEAD_COLOR = Color.NO_COLOR
    INFO_COLOR = Color.NO_COLOR
    WARN_COLOR = Color.NO_COLOR
    ERROR_COLOR = Color.NO_COLOR
    FOLD_COLOR = Color.NO_COLOR


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
                    i = input(WARN_COLOR.wrap("Are you sure you want to add this configuration? Y/N:"))
                    if i.lower() == 'y' or i.lower() == 'yes':
                        read_info['use']['conf'] = set_conf_value
                        read_info['conf'][read_info['use']['env']][read_info['use']['conf']] = {}
                        print(INFO_COLOR.wrap(
                            "Add \"{}\" conf in env={}".format(set_conf_value, read_info['use']['env'])))
                continue
            elif conf_key == 'servertype' and not DatabaseType.support(set_conf_value):
                print(ERROR_COLOR.wrap("\"{}\" not supported".format(set_conf_value)))
                continue
            elif conf_key == 'autocommit':
                set_conf_value = set_conf_value == 'true'
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
    config_dic = {}
    config_dic['env'] = ENV_TYPE.value
    config_dic['conf'] = CONF_KEY
    proc_home = get_proc_home()
    file = os.path.join(proc_home, 'config/.db.info')
    with open(os.path.join(proc_home, 'config/.db.info.lock'), mode='w+') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        with open(file, mode='w+', encoding='UTF8') as info_file:
            info_file.write(json.dumps(config_dic, indent=2))
        with open(os.path.join(proc_home, 'config/.db.info'), mode='w+', encoding='UTF8') as dbconf:
            dbconf.write(json.dumps(info, indent=2))
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
                print(ERROR_COLOR.wrap('db is locked! can\'t set value.'))
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
    with open(os.path.join(get_proc_home(), 'config/.db.lock'), 'w') as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
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
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


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
sql         <sql> [false] [raw] [human] [format] [column index]
            [false], disable fold.
            [raw], disable all color.
            [human], print timestamp in human readable, the premise is that the field contains "time".
            [format], Print format: text, csv, table, html(2/3/4), markdown, xml, json and sql, the default is table.
            [column index], print specific columns, example: "0,1,2" or "0-2".
set         <key=val> set database configuration, example: "env=qa", "conf=main".
lock        <passwd> lock the current database configuration to prevent other users from switching database configuration operations.
unlock      <passwd> unlock database configuration.\n''')
    if not error_condition:
        write_history('help', '', Stat.OK)


def shell():
    conn, conf = get_connection_v2()
    if conn is None:
        return
    val = input('db>')
    while val != 'quit':
        if not (val == '' or val.strip() == ''):
            run_sql(val, conn)
        val = input('db>')
    conn.close()
    write_history('shell', '', Stat.OK)


def is_executable(sql):
    if sql is None:
        return False
    t_sql = sql.lower()
    return t_sql.startswith('insert') \
           or t_sql.startswith('create') \
           or t_sql.startswith('update') \
           or t_sql.startswith('select') \
           or t_sql.startswith('delete') \
           or t_sql.startswith('alter') \
           or t_sql.startswith('set') \
           or t_sql.startswith('drop') \
           or t_sql.startswith('go') \
           or t_sql.startswith('use') \
           or t_sql.startswith('if') \
           or t_sql.startswith('with')


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
            conn, config = get_connection_v2()
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
                            sql = "{}{}".format(sql, line)
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
            new_row = [env]
            new_row.append(conf)
            new_row.extend([dbconf[env][conf].get(key, '') for key in head[2:]])
            print_content.append(new_row)
    header, res = before_print(head, print_content, list(range(len(head))), fold=False)
    print_table(header, res, list(range(len(head))))
    write_history('conf', '', Stat.OK)


def test():
    print('test', end='')
    write_history('test', '', Stat.OK)


def export():
    global format
    format = 'sql'
    conn, conf = get_connection_v2()
    if conn is None:
        return
    tab_list = exe_query(get_list_tab_sql(conf['servertype'], conf['database']), conn)[1]
    try:
        for tab in tab_list:
            if export_type in ('all', 'ddl'):
                print('--\n-- Table structure for table "{}"\n--\n'.format(tab[0]))
                print_create_table(conf['servertype'], conn, tab[0])
                print('\n')
            if export_type in ('all', 'data'):
                print('--\n-- Dumping data for table "{}"\n--\n'.format(tab[0]))
                sql = 'select * from {}'.format(tab[0])
                run_sql(sql, conn, False)
                print('\n')
        write_history('export', export_type, Stat.OK)
    except BaseException as e:
        write_history('export', export_type, Stat.ERROR)
        print(ERROR_COLOR.wrap(e))
    finally:
        conn.close()


def parse_args(args):
    option = args[1].strip().lower() if len(args) > 1 else ''
    global format, human, export_type
    colums, fold, export_type, human, format, \
    set_format, set_human, set_export_type, set_colums, set_fold, set_raw = None, True, 'all', False, 'table' \
        , False, False, False, False, False, False
    option_val = args[2] if len(args) > 2 else ''
    parse_start_pos = 3 if option == 'sql' else 2
    if len(args) > parse_start_pos:
        for index in range(parse_start_pos, len(args)):
            p: str = args[index].strip().lower()
            if not set_fold and p == 'false':
                fold, set_fold = False, True
            elif not set_colums and p.isdigit() or ',' in p:
                colums = list(map(lambda x: int(x), p.split(',')))
                set_colums = True
            elif not set_colums and '-' in p:
                t = p.split('-')
                if t[0].isdigit() and t[1].isdigit():
                    colums = [i for i in range(int(t[0]), int(t[1]) + 1)] if int(t[0]) <= int(t[1]) \
                        else [i for i in range(int(t[0]), int(t[1]) - 1, -1)]
                set_colums = True
            elif not set_raw and p == 'raw':
                set_raw = True
                disable_color()
            elif not set_format and option in ('sql', 'desc', 'hist', 'history') and \
                    p in ('text', 'json', 'sql', 'html', 'html2', 'html3', 'html4', 'markdown', 'xml', 'csv'):
                disable_color()
                format, fold, set_format = p, False, True
            elif not set_export_type and option == 'export' and p in ('ddl', 'data', 'all'):
                disable_color()
                export_type, fold, set_export_type = p, False, True
            elif not set_human and p == 'human':
                human, set_human = True, True
            elif index == 2 and option in ('sql', 'desc', 'load', 'set', 'lock', 'unlock'):
                # 第3个参数可以自定义输入的操作
                continue
            else:
                print(ERROR_COLOR.wrap("Invalid param : \"{}\"".format(p)))
                sys.exit(-1)

    return option, colums, fold, option_val


if __name__ == '__main__':
    try:
        opt, colums, fold, option_val = parse_args(sys.argv)
        if opt == 'info' or opt == '':
            print_info()
        elif opt == 'show':
            list_tables()
        elif opt == 'conf':
            show_conf()
        elif opt == 'hist' or opt == 'history':
            show_history(fold)
        elif opt == 'desc':
            desc_table(option_val, fold, colums)
        elif opt == 'sql':
            run_one_sql(option_val, fold, colums)
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
        else:
            print(ERROR_COLOR.wrap("Invalid operation!!!"))
            print_usage(error_condition=True)
    except Exception as e:
        print(ERROR_COLOR.wrap(e))
