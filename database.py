import hashlib
import json
import os
import pymysql
import re
import sys
from datetime import datetime
from enum import Enum

import pymssql

from databaseconf import data_base_dict as dbconf


class DatabaseType(Enum):
    SQLSERVER = 'sqlserver'
    MYSQL = 'mysql'


class EnvType(Enum):
    DEV = 'dev'
    PROD = 'prod'
    QA = 'qa'


ENV_TYPE = EnvType.DEV
SERVER_TYPE = DatabaseType.SQLSERVER
CONF_KEY = 'main_sqlserver'
DATABASE = 'PersonalizedEngagement'


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
    # 无颜色
    NO_COLOR = "{}"

    def wrap(self, v):
        return self.value.format(v)


class Stat(Enum):
    OK = 'ok'
    ERROR = 'error'


FOLD_LIMIT = 50
ROW_MAX_WIDTH = 165
SHOW_BOTTOM_THRESHOLD = 150
DATA_COLOR = Color.NO_COLOR
TABLE_HEAD_COLOR = Color.RED
INFO_COLOR = Color.GREEN
ERROR_COLOR = Color.RED
WARN_COLOR = Color.KHAKI
# DATABASE = 'fe_plat'

config = {'env', 'conf'}

tab_set = {
    "tbl_wechat_qrcode", "tbl_data_group", "tbl_conf_lbs_sams", "tbl_tenant", "tbl_conf_cate_sams",
    "tbl_user_to_tenant", "tbl_tencent_info", "tbl_wechat_account", "tbl_recommend_task",
    "tbl_wechat_account_bind_hist", "tbl_etl_task_v2", "tbl_conf_category", "tbl_label_logic", "tbl_conf_department",
    "tbl_wechat_temp_origin", "tbl_conf_fineline", "tbl_ref_wechat_temp_material", "tbl_miniprogram_card",
    "tbl_ref_qrcode", "tbl_crowd_list", "tbl_wechat_temp_material", "tbl_label_activity", "tbl_campaign_batch",
    "tbl_crowd_statistic_recent3m", "tbl_lookalike_model", "tbl_campaign_relevance", "tbl_timer_task",
    "tbl_etl_refresh_task", "tbl_campaign_designer", "tbl_people_user", "tbl_sms_sign", "tbl_sms_material",
    "tbl_conf_label_bi", "tbl_activity_info", "tbl_ref_sms_material", "tbl_activity_analysis", "tbl_activity_task",
    "tbl_conf_brand", "tbl_conf_depart_cate_fineline", "tbl_campaign_activity", "tbl_conf_lbs", "tbl_crowd_basic",
    "tbl_conf_label_status", "tbl_wechat_media", "tbl_crowd_statistic_month", "tbl_crowd_task", "tbl_etl_task",
    "tbl_login_logging", "tbl_role2router", "tbl_operation_logging", "tbl_role2router_bak", "tbl_label_update_status",
    "tbl_system_parameters", "tbl_coupon_analysis_info", "tbl_user", "tbl_user_group", "tbl_coupon_analysis_status",
    "tbl_user_role", "tbl_user_to_group", "tbl_coupon_analysis_label_mapping", "tbl_user_to_router",
    "tbl_analysis_info", "tbl_label_bi_info", "tbl_analysis_task", "tbl_label_bi_task", "tbl_conf_pay_type",
    "tbl_evoucher_info", "tbl_wechat_policy", "tbl_popup_info", "tbl_wechat_info", "tbl_wechat_response_record",
    "tbl_wechat_selfmenu", "tbl_conf_label", "tbl_wechat_autoreply", "tbl_process_action_record"
}

sms_tab_set = {
    "tbl_sms_result_record", "tbl_sms_auth", "tbl_sms_black", "tbl_sms_send_task"
}


def get_database_info_v2():
    return dbconf[ENV_TYPE.value][CONF_KEY]


def show_database_info(**kwargs):
    print(INFO_COLOR.wrap(
        '[DATABASE INFO]: env={}, serverType={}, host={}, port={}, user={}, password={}, database={}, lockStat={}.'.format(
            ENV_TYPE.value,
            kwargs['servertype'],
            kwargs['host'], kwargs['port'], kwargs['user'],
            kwargs['password'],
            kwargs['database'],
            is_locked())))


def get_proc_home():
    return os.path.dirname(os.path.abspath(sys.argv[0]))


def write_history(option: str, content: str, stat: Stat):
    filename = '.{}_db.history'.format(datetime.now().date())
    file = os.path.join(get_proc_home(), filename)
    time = datetime.now().strftime('%H:%M:%S')
    with open(file, mode='a+', encoding='UTF-8') as history_file:
        data = '{}|{}|{}|{}\n'.format(time, option, content, stat.value)
        history_file.write(data)
        history_file.flush()


def read_history():
    filename = '.{}_db.history'.format(datetime.now().date())
    file = os.path.join(get_proc_home(), filename)
    history_list = []
    if os.path.exists(file):
        with open(file, mode='r', encoding='UTF-8') as history_file:
            history_list = history_file.readlines()
        # history_list.reverse()
    return history_list


def show_history():
    try:
        for line in read_history():
            print(line, end='')
        write_history('history', '', Stat.OK)
    except Exception as e:
        write_history('history', '', Stat.ERROR)
        print(ERROR_COLOR.wrap(e))


def get_connection_v2():
    config = get_database_info_v2()
    if format == 'table':
        show_database_info(**config)
    server_type = config['servertype']
    if server_type == 'mysql':
        return pymysql.connect(host=config['host'], user=config['user'], password=config['password'],
                               database=config['database'], port=config['port'], charset=config.get('charset', 'UTF8'),
                               autocommit=config.get('autocommit', False)), config
    elif server_type == 'sqlserver':
        return pymssql.connect(host=config['host'], user=config['user'], password=config['password'],
                               database=config['database'], port=config['port'], charset=config.get('charset', 'UTF8'),
                               autocommit=config.get('autocommit', False)), config
    else:
        print(ERROR_COLOR.wrap("servertype error! servertype={}".format(server_type)))


def get_tab_name_from_sql(sql: str):
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


def ExecQuery(sql):
    tab_name = get_tab_name_from_sql(sql)
    conn, config = get_connection_v2()
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
    except Exception as e:
        write_history('sql', sql, Stat.ERROR)
        print(ERROR_COLOR.wrap(e))
    finally:
        conn.close()
    return description, res_list


def ExecNonQuery(sql):
    sql = sql.strip()
    tab_name = get_tab_name_from_sql(sql.lower())
    conn, config = get_connection_v2()
    rows, description, res = None, None, []
    try:
        if sql.lower().startswith('desc'):
            if tab_name and tab_name.startswith('tbl_process_action_record'):
                tab_name = 'tbl_process_action_record_0'
            if config['server_type'] == 'sqlserver':
                sql = 'sp_columns ' + tab_name
            elif config['server_type'] == 'mysql':
                sql = 'desc ' + tab_name

        cur = conn.cursor()
        rows = cur.execute(sql)
        description = cur.description
        res = None
        try:
            res = cur.fetchall()
        except:
            print(WARN_COLOR.wrap('No Result Sets!'))
        conn.commit()
        write_history('sql', sql, Stat.OK)
    except Exception as e:
        write_history('sql', sql, Stat.ERROR)
        print(ERROR_COLOR.wrap(e))
    finally:
        conn.close()
    return rows, description, res


def chinese_length_str(str):
    l = 0
    for char in str:
        l = l + 2 if '\u4e00' <= char <= '\u9fff' else l + 1
    return l


def print_by_align(data, space_num, align_type, color=Color.NO_COLOR, end_str=' | '):
    if space_num == 0:
        print(color.wrap(data), end=end_str)
        return
    if align_type == Align.ALIGN_RIGHT:
        print('{}{}'.format(' ' * space_num, color.wrap(data)), end=end_str)
    elif align_type == Align.ALIGN_LEFT:
        print('{}{}'.format(color.wrap(data), ' ' * space_num), end=end_str)
    elif align_type == Align.ALIGN_CENTER:
        half_space_num = int(space_num / 2)
        left_space_num = space_num - half_space_num
        print('{}{}{}'.format(' ' * left_space_num, color.wrap(data), ' ' * half_space_num), end=end_str)


def format_print_iterable(iterable, head_length,
                          digit_align_type=Align.ALIGN_RIGHT,
                          other_align_type=Align.ALIGN_CENTER,
                          color=Color.NO_COLOR):
    end_str = ',' if format == 'csv' else '|'
    for index, e in enumerate(iterable):
        e = str(e)
        space_num = 0
        if format == 'table':
            space_num = abs(chinese_length_str(e) - head_length[index])
        if index == 0:
            if format == 'csv':
                print("\ufeff", end='')
            elif format == 'table':
                print('| ', end='')
        if format == 'csv' and index == len(iterable) - 1:
            end_str = ''
        if e.isdigit():
            # 数字采用右对齐
            print_by_align(e, space_num, align_type=digit_align_type, color=color, end_str=end_str)
        else:
            print_by_align(e, space_num, align_type=other_align_type, color=color, end_str=end_str)
        if index == len(iterable) - 1:
            # 行尾
            print()


def get_fields_length(iterable, func):
    def length(iterable):
        l = 0
        for line in iterable:
            l = len(line) if l < len(line) else l
        return l

    length_head = [0 for i in range(length(iterable))]
    for line in iterable:
        for index, e in enumerate(line):
            length_head[index] = func(str(e)) if func(str(e)) > length_head[index] else length_head[index]
    return length_head


def show_table():
    try:
        print('%13s' % '{}{}{}'.format('-' * 24, 'PersonalizedEngagement', '-' * 25))
        for tab_name in tab_set:
            print('%-70s' % tab_name, end='|\n')
        print('%13s' % '{}{}{}'.format('-' * 30, 'SMSChannel', '-' * 31))
        for tab_name in sms_tab_set:
            print('%-70s' % tab_name, end='|\n')
        print('-' * 71)
        write_history('show', '', Stat.OK)
    except Exception as e:
        write_history('show', '', Stat.ERROR)
        print(ERROR_COLOR.wrap(e))


def show_sys_tables():
    servertype = get_database_info_v2()['servertype']
    if servertype == 'mysql':
        sql = 'SELECT table_name FROM information_schema.tables where table_type=1'
    else:
        sql = 'SELECT name FROM sys.tables'
    run_sql(sql, False, [0])


def desc_table(tab_name, fold, columns):
    run_no_sql('desc {}'.format(tab_name), fold, columns)


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
    res = []
    for desc in description:
        res.append(desc[0])
    return res


def fold_res(res):
    if res:
        res_new = []
        for line in res:
            line_new = []
            for e in line:
                str_e = str(e)
                line_new.append(e) if len(str_e) < FOLD_LIMIT else (line_new.append(str_e[:FOLD_LIMIT - 3] + "..."))
            res_new.append(line_new)
        return res_new
    return res


def run_sql(sql: str, fold=True, columns=None):
    sql = sql.strip()
    if sql.lower().startswith('select'):
        description, res = ExecQuery(sql)
        if not res:
            return
        res = fold_res(list(res)) if fold else list(res)
        fields = get_table_head_from_description(description)
        print_result_set(fields, res, columns)
    else:
        run_no_sql(sql, fold, columns)


def select_columns(res, columns):
    res_new = []
    for line in res:
        res_new.append([line[i] for i in columns])
    return res_new


def deal_csv(res):
    def to_csv_cell(cell_data):
        if cell_data is None:
            return ''
        elif isinstance(cell_data, str) and ('"' in cell_data or '\n' in cell_data or '\r' in cell_data):
            cell_data = cell_data.replace('"', '""')
            return '"{}"'.format(cell_data)
        else:
            return cell_data

    new_res = []
    for row in res:
        new_row = []
        new_res.append(new_row)
        for cell_data in row:
            new_row.append(to_csv_cell(cell_data))
    return new_res


def print_result_set(fields, res, columns):
    fields = ['{}({})'.format(str(i), str(index)) for index, i in
              enumerate(fields)] if not columns and format == 'table' else fields
    res.insert(0, list(fields))
    res = select_columns(res, columns) if columns else res
    res = deal_csv(res) if format == 'csv' else res
    chinese_head_length = get_fields_length(res, chinese_length_str)
    max_row_length = sum(chinese_head_length)
    max = 1 + max_row_length + 3 * len(chinese_head_length)
    fields = res.pop(0)
    space_list_down = ['-' * i for i in chinese_head_length]
    if format == 'table':
        print(INFO_COLOR.wrap('Result Sets:'))
        print('{}'.format('-' * (max if max < ROW_MAX_WIDTH else ROW_MAX_WIDTH)))
    for index, r in enumerate(res):
        if index == 0:
            format_print_iterable(fields, chinese_head_length, other_align_type=Align.ALIGN_CENTER,
                                  color=TABLE_HEAD_COLOR)
            if format == 'table':
                format_print_iterable(space_list_down, chinese_head_length, color=Color.NO_COLOR)
        format_print_iterable(r, chinese_head_length, other_align_type=Align.ALIGN_LEFT, color=DATA_COLOR)
        if format == 'table' and max_row_length > SHOW_BOTTOM_THRESHOLD:
            print('{}'.format('*' * (max if max < ROW_MAX_WIDTH else ROW_MAX_WIDTH)))
    if format == 'table':
        print('{}'.format('-' * (max if max < ROW_MAX_WIDTH else ROW_MAX_WIDTH)))
        print(INFO_COLOR.wrap('Total Records: {}'.format(len(res))))


def run_no_sql(no_sql, fold=True, columns=None):
    no_sql = no_sql.strip()
    rows, description, res = ExecNonQuery(sql=no_sql)
    res = list(res) if res else res
    res = fold_res(res) if fold else res
    if description and res and len(description) > 0:
        print_result_set(get_table_head_from_description(description), res, columns)
    if rows and format == 'table':
        print(INFO_COLOR.wrap('Effect rows:{}'.format(rows)))


def print_info():
    try:
        show_database_info(**get_database_info_v2())
        write_history('info', '', Stat.OK)
    except Exception as  e:
        write_history('info', '', Stat.ERROR)
        print(ERROR_COLOR.wrap(e))


def disable_color():
    global DATA_COLOR, TABLE_HEAD_COLOR, INFO_COLOR, ERROR_COLOR, WARN_COLOR
    DATA_COLOR = Color.NO_COLOR
    TABLE_HEAD_COLOR = Color.NO_COLOR
    INFO_COLOR = Color.NO_COLOR
    WARN_COLOR = Color.NO_COLOR
    ERROR_COLOR = Color.NO_COLOR


def parse_info_obj(info_obj):
    if info_obj:
        conf_key_list = [conf_key for conf_key in info_obj.keys() if conf_key.lower() in config]
        if len(conf_key_list) >= 2 and 'env' in conf_key_list:
            conf_key_list.remove('env')
            conf_key_list.insert(0, 'env')
        for conf_key in conf_key_list:
            global DATABASE, SERVER_TYPE, ENV_TYPE, CONF_KEY
            # if conf_key.lower() == 'database':
            #     DATABASE = info_obj[conf_key]
            if conf_key.lower() == 'env':
                ENV_TYPE = EnvType(info_obj[conf_key].lower())
            elif conf_key.lower() == 'conf':
                if info_obj[conf_key].lower() in dbconf[ENV_TYPE.value].keys():
                    CONF_KEY = info_obj[conf_key].lower()
                else:
                    print(ERROR_COLOR.wrap("no \"{}\" in env={}".format(info_obj[conf_key].lower(), ENV_TYPE.value)))


def read_info():
    filename = '.db.info'
    file = os.path.join(get_proc_home(), filename)
    if os.path.exists(file):
        with open(file, mode='r', encoding='UTF8') as info_file:
            info_obj = json.loads(''.join(info_file.readlines()))
            parse_info_obj(info_obj)


def write_info():
    config_dic = {}
    config_dic['env'] = ENV_TYPE.value
    # config_dic['serverType'] = SERVER_TYPE.value
    config_dic['conf'] = CONF_KEY
    file = os.path.join(get_proc_home(), '.db.info')
    with open(file, mode='w+', encoding='UTF8') as info_file:
        info_file.write(json.dumps(config_dic, indent=2))


def set_info(kv):
    info_obj = {}
    try:
        if is_locked():
            print(ERROR_COLOR.wrap('db is locked! can\'t set value.'))
            write_history('set', kv, Stat.ERROR)
            return
        kv_pair = kv.split('=')
        info_obj[kv_pair[0]] = kv_pair[1]
        parse_info_obj(info_obj)
        write_history('set', kv, Stat.OK)
    except Exception as e:
        write_history('set', kv, Stat.ERROR)
        print(ERROR_COLOR.wrap(e))


def is_locked():
    lock_file = os.path.join(get_proc_home(), '.db.lock')
    return os.path.exists(lock_file)


def lock_value():
    lock_file = os.path.join(get_proc_home(), '.db.lock')
    val = None
    if is_locked():
        with open(lock_file, 'r') as f:
            val = f.read()
    return val


def unlock(key: str):
    lock_val = lock_value()
    if not lock_val:
        print(ERROR_COLOR.wrap('The db is not locked.'))
        write_history('unlock', key, Stat.ERROR)
        return
    key = key if key else ""
    m = hashlib.md5()
    m.update(key.encode('UTF-8'))
    if m.hexdigest() == lock_val:
        os.remove(os.path.join(get_proc_home(), '.db.lock'))
        print(INFO_COLOR.wrap('db unlocked!'))
        write_history('unlock', key, Stat.OK)
    else:
        print(ERROR_COLOR.wrap('Incorrect key.'))
        write_history('unlock', key, Stat.ERROR)


def lock(key: str):
    if is_locked():
        print(ERROR_COLOR.wrap('The db is already locked, you must unlock it first.'))
        write_history('lock', '*' * 10, Stat.ERROR)
        return
    key = key if key else ""
    m = hashlib.md5()
    m.update(key.encode('UTF-8'))
    lock_file = os.path.join(get_proc_home(), '.db.lock')
    with open(lock_file, mode='w+') as f:
        f.write(m.hexdigest())
    print(INFO_COLOR.wrap('db locked!'))
    write_history('lock', '*' * 10, Stat.OK)


def parse_args(args):
    option = args[1].strip().lower() if len(args) > 1 else ''
    global format
    format = 'table'
    colums, fold, show_sys = None, True, False
    option_val = args[2] if len(args) > 2 else ''
    parse_start_pos = 3 if option == 'sql' else 2
    if len(args) > parse_start_pos:
        for index in range(parse_start_pos, len(args)):
            p: str = args[index].strip().lower()
            if p == 'false':
                fold = False
            elif p == 'all':
                show_sys = True
            elif p.isdigit() or ',' in p:
                colums = list(map(lambda x: int(x), p.split(',')))
            elif p == 'raw':
                disable_color()
            elif p == 'csv':
                disable_color()
                format = 'csv'
                fold = False

    return option, colums, fold, show_sys, option_val


def start():
    read_info()


def end():
    write_info()


def shell():
    val = input('db>')
    while val != 'quit':
        if not (val == '' or val.strip() == ''):
            run_sql(val)
        print(val)
        val = input('db>')
    sys.exit(0)


def load(path):
    try:
        if os.path.exists(path):
            conn, config = get_connection_v2()
            cur = conn.cursor()
            with open(path, mode='r', encoding='UTF8') as sql_file:
                for sql in sql_file.readlines():
                    t_sql = sql.lower()
                    if t_sql.startswith('insert') \
                            or t_sql.startswith('update') \
                            or t_sql.startswith('select') \
                            or t_sql.startswith('alter') \
                            or t_sql.startswith('set'):
                        try:
                            cur.execute(sql)
                        except Exception as e:
                            print(ERROR_COLOR.wrap(e))
            conn.commit()
            conn.close()
            write_history('load', path, Stat.OK)
        else:
            print(ERROR_COLOR.wrap("path:{} not exist!".format(path)))
            write_history('load', path, Stat.ERROR)
    except Exception as e:
        print(ERROR_COLOR.wrap("load {} fail! {}".format(path, e)))
        write_history('load', path, Stat.ERROR)


def show_conf():
    for env in dbconf.keys():
        print(Color.RED.wrap(env))
        for conf in dbconf[env].keys():
            print(INFO_COLOR.wrap('{}:{}'.format(conf, dbconf[env][conf])))


if __name__ == '__main__':
    start()
    opt, colums, fold, show_sys, option_val = parse_args(sys.argv)
    if opt == 'info' or opt == '':
        print_info()
    elif opt == 'show':
        show_sys_tables() if show_sys else show_table()
    elif opt == 'conf':
        show_conf()
    elif opt == 'hist' or opt == 'history':
        show_history()
    elif opt == 'desc':
        desc_table(option_val, fold, colums)
    elif opt == 'sql':
        run_sql(option_val, fold, colums)
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
    else:
        print(Color.ERROR_COLOR.wrap("Invalid operation!!!"))
    end()
