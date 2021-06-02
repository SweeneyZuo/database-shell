import platform
import re
import sys

from core import print_utils as pu

PRINT_CONF = pu.PrintConf()
PRINTER = pu.MsgPrinter()
PRINT_FORMAT_SET = {'table', 'text', 'json', 'sql', 'html', 'html2', 'html3', 'html4', 'markdown', 'xml', 'csv'}
FOLD_LIMIT = 50


def disable_color():
    global PRINT_CONF, PRINTER
    PRINT_CONF.disable_color()
    PRINTER.disable_info_color()
    PRINTER.disable_warn_color()


def error_param_exit(param):
    PRINTER.print_error_msg(f'Invalid param : "{param}"!')
    sys.exit(-1)


def parse_args(args):
    params = {
        'printer': PRINTER,
        'print_conf': PRINT_CONF,
        'out_format': 'table',
        'fold_limit': FOLD_LIMIT,
        'fold': True,
        'human': False,
        'columns': None,
        'limit_rows': None,
        'export_type': 'all'
    }
    set_format = set_human = set_export_type = set_columns = set_fold = set_raw = set_row_limit = set_show_obj = False
    option = args[1].strip().lower() if len(args) > 1 else ''
    option_val, parse_start_pos = args[2] if len(args) > 2 else '', 3 if option == 'sql' else 2
    if len(args) > parse_start_pos:
        for index in range(parse_start_pos, len(args)):
            p: str = args[index].strip().lower()
            limit_row_re = re.match("^(row)\[\s*(-?\d+)\s*:\s*(-?\d+)\s*(\])$", p)
            limit_column_re = re.match("^(col)\[((\s*\d+\s*-\s*\d+\s*)|(\s*(\d+)\s*(,\s*(\d+)\s*)*))(\])$", p)
            if option in {'info', 'shell', 'help', 'test', 'version'}:
                error_param_exit(p)
            elif index == 2 and option in {'sql', 'scan', 'peek', 'count', 'desc', 'load', 'set', 'lock', 'unlock'}:
                # 第3个参数可以自定义输入的操作
                continue
            elif option == 'show':
                if p not in {'database', 'table', 'databases', 'tables', 'view', 'views'} or set_show_obj:
                    error_param_exit(p)
                set_show_obj = set_fold = set_raw = set_row_limit = set_human = set_columns = set_format = True
            elif not set_fold and p == 'false':
                params['fold'], set_fold = False, True
            elif not set_row_limit and option not in {'export'} and limit_row_re:
                set_row_limit, params['limit_rows'] = True, (int(limit_row_re.group(2)), int(limit_row_re.group(3)))
            elif not set_columns and option not in {'export'} and limit_column_re:
                group2 = limit_column_re.group(2).strip()
                if ',' in group2 or group2.isdigit():
                    params['columns'] = [int(i.strip()) for i in group2.split(',')]
                else:
                    t = [int(i.strip()) for i in group2.split('-')]
                    params['columns'] = [i for i in range(int(t[0]), int(t[1]) + 1)] if int(t[0]) <= int(t[1]) \
                        else [i for i in range(int(t[0]), int(t[1]) - 1, -1)]
                set_columns = True
            elif not set_format and option in {'export', 'sql', 'scan', 'peek', 'count', 'desc', 'hist', 'history'} and \
                    p in PRINT_FORMAT_SET:
                if option != 'table':
                    disable_color()
                else:
                    params['fold'] = False
                params['out_format'], set_format = p, True
            elif not set_raw and params['out_format'] == 'table' and p == 'raw':
                set_raw = True
                disable_color()
            elif not set_export_type and option == 'export' and p in {'ddl', 'data', 'all'}:
                params['export_type'], set_export_type = p, True
            elif not set_human and p == 'human':
                params['human'], set_human = True, True
            else:
                error_param_exit(p)
    return option, option_val, params


if __name__ == '__main__':
    if platform.system().lower() == 'windows':
        disable_color()
    try:
        opt, option_val, params = parse_args(sys.argv)
        cmd, server = None, None
        if opt in {'info', ''}:
            from cmd.info_cmd import InfoCmd

            cmd = InfoCmd('info', **params)
        elif opt == 'conf':
            from cmd.conf_cmd import ConfCmd

            cmd = ConfCmd(opt, **params)
        elif opt in {'hist', 'history'}:
            from cmd.history_cmd import HistoryCmd

            cmd = HistoryCmd(opt, **params)
        elif opt == 'set':
            from cmd.set_cmd import SetCmd

            cmd = SetCmd(opt, kv=option_val, **params)
        elif opt == 'lock':
            from cmd.lock_cmd import LockCmd

            cmd = LockCmd(opt, key=option_val, **params)
        elif opt == 'unlock':
            from cmd.unlock_cmd import UnlockCmd

            cmd = UnlockCmd(opt, key=option_val, **params)
        elif opt == 'help':
            from cmd.help_cmd import HelpCmd

            cmd = HelpCmd(opt, **params)
        elif opt == 'test':
            from cmd.test_cmd import TestCmd

            cmd = TestCmd(opt, **params)
        elif opt == 'version':
            from cmd.version_cmd import VersionCmd

            cmd = VersionCmd(opt, **params)
        elif opt == 'show':
            from cmd.show_cmd import ShowCmd

            cmd = ShowCmd(opt, show_obj=option_val.lower() if option_val.lower() else 'table', **params)
        elif opt == 'desc':
            from cmd.desc_cmd import DescCmd

            cmd = DescCmd(opt, tab_name=option_val, **params)
        elif opt == 'sql':
            from cmd.sql_cmd import SqlCmd

            cmd = SqlCmd(opt, sql=option_val, **params)
        elif opt == 'scan':
            from cmd.scan_cmd import ScanCmd

            cmd = ScanCmd(opt, tab_name=option_val, **params)
        elif opt == 'peek':
            from cmd.peek_cmd import PeekCmd

            cmd = PeekCmd(opt, tab_name=option_val, **params)
        elif opt == 'count':
            from cmd.count_cmd import CountCmd

            cmd = CountCmd(opt, tab_name=option_val, **params)
        elif opt == 'shell':
            from cmd.shell_cmd import ShellCmd

            cmd = ShellCmd(opt, **params)
        elif opt == 'load':
            from cmd.load_cmd import LoadCmd

            cmd = LoadCmd(opt, path=option_val, **params)
        elif opt == 'export':
            from cmd.export_cmd import ExportCmd

            cmd = ExportCmd(opt, **params)
        else:
            PRINTER.print_error_msg("Invalid operation!")
            from cmd.help_cmd import HelpCmd

            cmd = HelpCmd(opt, **params)
        if cmd:
            cmd.exe()
    except Exception as ex:
        PRINTER.print_error_msg(ex)
        import traceback

        traceback.print_exc(chain=ex)
