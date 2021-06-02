import fcntl
import json
import os

import core.print_utils as pu
from cmd.history_cmd import HistoryCmd


class InfoCmd(HistoryCmd):
    name = 'info'

    def is_locked(self):
        return os.path.exists(os.path.join(self.get_proc_home(), 'config/.db.lock.value'))

    def show_database_info(self, info):
        env = info['use']['env'] if info else ''
        conf_name = info['use']['conf'] if info else ''
        conf = info['conf'][env].get(conf_name, {}) if info else {}

        def print_start_info(table_width, mp):
            start_info_msg = '[CONNECTION INFO]'
            f = (table_width - len(start_info_msg)) >> 1
            b = table_width - len(start_info_msg) - f
            mp.output(f'{"#" * f}{start_info_msg}{"#" * b}')

        pu.print_table(['env', 'conf', 'serverType', 'host', 'port', 'user', 'password', 'database', 'lockStat'],
                       [[env, conf_name,
                         conf.get('servertype', ''),
                         conf.get('host', ''),
                         conf.get('port', ''),
                         conf.get('user', ''),
                         conf.get('password', ''),
                         conf.get('database', ''),
                         self.is_locked()]],
                       self.params['print_conf'], self.printer,
                       start_func=print_start_info, end_func=lambda a, b, mp: mp.output('#' * a))

    def lock_value(self):
        lock_value_file = os.path.join(self.get_proc_home(), 'config/.db.lock.value')
        if self.is_locked():
            with open(lock_value_file, 'r') as f:
                return f.read()

    def _read_info(self):
        proc_home = self.get_proc_home()
        with open(os.path.join(proc_home, 'config/.db.info.lock'), mode='w+') as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            with open(os.path.join(proc_home, 'config/.db.info'), mode='r', encoding='UTF8') as info_file:
                info_obj = json.loads(''.join(info_file.readlines()))
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        return info_obj

    def _write_info(self, info):
        proc_home = self.get_proc_home()
        with open(os.path.join(proc_home, 'config/.db.info.lock'), mode='w+') as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            with open(os.path.join(proc_home, 'config/.db.info'), mode='w+', encoding='UTF8') as info_file:
                info_file.write(json.dumps(info, indent=2))
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def exe(self):
        try:
            read_config = self._read_info()
            self.show_database_info({} if read_config is None else read_config)
            self.write_ok_history('')
        except Exception as be:
            self.write_error_history('')
            self.printer.print_error_msg(be)
