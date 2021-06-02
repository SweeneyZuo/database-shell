import fcntl
import os
from datetime import datetime
from enum import Enum

from cmd.print_cmd import PrintCmd
from core.core import Query


class Stat(Enum):
    OK = 'ok'
    ERROR = 'error'


class HistoryCmd(PrintCmd):

    def __write_history(self, content, stat):
        proc_home = self.get_proc_home()
        file = os.path.join(proc_home, f'hist/.{datetime.now().date()}_db.history')
        with open(os.path.join(proc_home, 'config/.db.history.lock'), mode='w+', encoding='UTF-8') as file_lock:
            fcntl.flock(file_lock.fileno(), fcntl.LOCK_EX)
            with open(file, mode='a+', encoding='UTF-8') as history_file:
                history_file.write(
                    f'{datetime.now().strftime("%H:%M:%S")}\0\0{self.opt}\0\0{content}\0\0{stat.value}\n')
            fcntl.flock(file_lock.fileno(), fcntl.LOCK_UN)

    def _read_history(self):
        proc_home = self.get_proc_home()
        file = os.path.join(proc_home, f'hist/.{datetime.now().date()}_db.history')
        history_list = []
        if os.path.exists(file):
            with open(os.path.join(proc_home, 'config/.db.history.lock'), mode='w+', encoding='UTF-8') as file_lock:
                fcntl.flock(file_lock.fileno(), fcntl.LOCK_EX)
                with open(file, mode='r', encoding='UTF-8') as history_file:
                    history_list = history_file.readlines()
                fcntl.flock(file_lock.fileno(), fcntl.LOCK_UN)
        return history_list

    def write_error_history(self, content):
        self.__write_history(content, Stat.ERROR)

    def write_ok_history(self, content):
        self.__write_history(content, Stat.OK)

    def exe(self):
        try:
            res = [list(map(lambda cell: cell.replace("\n", " "), hs.split('\0\0'))) for hs in self._read_history()]
            self.print_result_set(['time', 'option', 'value', 'stat'], res, Query(None, None, None, fold=self.fold))
            self.__write_history(self.out_format, Stat.OK)
        except Exception as e:
            self.__write_history(self.out_format, Stat.ERROR)
            self.printer.print_error_msg(e)
