import fcntl
import hashlib
import os

from cmd.conf_cmd import ConfCmd


class LockCmd(ConfCmd):
    def exe(self):
        key = self.params['key']
        with open(os.path.join(self.get_proc_home(), 'config/.db.lock'), 'w+') as file_lock:
            try:
                fcntl.flock(file_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                self.printer.print_error_msg("lock fail, please retry!")
                self.write_error_history('*' * 6)
                return
            if self.is_locked():
                self.printer.print_error_msg('The db is already locked, you must unlock it first!')
                self.write_error_history('*' * 6)
                return
            key = key if key else ""
            m = hashlib.md5()
            m.update(key.encode('UTF-8'))
            lock_file = os.path.join(self.get_proc_home(), 'config/.db.lock.value')
            with open(lock_file, mode='w+') as f:
                f.write(m.hexdigest())
            fcntl.flock(file_lock.fileno(), fcntl.LOCK_UN)
            self.printer.print_info_msg('db locked!')
            self.write_ok_history('*' * 6)
