import core.print_utils  as pu
from cmd.info_cmd import InfoCmd
from core.core import Query


class ConfCmd(InfoCmd):
    name = 'conf'
    CONFIG = ['env', 'conf', 'servertype', 'host', 'port', 'user', 'password', 'database', 'charset', 'autocommit']
    DEFAULT_CONF = 'default'
    CONF_KEY = DEFAULT_CONF

    def exe(self):
        print_content = []
        db_conf = self._read_info()['conf']
        head = ['env', 'conf', 'servertype', 'host', 'port', 'database', 'user', 'password', 'charset', 'autocommit']
        for env in db_conf.keys():
            for conf in db_conf[env].keys():
                new_row = [env, conf]
                new_row.extend([db_conf[env][conf].get(key, '') for key in head[2:]])
                print_content.append(new_row)
        header, res = self.before_print(head, print_content, Query(None, None, None, fold=self.fold))
        pu.print_table(header, res, self.params['print_conf'], self.printer)
        self.write_ok_history('')
