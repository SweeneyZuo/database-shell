import core.print_utils  as pu
from cmd.history_cmd import HistoryCmd


class HelpCmd(HistoryCmd):
    name = 'help'

    def exe(self):
        pu.print_config('.db.usage', self.printer)
        self.write_ok_history('')
