from cmd.history_cmd import HistoryCmd


class TestCmd(HistoryCmd):
    name = 'test'

    def exe(self):
        self.printer.output('test', end='')
        self.write_ok_history('')
