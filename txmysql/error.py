class MysqlError(Exception):
    def __init__(self, message, errno=None, sqlstate=None):
        super(MysqlError, self).__init__(message, errno, sqlstate)
        self.msg, self.errno, self.sqlstate = message, errno, sqlstate
